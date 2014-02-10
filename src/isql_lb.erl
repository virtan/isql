-module(isql_lb).
-behaviour(gen_server).
-export([
	 start_link/1,
	 spawn_connection/5,
         stop/1
	]).

-export([
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3
	]).

-record(state, {parent_pid,
		ets_tid,
                entity,
		max_sessions,
		max_pipeline_size,
		num_cur_sessions = 0,
                proc_state
               }).

-include("isql.hrl").

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Entity) ->
    process_flag(trap_exit, true),
    Max_sessions = isql:get_config_value({max_sessions, Entity}, 10),
    Max_pipe_sz = isql:get_config_value({max_pipeline_size, Entity}, 10),
    put(my_trace_flag, isql_lib:get_trace_status(Entity)),
    put(isql_trace_token, [Entity]),
    Tid = ets:new(isql_lb, [public, ordered_set]),
    {ok, #state{parent_pid = whereis(isql),
                entity = Entity,
		ets_tid = Tid,
		max_pipeline_size = Max_pipe_sz,
	        max_sessions = Max_sessions}}.

spawn_connection(Lb_pid, Entity,
		 Max_sessions,
		 Max_pipeline_size,
		 Process_options)
  when is_pid(Lb_pid),
       is_record(Entity, sql_entity),
       is_integer(Max_pipeline_size),
       is_integer(Max_sessions) ->
    gen_server:call(Lb_pid,
		    {spawn_connection, Entity, Max_sessions, Max_pipeline_size, Process_options}).

stop(Lb_pid) ->
    case catch gen_server:call(Lb_pid, stop) of
        {'EXIT', {timeout, _}} ->
            exit(Lb_pid, kill);
        ok ->
            ok
    end.

handle_call(stop, _From, #state{ets_tid = undefined} = State) ->
    gen_server:reply(_From, ok),
    {stop, normal, State};

handle_call(stop, _From, #state{ets_tid = Tid} = State) ->
    ets:foldl(fun({Pid, _, _}, Acc) ->
                      isql_mysql_client:stop(Pid),
                      Acc
              end, [], Tid),
    gen_server:reply(_From, ok),
    {stop, normal, State};

handle_call(_, _From, #state{proc_state = shutting_down} = State) ->
    {reply, {error, shutting_down}, State};

%% Update max_sessions in #state with supplied value
handle_call({spawn_connection, _Entity, Max_sess, Max_pipe, _}, _From,
	    #state{num_cur_sessions = Num} = State)
    when Num >= Max_sess ->
    State_1 = maybe_create_ets(State),
    Reply = find_best_connection(State_1#state.ets_tid, Max_pipe),
    {reply, Reply, State_1#state{max_sessions = Max_sess,
                                 max_pipeline_size = Max_pipe}};

handle_call({spawn_connection, Entity, Max_sess, Max_pipe, Process_options}, _From,
	    #state{num_cur_sessions = Cur} = State) ->
    State_1 = maybe_create_ets(State),
    Tid = State_1#state.ets_tid,
    {ok, Pid} = isql_mysql_client:start_link({Tid, Entity}, Process_options),
    ets:insert(Tid, {Pid, 0, 0}),
    {reply, {ok, Pid}, State_1#state{num_cur_sessions = Cur + 1,
                                     max_sessions = Max_sess,
                                     max_pipeline_size = Max_pipe}};

handle_call(Request, _From, State) ->
    Reply = {unknown_request, Request},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Parent, _Reason}, #state{parent_pid = Parent} = State) ->
    {stop, normal, State};

handle_info({'EXIT', _Pid, _Reason}, #state{ets_tid = undefined} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, _Reason},
	    #state{num_cur_sessions = Cur,
		   ets_tid = Tid} = State) ->
    ets:match_delete(Tid, {{'_', Pid}, '_'}),
    Cur_1 = Cur - 1,
    case Cur_1 of
		  0 ->
		      ets:delete(Tid),
			  {noreply, State#state{ets_tid = undefined, num_cur_sessions = 0}, 10000};
		  _ ->
		      {noreply, State#state{num_cur_sessions = Cur_1}}
	      end;

handle_info({trace, Bool}, #state{ets_tid = undefined} = State) ->
    put(my_trace_flag, Bool),
    {noreply, State};

handle_info({trace, Bool}, #state{ets_tid = Tid} = State) ->
    ets:foldl(fun({{_, Pid}, _}, Acc) when is_pid(Pid) ->
		      catch Pid ! {trace, Bool},
		      Acc;
		 (_, Acc) ->
		      Acc
	      end, undefined, Tid),
    put(my_trace_flag, Bool),
    {noreply, State};

handle_info(timeout, State) ->
    %% We can't shutdown the process immediately because a request
    %% might be in flight. So we first remove the entry from the
    %% isql_lb ets table, and then shutdown a couple of seconds
    %% later
    ets:delete(isql_lb, State#state.entity),
    erlang:send_after(2000, self(), shutdown),
    {noreply, State#state{proc_state = shutting_down}};

handle_info(shutdown, State) ->
    {stop, normal, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

find_best_connection(Tid, Max_pipe) ->
    ets:safe_fixtable(Tid, true),
    Res = find_best_connection(ets:first(Tid), Tid, Max_pipe),
    ets:safe_fixtable(Tid, false),
    Res.

find_best_connection('$end_of_table', _, _) ->
    {error, retry_later};
find_best_connection(Pid, Tid, Max_pipe) ->
    case ets:lookup(Tid, Pid) of
        [{Pid, Cur_sz, Speculative_sz}] when Cur_sz < Max_pipe,
                                             Speculative_sz < Max_pipe ->
            case catch ets:update_counter(Tid, Pid, {3, 1, 9999999, 9999999}) of
                {'EXIT', _} ->
                    %% The selected process has shutdown
                    find_best_connection(ets:next(Tid, Pid), Tid, Max_pipe);
                _ ->
                    {ok, Pid}
            end;
         _ ->
            find_best_connection(ets:next(Tid, Pid), Tid, Max_pipe)
    end.

maybe_create_ets(#state{ets_tid = undefined} = State) ->
    Tid = ets:new(isql_lb, [public, ordered_set]),
    State#state{ets_tid = Tid};
maybe_create_ets(State) ->
    State.
