-module(isql).
-behaviour(gen_server).

-export([start_link/0, start/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([
         rescan_config/0,
         rescan_config/1,
         add_config/1,
         get_config_value/1,
         get_config_value/2,
         spawn_worker_process/1,
         spawn_worker_process/2,
         spawn_link_worker_process/1,
         spawn_link_worker_process/2,
         stop_worker_process/1,
         send_req/2,
         send_req/3,
         send_req/4,
         %send_req_direct/4,
         %send_req_direct/5,
         %send_req_direct/6,
         %send_req_direct/7,
         %stream_next/1,
         %stream_close/1,
         set_max_sessions/2,
         set_max_pipeline_size/2,
         %set_dest/3,
         trace_on/0,
         trace_off/0,
         trace_on/1,
         trace_off/1,
         all_trace_off/0,
         show_dest_status/0,
         show_dest_status/1,
         get_metrics/0,
         get_metrics/1
        ]).

-ifdef(debug).
-compile(export_all).
-endif.

-import(isql_lib, [
                      get_value/3,
                      do_trace/2
                     ]).
                      
-record(state, {trace = false}).

-include("isql.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(DEF_MAX_SESSIONS,10).
-define(DEF_MAX_PIPELINE_SIZE,10).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], [{debug, []}]).

stop() ->
    case catch gen_server:call(isql, stop) of
        {'EXIT',{noproc,_}} ->
            ok;
        Res ->
            Res
    end.

send_req(Entity, SQL) ->
    send_req(Entity, SQL, []).

%% option() = {max_sessions, integer()}        |
%%          {response_format,response_format()}|
%%          {stream_chunk_size, integer()}     |
%%          {max_pipeline_size, integer()}     |
%%          {trace, boolean()}                 | 
%%          {pool_name, atom()}                |
%%          {save_response_to_file, srtf()}    |
%%          {stream_to, stream_to()}           |
%%          {inactivity_timeout, integer()}    |
%%          {connect_timeout, integer()}       |
%%          {socket_options, Sock_opts}        |
%%          {worker_process_options, list()}
send_req(Entity, SQL, Options) ->
    send_req(Entity, SQL, Options, 30000).

send_req(Entity, SQL, Options, Timeout) ->
    Lb_pid = case ets:lookup(isql_lb, Entity) of
                 [] ->
                     get_lb_pid(Entity);
                 [#lb_pid{pid = Lb_pid_1}] ->
                     Lb_pid_1
             end,
    Max_sessions = get_max_sessions(Entity, Options),
    Max_pipeline_size = get_max_pipeline_size(Entity, Options),
    Options_1 = merge_options(Entity, Options),
    try_routing_request(Lb_pid, Entity,
                        Max_sessions, 
                        Max_pipeline_size,
                        SQL, Options_1, Timeout, 0).

try_routing_request(Lb_pid, EntityHT,
                    Max_sessions, 
                    Max_pipeline_size,
                    SQL, Options_1, Timeout, Try_count) when Try_count < 3 ->
    ProcessOptions = get_value(worker_process_options, Options_1, []),
    case isql_lb:spawn_connection(Lb_pid, EntityHT,
                                  Max_sessions, 
                                  Max_pipeline_size,
                                  ProcessOptions) of
        {ok, Conn_Pid} ->
            case do_send_req(Conn_Pid, EntityHT, SQL, Options_1, Timeout) of
                {error, sel_conn_closed} ->
                    try_routing_request(Lb_pid, EntityHT,
                                        Max_sessions, 
                                        Max_pipeline_size,
                                        SQL, Options_1, Timeout, Try_count + 1);
                Res ->
                    Res
            end;
        Err ->
            Err
    end;
try_routing_request(_, _, _, _, _, _, _, _) ->
    {error, retry_later}.

merge_options(Entity, Options) ->
    Config_options = get_config_value({options, Entity}, []) ++
                     get_config_value({options, global}, []),
    lists:foldl(
      fun({Key, Val}, Acc) ->
              case lists:keysearch(Key, 1, Options) of
                  false ->
                      [{Key, Val} | Acc];
                  _ ->
                      Acc
              end
      end, Options, Config_options).

get_lb_pid(Url) ->
    gen_server:call(?MODULE, {get_lb_pid, Url}).

get_max_sessions(Entity, Options) ->
    get_value(max_sessions, Options,
              get_config_value({max_sessions, Entity},
                               default_max_sessions())).

get_max_pipeline_size(Entity, Options) ->
    get_value(max_pipeline_size, Options,
              get_config_value({max_pipeline_size, Entity},
                               default_max_pipeline_size())).

default_max_sessions() ->
    safe_get_env(isql, default_max_sessions, ?DEF_MAX_SESSIONS).

default_max_pipeline_size() ->
    safe_get_env(isql, default_max_pipeline_size, ?DEF_MAX_PIPELINE_SIZE).

safe_get_env(App, Key, Def_val) ->
    case application:get_env(App, Key) of
        undefined ->
            Def_val;
        {ok, Val} ->
            Val
    end.

%% @doc Set the maximum number of connections allowed to a specific Host:Port.
%% @spec set_max_sessions(Host::string(), Port::integer(), Max::integer()) -> ok
set_max_sessions(Entity, Max) when is_integer(Max), Max > 0 ->
    gen_server:call(?MODULE, {set_config_value, {max_sessions, Entity}, Max}).

%% @doc Set the maximum pipeline size for each connection to a specific Host:Port.
%% @spec set_max_pipeline_size(Host::string(), Port::integer(), Max::integer()) -> ok
set_max_pipeline_size(Entity, Max) when is_integer(Max), Max > 0 ->
    gen_server:call(?MODULE, {set_config_value, {max_pipeline_size, Entity}, Max}).

do_send_req(Conn_Pid, EntityHT, SQL, Options, Timeout) ->
    case catch isql_mysql_client:send_req(Conn_Pid, ensure_bin(SQL),
                                          Options, Timeout) of
        {'EXIT', {timeout, _}} ->
            {error, req_timedout};
        {'EXIT', {noproc, {gen_server, call, [Conn_Pid, _, _]}}} ->
            {error, sel_conn_closed};
        {'EXIT', {normal, _}} ->
            {error, req_timedout};
        {'EXIT', {connection_closed, _}} ->
            {error, sel_conn_closed};
        {error, connection_closed} ->
            {error, sel_conn_closed};
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}};
        Ret -> Ret
    end.

ensure_bin(L) when is_list(L)                     -> list_to_binary(L);
ensure_bin(B) when is_binary(B)                   -> B;
ensure_bin(Fun) when is_function(Fun)             -> Fun;
ensure_bin({Fun}) when is_function(Fun)           -> Fun;
ensure_bin({Fun, _} = Body) when is_function(Fun) -> Body.

spawn_worker_process(#sql_entity{} = Entity) ->
    spawn_worker_process(Entity, []);
spawn_worker_process(Args) ->
    spawn_worker_process(Args, []).

spawn_worker_process(Args, Options) ->
    isql_http_client:start(Args, Options).

spawn_link_worker_process(#sql_entity{} = Entity) ->
    spawn_link_worker_process(Entity, []);
spawn_link_worker_process(Args) ->
    spawn_link_worker_process(Args, []).

spawn_link_worker_process(Args, Options) ->
    isql_http_client:start_link(Args, Options).

stop_worker_process(Conn_pid) ->
    isql_http_client:stop(Conn_pid).

%% TODO dont know to do yet
%
%%% @doc Same as send_req/3 except that the first argument is the PID
%%% returned by spawn_worker_process/2 or spawn_link_worker_process/2
%send_req_direct(Conn_pid, Url, Headers, Method) ->
%    send_req_direct(Conn_pid, Url, Headers, Method, [], []).
%
%%% @doc Same as send_req/4 except that the first argument is the PID
%%% returned by spawn_worker_process/2 or spawn_link_worker_process/2
%send_req_direct(Conn_pid, Url, Headers, Method, Body) ->
%    send_req_direct(Conn_pid, Url, Headers, Method, Body, []).
%
%%% @doc Same as send_req/5 except that the first argument is the PID
%%% returned by spawn_worker_process/2 or spawn_link_worker_process/2
%send_req_direct(Conn_pid, Url, Headers, Method, Body, Options) ->
%    send_req_direct(Conn_pid, Url, Headers, Method, Body, Options, 30000).
%
%%% @doc Same as send_req/6 except that the first argument is the PID
%%% returned by spawn_worker_process/2 or spawn_link_worker_process/2
%send_req_direct(Conn_pid, Url, Headers, Method, Body, Options, Timeout) ->
%    case catch parse_url(Url) of
%        #url{host = Host,
%             port = Port} = Parsed_url ->
%            Options_1 = merge_options(Host, Port, Options),
%            case do_send_req(Conn_pid, Parsed_url, Headers, Method, Body, Options_1, Timeout) of
%                {error, {'EXIT', {noproc, _}}} ->
%                    {error, worker_is_dead};
%                Ret ->
%                    Ret
%            end;
%        Err ->
%            {error, {url_parsing_failed, Err}}
%    end.
%
%%% @doc Tell ibrowse to stream the next chunk of data to the
%%% caller. Should be used in conjunction with the
%%% <code>stream_to</code> option
%%% @spec stream_next(Req_id :: req_id()) -> ok | {error, unknown_req_id}
%stream_next(Req_id) ->    
%    case ets:lookup(isql_stream, {req_id_pid, Req_id}) of
%        [] ->
%            {error, unknown_req_id};
%        [{_, Pid}] ->
%            catch Pid ! {stream_next, Req_id},
%            ok
%    end.
%
%%% @doc Tell ibrowse to close the connection associated with the
%%% specified stream.  Should be used in conjunction with the
%%% <code>stream_to</code> option. Note that all requests in progress on
%%% the connection which is serving this Req_id will be aborted, and an
%%% error returned.
%%% @spec stream_close(Req_id :: req_id()) -> ok | {error, unknown_req_id}
%stream_close(Req_id) ->    
%    case ets:lookup(isql_stream, {req_id_pid, Req_id}) of
%        [] ->
%            {error, unknown_req_id};
%        [{_, Pid}] ->
%            catch Pid ! {stream_close, Req_id},
%            ok
%    end.

%% @doc Turn tracing on for the ibrowse process
trace_on() ->
    isql ! {trace, true}.
%% @doc Turn tracing off for the ibrowse process
trace_off() ->
    isql ! {trace, false}.

%% @doc Turn tracing on for all connections to the specified HTTP
%% server. Host is whatever is specified as the domain name in the URL
%% @spec trace_on(Host, Port) -> ok
%% Host = string() 
%% Port = integer()
trace_on(Entity) ->
    isql ! {trace, true, Entity},
    ok.

%% @doc Turn tracing OFF for all connections to the specified HTTP
%% server.
%% @spec trace_off(Host, Port) -> ok
trace_off(Entity) ->
    isql ! {trace, false, Entity},
    ok.

%% @doc Turn Off ALL tracing
%% @spec all_trace_off() -> ok
all_trace_off() ->
    isql ! all_trace_off,
    ok.

%% @doc Shows some internal information about load balancing. Info
%% about workers spawned using spawn_worker_process/2 or
%% spawn_link_worker_process/2 is not included.
show_dest_status() ->
    io:format("~-40.40s | ~-5.5s | ~-10.10s | ~s~n",
              ["Server:port", "ETS", "Num conns", "LB Pid"]),
    io:format("~80.80.=s~n", [""]),
    Metrics = get_metrics(),
    lists:foreach(
      fun({Entity, Lb_pid, Tid, Size}) ->
              io:format("~40p | ~-5.5s | ~-5.5s | ~p~n",
                        [Entity,
                         integer_to_list(Tid),
                         integer_to_list(Size), 
                         Lb_pid])
      end, Metrics).

%% @doc Shows some internal information about load balancing to a
%% specified Host:Port. Info about workers spawned using
%% spawn_worker_process/2 or spawn_link_worker_process/2 is not
%% included.
show_dest_status(Entity) ->
    case get_metrics(Entity) of
        {Lb_pid, MsgQueueSize, Tid, Size,
         {{First_p_sz, First_speculative_sz},
          {Last_p_sz, Last_speculative_sz}}} ->
            io:format("Load Balancer Pid     : ~p~n"
                      "LB process msg q size : ~p~n"
                      "LB ETS table id       : ~p~n"
                      "Num Connections       : ~p~n"
                      "Smallest pipeline     : ~p:~p~n"
                      "Largest pipeline      : ~p:~p~n",
                      [Lb_pid, MsgQueueSize, Tid, Size, 
                       First_p_sz, First_speculative_sz,
                       Last_p_sz, Last_speculative_sz]);
        _Err ->
            io:format("Metrics not available~n", [])
    end.

get_metrics() ->
    Dests = lists:filter(fun({lb_pid, Entity, _}) ->
                                 true;
                            (_) ->
                                 false
                         end, ets:tab2list(isql_lb)),
    All_ets = ets:all(),
    lists:map(fun({lb_pid, Entity, Lb_pid}) ->
                  case lists:dropwhile(
                         fun(Tid) ->
                                 ets:info(Tid, owner) /= Lb_pid
                         end, All_ets) of
                      [] ->
                          {Entity, Lb_pid, unknown, 0};
                      [Tid | _] ->
                          Size = case catch (ets:info(Tid, size)) of
                                     N when is_integer(N) -> N;
                                     _ -> 0
                                 end,
                          {Entity, Lb_pid, Tid, Size}
                  end
              end, Dests).

get_metrics(Entity) ->
    case ets:lookup(isql_lb, Entity) of
        [] ->
            no_active_processes;
        [#lb_pid{pid = Lb_pid}] ->
            MsgQueueSize = (catch process_info(Lb_pid, message_queue_len)),
            %% {Lb_pid, MsgQueueSize,
            case lists:dropwhile(
                   fun(Tid) ->
                           ets:info(Tid, owner) /= Lb_pid
                   end, ets:all()) of
                [] ->
                    {Lb_pid, MsgQueueSize, unknown, 0, unknown};
                [Tid | _] ->
                    try
                        Size = ets:info(Tid, size),
                        case Size of
                            0 ->
                                ok;
                            _ ->
                                First = ets:first(Tid),
                                Last = ets:last(Tid),
                                [{_, First_p_sz, First_speculative_sz}] = ets:lookup(Tid, First),
                                [{_, Last_p_sz, Last_speculative_sz}] = ets:lookup(Tid, Last),
                                {Lb_pid, MsgQueueSize, Tid, Size,
                                 {{First_p_sz, First_speculative_sz}, {Last_p_sz, Last_speculative_sz}}}
                        end
                    catch _:_ ->
                            not_available
                    end
            end
    end.

%% @doc Clear current configuration for ibrowse and load from the file
%% ibrowse.conf in the IBROWSE_EBIN/../priv directory. Current
%% configuration is cleared only if the ibrowse.conf file is readable
%% using file:consult/1
rescan_config() ->
    gen_server:call(?MODULE, rescan_config).

%% Clear current configuration for ibrowse and load from the specified
%% file. Current configuration is cleared only if the specified
%% file is readable using file:consult/1
rescan_config([{_,_}|_]=Terms) ->
    gen_server:call(?MODULE, {rescan_config_terms, Terms});
rescan_config(File) when is_list(File) ->
    gen_server:call(?MODULE, {rescan_config, File}).

%% @doc Add additional configuration elements at runtime.
add_config([{_,_}|_]=Terms) ->
    gen_server:call(?MODULE, {add_config_terms, Terms}).

%%====================================================================
%% Server functions
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%--------------------------------------------------------------------
init(_) ->
    process_flag(trap_exit, true),
    State = #state{},
    put(my_trace_flag, State#state.trace),
    put(isql_trace_token, "isql"),
    isql_lb     = ets:new(isql_lb, [named_table, public, {keypos, 2}]),
    isql_conf   = ets:new(isql_conf, [named_table, protected, {keypos, 2}]),
    isql_stream = ets:new(isql_stream, [named_table, public]),
    import_config(),
    {ok, #state{}}.

import_config() ->
    case code:priv_dir(isql) of
        {error, _} ->
            ok;
        PrivDir ->
            Filename = filename:join(PrivDir, "isql.conf"),
            import_config(Filename)
    end.

import_config(Filename) ->
    case file:consult(Filename) of
        {ok, Terms} ->
            apply_config(Terms);
        _Err ->
            ok
    end.

apply_config(Terms) ->
    ets:delete_all_objects(isql_conf),
    insert_config(Terms).

insert_config(Terms) ->
    Fun = fun({dest, Entity, MaxSess, MaxPipe, Options}) 
             when is_integer(MaxSess), MaxSess > 0,
                  is_integer(MaxPipe), MaxPipe > 0, is_list(Options) ->
                  I = [{{max_sessions, Entity}, MaxSess},
                       {{max_pipeline_size, Entity}, MaxPipe},
                       {{options, Entity}, Options}],
                  lists:foreach(
                    fun({X, Y}) ->
                            ets:insert(isql_conf,
                                       #isql_conf{key = X, 
                                                     value = Y})
                    end, I);
             ({K, V}) ->
                  ets:insert(isql_conf,
                             #isql_conf{key = K,
                                           value = V});
             (X) ->
                  io:format("Skipping unrecognised term: ~p~n", [X])
          end,
    lists:foreach(Fun, Terms).

%% @doc Internal export
get_config_value(Key) ->
    try
        [#isql_conf{value = V}] = ets:lookup(isql_conf, Key),
        V
    catch
        error:badarg ->
            throw({error, isql_not_running})
    end.

%% @doc Internal export
get_config_value(Key, DefVal) ->
    try
        case ets:lookup(isql_conf, Key) of
            [] ->
                DefVal;
            [#isql_conf{value = V}] ->
                V
        end
    catch
        error:badarg ->
            throw({error, isql_not_running})
    end.

set_config_value(Key, Val) ->
    ets:insert(isql_conf, #isql_conf{key = Key, value = Val}).
%%--------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call({get_lb_pid, #sql_entity{} = Entity}, _From, State) ->
    Pid = do_get_connection(Entity, ets:lookup(isql_lb, Entity)),
    {reply, Pid, State};

handle_call(stop, _From, State) ->
    do_trace("ISQL shutting down~n", []),
    ets:foldl(fun(#lb_pid{pid = Pid}, Acc) ->
                      isql_lb:stop(Pid),
                      Acc
              end, [], isql_lb),
    {stop, normal, ok, State};

handle_call({set_config_value, Key, Val}, _From, State) ->
    set_config_value(Key, Val),
    {reply, ok, State};

handle_call(rescan_config, _From, State) ->
    Ret = (catch import_config()),
    {reply, Ret, State};

handle_call({rescan_config, File}, _From, State) ->
    Ret = (catch import_config(File)),
    {reply, Ret, State};

handle_call({rescan_config_terms, Terms}, _From, State) ->
    Ret = (catch apply_config(Terms)),
    {reply, Ret, State};

handle_call({add_config_terms, Terms}, _From, State) ->
    Ret = (catch insert_config(Terms)),
    {reply, Ret, State};

handle_call(Request, _From, State) ->
    Reply = {unknown_request, Request},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_info(all_trace_off, State) ->
    Mspec = [{{isql_conf,{trace,'$1','$2'},true},[],[{{'$1','$2'}}]}],
    Trace_on_dests = ets:select(isql_conf, Mspec),
    Fun = fun(#lb_pid{entity = Entity, pid = Pid}, _) ->
                  case lists:member(Entity, Trace_on_dests) of
                      false ->
                          ok;
                      true ->
                          catch Pid ! {trace, false}
                  end;
             (_, Acc) ->
                  Acc
          end,
    ets:foldl(Fun, undefined, isql_lb),
    ets:select_delete(isql_conf, [{{isql_conf,{trace,'$1','$2'},true},[],['true']}]),
    {noreply, State};
                                  
handle_info({trace, Bool}, State) ->
    put(my_trace_flag, Bool),
    {noreply, State};

handle_info({trace, Bool, Entity}, State) ->
    Fun = fun(#lb_pid{entity = Entity, pid = Pid}, _) -> 
                  catch Pid ! {trace, Bool};
             (_, Acc) ->
                  Acc
          end,
    ets:foldl(Fun, undefined, isql_lb),
    ets:insert(isql_conf, #isql_conf{key = {trace, Entity},
                                           value = Bool}),
    {noreply, State};
                     
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
do_get_connection(#sql_entity{} = Entity, []) ->
    {ok, Pid} = isql_lb:start_link(Entity),
    ets:insert(isql_lb, #lb_pid{entity = Entity, pid = Pid}),
    Pid;
do_get_connection(_Entity, [#lb_pid{pid = Pid}]) ->
    Pid.
