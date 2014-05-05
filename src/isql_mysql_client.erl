-module(isql_mysql_client).
-behaviour(gen_server).

-export([
         start_link/1,
         start_link/2,
         start/1,
         start/2,
         stop/1,
         send_req/4
        ]).

-ifdef(debug).
-compile(export_all).
-endif.

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-export([
         state_idle/1,
         state_header/1,
         state_body/1,
         state_protocol/1,
         protocol_generic/2,
         protocol_fields/2,
         protocol_rows/2,
         protocol_reply/2
        ]).

-include("isql.hrl").
-include("ems.hrl").
-include_lib("kernel/include/inet.hrl").

-record(state, {
          entity,
          connect_timeout,

          inactivity_timer_ref,
          connection,
          lb_ets_tid,

          reqs = queue:new(),
          cur_req = undefined,
          req_id = undefined,
          prev_req_id = undefined,
          from = undefined,
          cur_pipeline_size = 0,

          buffer = <<>>,
          want_bytes,

          state_handler,
          protocol_handler,
          packet,
          seq_num,
          response,

          server_status,
          fields_queue,
          fields_key,
          rows_queue,
          mysql_extra,
          field_count
         }).

-record(request, {
          entity,
          options,
          from,
          caller_socket_options = [],
          req_id,
          save_response_to_file = false,
          tmp_file_name,
          tmp_file_fd,
          preserve_chunked_encoding,
          response_format,
          timer_ref
         }).

-import(isql_lib, [
                      do_trace/2
                     ]).

-define(DEFAULT_STREAM_CHUNK_SIZE, 1024*1024).
-define(dec2hex(X), erlang:integer_to_list(X, 16)).

start(Args) ->
    start(Args, []).

start(Args, Options) ->
    {[ControllingProcess], Options1} = proplists:split(Options, [controlling_process]),
    gen_server:start(?MODULE, [Args, ControllingProcess], Options1).

start_link(Args) ->
    start_link(Args, []).

start_link(Args, Options) ->
    {[ControllingProcess], Options1} = proplists:split(Options, [controlling_process]),
    gen_server:start_link(?MODULE, [Args, ControllingProcess], Options1).

stop(Conn_pid) ->
    case catch gen_server:call(Conn_pid, stop) of
        {'EXIT', {timeout, _}} ->
            exit(Conn_pid, kill),
            ok;
        _ ->
            ok
    end.

send_req(Conn_Pid, SQL, Options, Timeout) ->
    gen_server:call(
      Conn_Pid,
      {send_req, {SQL, Options, Timeout}}, Timeout).

init([RestOptions, [{controlling_process, Pid}]]) ->
    monitor(process, Pid),
    init(RestOptions);
init([RestOptions, _]) ->
    init(RestOptions);
init({Lb_Tid, #sql_entity{} = Entity}) ->
    State = #state{entity = Entity,
                   lb_ets_tid = Lb_Tid},
    put(isql_trace_token, [Entity]),
    put(my_trace_flag, isql_lib:get_trace_status(Entity)),
    {ok, set_inac_timer(reset_state(State))};
init(#sql_entity{} = Entity) ->
    State = #state{entity = Entity},
    put(isql_trace_token, [Entity]),
    put(my_trace_flag, isql_lib:get_trace_status(Entity)),
    {ok, set_inac_timer(reset_state(State))}.

handle_call(stop, _From, State) ->
    do_close(State),
    do_error_reply(State, closing_on_request),
    {stop, normal, ok, State};

handle_call({send_req, {SQL, Options, Timeout}},
            From, State) ->
    send_req_1(From, SQL, Options, Timeout, State);

handle_call(Request, _From, State) ->
    Reply = {unknown_request, Request},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, _Sock, Data}, #state{} = State) ->
    handle_sock_data(Data, State);

handle_info({tcp_closed, _Sock}, State) ->
    do_trace("TCP connection closed by peer!~n", []),
    handle_sock_closed(State),
    {stop, connection_closed, State};

handle_info({tcp_error, _Sock, Reason}, State) ->
    do_trace("Error on connection to ~1000.p -> ~1000.p~n",
             [State#state.entity, Reason]),
    handle_sock_closed(State),
    {stop, normal, State};

handle_info({req_timedout, From}, State) ->
    case lists:keysearch(From, #request.from, queue:to_list(State#state.reqs)) of
        false ->
            {noreply, State};
        {value, #request{}} ->
            shutting_down(State),
            {stop, normal, State}
    end;

handle_info(timeout, State) ->
    do_trace("Inactivity timeout triggered. Shutting down connection~n", []),
    shutting_down(State),
    do_error_reply(State, req_timedout),
    {stop, normal, State};

handle_info({trace, Bool}, State) ->
    put(my_trace_flag, Bool),
    {noreply, State};

handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    do_trace("Controlling process down!~n", []),
    shutting_down(State),
    do_error_reply(State, controlling_process_death),
    {stop, normal, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    do_close(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_sock_data(Data, #state{buffer = Buf, want_bytes = WantBytes}=State)
        when size(Data) + size(Buf) < WantBytes ->
    do_trace("Got ~p bytes, keeping ~p bytes, waiting for ~p~n", [size(Data), size(Buf), WantBytes]),
    read_more(State#state{buffer = <<Buf/binary, Data/binary>>});
handle_sock_data(Data, #state{state_handler = StateHandler, buffer = Buf}=State) ->
    State_1 = State#state{buffer = <<Buf/binary, Data/binary>>},
    do_trace("calling ~p(#state{})~n", [StateHandler]),
    case erlang:apply(?MODULE, StateHandler, [State_1]) of
        {error, Reason, State_2} -> error_behavior(Reason, State_2);
        #state{} = State_2 -> handle_sock_data(<<>>, State_2)
    end.

read_more(State) ->
    active_once(State),
    {noreply, set_inac_timer(State)}.

error_behavior(Reason, State) ->
    do_error_reply(State, data_in_status_idle),
    do_trace("found error: ~1000p~n", [Reason]),
    fail_pipelined_requests(State, {error, Reason}),
    shutting_down(State),
    do_close(State),
    {stop, normal, State}.

state_idle(#state{buffer = <<>>} = State) -> State;
state_idle(#state{} = State) -> {error, data_in_status_idle, State}.

state_header(#state{buffer = Buf} = State) ->
    <<PacketLength:24/little-integer, SeqNum:8/integer, Buf_1/binary>> = Buf,
    do_trace("starting reading body, size ~p.~n", [PacketLength]),
    change_state(body, PacketLength, State#state{buffer = Buf_1, seq_num = SeqNum}).

state_body(#state{buffer = Buf, want_bytes = PacketLength, seq_num = SeqNum} = State) ->
    do_trace("state_body (have ~p bytes).~n", [size(Buf)]),
    <<MBody:PacketLength/binary, Buf_1/binary>> = Buf,
    Packet = #packet{size = PacketLength, seq_num = SeqNum, data = MBody},
    change_state(protocol, State#state{packet = Packet, buffer = Buf_1}).

state_protocol(#state{protocol_handler = ProtocolHandler, packet = Packet} = State) ->
    do_trace("calling ~p(#state{})~n", [ProtocolHandler]),
    try
        erlang:apply(?MODULE, ProtocolHandler, [Packet, State])
    catch
        _:Reason -> {error, Reason, State}
    end.

protocol_generic(#packet{seq_num = SeqNum, data = <<0:8, Rest/binary>>}, #state{response = RQ} = State) ->
    {AffectedRows, Rest1} = ems_util:length_coded_binary(Rest),
    {InsertId, Rest2} = ems_util:length_coded_binary(Rest1),
    <<ServerStatus:16/little, WarningCount:16/little, Msg/binary>> = Rest2,
    RQ_1 = queue:in(#ok_packet{
                       seq_num = SeqNum,
                       affected_rows = AffectedRows,
                       insert_id = InsertId,
                       status = ServerStatus,
                       warning_count = WarningCount,
                       msg = unicode:characters_to_list(Msg)}, RQ),
    protocol_continuation(State#state{response = RQ_1, server_status = ServerStatus});
protocol_generic(#packet{seq_num = SeqNum, data = <<?RESP_EOF:8>>}, #state{response = RQ} = State) ->
    RQ_1 = queue:in(#eof_packet{seq_num = SeqNum}, RQ),
    protocol_continuation(State#state{response = RQ_1, server_status = ?SERVER_NO_STATUS});
protocol_generic(#packet{seq_num = SeqNum, data = <<?RESP_EOF:8, WarningCount:16/little, ServerStatus:16/little>>},
                 #state{response = RQ} = State) ->
    RQ_1 = queue:in(#eof_packet{
                       seq_num = SeqNum,
                       status = ServerStatus,
                       warning_count = WarningCount}, RQ),
    protocol_continuation(State#state{response = RQ_1, server_status = ServerStatus});
protocol_generic(#packet{seq_num = SeqNum, data = <<255:8, ErrNo:16/little, "#", SQLState:5/binary-unit:8, Msg/binary>>},
                 #state{response = RQ} = State) ->
    RQ_1 = queue:in(#error_packet{
                       seq_num = SeqNum,
                       code = ErrNo,
                       status = SQLState,
                       msg = binary_to_list(Msg)}, RQ),
    protocol_continuation(State#state{response = RQ_1, server_status = ?SERVER_NO_STATUS});
protocol_generic(#packet{seq_num = SeqNum, data = <<255:8, ErrNo:16/little, Msg/binary>>},
                 #state{response = RQ} = State) ->
    RQ_1 = queue:in(#error_packet{
                       seq_num = SeqNum,
                       code = ErrNo,
                       status = 0,
                       msg = binary_to_list(Msg)}, RQ),
    protocol_continuation(State#state{response = RQ_1, server_status = ?SERVER_NO_STATUS});
protocol_generic(#packet{seq_num = SeqNum, data = Data}, State) ->
    {FieldCount, Rest1} = ems_util:length_coded_binary(Data),
    {Extra, _} = ems_util:length_coded_binary(Rest1),
    change_state(header, change_protocol(fields, State#state{seq_num = SeqNum + 1, mysql_extra = Extra, field_count = FieldCount})).

protocol_continuation(#state{server_status = ServerStatus, response = RQ} = State) ->
    do_trace("ems returned good body ~1000p, ~1000p.~n", [RQ, ServerStatus]),
    case ServerStatus band ?SERVER_MORE_RESULTS_EXIST of
        0 ->
            Reply = simplify_reply(queue:to_list(RQ)),
            protocol_reply(Reply, State);
        ?SERVER_MORE_RESULTS_EXIST ->
            change_state(header, State)
    end.

protocol_fields(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _WarningCount:16/little, _ServerStatus:16/little>>},
                #state{} = State) ->
    protocol_fields_continuation(State#state{seq_num = SeqNum1});
protocol_fields(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _/binary>>},
                #state{} = State) ->
    protocol_fields_continuation(State#state{seq_num = SeqNum1});
protocol_fields(#packet{seq_num = SeqNum1, data = Data},
                #state{fields_queue = Q, fields_key = Key} = State) ->
    {Catalog, Rest2} = ems_util:length_coded_string(Data),
    {Db, Rest3} = ems_util:length_coded_string(Rest2),
    {Table, Rest4} = ems_util:length_coded_string(Rest3),
    {OrgTable, Rest5} = ems_util:length_coded_string(Rest4),
    {Name, Rest6} = ems_util:length_coded_string(Rest5),
    {OrgName, Rest7} = ems_util:length_coded_string(Rest6),
    <<_:1/binary, CharSetNr:16/little, Length:32/little, Rest8/binary>> = Rest7,
    <<Type:8/little, Flags:16/little, Decimals:8/little, _:2/binary, Rest9/binary>> = Rest8,
    {Default, _} = ems_util:length_coded_binary(Rest9),
    Field = #field{
               seq_num = SeqNum1,
               catalog = Catalog,
               db = Db,
               table = Table,
               org_table = OrgTable,
               name = Name,
               org_name = OrgName,
               type = Type,
               default = Default,
               charset_nr = CharSetNr,
               length = Length,
               flags = Flags,
               decimals = Decimals
              },
    change_state(header, State#state{seq_num = SeqNum1, fields_queue = queue:in(Field, Q), fields_key = Key + 1}).

protocol_fields_continuation(#state{seq_num = SeqNum1, fields_queue = Q, field_count = FieldCount} = State) ->
    FieldList = queue:to_list(Q),
    if
        length(FieldList) =/= FieldCount -> {error, query_returned_incorrect_field_count, State};
        true -> change_state(header, change_protocol(rows, State#state{seq_num = SeqNum1 + 1, fields_queue = FieldList}))
    end.

protocol_rows(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _WarningCount:16/little, ServerStatus:16/little>>},
              #state{} = State) ->
    protocol_rows_continuation(State#state{seq_num = SeqNum1, server_status = ServerStatus});
protocol_rows(#packet{seq_num = SeqNum1, data = <<?RESP_EOF, _/binary>>},
              #state{} = State) ->
    protocol_rows_continuation(State#state{seq_num = SeqNum1, server_status = ?SERVER_NO_STATUS});
protocol_rows(#packet{seq_num = SeqNum1, data = RowData},
              #state{rows_queue = Q, fields_key = Key, fields_queue = FieldList} = State) ->
    Row = ems_tcp:decode_row_data(RowData, FieldList, []),
    change_state(header, State#state{seq_num = SeqNum1, rows_queue = queue:in(Row, Q), fields_key = Key + 1}).

protocol_rows_continuation(#state{seq_num = SeqNum1, fields_queue = FieldList,
                                       rows_queue = Q, mysql_extra = Extra, response = RQ} = State) ->
    RQ_1 = queue:in(#result_packet{
                       seq_num = SeqNum1,
                       field_list = FieldList,
                       rows = queue:to_list(Q),
                       extra = Extra}, RQ),
    protocol_continuation(change_protocol(generic, State#state{response = RQ_1})).

protocol_reply(Reply, #state{from = From, req_id = ReqId} = State) ->
    do_trace("parse_response reply: ~1000p, ~1000p~n", [Reply, State]),
    State_1 = do_reply(State, From, ReqId, {ok, Reply}),
    State_2 = reset_state(State_1),
    next_request(State_2).

next_request(#state{reqs = Reqs} = State) ->
    {_, Reqs_1} = queue:out(Reqs),
    set_cur_request(State#state{reqs = Reqs_1}).

simplify_reply([JustOneValue]) -> JustOneValue;
simplify_reply(ListOfValues) -> ListOfValues.

change_state(idle, State) ->
    State#state{state_handler = state_idle, want_bytes = 1, packet = undefined};
change_state(header, State) ->
    State#state{state_handler = state_header, want_bytes = 4, packet = undefined};
change_state(protocol, State) ->
    State#state{state_handler = state_protocol, want_bytes = 0}.

change_state(body, PacketLength, State) ->
    State#state{state_handler = state_body, want_bytes = PacketLength}.

change_protocol(generic, State) ->
    State#state{protocol_handler = protocol_generic};
change_protocol(fields, State) ->
    State#state{protocol_handler = protocol_fields, fields_queue = queue:new(), fields_key = 0};
change_protocol(rows, State) ->
    State#state{protocol_handler = protocol_rows, rows_queue = queue:new(), fields_key = 0}.

handle_sock_closed(#state{cur_req = undefined} = State) ->
    shutting_down(State);

handle_sock_closed(#state{} = State) ->
    do_error_reply(State, connection_closed).

do_connect(#sql_entity{host = Host, port = Port, username = Username, password = Password,
                       database = Database, encoding = Encoding} = Entity,
           Options, _State, _Timeout) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false} | get_sock_options(Entity, Options)]) of
        {ok, Sock} ->
            do_trace("Connection is opened~n", []),
            try
                case ems_auth:do_handshake(Sock, Username, Password) of
                    Greeting when is_record(Greeting, greeting) ->
                        do_trace("MySQL greeting: ~1000.p~n", [Greeting]),
                        Connection = #ems_connection{
                                        socket = Sock,
                                        version = Greeting#greeting.server_version,
                                        thread_id = Greeting#greeting.thread_id,
                                        caps = Greeting#greeting.caps,
                                        language = Greeting#greeting.language
                                       },
                        case ems_conn:set_database(Connection, Database) of
                            OK1 when is_record(OK1, ok_packet) ->
                                do_trace("MySQL database set~n", []),
                                case ems_conn:set_encoding(Connection, Encoding) of
                                    OK2 when is_record(OK2, ok_packet) ->
                                        do_trace("MySQL encoding set~n", []),
                                        {ok, Connection};
                                    Err2 when is_record(Err2, error_packet) ->
                                        do_trace("MySQL encoding didn't set: ~1000p~n", [Err2]),
                                        {error, Err2#error_packet.msg}
                                end;
                            Err1 when is_record(Err1, error_packet) ->
                                do_trace("MySQL database didn't set: ~1000p~n", [Err1]),
                                {error, Err1#error_packet.msg}
                        end;
                    {error, Reason} = Error ->
                        do_trace("Can't connect to server!. ~1000.p~n", [Reason]),
                        Error;
                    What ->
                        do_trace("Can't connect to server!. ~1000.p~n", [What]),
                        {error, What}
                end
            catch
                _:{_, Reason2} -> {error, Reason2};
                _:Error2 -> {error, Error2}
            end;
        Error -> Error
    end.

get_sock_options(Entity, Options) ->
    Caller_socket_options = isql_lib:get_value(socket_options, Options, []),
    Ipv6Options = case is_ipv6_host(Entity#sql_entity.host) of
        true ->
            [inet6];
        false ->
            []
    end,
    Other_sock_options = filter_sock_options(Caller_socket_options ++ Ipv6Options),
    case lists:keysearch(nodelay, 1, Other_sock_options) of
        false ->
            [{nodelay, true}, binary, {active, false} | Other_sock_options];
        {value, _} ->
            [binary, {active, false} | Other_sock_options]
    end.

is_ipv6_host(Host) ->
    case inet_parse:address(Host) of
        {ok, {_, _, _, _, _, _, _, _}} ->
            true;
        {ok, {_, _, _, _}} ->
            false;
        _  ->
            case inet:gethostbyname(Host) of
                {ok, #hostent{h_addrtype = inet6}} ->
                    true;
                _ ->
                    false
            end
    end.

%% We don't want the caller to specify certain options
filter_sock_options(Opts) ->
    lists:filter(fun({active, _}) ->
                         false;
                    ({packet, _}) ->
                         false;
                    (list) ->
                         false;
                    (_) ->
                         true
                 end, Opts).

do_close(#state{connection = undefined})            ->  ok;
do_close(#state{connection = #ems_connection{socket = undefined}})            ->  ok;
do_close(#state{connection = #ems_connection{socket = Sock}}) ->  catch gen_tcp:close(Sock).

active_once(#state{connection = undefined}) ->
    do_trace("Error! Activating undefined connection.~n", []);
active_once(#state{connection = #ems_connection{socket = undefined}}) ->
    do_trace("Error! Activating undefined socket.~n", []);
active_once(#state{connection = #ems_connection{socket = Socket}}) ->
    do_trace("Activating socket.~n", []),
    do_setopts(Socket, [{active, once}]).

do_setopts(_Sock, []) -> ok;
do_setopts(Sock, Opts) -> inet:setopts(Sock, Opts).

send_req_1(From, SQL, Options, Timeout,
           #state{connection = undefined, entity = Entity} = State) ->
    do_trace("Connecting...~n", []),
    Conn_timeout = isql_lib:get_value(connect_timeout, Options, Timeout),
    case do_connect(Entity, Options, State, Conn_timeout) of
        {ok, Connection} ->
            do_trace("Connected! Socket: ~1000.p~n", [Connection#ems_connection.socket]),
            State_3 = State#state{connection = Connection, connect_timeout = Conn_timeout},
            send_req_1(From, SQL, Options, Timeout, State_3);
        Err ->
            Reason = case Err of
                         {error, #error_packet{msg = Msg}} -> Msg;
                         {error, Reason2} -> Reason2;
                         Reason3 -> Reason3
                     end,
            shutting_down(State),
            do_trace("Error connecting or handshaking. Reason: ~1000.p~n", [Err]),
            gen_server:reply(From, {error, {conn_failed, Reason}}),
            {stop, normal, State}
    end;

send_req_1(From, SQL, Options, Timeout,
           #state{connection = Connection, entity = Entity} = State) ->
    do_trace("in send_req_1 with connection established~n", []),
    cancel_timer(State#state.inactivity_timer_ref, {eat_message, timeout}),
    ReqId = make_req_id(),
    Caller_socket_options = isql_lib:get_value(socket_options, Options, []),
    SaveResponseToFile = isql_lib:get_value(save_response_to_file, Options, false),
    Ref = case Timeout of
              infinity -> undefined;
              _ -> erlang:send_after(Timeout, self(), {req_timedout, From})
          end,
    NewReq = #request{entity                 = Entity,
                      caller_socket_options  = Caller_socket_options,
                      options                = Options,
                      req_id                 = ReqId,
                      save_response_to_file  = SaveResponseToFile,
                      from                   = From,
                      timer_ref              = Ref
                     },
    State_1 = State#state{reqs=queue:in(NewReq, State#state.reqs)},
    do_trace("requests queue of size ~b~n", [queue:len(State#state.reqs)]),
    Packet = <<?COM_QUERY, (ems_util:any_to_binary(SQL))/binary>>,
    do_setopts(Connection#ems_connection.socket, Caller_socket_options),
    do_trace("sending packet ~1000p, ~1000p~n", [Connection, Packet]),
    case catch ems_tcp:send_packet(Connection#ems_connection.socket, Packet, 0) of
        ok ->
            State_2 = inc_pipeline_counter(State_1),
            active_once(State_2),
            State_3 = set_cur_request(State_2),
            State_4 = set_inac_timer(State_3),
            {noreply, State_4};
        {_, Error} ->
            Err = case Error of
                      {_, TrueError} -> TrueError;
                      _ -> Error
                  end,
            shutting_down(State_1),
            do_trace("Send failed... Reason: ~p~n", [Err]),
            gen_server:reply(From, {error, {send_failed, Err}}),
            {stop, normal, State_1}
    end.


reset_state(State) ->
    (change_state(idle, change_protocol(generic, State)))
    #state{packet = undefined, seq_num = undefined, response = undefined,
           server_status = undefined, fields_queue = undefined, fields_key = undefined,
           rows_queue = undefined, mysql_extra = undefined, field_count = undefined}.

set_cur_request(#state{reqs = Reqs, cur_req = CurReq} = State) ->
    case {queue:peek(Reqs), CurReq} of
        {empty, _} ->
            reset_state(State#state{cur_req = undefined, from = undefined});
        {{value, #request{} = NextReq}, NextReq} -> State;
        {{value, #request{} = NextReq}, _} ->
            change_state(header, State#state{cur_req = NextReq, from = NextReq#request.from, response = queue:new()})
    end.

do_reply(State, From, _, Msg) ->
    gen_server:reply(From, Msg),
    dec_pipeline_counter(State).
%do_reply(#state{prev_req_id = Prev_req_id} = State,
%         _From, StreamTo, ReqId, Msg) ->
%    State_1 = dec_pipeline_counter(State),
%	catch StreamTo ! {isql_async_response, ReqId, Msg},
%    catch StreamTo ! {isql_async_response_end, ReqId},
%    ets:delete(isql_stream, {req_id_pid, Prev_req_id}),
%    State_1#state{prev_req_id = ReqId};
%do_reply(State, _From, StreamTo, ReqId, Msg) ->
%    State_1 = dec_pipeline_counter(State),
%    catch StreamTo ! {isql_async_response, ReqId, Msg},
%    State_1.

do_error_reply(#state{reqs = Reqs} = State, Err) ->
    ReqList = queue:to_list(Reqs),
    lists:foreach(fun(#request{from=From, req_id=ReqId}) ->
                          ets:delete(isql_stream, {req_id_pid, ReqId}),
                          do_reply(State, From, ReqId, {error, Err})
                  end, ReqList).

fail_pipelined_requests(#state{reqs = Reqs, cur_req = CurReq} = State, Reply) ->
    {_, Reqs_1} = queue:out(Reqs),
    #request{from=From, req_id=ReqId} = CurReq,
    State_1 = do_reply(State, From, ReqId, Reply),
    do_error_reply(State_1#state{reqs = Reqs_1}, previous_request_failed).

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref), ok.

cancel_timer(Ref, {eat_message, Msg}) ->
    cancel_timer(Ref),
    receive
        Msg -> ok
    after
        0 -> ok
    end.

make_req_id() ->
    now().

shutting_down(#state{lb_ets_tid = undefined}) -> ok;
shutting_down(#state{lb_ets_tid = Tid, cur_pipeline_size = _Sz}) ->
    catch ets:delete(Tid, self()).

inc_pipeline_counter(#state{lb_ets_tid = undefined} = State) -> State;
inc_pipeline_counter(#state{cur_pipeline_size = Pipe_sz, lb_ets_tid = Tid} = State) ->
    update_counter(Tid, self(), {2,1,99999,9999}),
    State#state{cur_pipeline_size = Pipe_sz + 1}.

update_counter(Tid, Key, Args) -> ets:update_counter(Tid, Key, Args).

dec_pipeline_counter(#state{lb_ets_tid = undefined} = State) -> State;
dec_pipeline_counter(#state{cur_pipeline_size = Pipe_sz, lb_ets_tid = Tid} = State) ->
    try
        update_counter(Tid, self(), {2,-1,0,0}),
        update_counter(Tid, self(), {3,-1,0,0}),
        ok
    catch
        _:_ -> ok
    end,
    State#state{cur_pipeline_size = Pipe_sz - 1}.

set_inac_timer(State) ->
    cancel_timer(State#state.inactivity_timer_ref),
    set_inac_timer(State#state{inactivity_timer_ref = undefined}, get_inac_timeout(State)).

set_inac_timer(State, Timeout) when is_integer(Timeout) ->
    Ref = erlang:send_after(Timeout, self(), timeout),
    State#state{inactivity_timer_ref = Ref};
set_inac_timer(State, _) -> State.

get_inac_timeout(#state{cur_req = #request{options = Opts}}) ->
    isql_lib:get_value(inactivity_timeout, Opts, infinity);
get_inac_timeout(#state{cur_req = undefined}) ->
    case isql:get_config_value(inactivity_timeout, undefined) of
        Val when is_integer(Val) -> Val;
        _ ->
            case application:get_env(isql, inactivity_timeout) of
                {ok, Val} when is_integer(Val), Val > 0 -> Val;
                _ -> 10000
            end
    end.

