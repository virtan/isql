-module(emysql_conn).
-export([set_database/2, set_encoding/2,
         execute/3,
         hstate/1
]).

-include("emysql.hrl").

set_database(_, undefined) -> ok;
set_database(Connection, Database) ->
	Packet = <<?COM_QUERY, "use ", (iolist_to_binary(Database))/binary>>,  % todo: utf8?
	emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).

set_encoding(Connection, Encoding) ->
	Packet = <<?COM_QUERY, "set names '", (erlang:atom_to_binary(Encoding, utf8))/binary, "'">>,
	emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).

execute(Connection, Query, []) when is_list(Query); is_binary(Query) ->
	 %-% io:format("~p execute: ~p using connection: ~p~n", [self(), iolist_to_binary(Query), Connection#emysql_connection.id]),
	Packet = <<?COM_QUERY, (emysql_util:any_to_binary(Query))/binary>>,
	% Packet = <<?COM_QUERY, (iolist_to_binary(Query))/binary>>,
	emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).


hstate(State) ->

	   case (State band ?SERVER_STATUS_AUTOCOMMIT) of 0 -> ""; _-> "AUTOCOMMIT " end
	++ case (State band ?SERVER_MORE_RESULTS_EXIST) of 0 -> ""; _-> "MORE_RESULTS_EXIST " end
	++ case (State band ?SERVER_QUERY_NO_INDEX_USED) of 0 -> ""; _-> "NO_INDEX_USED " end.
