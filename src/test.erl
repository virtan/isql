-module(test).
-export([
         test/0
        ]).

-include("isql.hrl").
-include("ems.hrl").

init() ->
    application:start(isql).

sql_init() ->
    SQLEntity = #sql_entity{
                   host = "localhost", port = 3306,
                   username = "jd", password = "jd",
                   database = "test", encoding = utf8
                  },
    isql:set_max_sessions(SQLEntity, 2),
    isql:set_max_pipeline_size(SQLEntity, 10),
    SQLEntity.

sql_execute(SQLEntity, SQL) ->
    case isql:send_req(SQLEntity, SQL) of
        {ok, #result_packet{rows = Rows}} -> {ok, Rows};
        {ok, #ok_packet{affected_rows = Affected}} -> {ok, Affected};
        {ok, #error_packet{msg = Reason}} -> {error, Reason};
        {error, _} = Error -> Error
    end.


compile_sql(QueryBinary, Args) ->
    Sd = binary:split(QueryBinary, <<"?">>, [global]),
    case {length(Sd), length(Args)} of
        {N, M} when N == M + 1 ->
            Args1 = [ems_util:encode(A, true) || A <- Args],
            iolist_to_binary(lists:zipwith(fun(A, B) -> [B, A] end, Sd, [<<>> | Args1]));
        _ ->
            throw(sql_wrong_number_of_arguments)
    end.

test() ->
    init(),
    SQLEntity = sql_init(),
    SQL = compile_sql(<<"select * from data where id = ? and age < ?">>, ["a123", 21]),
    {ok, [[<<"a123">>, <<"Valya">>, 20],
          [<<"a123">>, <<"Kolya">>, 1]]} = sql_execute(SQLEntity, SQL),
    {ok, 1} = sql_execute(SQLEntity, "insert into data(id, name, age) values('a123', 'Igor', 35)"),
    {error, "You have an error in your SQL syntax" ++ _} = sql_execute(SQLEntity, "insert into nonexistent(id), values(1)"),
    {error, {conn_failed, eacces}} = sql_execute(SQLEntity#sql_entity{host = "255.255.255.255"}, "select 2").
