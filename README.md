iSQL 
====

Clone of ibrowse [1] for pipelined mysql requests.
Performs load balancing of requests among mysql connections with pipelining.
Alpha version.

  [1]: https://github.com/cmullaparthi/ibrowse


Usage
-----

<pre>
-include("isql.hrl").
-include("ems.hrl").

init() ->
    application:start(isql).

sql_init() ->
    SQLEntity = #sql_entity{
                    host = "localhost", port = 3306,
                    username = "virtan", password = "virtan.com",
                    database = "life", encoding = utf8
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
</pre>

It's recommended to use SafetyValve [2] to control overall load on database server.

<pre>
sql_init(Supervisor, SVConfig) ->
    SQLEntity = #sql_entity{
                    host = "localhost", port = 3306,
                    username = "virtan", password = "virtan.com",
                    database = "life", encoding = utf8
                },
    isql:set_max_sessions(SQLEntity, 2),
    isql:set_max_pipeline_size(SQLEntity, 10),
    {ok, _} = supervisor:start_child(Supervisor, {life_sv,
                {sv_queue, start_link, [life_sv, sv_queue:parse_configuration(SVConfig)]},
                permanent, 10000, worker, [sv_queue]}),
    SQLEntity.

sql_execute(SQLEntity, SQL) ->
    case sv_run(life_sv, fun() -> isql:send_req(SQLEntity, SQL) end) of
        {ok, {ok, #result_packet{rows = Rows}}} -> {ok, Rows};
        {ok, {ok, #ok_packet{affected_rows = Affected}}} -> {ok, Affected};
        {ok, {ok, #error_packet{msg = Reason}}} -> {error, Reason};
        {ok, {error, _} = Error} -> Error;
        {error, _} = Error -> Error
    end.
</pre>

  [2]: https://github.com/jlouis/safetyvalve


Author
------

Igor Milyakov
[virtan@virtan.com] [3]

  [3]: mailto:virtan@virtan.com?subject=isql


License
-------

The MIT License (MIT)

Copyright (c) 2013 Igor Milyakov virtan@virtan.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
