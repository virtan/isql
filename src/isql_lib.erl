-module(isql_lib).
-ifdef(debug).
-compile(export_all).
-endif.

-include("isql.hrl").

-export([
         get_trace_status/1,
         do_trace/2,
         do_trace/3,
         get_value/2,
         get_value/3,
         printable_date/0
        ]).

get_trace_status(Entity) ->
    isql:get_config_value({trace, Entity}, false).

get_value(Tag, TVL, DefVal) ->
    case lists:keysearch(Tag, 1, TVL) of
        false ->
            DefVal;
        {value, {_, Val}} ->
            Val
    end.

get_value(Tag, TVL) ->
    {value, {_, V}} = lists:keysearch(Tag,1,TVL),
    V.

printable_date() ->
    {{Y,Mo,D},{H, M, S}} = calendar:local_time(),
    {_,_,MicroSecs} = now(),
    [integer_to_list(Y),
     $-,
     integer_to_list(Mo),
     $-,
     integer_to_list(D),
     $_,
     integer_to_list(H),
     $:,
     integer_to_list(M),
     $:,
     integer_to_list(S),
     $:,
     integer_to_list(MicroSecs div 1000)].

do_trace(Fmt, Args) ->
    do_trace(get(my_trace_flag), Fmt, Args).

-ifdef(DEBUG).
do_trace(_, Fmt, Args) ->
    io:format("~s -- (~s) - "++Fmt,
              [printable_date(), 
               get(isql_trace_token) | Args]).
-else.
do_trace(true, Fmt, Args) ->
    io:format("~s -- (~s) - "++Fmt,
              [printable_date(), 
               get(isql_trace_token) | Args]);
do_trace(_, _, _) ->
    ok.
-endif.

