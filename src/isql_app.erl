-module(isql_app).

-behaviour(application).
-export([
	 start/2,
	 stop/1
        ]).

-export([
        ]).

start(_Type, _StartArgs) ->
    case isql_sup:start_link() of
	{ok, Pid} -> 
	    {ok, Pid};
	Error ->
	    Error
    end.

stop(_State) ->
    ok.

