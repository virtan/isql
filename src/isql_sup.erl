-module(isql_sup).
-behaviour(supervisor).
-export([
	 start_link/0
        ]).

-export([
	 init/1
        ]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    AChild = {isql,{isql,start_link,[]},
	      permanent,2000,worker,[isql, isql_mysql_client]},
    {ok,{{one_for_all,10,1}, [AChild]}}.

