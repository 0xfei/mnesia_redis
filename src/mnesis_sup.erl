-module(mnesis_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link([Tables, Time]) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Tables, Time]).

init([Tables, Time]) ->
    Procs = [
        {redis_expire, {redis_expire, start_link, [Tables, Time]},
            permanent, 5000, worker, [redis_expire]}
    ],
	{ok, {{one_for_one, 1, 5}, Procs}}.
