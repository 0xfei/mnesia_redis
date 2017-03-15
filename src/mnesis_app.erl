-module(mnesis_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
	mnesia_create(),
	{ok, _} = ranch:start_listener(
		?MODULE,
		100,
		ranch_tcp,
		[{port, 9527}],
		redis_interface,
		[]
	),
	mnesis_sup:start_link().

stop(_State) ->
	ok.

mnesia_create() ->
	mnesia:create_table(redis_mnesia_table0, []),
	mnesia:create_table(redis_mnesia_table1, []),
	mnesia:create_table(redis_mnesia_table2, []),
	mnesia:create_table(redis_mnesia_table3, []),
	mnesia:create_table(redis_mnesia_table4, []),
	mnesia:create_table(redis_mnesia_table5, []),
	mnesia:create_table(redis_mnesia_table6, []),
	mnesia:create_table(redis_mnesia_table7, []),
	mnesia:create_table(redis_mnesia_table8, []),
	mnesia:create_table(redis_mnesia_table9, []),
	mnesia:create_table(redis_mnesia_tablea, []),
	mnesia:create_table(redis_mnesia_tableb, []),
	mnesia:create_table(redis_mnesia_tablec, []),
	mnesia:create_table(redis_mnesia_tabled, []),
	mnesia:create_table(redis_mnesia_tablee, []),
	mnesia:create_table(redis_mnesia_tablef, []).
