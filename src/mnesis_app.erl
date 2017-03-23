-module(mnesis_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
	% create tables
	Tables = [
		mnesis_watch,
		mnesis_mnesia_table0,
		mnesis_mnesia_table1,
		mnesis_mnesia_table2,
		mnesis_mnesia_table3,
		mnesis_mnesia_table4,
		mnesis_mnesia_table5
	],
	mnesia_create(Tables),

	% start lisener
	{ok, _} = ranch:start_listener(
		?MODULE,
		100,
		ranch_tcp,
		[{port, 9527}],
		mnesis_interface,
		[]
	),

	% create cleaner
	mnesis_sup:start_link([Tables, 1000]).

stop(_State) ->
	ok.

mnesia_create(Tables) ->
	[mnesia:create_table(T, []) || T <- Tables].
