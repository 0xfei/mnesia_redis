-module(mnesis_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
	% config
	{ok, Config} = application:get_env(mnesis, server_config),
	Port = case lists:keyfind(port, 1, Config) of
			   {port, _Port} ->
				   _Port;
			   _ ->
				   6319
		   end,
	Period = case lists:keyfind(period, 1, Config) of
			   {period, _Period} ->
				   _Period;
			   _ ->
				   1000
		   end,
	Num = case lists:keyfind(num, 1, Config) of
			   {num, _Num} ->
				   _Num;
			   _ ->
				   100
		   end,

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
		Num,
		ranch_tcp,
		[{port, Port}],
		mnesis_interface,
		[]
	),

	% create cleaner
	mnesis_sup:start_link([Tables, Period]).

stop(_State) ->
	ok.

mnesia_create(Tables) ->
	[mnesia:create_table(T, []) || T <- Tables].
