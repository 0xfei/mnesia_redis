-module(mnesis_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	mnesia_start(),
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

mnesia_start() ->
	ok.
	% ok = mnesia:create_schema([node()]).
