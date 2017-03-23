-module(mnesis_interface).
-behaviour(ranch_protocol).

-include("mnesis_operation.hrl").

-export([start_link/4, init/4]).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

-spec init(ranch:ref(), inet:socket(), module(), list()) -> ok.
init(Ref, Socket, Transport, _Opts) ->
    ok = Transport:setopts(Socket, [{active, once}]),
	ok = ranch:accept_ack(Ref),
	mnesis_operation:enter_loop(Socket, inet:peername(Socket), Transport).
