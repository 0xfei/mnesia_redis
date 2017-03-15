-module(redis_interface).
-behaviour(ranch_protocol).

-include("redis_operation.hrl").

-export([start_link/4, init/4]).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

-spec init(ranch:ref(), inet:socket(), module(), opts()) -> ok.
init(Ref, Socket, Transport, _Opts) ->
    {ok, PeerName} = inet:peername(Socket),
    ok = check_peername(PeerName),
    ok = Transport:setopts(Socket, [{active, once}]),
	ok = ranch:accept_ack(Ref),
	loop(#state{socket=Socket, peername=PeerName, transport=Transport}).

loop(State = #state{socket=Socket, transport=Transport}) ->
    receive
        {tcp, Socket, Data} ->
            {_Num, Cmd, Param} = redis_parser:parse_data(Data),
            io:format("Recive data: ~p, peername: ~p~n", [Data, State#state.peername]),
            io:format("Recive command: ~p ~p ~p~n", [Cmd, _Num, Param]),
            {Reply, NewState} = redis_operation:do_operation(Cmd, Param, State),
            Transport:send(Socket, Reply),
            Transport:setopts(Socket, [{active, once}]),
            loop(NewState);
        {tcp_closed, Socket} ->
            ok = Transport:close(Socket);
        _ ->
            ok = Transport:close(Socket)
    end.

%% check peername
check_peername({_Address, _Port}) ->
    ok.
