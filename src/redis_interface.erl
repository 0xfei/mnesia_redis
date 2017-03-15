-module(redis_interface).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-type opts() :: [].
-export_type([opts/0]).

-record(state, {
	socket :: inet:socket(),
    peername :: {inet:ip_address(), non_neg_integer()},
	transport :: module()
}).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

-spec init(ranch:ref(), inet:socket(), module(), opts()) -> ok.
init(Ref, Socket, Transport, _Opts) ->
    {ok, PeerName} = inet:peername(Socket),
    ok = check_peername(PeerName),
	ok = ranch:accept_ack(Ref),
	loop(#state{socket=Socket, peername=PeerName, transport=Transport}).

loop(State = #state{socket=Socket, transport=Transport}) ->
	case Transport:recv(Socket, 0, infinity) of
		{ok, Data} ->
            io:format("Recive data: ~p, peername: ~p~n",
                [Data, Transport:peername(Socket)]),
            Reply = redis_parser:execute(Data),
			Transport:send(Socket, Reply),
			loop(State);
		_ ->
			ok = Transport:close(Socket)
	end.

%% check peername
check_peername({_Address, _Port}) ->
    ok.
