-module(redis_operation).

-include("redis_operation.hrl").

%% API
-export([enter_loop/3]).
-export([get/1]).


%% socket state
-record(state, {
    socket :: inet:socket(),
    peername :: {inet:ip_address(), non_neg_integer()},
    transport :: module(),
    database = redis_mnesia_table0 :: atom(),
    trans = false :: boolean(),
    error = false :: boolean(),
    dirty = false :: boolean(),
    wlist = []:: list(),
    optlist = []:: [{atom(), [binary()]}]
}).

enter_loop(Socket, Peername, Transport) ->
    loop(#state{socket=Socket, peername=Peername, transport=Transport}).

loop(State = #state{socket=Socket, transport=Transport}) ->
    receive
        {tcp, Socket, Data} ->
            {_Num, Cmd, Param} = redis_parser:parse_data(Data),
            io:format("Recive data: ~p, peername: ~p~n", [Data, State#state.peername]),
            io:format("Recive command: ~p ~p ~p~n", [Cmd, _Num, Param]),
            {Reply, NewState} = do_operation(Cmd, Param, State),
            Transport:send(Socket, Reply),
            Transport:setopts(Socket, [{active, once}]),
            loop(NewState);
        {tcp_closed, Socket} ->
            ok = Transport:close(Socket);
        _ ->
            ok = Transport:close(Socket)
    end.


%% do operation
do_operation(Cmd, Param, State) ->
    try binary_to_existing_atom(Cmd, latin1) of
        Func ->
            try erlang:function_exported(redis_operation, Func, 1) of
                false ->
                    {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State};
                true ->
                    case do_transaction(Func, Param, State) of
                        {ok, old} ->
                            {redis_operation:Func(Param), State};
                        {ok, new, Reply, NewState} ->
                            {Reply, NewState}
                    end
            catch
                _Error:_Code ->
                    {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State}
            end
    catch
        _Error:_Code ->
            {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State}
    end.

%% check transaction operation
%% todo:
%% 1.Check command before queued
%% 2.Check watch list (need it ? ) after finish redis_operation.erl
%% 3.Finish exec command
do_transaction(Cmd, Param, State=#state{trans=false, wlist=Wlist}) ->
    case Cmd of
        multi ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=true}};
        watch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=Param++Wlist}};
        unwatch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=[]}};
        exec ->
            {ok, new, redis_parser:reply_error(<<"EXEC without MULTI">>), State#state{error=true}};
        discard ->
            {ok, new, redis_parser:reply_error(<<"DISCARD without MULTI">>), State#state{error=true}};
        _ ->
            {ok, old}
    end;
do_transaction(Cmd, Param, State=#state{trans=_, wlist=_Wlist, optlist=OptList}) ->
    case Cmd of
        multi ->
            {ok, new, redis_parser:reply_error(<<"MULTI calls can not be nested">>), State#state{error=true}};
        watch ->
            {ok, new, redis_parser:reply_error(<<"WATCH inside MULTI is not allowed">>), State#state{error=true}};
        unwatch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=[]}};
        exec ->
            _Operation = lists:reverse(OptList),
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=false, wlist=[], optlist=[]}};
        discard ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=false, wlist=[], optlist=[]}};
        Cmd ->
            {ok, new, redis_parser:reply_status(<<"QUEUED">>), State#state{optlist=[{Cmd, Param}|OptList]}}
    end.

%% operation
get(_List) ->
    redis_parser:reply_single(<<"def">>).

