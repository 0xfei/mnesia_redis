%%
%% todo: 
%%	replace List++ListB by join two list with a fun
%%	move help function to another file
%%
-module(redis_operation).

-include("redis_operation.hrl").

%% API
-export([enter_loop/3]).
-export([del/3]).
-export([get/3, set/3]).
-export([rpush/3, lpop/3, lindex/3, lrange/3]).


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
    optlist = []:: [{atom(), integer(), [binary()]}]
}).

enter_loop(Socket, Peername, Transport) ->
    loop(#state{socket=Socket, peername=Peername, transport=Transport}).

loop(State = #state{socket=Socket, transport=Transport}) ->
    receive
        {tcp, Socket, Data} ->
            {Num, Cmd, Param} = redis_parser:parse_data(Data),
            io:format("Recive data: ~p, peername: ~p~n", [Data, State#state.peername]),
            io:format("Recive command: ~p ~p ~p~n", [Cmd, Num, Param]),
            {Reply, NewState} = do_operation(Cmd, Num, Param, State),
            Transport:send(Socket, Reply),
            Transport:setopts(Socket, [{active, once}]),
            loop(NewState);
        {tcp_closed, Socket} ->
            ok = Transport:close(Socket);
        _ ->
            ok = Transport:close(Socket)
    end.


%% do operation
do_operation(Cmd, Num, Param, State=#state{database=Database}) ->
    try binary_to_existing_atom(Cmd, latin1) of
        Func ->
            try erlang:function_exported(redis_operation, Func, 3) of
                false ->
                    {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State};
                true ->
                    case do_transaction(Func, Num, Param, State) of
                        {ok, old} ->
                            {redis_operation:Func(Database, Num, Param), State};
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
do_transaction(Cmd, _Num, Param, State=#state{trans=false, wlist=Wlist}) ->
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
do_transaction(Cmd, Num, Param, State=#state{trans=_, wlist=_Wlist, optlist=OptList}) ->
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
            {ok, new, redis_parser:reply_status(<<"QUEUED">>), State#state{optlist=[{Cmd, Num, Param}|OptList]}}
    end.

%%
%% key
%%

%% del
del(_Database, 0, []) ->
    redis_parser:reply_status(<<"OK">>);
del(Database, N, [Key|Left]) ->
    mnesia:dirty_delete({Database, Key}),
    del(Database, N-1, Left).

%%
%% string
%%

%% get
get(Database, 1, [Key]) ->
    try mnesia:dirty_read({Database, Key}) of
        [{Database, Key, Value}] when is_binary(Value)->
            redis_parser:reply_single(Value);
        [] ->
            redis_parser:reply_single(<<>>);
        _ ->
            redis_parser:reply_error(<<"ERR Operation against a key holding the wrong kind of value">>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"ERR wrong number of arguments for 'get' command">>)
    end;
get(_, _, _) ->
    redis_parser:reply_error(<<"ERR wrong number of arguments for 'get' command">>).

%% set
%% todo: expire
set(Database, 2, [Key, Value]) ->
    try mnesia:dirty_write({Database, Key, Value}) of
        ok ->
            redis_parser:reply_status(<<"OK">>);
        _ ->
            redis_parser:reply_error(<<"ERR syntax error">>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"ERR syntax error">>)
    end;
set(_, _, _) ->
    redis_parser:reply_error(<<"ERR syntax error">>).

%%
%% list
%%

%% rpush
rpush(Database, N, [Key | Left]) when N > 1 ->
    try mnesia:dirty_read({Database, Key}) of
        [{Database, Key, [M|Value]}] when is_integer(M) ->
            mnesia:dirty_write({Database, Key, [N-1+M|Value ++ Left]}),
            redis_parser:reply_integer(N-1+M);
        [] ->
            mnesia:dirty_write({Database, Key, [N-1|Left]}),
            redis_parser:reply_integer(N-1);
        _ ->
            redis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
rpush(_, _, _) ->
    redis_parser:reply_error(<<"ERR wrong number of arguments for 'rpush' command">>).

%% lpop
%% todo: maybe more check
lpop(Database, 1, [Key]) ->
    try mnesia:dirty_read({Database, Key}) of
        [{Database, Key, [1 , H]}] ->
            mnesia:dirty_delete({Database, Key}),
            redis_parser:reply_single(H);
        [{Database, Key, [M, H | Value]}] when is_integer(M)->
            mnesia:dirty_write({Database, Key, [M-1 | Value]}),
            redis_parser:reply_single(H);
        _ ->
            redis_parser:reply_single(<<>>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lpop(_, _, _) ->
    redis_parser:reply_error(<<"ERR wrong number of arguments for 'rpop' command">>).

%% lindex
lindex(Database, 2, [Key, SIndex]) ->
    try
        _ = binary_to_integer(SIndex),
        mnesia:dirty_read({Database, Key})
    of
        [{Database, Key, [M | List]}] when is_integer(M) ->
            Index = binary_to_integer(SIndex),
            H = if
                    Index < 0 andalso Index + M >= 0 ->
                        lists:nth(M + Index + 1, List);
                    M > Index ->
                        lists:nth(Index + 1, List);
                    true ->
                        <<>>
                end,
            redis_parser:reply_single(H);
        _ ->
            redis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lindex(_, _, _) ->
    redis_parser:reply_error(<<"ERR wrong number of arguments for 'lindex' command">>).

%% lrange
lrange(Database, 3, [Key, Start, End]) ->
    try
        _ = binary_to_integer(Start),
        _ = binary_to_integer(End),
        mnesia:dirty_read({Database, Key})
    of
        [{Database, Key, [M | List]}] when is_integer(M) ->
            _Index = binary_to_integer(Start),
            S = if
                    _Index < 0 andalso _Index + M >= 0 ->
                        M + _Index + 1;
                    true ->
                        _Index + 1
                end,
            _Index2 = binary_to_integer(End),
            T = if
                    _Index2 < 0 andalso _Index2 + M >= 0 ->
                        M + _Index2 + 1;
                    true ->
                        _Index2 + 1
                end,
            if
                S =< T andalso T =< M ->
                    redis_parser:reply_multi(
                        [redis_parser:reply_single(K) ||
                            K <- lists:sublist(List, S, T-S+1)]
                    );
                true ->
                    redis_parser:reply_single(<<>>)
            end;
        _ ->
            redis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    catch
        _:_ ->
            redis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lrange(_, _, _) ->
    redis_parser:reply_error(<<"ERR wrong number of arguments for 'lrange' command">>).
