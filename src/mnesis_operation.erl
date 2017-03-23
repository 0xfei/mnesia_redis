%%
%% todo:
%%
-module(mnesis_operation).

-include("mnesis_operation.hrl").

%% API
-export([enter_loop/3]).
-export([multi/3, watch/3, unwatch/3, exec/3, discard/3]).
-export([del/3, exists/3, keys/3, expire/3, ttl/3, persist/3]).
-export([get/3, set/3, incr/3, incrby/3]).
-export([rpush/3, lpop/3, lindex/3, lrange/3]).
-export([sadd/3, sismember/3, smembers/3, srem/3]).
-export([hget/3, hset/3, hincrby/3, hexists/3, hgetall/3, hdel/3, hmget/3, hmset/3]).
-export([zadd/3, zrem/3, zrange/3, zrevrange/3, zrangebyscore/3, zrevrangebyscore/3, zincrby/3, zscore/3]).

%% socket state
-record(state, {
    socket :: inet:socket(),
    peername :: {inet:ip_address(), non_neg_integer()},
    transport :: module(),
    database = mnesis_mnesia_table0 :: atom(),
    trans = false :: boolean(),
    error = false :: boolean(),
    optlist = []:: [{atom(), integer(), [binary()]}]
}).

enter_loop(Socket, Peername, Transport) ->
    mnesis_opt:init_mnesis(),
    loop(#state{socket=Socket, peername=Peername, transport=Transport}).

loop(State = #state{socket=Socket, transport=Transport}) ->
    receive
        {tcp, Socket, Data} ->
            {Num, Cmd, Param} = mnesis_parser:parse_data(Data),
            io:format("Recive command from ~p: ~p ~p ~p~n", [State#state.peername, Cmd, Num, Param]),
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
    try
        Func = binary_to_existing_atom(Cmd, latin1),
        case erlang:function_exported(mnesis_operation, Func, 3) of
            false ->
                {mnesis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>),
                    State#state{error=true}};
            true ->
                case do_transaction(Func, Num, Param, State) of
                    {ok, old} ->
                        {mnesis_operation:Func(Database, Num, Param), State};
                    {ok, new, Reply, NewState} ->
                        {Reply, NewState}
                end
        end
    catch
        _Error:_Code ->
            {mnesis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>),
                State#state{error=true}}
    end.

%% check transaction operation
%% todo:
%% 1.Check command before queued
%% 2.Check watch list (need it ? ) after finish mnesis_operation.erl
%% 3.Finish exec command
do_transaction(Cmd, _Num, Param, State=#state{trans=false, database=Database}) ->
    case Cmd of
        multi ->
            mnesis_server:multi_start(self()),
            {ok, new, mnesis_parser:reply_status(<<"OK">>), State#state{trans=true}};
        watch ->
            mnesis_server:insert_watch(self(), Database, Param),
            {ok, new, mnesis_parser:reply_status(<<"OK">>), State};
        unwatch ->
            mnesis_server:discard_watch(self()),
            {ok, new, mnesis_parser:reply_status(<<"OK">>), State};
        exec ->
            {ok, new, mnesis_parser:reply_error(<<"EXEC without MULTI">>), State};
        discard ->
            {ok, new, mnesis_parser:reply_error(<<"DISCARD without MULTI">>), State};
        _ ->
            {ok, old}
    end;
do_transaction(Cmd, Num, Param, State=#state{trans=_, optlist=OptList, database=Database}) ->
    case Cmd of
        multi ->
            {ok, new, mnesis_parser:reply_error(<<"MULTI calls can not be nested">>), State#state{error=true}};
        watch ->
            {ok, new, mnesis_parser:reply_error(<<"WATCH inside MULTI is not allowed">>), State#state{error=true}};
        unwatch ->
            mnesis_server:discard_watch(self()),
            {ok, new, mnesis_parser:reply_status(<<"OK">>), State};
        exec ->
            mnesis_opt:enter_trans(),
            Reply =
                case mnesia:transaction(
                    fun () ->
                        clean = mnesis_server:check_watch(self()),
                        [mnesis_operation:C(Database, N, P) ||
                            {C, N, P} <- lists:reverse(OptList)]
                    end)
                of
                    {atomic,Result} ->
                        mnesis_parser:reply_multi(Result);
                    _ ->
                        mnesis_parser:reply_error(<<"EXECABORT">>)
                end,
            mnesis_opt:leave_trans(),
            {ok, new, Reply, State#state{trans=false, optlist=[]}};
        discard ->
            mnesis_server:discard_watch(self()),
            {ok, new, mnesis_parser:reply_status(<<"OK">>), State#state{trans=false, optlist=[]}};
        Cmd ->
            {ok, new, mnesis_parser:reply_status(<<"QUEUED">>), State#state{optlist=[{Cmd, Num, Param}|OptList]}}
    end.

%% empty
multi(_A, _B, _C) ->
    ok.
watch(_A, _B, _C) ->
    ok.
unwatch(_A, _B, _C) ->
    ok.
exec(_A, _B, _C) ->
    ok.
discard(_A, _B, _C) ->
    ok.

%%
%% key
%%

%% exists
exists(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, _Value}] ->
                mnesis_parser:reply_integer(1);
            _ ->
                mnesis_parser:reply_integer(0)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'exists' command">>)
    end;
exists(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'exists' command">>).

%% expire
expire(Database, 2, [Key, Sec]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, _Value}] ->
                mnesis_parser:reply_integer(
                    mnesis_help:add_key_expire(Database, Key, Sec)
                );
            _ ->
                mnesis_parser:reply_integer(0)
        end
    catch
        _:_  ->
            mnesis_parser:reply_integer(0)
    end;
expire(_, _, _) ->
    mnesis_parser:reply_integer(0).

%% ttl
ttl(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, _Value}] ->
                mnesis_parser:reply_integer(
                    mnesis_help:get_expire_time(Database, Key)
                );
            _ ->
                mnesis_parser:reply_integer(-2)
        end
    catch
        _:_  ->
            mnesis_parser:reply_integer(-2)
    end;
ttl(_, _, _) ->
    mnesis_parser:reply_integer(-2).

%% persist
persist(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, _Value}] ->
                mnesis_parser:reply_integer(
                    mnesis_help:del_expire_time(Database, Key)
                );
            _ ->
                mnesis_parser:reply_integer(0)
        end
    catch
        _:_  ->
            mnesis_parser:reply_integer(0)
    end;
persist(_, _, _) ->
    mnesis_parser:reply_integer(0).

%% keys
keys(Database, 1, [Pattern]) ->
    try
        mnesis_parser:reply_multi(
            [mnesis_parser:reply_single(K) ||
                K <- mnesis_opt:all_keys(Database), mnesis_help:pattern_match(K, Pattern)]
        )
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'keys' command">>)
    end;
keys(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'keys' command">>).

%% del
del(Database, N, Keys) ->
    del_int(Database, N, Keys, 0).

del_int(_Database, 0, [], N) ->
    mnesis_parser:reply_integer(N);
del_int(Database, N, [Key|Left], Num) ->
    case mnesis_opt:read({Database, Key}) of
        [{Database, Key, _Value}] ->
            mnesis_server:clear(Database, Key),
            mnesis_opt:delete({Database, Key}),
            del_int(Database, N-1, Left, Num+1);
        _ ->
            del_int(Database, N-1, Left, Num)
    end.

%%
%% string
%%

%% get
get(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Value}] when is_binary(Value)->
                mnesis_parser:reply_single(Value);
            [] ->
                mnesis_parser:reply_single(<<>>);
            _ ->
                mnesis_parser:reply_error(<<"ERR Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'get' command">>)
    end;
get(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'get' command">>).

%% set
%% todo: expire
set(Database, 2, [Key, Value]) ->
    try
        case mnesis_opt:write({Database, Key, Value}) of
            ok ->
                mnesis_parser:reply_status(<<"OK">>);
            _ ->
                mnesis_parser:reply_error(<<"ERR syntax error">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR syntax error">>)
    end;
set(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR syntax error">>).

%% incr
incr(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Value}] ->
                NewValue = mnesis_help:number_to_binary(
                    mnesis_help:binary_to_number(Value) + 1
                ),
                mnesis_opt:write({Database, Key, NewValue}),
                mnesis_parser:reply_single(NewValue);
            _ ->
                mnesis_parser:reply_error(<<"ERR Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'incr' command">>)
    end;
incr(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'incr' command">>).

%% incrby
incrby(Database, 2, [Key, Incr]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Value}] ->
                NewValue = mnesis_help:number_to_binary(
                    mnesis_help:binary_to_number(Value) +
                    mnesis_help:binary_to_number(Incr)
                ),
                mnesis_opt:write({Database, Key, NewValue}),
                mnesis_parser:reply_single(NewValue);
            _ ->
                mnesis_parser:reply_error(<<"ERR Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'incr' command">>)
    end;
incrby(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'incr' command">>).

%%
%% list
%%

%% rpush
rpush(Database, N, [Key | Left]) when N > 1 ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, [M|Value]}] when is_integer(M) ->
                mnesis_opt:write({Database, Key, [N-1+M|mnesis_help:join_list(Value,Left)]}),
                mnesis_parser:reply_integer(N-1+M);
            [] ->
                mnesis_opt:write({Database, Key, [N-1|Left]}),
                mnesis_parser:reply_integer(N-1);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
rpush(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'rpush' command">>).

%% lpop
%% todo: maybe more check
lpop(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, [1 , H]}] ->
                mnesis_opt:delete({Database, Key}),
                mnesis_parser:reply_single(H);
            [{Database, Key, [M, H | Value]}] when is_integer(M)->
                mnesis_opt:write({Database, Key, [M-1 | Value]}),
                mnesis_parser:reply_single(H);
            _ ->
                mnesis_parser:reply_single(<<>>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lpop(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'rpop' command">>).

%% lindex
lindex(Database, 2, [Key, SIndex]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, [M | List]}] when is_integer(M) ->
                Index = mnesis_help:calc_index(SIndex, M),
                mnesis_parser:reply_single(lists:nth(Index, List));
            _ ->
                mnesis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lindex(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'lindex' command">>).

%% lrange
lrange(Database, 3, [Key, Start, End]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, [M | List]}] when is_integer(M) ->
                S = mnesis_help:calc_index(Start, M),
                T = mnesis_help:calc_index(End, M),
                if
                    S =< T ->
                        mnesis_parser:reply_multi(
                            [mnesis_parser:reply_single(K) || K <- lists:sublist(List, S, T-S+1)]
                        );
                    true ->
                        mnesis_parser:reply_single(<<>>)
                end;
            _ ->
                mnesis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
lrange(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'lrange' command">>).


%%
%% set
%%

%% sadd
sadd(Database, N, [Key|Value]) when N > 1->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Set}] ->
                {Number, NewSet} = mnesis_help:add_elements(Value, Set, 0),
                mnesis_opt:write({Database, Key, NewSet}),
                mnesis_parser:reply_integer(Number);
            [] ->
                {Number, NewSet} = mnesis_help:add_elements(Value, sets:new(), 0),
                mnesis_opt:write({Database, Key, NewSet}),
                mnesis_parser:reply_integer(Number);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
sadd(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'sadd' command">>).

%% SISMEMBER
sismember(Database, 2, [Key, Value]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Set}] ->
                case sets:is_element(Value, Set) of
                    true ->
                        mnesis_parser:reply_integer(1);
                    _ ->
                        mnesis_parser:reply_integer(0)
                end;
            [] ->
                mnesis_parser:reply_integer(0);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
sismember(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'sismember' command">>).

%% smembers
smembers(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Set}] ->
                mnesis_parser:reply_multi(
                    [mnesis_parser:reply_single(K) || K <- sets:to_list(Set)]
                );
            [] ->
                mnesis_parser:reply_multi([], 0, <<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
smembers(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'smembers' command">>).

%% srem
srem(Database, N, [Key|Value]) when N > 1->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Set}] ->
                {Number, NewSet} = mnesis_help:remove_elements(Value, Set, 0),
                mnesis_opt:write({Database, Key, NewSet}),
                mnesis_parser:reply_integer(Number);
            [] ->
                mnesis_parser:reply_integer(0);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
srem(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'srem' command">>).


%%
%% hash map
%%

%% hset
hset(Database, 3, [Key, K, V]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                N = case maps:is_key(K, Map) of
                        true ->
                            0;
                        _ ->
                            1
                    end,
                mnesis_opt:write({Database, Key, maps:put(K, V, Map)}),
                mnesis_parser:reply_integer(N);
            [] ->
                mnesis_opt:write({Database, Key, maps:put(K, V, maps:new())}),
                mnesis_parser:reply_integer(1);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hset(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hset' command">>).

%% hmset
hmset(Database, N, [Key, K | KV]) when N rem 2 == 1 ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                mnesis_opt:write({Database, Key, mnesis_help:add_hash([K | KV], Map)}),
                mnesis_parser:reply_status(<<"OK">>);
            [] ->
                mnesis_opt:write({Database, Key, mnesis_help:add_hash([K | KV], maps:new())}),
                mnesis_parser:reply_status(<<"OK">>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hmset(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hmset' command">>).

%% hget
hget(Database, 2, [Key, K]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                N = case maps:is_key(K, Map) of
                        true ->
                            maps:get(K, Map);
                        _ ->
                            <<>>
                    end,
                mnesis_parser:reply_single(N);
            [] ->
                mnesis_parser:reply_single(<<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hget(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hget' command">>).

%% hmget
hmget(Database, N, [Key | Ks]) when N > 1 ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                mnesis_parser:reply_multi(
                    lists:map(
                        fun (K) ->
                            mnesis_parser:reply_single(
                                case maps:is_key(K, Map) of
                                    true ->
                                        maps:get(K, Map);
                                    _ ->
                                        <<>>
                                end)
                        end,
                        Ks)
                );
            [] ->
                mnesis_parser:reply_single(<<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hmget(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hmget' command">>).


%% hincrby
hincrby(Database, 3, [Key, K, Incr]) ->
    try
        Increment = binary_to_integer(Incr),
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                NewValue = case maps:is_key(K, Map) of
                               true ->
                                   integer_to_binary(
                                       binary_to_integer(maps:get(K, Map)) + Increment
                                   );
                               _ ->
                                   Incr
                           end,
                mnesis_opt:write({Database, Key, maps:put(K, NewValue, Map)}),
                mnesis_parser:reply_single(NewValue);
            [] ->
                mnesis_opt:write({Database, Key, maps:put(K, Incr, maps:new())}),
                mnesis_parser:reply_single(Incr);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hincrby(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hincrby' command">>).

%% hexists
hexists(Database, 2, [Key, K]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                case maps:is_key(K, Map) of
                    true ->
                        mnesis_parser:reply_integer(1);
                    _ ->
                        mnesis_parser:reply_integer(0)
                end;
            [] ->
                mnesis_parser:reply_integer(0);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hexists(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hexists' command">>).

%% hgetall
hgetall(Database, 1, [Key]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                mnesis_parser:reply_multi(
                    lists:flatten(
                        [[mnesis_parser:reply_single(K), mnesis_parser:reply_single(V)] ||
                            {K, V} <- maps:to_list(Map)]));
            [] ->
                mnesis_parser:reply_multi([], 0, <<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hgetall(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hgetall' command">>).

%% hdel
hdel(Database, N, [Key|Value]) when N > 1->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, Map}] ->
                {Number, NewMap} = mnesis_help:remove_hash(Value, Map, 0),
                mnesis_opt:write({Database, Key, NewMap}),
                mnesis_parser:reply_integer(Number);
            [] ->
                mnesis_parser:reply_integer(0);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
hdel(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'hdel' command">>).

%%
%% orderdset
%%

%% zadd
zadd(Database, N, [Key | Value]) when N rem 2 == 1 ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {Set, Dict}}] ->
                {Number, NewSet, NewDict} = mnesis_help:add_orddict(Value, Set, Dict, 0),
                mnesis_opt:write({Database, Key, {NewSet, NewDict}}),
                mnesis_parser:reply_integer(Number);
            [] ->
                {Number, NewSet, NewDict} = mnesis_help:add_orddict(Value, ordsets:new(), dict:new(), 0),
                mnesis_opt:write({Database, Key, {NewSet, NewDict}}),
                mnesis_parser:reply_integer(Number);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
zadd(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zadd' command">>).

%% zrem
zrem(Database, N, [Key | Value]) when N > 1 ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {Set, Dict}}] ->
                {Number, NewSet, NewDict} = mnesis_help:del_orddict(Value, Set, Dict, 0),
                mnesis_opt:write({Database, Key, {NewSet, NewDict}}),
                mnesis_parser:reply_integer(Number);
            [] ->
                mnesis_parser:reply_integer(0);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
zrem(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrem' command">>).

%% zscore
zscore(Database, 2, [Key , DictKey]) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {_Set, Dict}}] ->
                Score = case dict:find(DictKey, Dict) of
                            {ok, Value} ->
                                Value;
                            _ ->
                                <<>>
                        end,
                mnesis_parser:reply_single(Score);
            [] ->
                mnesis_parser:reply_single(<<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
zscore(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zscore' command">>).

%% zincrby
zincrby(Database, 3, [Key, Incr, DictKey]) ->
    try
        Increment = mnesis_help:binary_to_number(Incr),
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {Set, Dict}}] ->
                Score = case dict:find(DictKey, Dict) of
                            {ok, Value} ->
                                OldScore = mnesis_help:binary_to_number(Value),
                                NewScore = OldScore + Increment,
                                NewValue = mnesis_help:number_to_binary(NewScore),
                                mnesis_opt:write({
                                    Database, Key,
                                    {ordsets:add_element({NewScore, NewValue, DictKey},
                                        ordsets:del_element({OldScore, Value, DictKey}, Set)),
                                     dict:store(DictKey, NewValue, Dict)}
                                }),
                                NewValue;
                            _ ->
                                mnesis_opt:write({
                                    Database, Key,
                                    {ordsets:add_element({Increment, Incr, DictKey}, Set),
                                    dict:store(DictKey, Incr, Dict)}
                                }),
                                Incr
                        end,
                mnesis_parser:reply_single(Score);
            [] ->
                mnesis_parser:reply_single(<<>>);
            _ ->
                mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end;
zincrby(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zincrby' command">>).

%% zrange
zrange(Database, 3, [Key, Start, End]) ->
    zrange_int(Database, Key, Start, End, 0, fun(X) -> X end);
zrange(Database, 4, [Key, Start, End, WithScore]) ->
    case mnesis_help:lower_binary(WithScore) of
        <<"withscores">> ->
            zrange_int(Database, Key, Start, End, 1, fun(X) -> X end);
        _ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrange' command">>)
    end;
zrange(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrange' command">>).

%% zrevrange
zrevrange(Database, 3, [Key, Start, End]) ->
    zrange_int(Database, Key, Start, End, 0, fun(X) -> lists:reverse(X) end);
zrevrange(Database, 4, [Key, Start, End, WithScore]) ->
    case mnesis_help:lower_binary(WithScore) of
        <<"withscores">> ->
            zrange_int(Database, Key, Start, End, 1, fun(X) -> lists:reverse(X) end);
        _ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrevrange' command">>)
    end;
zrevrange(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrevrange' command">>).

zrange_int(Database, Key, Start, End, Withscore, Fun) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {Set, _Dict}}] ->
                M = ordsets:size(Set),
                S = mnesis_help:calc_index(Start, M),
                T = mnesis_help:calc_index(End, M),
                if
                    S > T ->
                        mnesis_parser:reply_single(<<>>);
                    Withscore == 1 ->
                        mnesis_parser:reply_multi(
                            lists:flatten(
                                Fun([[mnesis_parser:reply_single(DictKey), mnesis_parser:reply_single(Score)] ||
                                    {_, Score, DictKey} <- lists:sublist(ordsets:to_list(Set), S, T-S+1)]))
                        );
                    true ->
                        mnesis_parser:reply_multi(
                            Fun([mnesis_parser:reply_single(DictKey) ||
                                {_, _Score, DictKey} <- lists:sublist(ordsets:to_list(Set), S, T-S+1)])
                        )
                end;
            _ ->
                mnesis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end.

%% zrangebyscore
zrangebyscore(Database, 3, [Key, Start, End]) ->
    zrangebyscore_int(Database, Key, Start, End, 0, fun(X) -> X end);
zrangebyscore(Database, 4, [Key, Start, End, WithScore]) ->
    case mnesis_help:lower_binary(WithScore) of
        <<"withscores">> ->
            zrangebyscore_int(Database, Key, Start, End, 1, fun(X) -> X end);
        _ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrangebyscore' command">>)
    end;
zrangebyscore(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrangebyscore' command">>).

%% zrevrangebyscore
zrevrangebyscore(Database, 3, [Key, Start, End]) ->
    zrangebyscore_int(Database, Key, Start, End, 0, fun(X) -> lists:reverse(X) end);
zrevrangebyscore(Database, 4, [Key, Start, End, WithScore]) ->
    case mnesis_help:lower_binary(WithScore) of
        <<"withscores">> ->
            zrangebyscore_int(Database, Key, Start, End, 1, fun(X) -> lists:reverse(X) end);
        _ ->
            mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrevrangebyscore' command">>)
    end;
zrevrangebyscore(_, _, _) ->
    mnesis_parser:reply_error(<<"ERR wrong number of arguments for 'zrevrangebyscore' command">>).

zrangebyscore_int(Database, Key, Start, End, Withscore, Fun) ->
    try
        case mnesis_opt:read({Database, Key}) of
            [{Database, Key, {Set, _Dict}}] ->
                M = ordsets:size(Set),
                List = ordsets:to_list(Set),
                S = mnesis_help:list_find_low(List, mnesis_help:binary_to_number(Start), 1, M),
                T = mnesis_help:list_find_high(List, mnesis_help:binary_to_number(End), 1, M),
                if
                    S > T ->
                        mnesis_parser:reply_single(<<>>);
                    S > M ->
                        mnesis_parser:reply_single(<<>>);
                    T == 0 ->
                        mnesis_parser:reply_single(<<>>);
                    Withscore == 1 ->
                        mnesis_parser:reply_multi(
                            lists:flatten(
                                Fun([[mnesis_parser:reply_single(DictKey), mnesis_parser:reply_single(Score)] ||
                                    {_, Score, DictKey} <- lists:sublist(List, S, T-S+1)]))
                        );
                    true ->
                        mnesis_parser:reply_multi(
                            Fun([mnesis_parser:reply_single(DictKey) ||
                                {_, _Score, DictKey} <- lists:sublist(List, S, T-S+1)])
                        )
                end;
            _ ->
                mnesis_parser:reply_single(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
        end
    catch
        _:_ ->
            mnesis_parser:reply_error(<<"WRONGTYPE Operation against a key holding the wrong kind of value">>)
    end.
