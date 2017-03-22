-module(redis_help).

%% API
-export([
    lower_binary/1, find_number/2, pattern_match/2,
    binary_to_number/1, number_to_binary/1,
    join_list/2, calc_index/2,
    add_elements/3, remove_elements/3,
    add_hash/2, remove_hash/3,
    add_orddict/4, del_orddict/4,
    list_find_low/4, list_find_high/4,
    add_key_expire/3, get_expire_time/2, del_expire_time/2]
).

-define(EXPIRE_KEY, expire).
-define(LIMIT_MAX, 999999).

%% <<"ABC">> -> <<"abc">>
-spec lower_binary(Data::binary()) -> New::binary().
lower_binary(Data) ->
    lower_binary(Data, <<>>).

lower_binary(<<>>, Binary) ->
    Binary;
lower_binary(<<H:8, Left/binary>>, Binary) when H >= $A andalso H =< $Z ->
    T = H - $A + $a,
    lower_binary(Left, <<Binary/binary, T:8>>);
lower_binary(<<H:8, Left/binary>>, Binary) ->
    lower_binary(Left, <<Binary/binary, H:8>>).


%% find_number <<"123dfasdfasd">> -> {123, Binary}
-spec find_number(Data::binary(), Initialize::integer()) ->
    {Number::integer(), Bin::binary()}.
find_number(<<Num/integer, Data/binary>>, Now) when Num >= $0 andalso Num =< $9 ->
    find_number(Data, Now*10+Num-$0);
find_number(<<$\r, $\n, Data/binary>>, Now) ->
    {Now, Data}.

%% pattern_match , only 1 * now
-spec pattern_match(M::binary(), P::binary()) -> true | false.
pattern_match(M, P) ->
    int_match(M, P).

int_match(<<>>, <<>>) ->
    true;
int_match(_L1, <<$*>>) ->
    true;
int_match(L1, L2 = <<$*, _L2/binary>>) ->
    list_match(
        lists:reverse(binary_to_list(L1)),
        lists:reverse(binary_to_list(L2))
    );
int_match(<<_H:8, L1/binary>>, <<H:8, L2/binary>>) when H == $? ->
    int_match(L1, L2);
int_match(<<H:8, L1/binary>>, <<H:8, L2/binary>>) ->
    int_match(L1, L2);
int_match(_L1, _L2) ->
    false.

list_match([], []) ->
    true;
list_match([H|T], [H|T2]) ->
    list_match(T, T2);
list_match([_H|T], [H|T2]) when H == $? ->
    list_match(T, T2);
list_match(_L1, [H|_L2]) when H == $* ->
    true;
list_match(_, _) ->
    false.

%% join_list
-spec join_list(L1::list(), L2::list()) -> L3::list().
join_list(L1, L2) ->
    join_list_internal(lists:reverse(L1), L2).

join_list_internal([], L2) ->
    L2;
join_list_internal([H|L1], L2) ->
    join_list_internal(L1, [H|L2]).

%% calc_index
%% using with try
-spec calc_index(Offset::binary(), M::integer()) -> Index::integer().
calc_index(Offset, M) ->
    I = binary_to_integer(Offset),
    if
        I < 0 andalso I + M >= 0 ->
            M + I + 1;
        I >= 0 andalso I < M ->
            I + 1;
        true ->
            throw("Too long")
    end.

%% list_find_low
list_find_low(_List, _Value, S, E) when S > E ->
    S;
list_find_low(List, Value, S, E) ->
    Mid = (S + E + 1) div 2,
    {Score, _Bin, _Key} = lists:nth(Mid, List),
    if
        Score >= Value -> list_find_low(List, Value, S, Mid-1);
        true -> list_find_low(List, Value, Mid+1, E)
    end.

%% list_find_high
list_find_high(_List, _Value, S, E) when S > E ->
    E;
list_find_high(List, Value, S, E) ->
    Mid = (S + E + 1) div 2,
    {Score, _Bin, _Key} = lists:nth(Mid, List),
    if
        Score =< Value -> list_find_high(List, Value, Mid+1, E);
        true -> list_find_high(List, Value, S, Mid-1)
    end.

%% binary_to_number
-spec binary_to_number(B::binary()) -> integer() | float() .
binary_to_number(B) ->
    try
        binary_to_integer(B)
    catch _:_ ->
        binary_to_float(B)
    end.

%% number_to_binary
-spec number_to_binary(B::integer() | float()) -> binary().
number_to_binary(B) ->
    try
        integer_to_binary(B)
    catch _:_ ->
        float_to_binary(B)
    end.

%% insert sets
-spec add_elements(E::list(), Set::sets:set(), N::integer()) ->
    NSet::sets:set().
add_elements([], Set, N) ->
    {N, Set};
add_elements([H|T], Set, N) ->
    case sets:is_element(H, Set) of
        true ->
            add_elements(T, Set, N);
        _ ->
            add_elements(T, sets:add_element(H, Set), N+1)
    end.

%% remove sets
-spec remove_elements(E::list(), Set::sets:set(), N::integer()) ->
    NSet::sets:set().
remove_elements([], Set, N) ->
    {N, Set};
remove_elements([H|T], Set, N) ->
    case sets:is_element(H, Set) of
        true ->
            remove_elements(T, sets:del_element(H, Set), N+1);
        _ ->
            remove_elements(T, Set, N)
    end.

%% add multi hash
-spec add_hash(E::list(), Map::#{}) -> NMap::#{}.
add_hash([], Map) ->
    Map;
add_hash([K, V|Left], Map) ->
    add_hash(Left, maps:put(K, V, Map)).

%% remove hash
-spec remove_hash(E::list(), Set::#{}, N::integer()) ->
    NSet::#{}.
remove_hash([], Map, N) ->
    {N, Map};
remove_hash([H|T], Map, N) ->
    case maps:is_key(H, Map) of
        true ->
            remove_hash(T, maps:remove(H, Map), N+1);
        _ ->
            remove_hash(T, Map, N)
    end.

%% add orddict
-spec add_orddict(E::list(), Set::ordsets:set(), Dict::dict:dict(), N::integer()) ->
    {C::integer(), NewSet::ordsets:set(), NewDict::dict:dict()}.
add_orddict([], Set, Dict, N) ->
    {N, Set, Dict};
add_orddict([Score, Key|Left], Set, Dict, N) ->
    case dict:find(Key, Dict) of
        {ok, Value} ->
            add_orddict(
                Left,
                ordsets:add_element({binary_to_number(Score), Score, Key},
                    ordsets:del_element({binary_to_number(Value), Value, Key}, Set)),
                dict:store(Key, Score, Dict),
                N);
        _ ->
            add_orddict(
                Left,
                ordsets:add_element({binary_to_number(Score), Score, Key}, Set),
                dict:store(Key, Score, Dict),
                N+1)
    end.

%% del orddict
-spec del_orddict(E::list(), Set::ordsets:set(), Dict::dict:dict(), N::integer()) ->
    {C::integer(), NewSet::ordsets:set(), NewDict::dict:dict()}.
del_orddict([], Set, Dict, N) ->
    {N, Set, Dict};
del_orddict([Key|Left], Set, Dict, N) ->
    case dict:find(Key, Dict) of
        {ok, Value} ->
            del_orddict(
                Left,
                ordsets:del_element({binary_to_number(Value), Value, Key}, Set),
                dict:erase(Key, Dict),
                N+1);
        _ ->
            del_orddict(
                Left,
                Set,
                Dict,
                N)
    end.

%% expire key manage
-spec get_expire_time(Db::atom(), Key::binary()) -> integer().
get_expire_time(Database, Key) ->
    case mnesis:read({Database, ?EXPIRE_KEY}) of
        [{Database, ?EXPIRE_KEY, {_Set, Dict}}] ->
            case dict:find(Key, Dict) of
                {ok, ExpTime} ->
                    Now = erlang:system_time(1),
                    if
                        ExpTime =< Now ->
                            redis_expire:remove(Database, Key),
                            -2;
                        true ->
                            ExpTime - Now
                    end;
                _ ->
                    -1
            end;
        _ ->
            -1
    end.

-spec add_key_expire(Db::atom(), Key::binary(), Sec::binary()) -> 0 | 1.
add_key_expire(Database, Key, BinSec) ->
    Seconds = erlang:system_time(1) + binary_to_integer(BinSec),
    case mnesis:read({Database, ?EXPIRE_KEY}) of
        [{Database, ?EXPIRE_KEY, {_Set, _Dict}}] ->
            redis_expire:insert(Database, Key, Seconds),
            1;
        [] ->
            mnesis:write({Database, ?EXPIRE_KEY, {ordsets:new(), dict:new()}}),
            redis_expire:insert(Database, Key, Seconds),
            1;
        _ ->
            0
    end.

-spec del_expire_time(Db::atom(), Key::binary()) -> integer().
del_expire_time(Database, Key) ->
    case mnesis:read({Database, ?EXPIRE_KEY}) of
        [{Database, ?EXPIRE_KEY, {_Set, Dict}}] ->
            case dict:find(Key, Dict) of
                {ok, _ExpTime} ->
                    redis_expire:clear(Database, Key),
                    1;
                _ ->
                    0
            end;
        _ ->
            0
    end.
