-module(redis_help).

%% API
-export([lower_binary/1, find_number/2, join_list/2, calc_index/2]).

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
find_number(<<Num/integer, Data/binary>>, Now) when Num > $0 andalso Num =< $9 ->
    find_number(Data, Now*10+Num-$0);
find_number(<<$\r, $\n, Data/binary>>, Now) ->
    {Now, Data}.


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
