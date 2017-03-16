-module(redis_parser).

-include("redis_operation.hrl").

-export([
    parse_data/1,
    reply_error/1,
    reply_integer/1,
    reply_status/1,
    reply_single/1,
    reply_multi/1,
    lower_binary/1]
).

%% parse data
%% todo: support telnet data, and other data
%%       parameters longer than 10
-spec parse_data(Data::binary()) ->
    {Cmd::binary(), Num::integer(), Param::[binary()]}.
parse_data(<<$*, Num/integer, Data/binary>>) when Num > $0 andalso Num =< $9 ->
    {Number, Binary} = find_number(Data, Num-$0),
    [Cmd|Param] = parse_param(Binary, Number, []),
    {Number - 1, lower_binary(Cmd), Param}.

-spec parse_param(Data::binary(), Count::integer(), Result::[binary()]) ->
    Result::[binary()].
parse_param(<<>>, 0, Result) ->
    lists:reverse(Result);
parse_param(<<$$, Num/integer, Data/binary>>, Count, Result) when Num > $0 andalso Num =< $9 ->
    {Number, Binary} = find_number(Data, Num-$0),
    <<Param:Number/binary, $\r, $\n, Left/binary>> = Binary,
    parse_param(Left, Count-1, [Param | Result]).


%% Reply

reply_status(Status) when is_binary(Status) ->
    <<$+, Status/binary, $\r, $\n>>.

reply_error(Error) when is_binary(Error) ->
    <<$-, Error/binary, $\r, $\n>>.

reply_integer(Number) when is_integer(Number) ->
    Bin = integer_to_binary(Number),
    <<$:, Bin/binary, $\r, $\n>>.

reply_single(<<>>) ->
    <<"$-1\r\n">>;
reply_single(Data) when is_binary(Data) ->
    Num = integer_to_binary(byte_size(Data)),
    <<$$, Num/binary, $\r, $\n, Data/binary, $\r, $\n>>.

reply_multi(List) ->
    reply_multi(List, 0, <<>>).


%% internal implement

reply_multi([], Number, Result) ->
    Num = integer_to_binary(Number),
    <<
        $*,
        Num/binary,
        $\r, $\n,
        Result/binary
    >>;
reply_multi([H|T], Count, Result) ->
    reply_multi(
        T,
        Count+1,
        <<
            Result/binary,
            H/binary
        >>
    ).

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
