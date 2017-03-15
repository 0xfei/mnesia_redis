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
%% todo:
%% support telnet data, and other data
-spec parse_data(Data::binary()) ->
    {Cmd::binary(), Num::integer(), Param::[binary()]}.
parse_data(<<$*, Num/integer, $\r, $\n, Data/binary>>) when Num > $0, Num =< $9 ->
    [Cmd|Param] = parse_param(Data, Num - $0, []),
    {Num - $1, lower_binary(Cmd), Param}.

-spec parse_param(Data::binary(), Count::integer(), Result::[binary()]) ->
    Result::[binary()].
parse_param(<<>>, 0, Result) ->
    lists:reverse(Result);
parse_param(<<$$, Num/integer, $\r, $\n, Binary/binary>>, Count, Result) ->
    Number = Num - $0,
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
    Num = byte_size(Data) + $0,
    <<$$, Num/integer, $\r, $\n, Data/binary, $\r, $\n>>.

reply_multi(List) ->
    reply_multi(List, 0, <<>>).


%% internal implement

reply_multi([], Number, Result) ->
    Num = Number + $0,
    <<
        $*,
        Num/integer,
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
