-module(redis_parser).

-include("redis_operation.hrl").

-record(redis_param, {
    cmd :: string(),        %% command
    num :: integer(),       %% param number
    param :: [binary()]     %% parameters
}).

-compile(export_all).

%% API
-export([execute/1]).

%% execute
-spec execute(Data::binary()) ->
    Reply::binary().
execute(Data) when is_binary(Data) ->
    #redis_param{cmd = Cmd, num = _Num, param = Param} = parse_data(Data),
    try binary_to_existing_atom(Cmd, latin1) of
        Func ->
            case erlang:function_exported(redis_operation, Func, 1) of
                false ->
                    reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>);
                true ->
                    redis_operation:Func(Param)
            end
    catch
        _Error:_Code ->
            reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>)
    end.


%% parse data
-spec parse_data(Data::binary()) ->
    Param::#redis_param{}.
parse_data(<<$*, Num/integer, $\r, $\n, Data/binary>>) when Num > $0, Num =< $9 ->
    [Cmd|Param] = parse_param(Data, Num - $0, []),
    #redis_param{num = Num - $1, cmd = Cmd, param = Param}.

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
    Size = byte_size(H) + $0,
    reply_multi(
        T,
        Count+1,
        <<
            Result/binary,
            $$,
            Size/integer,
            $\r, $\n,
            H/binary,
            $\r, $\n
        >>
    ).
