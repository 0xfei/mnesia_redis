-module(redis_operation).

-include("redis_operation.hrl").

%% API
-export([get/1]).

get(_List) ->
    redis_parser:reply_single(<<"def">>).
