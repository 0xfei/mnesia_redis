-module(redis_operation).

%% API
-export([get/1]).

get(_List) ->
    redis_parser:reply_multi([<<"test">>, <<"abc">>, <<"def">>]).
