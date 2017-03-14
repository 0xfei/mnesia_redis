-module(redis_operation).

%% API
-export([get/1]).

get(_List) ->
    io:format("get called~n").
