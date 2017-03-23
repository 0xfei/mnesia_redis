-module(mnesis).

%% API
-export([init_mnesis/0, enter_trans/0, leave_trans/0]).
-export([read/1, write/1, delete/1, all_keys/1]).

init_mnesis() ->
    leave_trans().

enter_trans() ->
    put(read, read),
    put(write, write),
    put(delete, delete),
    put(all_keys, all_keys).

leave_trans() ->
    put(read, dirty_read),
    put(write, dirty_write),
    put(delete, dirty_delete),
    put(all_keys, dirty_all_keys).

read(K) ->
    F = get(read),
    mnesia:F(K).

write(K = {Db, Key, _Value}) ->
    mnesis_server:write_watch(Db,Key),
    F = get(write),
    mnesia:F(K);
write(K) ->
    F = get(write),
    mnesia:F(K).

delete(K = {Db, Key}) ->
    mnesis_server:write_watch(Db,Key),
    F = get(delete),
    mnesia:F(K);
delete(K) ->
    F = get(delete),
    mnesia:F(K).

all_keys(K) ->
    F = get(all_keys),
    mnesia:F(K).
