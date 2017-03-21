-ifndef(REDIS_OPERATION_HRL).
-define(REDIS_OPERATION_HRL, 1).

%% operation
-define(GET, get).
-define(SET, set).
-define(DEL, del).
-define(RPUSH, rpush).
-define(LPOP, lpop).
-define(LINDEX, lindex).
-define(LRANGE, lrange).
-define(SADD, sadd).
-define(SISMEMBER, sismember).
-define(SMEMBERS, smembers).
-define(SREM, srem).
-define(HGET, hget).
-define(HSET, hset).
-define(HINCRBY, hincrby).
-define(HEXISTS, hexists).
-define(HGETALL, hgetall).
-define(HDELM, hdel).
-define(ZADD, zadd).
-define(ZREM, zrem).
-define(ZSCORE, zscore).
-define(ZINCRBY, zincrby).
-define(ZRANGE, zrange).
-define(ZRANGEBYSCORE, zrangebyscore).

-endif.
