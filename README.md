# mnesia_redis

兼容Redis接口，后端用mnesia数据库。


## 使用方式


### On MacOS:

```
brew install erlang git homebrew/dupes/make
git clone https://github.com/0xfei/mnesia_redis
gmake
gmake run
```


### Client:

```
user@ubuntu:~/$ redis-cli -p 9527
127.0.0.1:9527> set hello world
OK
127.0.0.1:9527> get hello
"world"
127.0.0.1:9527> del hello
OK
127.0.0.1:9527> get hello
(nil)
127.0.0.1:9527> rpush list-key item
(integer) 1
127.0.0.1:9527> rpush list-key item2
(integer) 2
127.0.0.1:9527> rpush list-key item3
(integer) 3
127.0.0.1:9527> rpush list-key item
(integer) 4
127.0.0.1:9527> lrange list-key 0 -1
1) "item"
2) "item2"
3) "item3"
4) "item"
127.0.0.1:9527> lpop list-key
"item"
127.0.0.1:9527> lpop list-key
"item2"
127.0.0.1:9527> lrange list-key 0 -1
1) "item3"
2) "item"
127.0.0.1:9527> sadd set-key item
(integer) 1
127.0.0.1:9527> sadd set-key item2
(integer) 1
127.0.0.1:9527> sadd set-key item
(integer) 0
127.0.0.1:9527> smembers set-key
1) "item2"
2) "item"
127.0.0.1:9527> sismember set-key item
(integer) 1
127.0.0.1:9527> sismember set-key item4
(integer) 0
127.0.0.1:9527> srem set-key item
(integer) 1
127.0.0.1:9527> smembers set-key
1) "item2"
127.0.0.1:9527> hset hash-key sub-key1 value1
(integer) 1
127.0.0.1:9527> hset hash-key sub-key2 value2
(integer) 1
127.0.0.1:9527> hset hash-key sub-key1 value3
(integer) 0
127.0.0.1:9527> hgetall hash-key
1) "sub-key1"
2) "value3"
3) "sub-key2"
4) "value2"
127.0.0.1:9527> hdel hash-key sub-key2 
(integer) 1
127.0.0.1:9527> hgetkey hash-key sub-key2
(error) ERR unknown command 'hgetkey' 
127.0.0.1:9527> hget hash-key sub-key2
(nil)
127.0.0.1:9527> hget hash-key sub-key1
"value3"
127.0.0.1:9527> zadd zset-key 728 member1
(integer) 1
127.0.0.1:9527> zadd zset-key 982 member0
(integer) 1
127.0.0.1:9527> zadd zset-key 985 member0
(integer) 0
127.0.0.1:9527> zrange zset-key 0 -1 withscores
1) "member1"
2) "728"
3) "member0"
4) "985"
127.0.0.1:9527> zrangebyscore zset-key 0 800 withscores
1) "member1"
2) "728"
127.0.0.1:9527> zrem zset-key member1
(integer) 1
127.0.0.1:9527> zrem zset-key member1
(integer) 0
127.0.0.1:9527> zrange zset-key 0 -1 withscores
1) "member0"
2) "985"
127.0.0.1:9527> 

```
