#!/usr/bin/python

import redis


class RedisOpt:
    def __init__(self, h, p):
        self.conn = redis.Redis(host=h, port=p)

    def set(self, key, value):
        v = self.conn.set(key, value)
        print 'set ' + str(v)

    def get(self, key):
        v = self.conn.get(key)
        print 'get ' + str(key) + ' ' + str(v)

    def delete(self, key):
        v = self.conn.delete(key)
        print 'delete ' + str(v)

if __name__ == '__main__':
    c = RedisOpt('127.0.0.1', 9527)
    c.set('a', 'b')
    c.get('a')
    c.delete('a')
    c.get('a')
