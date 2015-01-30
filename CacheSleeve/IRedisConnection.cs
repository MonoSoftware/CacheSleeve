using StackExchange.Redis;
using System;

namespace CacheSleeve
{
    public interface IRedisConnection
    {
        ConnectionMultiplexer Connection { get; }
        int RedisDb { get; }
    }
}
