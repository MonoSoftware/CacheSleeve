using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CacheSleeve
{
    public class RedisConnection : CacheSleeve.IRedisConnection
    {
        private RedisConnection()
        {

        }

        public ConnectionMultiplexer Connection { get; private set; }

        public int RedisDb { get; private set; }

        public static RedisConnection Create(string redisHost, int redisPort = 6379, string redisPassword = null, int redisDb = 0)
        {
            var configuration =
                ConfigurationOptions.Parse(string.Format("{0}:{1}", redisHost, redisPort));
            configuration.AllowAdmin = true;

            return Create(configuration, redisDb);
        }

        public static RedisConnection Create(ConfigurationOptions config, int redisDb = 0)
        {
            var conn = new RedisConnection();
            conn.Connection = ConnectionMultiplexer.Connect(config);
            
            conn.RedisDb = redisDb;

            return conn;
        }
    }
}
