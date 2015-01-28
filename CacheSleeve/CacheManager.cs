using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using CacheSleeve.Models;
using CacheSleeve.Utilities;
using StackExchange.Redis;
using Encoding = System.Text.Encoding;

namespace CacheSleeve
{
    public sealed class CacheManager : ICacheManager
    {
        private bool _setup;
        private ConnectionMultiplexer _redisConnection;

        #region Singleton Setup

        private CacheManager()
        {
        }

        public static CacheManager Settings
        {
            get { return Nested.Settings; }
        }

        private class Nested
        {
            static Nested()
            {
            }

            internal static readonly CacheManager Settings = new CacheManager();
        }

        #endregion

        public static void Init(string redisHost, int redisPort = 6379, string redisPassword = null, int redisDb = 0, string keyPrefix = "cs.")
        {
            var configuration =
                ConfigurationOptions.Parse(string.Format("{0}:{1}", redisHost, redisPort));
            configuration.AllowAdmin = true;
            
            Init(configuration, redisDb, keyPrefix);
        }

        public static void Init(ConfigurationOptions config, int redisDb = 0, string keyPrefix = "cs.")
        {
            PopulateSettings(config, redisDb, keyPrefix);
            
            Settings._redisConnection = ConnectionMultiplexer.Connect(config);

            // Setup pub/sub for cache syncing
            var subscriber = Settings._redisConnection.GetSubscriber();
            subscriber.Subscribe("cacheSleeve.remove.*", (redisChannel, value) => Settings.LocalCacher.Remove(GetString(value)));
            subscriber.Subscribe("cacheSleeve.flush*", (redisChannel, value) => Settings.LocalCacher.FlushAll());
        }

        public static Task InitAsync(string redisHost, int redisPort = 6379, string redisPassword = null, int redisDb = 0, string keyPrefix = "cs.")
        {
            var configuration =
                ConfigurationOptions.Parse(string.Format("{0}:{1}", redisHost, redisPort));
            configuration.AllowAdmin = true;
            
            return InitAsync(configuration, redisDb, keyPrefix);
        }

        public async static Task InitAsync(ConfigurationOptions config, int redisDb = 0, string keyPrefix = "cs.")
        {
            PopulateSettings(config, redisDb, keyPrefix);

            Settings._redisConnection = ConnectionMultiplexer.Connect(config);

            // Setup pub/sub for cache syncing
            var subscriber = Settings._redisConnection.GetSubscriber();
            var removeSubscription = subscriber.SubscribeAsync("cacheSleeve.remove.*", (redisChannel, value) => Settings.LocalCacher.Remove(GetString(value)));
            var flushSubscription = subscriber.SubscribeAsync("cacheSleeve.flush*", (redisChannel, value) => Settings.LocalCacher.FlushAll());
            await Task.WhenAll(removeSubscription, flushSubscription);
        }

        private static void PopulateSettings(ConfigurationOptions config, int redisDb = 0, string keyPrefix = "cs.")
        {
            if (Settings._setup)
                if (!UnitTestDetector.IsRunningFromXunit) throw new InvalidOperationException("Cannot reinitialize CacheSleeve");
            Settings._setup = true;

            Settings.RedisConfiguration = config;
            Settings.KeyPrefix = keyPrefix;
            Settings.RedisDb = redisDb;

            Settings.RemoteCacher = new RedisCacher();
            Settings.LocalCacher = new HttpContextCacher();
        }

        /// <summary>
        /// If true then logging is enabled
        /// </summary>
        public bool Debug { get; set; }

        /// <summary>
        /// The out of band caching service used as a backplane to share cache across servers.
        /// </summary>
        public RedisCacher RemoteCacher { get; private set; }

        /// <summary>
        /// The local in-memory cache.
        /// </summary>
        public HttpContextCacher LocalCacher { get; private set; }
        
        /// <summary>
        /// The prefix added to keys of items cached by CacheSleeve to prevent collisions.
        /// </summary>
        public string KeyPrefix { get; private set; }

        /// <summary>
        /// Redis connection configuration.
        /// </summary>
        public ConfigurationOptions RedisConfiguration { get; private set; }

        /// <summary>
        /// The database to use on the Redis server.
        /// </summary>
        public int RedisDb { get; private set; }

        public IDatabase GetDatebase()
        {
            return _redisConnection.GetDatabase(RedisDb);
        }
        
        public IEnumerable<RedisKey> GetAllKeys(string pattern = null)
        {
            var keys = new List<RedisKey>();
            foreach (var endpoint in _redisConnection.GetEndPoints())
            {
                var server = _redisConnection.GetServer(endpoint);
                if (!server.IsSlave)
                {
                    keys.AddRange(server.Keys(database: Settings.RedisDb, pattern: pattern != null ? Settings.AddPrefix(pattern) : Settings.AddPrefix("*")));
                }
            }
            
            return keys;
        }

        /// <summary>
        /// Adds the prefix to the key.
        /// </summary>
        /// <param name="key">The specified key value.</param>
        /// <returns>The specified key with the prefix attached.</returns>
        public string AddPrefix(string key)
        {
            return string.Format("{0}{1}", KeyPrefix, key);
        }

        /// <summary>
        /// Converts a byte[] to a string.
        /// </summary>
        /// <param name="bytes">The bytes to convert.</param>
        /// <returns>The resulting string.</returns>
        private static string GetString(byte[] bytes)
        {
            var buffer = Encoding.Convert(Encoding.GetEncoding("iso-8859-1"), Encoding.UTF8, bytes);
            return Encoding.UTF8.GetString(buffer, 0, bytes.Count());
        }
    }
}