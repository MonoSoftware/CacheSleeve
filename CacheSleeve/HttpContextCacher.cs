using CacheSleeve.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Web.Caching;

namespace CacheSleeve
{
    public class HttpContextCacher : ICacher
    {
        #region Fields

        private readonly Cache _cache;
        private readonly ICacheLogger _logger;

        #endregion Fields

        #region Constructors

        public HttpContextCacher(
            ICacheLogger logger
            )
        {
            _logger = logger;
            _cache = System.Web.HttpContext.Current.Cache;
        }

        #endregion Constructors

        #region Methods

        public void FlushAll()
        {
            var enumerator = _cache.GetEnumerator();
            while (enumerator.MoveNext())
                _cache.Remove(enumerator.Key.ToString());
        }

        public T Get<T>(string key)
        {
            var cacheEntry = (CacheEntry)_cache.Get(key);

            if (cacheEntry != null)
            {
                return (T)cacheEntry.Value;
            }
            else
            {
                return default(T);
            }
        }

        public IEnumerable<Key> GetAllKeys()
        {
            var keys = _cache.Cast<DictionaryEntry>()
                .Where(de => de.Value.GetType() == typeof(CacheEntry))
                .Select(de => new Key((string)de.Key, ((CacheEntry)de.Value).ExpiresAt));
            return keys;
        }

        public bool Remove(string key)
        {
            if (_cache.Get(key) == null)
                return false;
            try
            {
                _cache.Remove(key);

                if (_logger.DebugEnabled)
                {
                    _logger.Debug(String.Format("CS HttpContext: Removed cache item with key {0}", key));
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool Set<T>(string key, T value, string parentKey = null)
        {
            var entry = new CacheEntry(value, null);
            return InternalSet(key, entry, parentKey);
        }

        public bool Set<T>(string key, T value, DateTime expiresAt, string parentKey = null)
        {
            var entry = new CacheEntry(value, expiresAt);
            return InternalSet(key, entry, parentKey);
        }

        public bool Set<T>(string key, T value, TimeSpan expiresIn, string parentKey = null)
        {
            return this.Set(key, value, DateTime.Now.Add(expiresIn), parentKey);
        }

        /// <summary>
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public int TimeToLive(string key)
        {
            var result = (CacheEntry)_cache.Get(key);
            if (result == null || result.ExpiresAt == null)
                return -1;
            return (int)(result.ExpiresAt.Value - DateTime.Now).TotalSeconds;
        }

        /// <summary>
        /// Shared insert for public wrappers.
        /// </summary>
        /// <param name="key">The key of the item to insert.</param>
        /// <param name="entry">The internal CacheEntry object to insert.</param>
        /// <param name="parentKey">The key of the item that this item is a child of.</param>
        private bool InternalSet(string key, CacheEntry entry, string parentKey = null)
        {
            CacheDependency cacheDependency = null;
            if (!string.IsNullOrWhiteSpace(parentKey))
                cacheDependency = new CacheDependency(null, new[] { parentKey });
            try
            {
                if (entry.ExpiresAt == null)
                    _cache.Insert(key, entry, cacheDependency, Cache.NoAbsoluteExpiration, Cache.NoSlidingExpiration, CacheItemPriority.NotRemovable, null);
                else
                    _cache.Insert(key, entry, cacheDependency, entry.ExpiresAt.Value, Cache.NoSlidingExpiration, CacheItemPriority.NotRemovable, null);

                if (_logger.DebugEnabled)
                {
                    _logger.Debug(String.Format("CS HttpContext: Set cache item with key {0}", key));
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        #endregion Methods

        #region Classes

        /// <summary>
        /// Private class for the wrapper around the cache items.
        /// </summary>
        private class CacheEntry
        {
            #region Constructors

            /// <summary>
            /// Creates a new instance of CacheEntry.
            /// </summary>
            /// <param name="value">The value being cached.</param>
            /// <param name="expiresAt">The UTC time at which CacheEntry expires.</param>
            public CacheEntry(object value, DateTime? expiresAt)
            {
                Value = value;
                ExpiresAt = expiresAt;
            }

            #endregion Constructors

            #region Properties

            /// <summary>
            /// UTC time at which CacheEntry expires.
            /// </summary>
            internal DateTime? ExpiresAt { get; private set; }

            /// <summary>
            /// The value that is cached.
            /// </summary>
            internal object Value { get; private set; }

            #endregion Properties
        }

        #endregion Classes
    }
}