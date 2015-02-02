using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CacheSleeve.Models;

namespace CacheSleeve
{
    public class HybridCacher : ICacher, IAsyncCacher
    {
        private readonly RedisCacher _remoteCacher;
        private readonly HttpContextCacher _localCacher;

        private readonly string _removeChannel;
        private readonly string _flushChannel;

        public RedisCacher RemoteCacher { get { return _remoteCacher; } }

        public HttpContextCacher LocalCacher { get { return _localCacher; } }

        public string KeyPrefix 
        { 
            get; 
            private set; 
        }

        public HybridCacher(
            IHybridCacherConfig config,
            RedisCacher redisCacher,
            HttpContextCacher httpContextCacher
            )
        {
            _remoteCacher = redisCacher;
            _localCacher = httpContextCacher;

            this.KeyPrefix = config.KeyPrefix;

            _removeChannel = "cacheSleeve.remove";
            _flushChannel = "cacheSleeve.flush";
            if (!String.IsNullOrEmpty(this.KeyPrefix))
            {
                var prefix = "." + this.KeyPrefix;
                _removeChannel += prefix;
                _flushChannel += prefix;
            }

            _remoteCacher.SubscribeToChannel(_removeChannel, (redisChannel, value) => _localCacher.Remove(value));
            _remoteCacher.SubscribeToChannel(_flushChannel, (redisChannel, value) => _localCacher.FlushAll());
        }

        /// <summary>
        /// Adds the prefix to the key.
        /// </summary>
        /// <param name="key">The specified key value.</param>
        /// <returns>The specified key with the prefix attached.</returns>
        public string AddPrefix(string key)
        {
            if (key != null && !String.IsNullOrEmpty(KeyPrefix))
            {
                return KeyPrefix + key;
            }
            else
            {
                return key;
            }
        }

        public T Get<T>(string key)
        {
            var cacheKey = this.AddPrefix(key);
            var result = _localCacher.Get<T>(cacheKey);
            if (result != null)
                return result;
            result = _remoteCacher.Get<T>(cacheKey);
            if (result != null)
            {
                var ttl = _remoteCacher.TimeToLive(cacheKey);
                var parentKey = _remoteCacher.Get<string>(cacheKey + ".parent");

                if (ttl > -1)
                    _localCacher.Set(cacheKey, result, TimeSpan.FromSeconds(ttl), parentKey);
                else
                    _localCacher.Set(cacheKey, result, parentKey);

                result = _localCacher.Get<T>(cacheKey);
            }
            return result;
        }

        public T GetOrSet<T>(string key, Func<string, T> valueFactory, DateTime expiresAt, string parentKey = null)
        {
            var value = this.Get<T>(key);
            if (value == null)
            {
                value = valueFactory(key);
                if (value != null && !value.Equals(default(T)))
                {
                    this.Set(key, value, expiresAt, parentKey);
                }
            }

            return value;
        }

        public bool Set<T>(string key, T value, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                _remoteCacher.Set(cacheKey, value, this.AddPrefix(parentKey));
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey);
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public bool Set<T>(string key, T value, DateTime expiresAt, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                _remoteCacher.Set(cacheKey, value, expiresAt,  this.AddPrefix(parentKey));
                
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey);
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public bool Set<T>(string key, T value, TimeSpan expiresIn, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                _remoteCacher.Set(cacheKey, value, expiresIn,  this.AddPrefix(parentKey));
                
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey);
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public bool Remove(string key)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                _remoteCacher.Remove(cacheKey);
                _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void FlushAll()
        {
            _remoteCacher.FlushAll();
            _remoteCacher.PublishToChannel(this._flushChannel, "");
        }

        public IEnumerable<Key> GetAllKeys()
        {
            var keys = _remoteCacher.GetAllKeys()
                    .Union(_localCacher.GetAllKeys())
                    .Distinct();

            if (this.KeyPrefix != null && this.KeyPrefix.Length > 0) {
                keys = keys
                    .Select(k => new Key(k.KeyName.Substring(this.KeyPrefix.Length), k.ExpirationDate));

            }

            return keys;
        }

        public async Task<T> GetAsync<T>(string key)
        {
            var cacheKey = this.AddPrefix(key);

            var result = _localCacher.Get<T>(cacheKey);
            if (result != null)
                return result;
            result = await _remoteCacher.GetAsync<T>(cacheKey);
            if (result != null)
            {
                var ttl = (int)(await _remoteCacher.TimeToLiveAsync(cacheKey));
                var parentKey = _remoteCacher.Get<string>(cacheKey + ".parent");

                if (ttl > -1)
                    _localCacher.Set(key, result, TimeSpan.FromSeconds(ttl), parentKey);
                else
                    _localCacher.Set(key, result, parentKey);

                result = _localCacher.Get<T>(cacheKey);
            }
            return result;
        }

        public async Task<T> GetOrSetAsync<T>(string key, Func<string, Task<T>> valueFactory, DateTime expiresAt, string parentKey = null)
        {
            var value = await this.GetAsync<T>(key);
            if (value == null)
            {
                value = await valueFactory(key);
                if (value != null && !value.Equals(default(T)))
                {
                    await this.SetAsync(key, value, expiresAt, parentKey);
                }
            }

            return value;
        }

        public async Task<bool> SetAsync<T>(string key, T value, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                await _remoteCacher.SetAsync(cacheKey, value,  this.AddPrefix(parentKey));
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey); // this might be a really bad idea
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public async Task<bool> SetAsync<T>(string key, T value, DateTime expiresAt, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                await _remoteCacher.SetAsync(cacheKey, value, expiresAt,  this.AddPrefix(parentKey));
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey); // this might be a really bad idea
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public async Task<bool> SetAsync<T>(string key, T value, TimeSpan expiresIn, string parentKey = null)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                await _remoteCacher.SetAsync(key, value, expiresIn,  this.AddPrefix(parentKey));
            }
            catch (Exception)
            {
                _localCacher.Remove(cacheKey);
                _remoteCacher.Remove(cacheKey); // this might be a really bad idea
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public async Task<bool> RemoveAsync(string key)
        {
            var cacheKey = this.AddPrefix(key);
            try
            {
                await _remoteCacher.RemoveAsync(cacheKey);
            }
            catch (Exception)
            {
                return false;
            }

            _remoteCacher.PublishToChannel(this._removeChannel, cacheKey);
            return true;
        }

        public async Task FlushAllAsync()
        {
            await _remoteCacher.FlushAllAsync();
            _remoteCacher.PublishToChannel(this._flushChannel, "");
        }

        public async Task<IEnumerable<Key>> GetAllKeysAsync()
        {
            return (await _remoteCacher.GetAllKeysAsync())
                            .Union(_localCacher.GetAllKeys())
                            .Distinct()
                            .Select(k => new Key(k.KeyName.Substring(this.KeyPrefix.Length), k.ExpirationDate));
        }
    }
}