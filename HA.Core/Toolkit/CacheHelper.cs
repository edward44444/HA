using System;
using System.Web.Caching;

namespace HA.Core.Toolkit
{
    public class CacheHelper
    {
        private static Cache _cache = System.Web.HttpRuntime.Cache;

        public static void Insert(string key, object value, CacheDependency dependencies, DateTime absoluteExpiration, TimeSpan slidingExpiration)
        {
            _cache.Insert(key, value, dependencies, absoluteExpiration, slidingExpiration);
        }

        public static void Insert(string key, object value, DateTime absoluteExpiration)
        {
            _cache.Insert(key, value, null, absoluteExpiration, Cache.NoSlidingExpiration);
        }

        public static object Remove(string key)
        {
            return _cache.Remove(key);
        }

        public static T GetValue<T>(string key)
        {
            return (T)_cache.Get(key);
        }

        public static object GetValue(string key)
        {
            return _cache.Get(key);
        }
    }
}
