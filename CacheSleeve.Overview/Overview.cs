using RazorEngine;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CacheSleeve.Overview
{
    public class Overview
    {
        public static string Generate(ICacheManager cacheManager)
        {
            const string resourceName = "CacheSleeve.Overview.Razor.Overview.cshtml";
            var model = new CacheSleeve.Models.Overview
            {
                RemoteKeys = cacheManager.RemoteCacher.GetAllKeys(),
                LocalKeys = cacheManager.LocalCacher.GetAllKeys()
            };
            var assembly = Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                    return "";
                using (var reader = new StreamReader(stream))
                    return Razor.Parse(reader.ReadToEnd(), model);
            }
        }
    }
}
