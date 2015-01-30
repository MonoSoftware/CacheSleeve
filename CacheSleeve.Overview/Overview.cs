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
        public static string Generate(HybridCacher cacher)
        {
            const string resourceName = "CacheSleeve.Overview.Razor.Overview.cshtml";
            var model = new CacheSleeve.Models.Overview
            {
                RemoteKeys = cacher.RemoteCacher.GetAllKeys(),
                LocalKeys = cacher.LocalCacher.GetAllKeys()
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
