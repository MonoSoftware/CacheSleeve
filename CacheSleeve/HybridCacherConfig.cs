using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CacheSleeve
{
    public class HybridCacherConfig : IHybridCacherConfig
    {
        public HybridCacherConfig()
        {
            this.KeyPrefix = "cs.";
        }

        public string KeyPrefix { get; set; }
    }
}
