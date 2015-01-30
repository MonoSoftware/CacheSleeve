using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CacheSleeve
{
    public class CacheTracer : ICacheLogger
    {
        public bool DebugEnabled { get { return true; } }

        public bool ErrorEnabled { get { return true; } }

        public bool InfoEnabled { get { return true; } }

        public void Debug(string message)
        {
            Trace.WriteLine(String.Format("DEBUG - {0}", message));
        }

        public void Info(string message)
        {
            Trace.WriteLine(String.Format("INFO - {0}", message));
        }

        public void Error(string message)
        {
            Trace.WriteLine(String.Format("ERROR - {0}", message));
        }

        public void Error(Exception exception, string message = null)
        {
            if (message != null)
            {
                Trace.WriteLine(String.Format("ERROR - {0}", message));
            }

            if (exception != null)
            {
                Trace.WriteLine(exception.ToString());
            }
        }
    }
}
