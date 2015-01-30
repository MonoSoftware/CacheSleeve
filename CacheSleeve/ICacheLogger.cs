using System;

namespace CacheSleeve
{
    public interface ICacheLogger
    {
        bool DebugEnabled { get; }

        bool ErrorEnabled { get; }

        bool InfoEnabled { get; }

        void Debug(string message);
        
        void Error(string message);

        void Error(Exception exception, string message = null);

        void Info(string message);
    }
}
