using System;

namespace CacheSleeve.Models
{
    public class Key
    {
        public Key(string keyName, DateTime? expirationDate = null)
        {
            KeyName = keyName;
            ExpirationDate = expirationDate;
        }


        public string KeyName { get; private set; }
        public DateTime? ExpirationDate { get; private set; }

        public override bool Equals(object obj)
        {
            var objType = obj.GetType();
            if (objType == typeof(Key))
            {
                return this.KeyName == ((Key)obj).KeyName;
            }
            else
            {
                return base.Equals(obj);
            }
        }
    }
}