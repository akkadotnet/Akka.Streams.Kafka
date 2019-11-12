using Akka.Streams.Util;
using Akka.Util;
using Newtonsoft.Json;

namespace Akka.Streams.Kafka.Extensions
{
    public static class ObjectExtensions
    {
        /// <summary>
        /// Returns object's json representation as string
        /// </summary>
        public static string ToJson(this object obj)
        {
            return JsonConvert.SerializeObject(obj);
        }
        
        /// <summary>
        /// Wraps object to the option
        /// </summary>
        public static Option<T> AsOption<T>(this T obj) => new Option<T>(obj);
    }
}