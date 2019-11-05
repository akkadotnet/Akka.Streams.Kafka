using Akka.Streams.Util;
using Akka.Util;

namespace Akka.Streams.Kafka.Extensions
{
    public static class OptionExtensions
    {
        /// <summary>
        /// Gets option value, if any - otherwise returns default value provided
        /// </summary>
        public static T GetOrElse<T>(this Option<T> option, T defaultValue)
        {
            return option.HasValue ? option.Value : defaultValue;
        }
    }
}