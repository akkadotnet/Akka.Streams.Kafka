using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Streams.Kafka.Stages
{
    internal static class ActorMaterializerHelper
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="materializer">TBD</param>
        /// <exception cref="ArgumentException">
        /// This exception is thrown when the specified <paramref name="materializer"/> is not of type <see cref="ActorMaterializer"/>.
        /// </exception>
        /// <returns>TBD</returns>
        internal static ActorMaterializer Downcast(IMaterializer materializer)
        {
            //FIXME this method is going to cause trouble for other Materializer implementations
            var downcast = materializer as ActorMaterializer;
            if (downcast != null)
                return downcast;

            throw new ArgumentException($"Expected {typeof(ActorMaterializer)} but got {materializer.GetType()}");
        }
    }
}
