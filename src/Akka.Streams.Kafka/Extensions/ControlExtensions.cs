using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Helpers;

namespace Akka.Streams.Kafka.Extensions
{
    /// <summary>
    /// ControlExtensions
    /// </summary>
    public static class ControlExtensions
    {
        /// <summary>
        /// Stop producing messages from the `Source`, wait for stream completion
        /// and shut down the consumer `Source` so that all consumed messages
        /// reach the end of the stream.
        /// Failures in stream completion will be propagated, the source will be shut down anyway.
        /// </summary>
        public static async Task<TResult> DrainAndShutdownDefault<TResult>(this IControl control, Task<TResult> streamCompletion)
        {
            TResult result;
            
            try
            {
                await control.Stop();

                result = await streamCompletion;
            }
            catch (Exception completionError)
            {
                try
                {
                    await control.Shutdown();

                    return await streamCompletion;
                }
                catch (Exception)
                {
                    throw completionError;
                }
            }

            await control.Shutdown();

            return result;
        }
    }
}