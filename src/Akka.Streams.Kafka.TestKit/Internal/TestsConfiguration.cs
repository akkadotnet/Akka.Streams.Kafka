using System;

namespace Akka.Streams.Kafka.TestKit.Internal
{
    /// <summary>
    /// Stores info about testing configuration, obtained from environment
    /// </summary>
    public static class TestsConfiguration
    {
        /// <summary>
        /// Allows to write logs to file (useful for debugging when tests are running forever and no output in console is available)
        /// </summary>
        public static readonly bool UseFileLogging = Environment.GetEnvironmentVariable("AKKA_STREAMS_KAFKA_TEST_FILE_LOGGING") != null;
        
        /// <summary>
        /// Allows to avoid delays on tests startup/shutdown, related to new containers creation/stopping process 
        /// </summary>
        /// <remarks>
        /// When this option is enabled, use docker-compose to start kafka manually (see docker-compose.yml file in the root folder)
        /// </remarks>
        public static readonly bool UseExistingDockerContainer = Environment.GetEnvironmentVariable("AKKA_STREAMS_KAFKA_TEST_CONTAINER_REUSE") != null;
    }
}