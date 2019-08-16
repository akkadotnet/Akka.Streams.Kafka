using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests
{
    [Collection(KafkaSpecsFixture.Name)]
    public class KafkaIntegrationTests : Akka.TestKit.Xunit2.TestKit
    {
        /// <summary>
        /// Allows to write logs to file (useful for debugging when tests are running forever and no output in console is available)
        /// </summary>
        private const bool UseFileLogging = true;
        
        public KafkaIntegrationTests(string actorSystemName, ITestOutputHelper output) 
            : base(Default(), actorSystemName, output)
        {
            Sys.Log.Info("Starting test: " + output.GetCurrentTestName());
        }

        private static Config Default()
        {
            var defaultSettings =
                ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf");
            
            var config = ConfigurationFactory.ParseString("akka.loglevel = DEBUG");

            if (UseFileLogging)
            {
                config = config.WithFallback(
                    ConfigurationFactory.ParseString("akka.loggers = [\"Akka.Streams.Kafka.Tests.Logging.SimpleFileLoggerActor, Akka.Streams.Kafka.Tests\"]"));
            }
            
            return config.WithFallback(defaultSettings);
        }
    }
}