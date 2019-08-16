using System.Reflection;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Gets current test name
        /// </summary>
        public static string GetCurrentTestName(this ITestOutputHelper output)
        {
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            var test = (ITest)testMember.GetValue(output);
            return test.DisplayName;
        }
    }
}