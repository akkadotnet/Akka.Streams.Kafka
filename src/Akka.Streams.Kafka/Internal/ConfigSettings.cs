using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Configuration;
using Akka.Configuration.Hocon;

namespace Akka.Streams.Kafka.Internal
{
    internal static class ConfigSettings
    {
        public static ImmutableDictionary<string, string> ParseKafkaClientsProperties(this Config config)
        {
            HashSet<string> CollectKeys(HoconValue c, HashSet<string> processedKeys, List<string> unprocessedKeys)
            {
                while (true)
                {
                    if (unprocessedKeys.Count == 0) return processedKeys;

                    var currentKey = unprocessedKeys[0];
                    unprocessedKeys.RemoveAt(0);

                    var v = c.ToConfig().GetValue(currentKey);
                    if (v.IsObject())
                    {
                        unprocessedKeys.AddRange(v.GetObject().Items.Select(kvp => $"{currentKey}.{kvp.Key}"));
                        continue;
                    }

                    processedKeys.Add(currentKey);
                }
            }

            var keys = CollectKeys(config.Root, new HashSet<string>(), config.Root.GetObject().Items.Keys.ToList());
            return keys.ToDictionary(k => k, v => config.GetString(v)).ToImmutableDictionary();
        }
    }
}
