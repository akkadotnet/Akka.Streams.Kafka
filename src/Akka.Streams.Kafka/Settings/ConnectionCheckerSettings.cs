using System;
using Akka.Configuration;

namespace Akka.Streams.Kafka.Settings
{
    public sealed record ConnectionCheckerSettings
    {
        public const string ConfigPath = "connection-checker";
        public static readonly string FullConfigPath = $"{ConsumerSettings.ConfigPath}.{ConfigPath}";

        public static readonly ConnectionCheckerSettings Disabled =
            new ConnectionCheckerSettings(false, 3, TimeSpan.FromSeconds(15), 2.0);

        public static ConnectionCheckerSettings Create(int maxRetries, TimeSpan checkInterval, double factor)
            => new ConnectionCheckerSettings(true, maxRetries, checkInterval, factor);

        public static ConnectionCheckerSettings Create(Config? config = null)
        {
            if (config == null)
                return Disabled;

            return config.GetBoolean("enabled", false)
                ? Create(
                    config.GetInt("max-retries", 3),
                    config.GetTimeSpan("check-interval", TimeSpan.FromSeconds(15)),
                    config.GetDouble("backoff-factor", 2.0)
                )
                : Disabled;
        }

        public bool Enabled { get; init; }
        public int MaxRetries { get; init; }
        public TimeSpan CheckInterval { get; init; }
        public double Factor { get; init; }

        internal ConnectionCheckerSettings(
            bool enabled, 
            int maxRetries, 
            TimeSpan checkInterval, 
            double factor)
        {
            if (factor <= 0 || double.IsInfinity(factor) || double.IsNaN(factor))
                throw new ArgumentException(
                    "Backoff factor for connection checker must be finite positive number", nameof(factor));

            if (maxRetries < 0)
                throw new ArgumentException("retries for connection checker must be not negative number");

            Enabled = enabled;
            MaxRetries = maxRetries;
            CheckInterval = checkInterval;
            Factor = factor;
        }

        [Obsolete("Use record copy methods")]
        private ConnectionCheckerSettings Copy(
            bool? enabled = null,
            int? maxRetries = null,
            TimeSpan? checkInterval = null,
            double? factor = null)
            => new ConnectionCheckerSettings(
                enabled: enabled ?? Enabled,
                maxRetries: maxRetries ?? MaxRetries,
                checkInterval: checkInterval ?? CheckInterval,
                factor: factor ?? Factor);

        public ConnectionCheckerSettings WithEnabled(bool enabled)
            => this with { Enabled = enabled };

        public ConnectionCheckerSettings WithMaxRetries(int maxRetries)
            => this with { MaxRetries = maxRetries };
        public ConnectionCheckerSettings WithFactor(double factor)
            => this with { Factor = factor };

        public ConnectionCheckerSettings WithCheckInterval(TimeSpan checkInterval)
            => this with { CheckInterval = checkInterval };

        public override string ToString() => $"Akka.Streams.Kafka.ConnectionCheckerSettings(Enabled={Enabled},MaxRetries={MaxRetries},CheckInterval={CheckInterval},Factor={Factor})";
    }
}
