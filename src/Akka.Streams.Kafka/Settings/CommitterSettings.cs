using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Streams.Kafka.Settings
{
    /// <summary>
    /// Settings for committer. See 'akka.kafka.committer' section in reference.conf.
    /// </summary>
    public sealed class CommitterSettings
    {
        /// <summary>
        /// CommitterSettings
        /// </summary>
        /// <param name="maxBatch">Max commit batch size</param>
        /// <param name="maxInterval">Max commit interval</param>
        /// <param name="parallelism">Level of parallelism</param>
        public CommitterSettings(int maxBatch, TimeSpan maxInterval, int parallelism)
        {
            MaxBatch = maxBatch;
            MaxInterval = maxInterval;
            Parallelism = parallelism;
        }

        /// <summary>
        /// Creates committer settings
        /// </summary>
        /// <param name="system">Actor system for stage materialization</param>
        /// <returns>Committer settings</returns>
        public static CommitterSettings Create(ActorSystem system)
        {
            var config = system.Settings.Config.GetConfig("akka.kafka.committer");
            return Create(config);
        }

        /// <summary>
        /// Creates committer settings
        /// </summary>
        /// <param name="config">Config to load properties from</param>
        /// <returns>Committer settings</returns>
        public static CommitterSettings Create(Config config)
        {
            var maxBatch = config.GetInt("max-batch");
            var maxInterval = config.GetTimeSpan("max-interval");
            var parallelism = config.GetInt("parallelism");
            return new CommitterSettings(maxBatch, maxInterval, parallelism);
        }
        
        /// <summary>
        /// Max commit batch size
        /// </summary>
        public int MaxBatch { get; }
        /// <summary>
        /// Max commit interval
        /// </summary>
        public TimeSpan MaxInterval { get; }
        /// <summary>
        /// Level of parallelism
        /// </summary>
        public int Parallelism { get; }

        /// <summary>
        /// Sets max batch size
        /// </summary>
        public CommitterSettings WithMaxBatch(int maxBatch) => Copy(maxBatch: maxBatch);
        /// <summary>
        /// Sets max commit interval
        /// </summary>
        public CommitterSettings WithMaxInterval(TimeSpan maxInterval) => Copy(maxInterval: maxInterval);
        /// <summary>
        /// Sets parallelism level
        /// </summary>
        public CommitterSettings WithParallelism(int parallelism) => Copy(parallelism: parallelism);

        private CommitterSettings Copy(int? maxBatch = null, TimeSpan? maxInterval = null, int? parallelism = null)
        {
            return new CommitterSettings(
                maxBatch: maxBatch ?? MaxBatch, 
                maxInterval: maxInterval ?? MaxInterval, 
                parallelism: parallelism ?? Parallelism);
        }
    }
}