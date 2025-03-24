// -----------------------------------------------------------------------
//  <copyright file="MacroBenchmarkConfig.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;

namespace Akka.Streams.Kafka.Benchmark.Configs;

public class MacroBenchmarkConfig : ManualConfig
{
    public MacroBenchmarkConfig()
    {
        AddExporter(MarkdownExporter.GitHub);
        AddColumn(new MessagesPerSecondColumn());
        AddColumn(new CategoriesColumn());
        AddLogger(ConsoleLogger.Default);

        // Safer affinity mask (optional; remove if not needed)
        var processorCount = Environment.ProcessorCount;
        var affinityMaskValue = processorCount == 64
            ? ulong.MaxValue
            : (1UL << processorCount) - 1;
        var affinityMask = (IntPtr)affinityMaskValue;

        AddJob(Job.LongRun
                .WithGcMode(new GcMode { Server = true, Concurrent = true })
                .WithEvaluateOverhead(false)
                .WithWarmupCount(3) // Reduced from 25
                .WithIterationCount(10) // Reduced from 50
                .WithStrategy(RunStrategy.Monitoring)
            //.WithAffinity(affinityMask) // Optional
        );
    }
}