// -----------------------------------------------------------------------
//  <copyright file="MessagesPerSecondColumn.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Akka.Streams.Kafka.Benchmark.Configs;

public class MessagesPerSecondColumn : IColumn
{
    public string Id
    {
        get { return nameof(MessagesPerSecondColumn); }
    }

    public string ColumnName
    {
        get { return "msg/sec"; }
    }

    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase) =>
        GetValue(summary, benchmarkCase, SummaryStyle.Default);

    public bool IsAvailable(Summary summary) => true;

    public bool AlwaysShow
    {
        get { return true; }
    }

    public ColumnCategory Category
    {
        get { return ColumnCategory.Custom; }
    }

    public int PriorityInCategory
    {
        get { return -1; }
    }

    public bool IsNumeric
    {
        get { return true; }
    }

    public UnitType UnitType
    {
        get { return UnitType.Dimensionless; }
    }

    public string Legend
    {
        get { return "Messages per Second"; }
    }

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var benchmarkAttribute = benchmarkCase.Descriptor.WorkloadMethod.GetCustomAttribute<BenchmarkAttribute>();
        var totalOperations = benchmarkAttribute?.OperationsPerInvoke ?? 1;

        if (!summary.HasReport(benchmarkCase))
            return "<not found>";

        var report = summary[benchmarkCase];
        var statistics = report?.ResultStatistics;
        if (statistics is null)
            return "<not found>";

        var nsPerOperation = statistics.Mean;
        var operationsPerSecond = 1 / (nsPerOperation / 1e9);

        return operationsPerSecond.ToString("N2"); // or format as you like
    }
}