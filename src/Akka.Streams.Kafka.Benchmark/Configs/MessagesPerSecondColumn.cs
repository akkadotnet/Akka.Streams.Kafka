using System.Reflection;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Akka.Streams.Kafka.Benchmark.Configs;

public class MessagesPerSecondColumn : IColumn
{
    public string Id => nameof(MessagesPerSecondColumn);
    public string ColumnName => "msg/sec";

    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
    public string GetValue(Summary summary, BenchmarkCase benchmarkCase) => GetValue(summary, benchmarkCase, SummaryStyle.Default);
    public bool IsAvailable(Summary summary) => true;
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Custom;
    public int PriorityInCategory => -1;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Dimensionless;
    public string Legend => "Messages per Second";

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var benchmarkAttribute = benchmarkCase.Descriptor.WorkloadMethod.GetCustomAttribute<BenchmarkAttribute>();
        var totalOperations = benchmarkAttribute?.OperationsPerInvoke ?? 1;

        if (!summary.HasReport(benchmarkCase)) 
            return "<not found>";
            
        var report = summary[benchmarkCase];
        var statistics = report?.ResultStatistics;
        if(statistics is null) 
            return "<not found>";
            
        var nsPerOperation = statistics.Mean;
        var operationsPerSecond = 1 / (nsPerOperation / 1e9);

        return operationsPerSecond.ToString("N2");  // or format as you like

    }
}