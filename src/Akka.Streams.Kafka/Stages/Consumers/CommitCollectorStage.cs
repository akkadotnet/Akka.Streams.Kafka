using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Stages.Consumers;

/// <summary>
/// INTERNAL API
///
/// Combined stage for committing incoming offsets in batches.
/// </summary>
internal sealed class CommitCollectorStage : GraphStage<FlowShape<ICommittable, ICommittableOffsetBatch>>
{
    private readonly CommitterSettings _settings;
    
    public Inlet<ICommittable> In { get; } = new("FlowIn");
    public Outlet<ICommittableOffsetBatch> Out { get; } = new("FlowOut");

    public CommitCollectorStage(CommitterSettings settings)
    {
        _settings = settings;
        Shape = new FlowShape<ICommittable, ICommittableOffsetBatch>(In, Out);
    }

    public override FlowShape<ICommittable, ICommittableOffsetBatch> Shape { get; }
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => throw new System.NotImplementedException();
}