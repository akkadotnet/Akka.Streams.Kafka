using System;
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

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) =>
        throw new System.NotImplementedException();

    private class CommitCollectorStageLogic : TimerGraphStageLogic
    {
        private const string CommitNow = "FlowStageCommit";

        public CommitCollectorStageLogic(Shape shape, CommitCollectorStage stage, Attributes inheritedAttributes) :
            base(shape)
        {
            Stage = stage;
            InheritedAttributes = inheritedAttributes;
            ObservationLogic = new CommitObservationLogic(stage._settings);
            SetHandler(Stage.Out, OnPull);
            SetHandler(Stage.In, OnPush, OnUpstreamFinish, OnUpstreamFailure);
        }

        private void OnUpstreamFailure(Exception ex)
        {
            Log.Debug(ex, "OnUpstreamFailure with exception with {0}", ObservationLogic.OffsetBatch);
            if (ActiveBatchInProgress)
            {
                ObservationLogic.OffsetBatch.TellCommitEmergency();
                ObservationLogic.OffsetBatch = CommittableOffsetBatch.Empty;
            }
            FailStage(ex);
        }

        private void OnUpstreamFinish()
        {
            if (ActiveBatchInProgress)
            {
                Log.Debug("PushDownStream triggered by {0}, outstanding batch {1}", CommitTrigger.UpstreamFinish.Instance,
                    ObservationLogic.OffsetBatch);
                Emit(Stage.Out, ObservationLogic.OffsetBatch);
            }
            CompleteStage();
        }

        private void OnPush()
        {
            var offset = Grab(Stage.In);
            Log.Debug("Consuming offset {0}", offset);
            if (ObservationLogic.UpdateBatch(offset))
            {
                // Push only of the outlet is available, a commit on interval might have taken the pending demand.
                // This is very hard to get tested consistently, so it gets this big comment instead.
                if (IsAvailable(Stage.Out))
                {
                    PushDownStream(CommitTrigger.BatchSize.Instance);
                }
                else
                {
                    TryPull(Stage.In);
                }
            }
        }

        private void OnPull()
        {
            if (_pushOnNextPull)
            {
                PushDownStream(CommitTrigger.Interval.Instance);
                _pushOnNextPull = false;
            }
            else if (!HasBeenPulled(Stage.In))
            {
                TryPull(Stage.In);
            }
        }

        public CommitObservationLogic ObservationLogic { get; }
        public CommitCollectorStage Stage { get; }
        public Attributes InheritedAttributes { get; }

        public CommitterSettings Settings => Stage._settings;

        public bool ActiveBatchInProgress => !ObservationLogic.OffsetBatch.IsEmpty;

        protected override object LogSource => typeof(CommitCollectorStageLogic);

        private bool _pushOnNextPull = false;

        public override void PreStart()
        {
            base.PreStart();
            ScheduleCommit();
            Log.Debug("CommitCollectorStage initialized");
        }

        protected override void OnTimer(object timerKey)
        {
            switch (timerKey)
            {
                case CommitNow:
                {
                    if (ActiveBatchInProgress)
                    {
                        // Push only of the outlet is available, as timers may occur outside a push/pull cycle.
                        // Otherwise instruct `OnPull` to emit what is there when the next pull occurs.
                        // This is very hard to get tested consistently, so it gets this big comment instead.
                        if (IsAvailable(Stage.Out))
                        {
                            PushDownStream(CommitTrigger.Interval.Instance);
                        }
                        else
                        {
                            _pushOnNextPull = true;
                        }
                    }
                    else
                    {
                        ScheduleCommit();
                    }

                    break;
                }
                default:
                    Log.Warning("Unexpected timer [{0}]", timerKey);
                    break;
            }
        }

        private void PushDownStream(CommitTrigger.ITriggeredBy triggeredBy)
        {
            Log.Debug("PushDownStream triggered by {0}, outstanding batch {1}", triggeredBy,
                ObservationLogic.OffsetBatch);
            Push(Stage.Out, ObservationLogic.OffsetBatch);
            ObservationLogic.OffsetBatch = CommittableOffsetBatch.Empty;
            ScheduleCommit();
        }

        private void ScheduleCommit()
        {
            ScheduleOnce(CommitNow, Settings.MaxInterval);
        }
    }
}