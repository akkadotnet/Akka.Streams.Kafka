using System.Threading.Tasks;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    public abstract class KafkaSourceStage<K, V, TMessage> : GraphStageWithMaterializedValue<SourceShape<TMessage>, Task>
    {
        public string StageName { get; }
        public Outlet<TMessage> Out => new Outlet<TMessage>("out");
        public override SourceShape<TMessage> Shape => new SourceShape<TMessage>(Out);

        protected KafkaSourceStage(string stageName)
        {
            StageName = stageName;
        }

        protected override Attributes InitialAttributes => Attributes.CreateName(StageName);

        protected abstract GraphStageLogic Logic(SourceShape<TMessage> shape, TaskCompletionSource<NotUsed> completion);

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            var result = Logic(Shape, completion);
            return new LogicAndMaterializedValue<Task>(result, completion.Task);
        }
    }
}