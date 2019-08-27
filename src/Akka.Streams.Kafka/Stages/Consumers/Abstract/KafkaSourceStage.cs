using System.Threading.Tasks;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    public abstract class KafkaSourceStage<K, V, TMessage> : GraphStageWithMaterializedValue<SourceShape<TMessage>, Task>
    {
        public string StageName { get; }
        public Outlet<TMessage> Out { get; } = new Outlet<TMessage>("out");
        public override SourceShape<TMessage> Shape { get; }

        protected KafkaSourceStage(string stageName)
        {
            StageName = stageName;
            Shape = new SourceShape<TMessage>(Out);
        }

        protected override Attributes InitialAttributes => Attributes.CreateName(StageName);
        
        /// <summary>
        /// Provides actual stage logic
        /// </summary>
        /// <param name="shape">Shape of the stage</param>
        /// <param name="completion">Used to specify stage task completion</param>
        /// <param name="inheritedAttributes">Stage attributes</param>
        /// <returns>Stage logic</returns>
        protected abstract GraphStageLogic Logic(SourceShape<TMessage> shape, TaskCompletionSource<NotUsed> completion, Attributes inheritedAttributes);

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            var result = Logic(Shape, completion, inheritedAttributes);
            return new LogicAndMaterializedValue<Task>(result, completion.Task);
        }
    }
}