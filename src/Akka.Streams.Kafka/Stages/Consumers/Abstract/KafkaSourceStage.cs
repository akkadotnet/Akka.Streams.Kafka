using System.Threading.Tasks;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// KafkaSourceStage
    /// </summary>
    /// <remarks>
    /// This is a base stage for any source-type kafka stages.
    /// </remarks>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    /// <typeparam name="TMessage">Stage output messages type</typeparam>
    public abstract class KafkaSourceStage<K, V, TMessage> : GraphStageWithMaterializedValue<SourceShape<TMessage>, Task>
    {
        /// <summary>
        /// Name of the stage
        /// </summary>
        public string StageName { get; }
        
        /// <summary>
        /// Stage out port
        /// </summary>
        public Outlet<TMessage> Out { get; } = new Outlet<TMessage>("out");
        
        /// <inheritdoc />
        public override SourceShape<TMessage> Shape { get; }

        /// <summary>
        /// KafkaSourceStage
        /// </summary>
        /// <param name="stageName">Stage name</param>
        protected KafkaSourceStage(string stageName)
        {
            StageName = stageName;
            Shape = new SourceShape<TMessage>(Out);
        }

        /// <inheritdoc />
        protected override Attributes InitialAttributes => Attributes.CreateName(StageName);
        
        /// <summary>
        /// Provides actual stage logic
        /// </summary>
        /// <param name="shape">Shape of the stage</param>
        /// <param name="completion">Used to specify stage task completion</param>
        /// <param name="inheritedAttributes">Stage attributes</param>
        /// <returns>Stage logic</returns>
        protected abstract GraphStageLogic Logic(SourceShape<TMessage> shape, TaskCompletionSource<NotUsed> completion, Attributes inheritedAttributes);

        /// <inheritdoc />
        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            var result = Logic(Shape, completion, inheritedAttributes);
            return new LogicAndMaterializedValue<Task>(result, completion.Task);
        }
    }
}