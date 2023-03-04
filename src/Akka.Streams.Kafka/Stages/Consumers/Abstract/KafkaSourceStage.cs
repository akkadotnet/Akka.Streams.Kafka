using System.Threading.Tasks;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Supervision;
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
    public abstract class KafkaSourceStage<K, V, TMessage> : GraphStageWithMaterializedValue<SourceShape<TMessage>, IControl>
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
            InitialAttributes = Attributes.CreateName(StageName)
                .And(ActorAttributes.CreateSupervisionStrategy(new DefaultConsumerDecider(true).Decide));
        }

        /// <inheritdoc />
        protected override Attributes InitialAttributes { get; }
        
        /// <summary>
        /// Provides actual stage logic
        /// </summary>
        /// <param name="shape">Shape of the stage</param>
        /// <param name="inheritedAttributes">Stage attributes</param>
        /// <returns>Stage logic</returns>
        protected abstract (GraphStageLogic, IControl) Logic(SourceShape<TMessage> shape, Attributes inheritedAttributes);

        /// <inheritdoc />
        public override ILogicAndMaterializedValue<IControl> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var (logic, materializedValue) = Logic(Shape, inheritedAttributes);
            return new LogicAndMaterializedValue<IControl>(logic, materializedValue);
        }
    }
}