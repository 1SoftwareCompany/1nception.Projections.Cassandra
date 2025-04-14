//using System.Runtime.Serialization;
//using System.Threading.Tasks;
//using One.Inception.Projections.Versioning;

//namespace One.Inception.Projections.Cassandra.Infrastructure
//{
//    [DataContract(Name = "a25d65d4-7172-43be-9ebe-9b3a5c8928a0")]
//    public class ProjectionVersionHandler : IProjection, ISystemProjection,
//        IEventHandler<NewProjectionVersionIsNowLive>
//    {
//        private readonly IProjectionTableRetentionStrategy strategy;

//        public ProjectionVersionHandler(IProjectionTableRetentionStrategy strategy)
//        {
//            this.strategy = strategy;
//        }

//        public Task HandleAsync(NewProjectionVersionIsNowLive @event)
//        {
//            strategy.ApplyAsync(@event.ProjectionVersion);
//            return Task.CompletedTask;
//        }
//    }
//}
