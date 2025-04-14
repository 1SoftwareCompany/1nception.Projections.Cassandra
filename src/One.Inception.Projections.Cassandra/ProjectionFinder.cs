using System;
using System.Collections.Generic;
using One.Inception.EventStore;
using One.Inception.MessageProcessing;
using One.Inception.Projections.Versioning;
using One.Inception.Testing;

namespace One.Inception.Projections.Cassandra
{
    public class ProjectionFinder
    {
        private readonly TypeContainer<IProjection> projectionsContainer;
        private readonly IEventStore eventStore;
        private readonly IInceptionContextAccessor inceptionContextAccessor;

        public ProjectionFinder(TypeContainer<IProjection> projectionsContainer, IEventStore eventStore, IInceptionContextAccessor inceptionContextAccessor)
        {
            this.projectionsContainer = projectionsContainer;
            this.eventStore = eventStore;
            this.inceptionContextAccessor = inceptionContextAccessor;
        }

        public IEnumerable<ProjectionVersion> GetProjectionVersionsToBootstrap()
        {
            foreach (Type projectionType in projectionsContainer.Items)
            {
                var arId = new ProjectionVersionManagerId(projectionType.GetContractId(), inceptionContextAccessor.Context.Tenant);
                EventStream projectionVersionManagerEventStream = eventStore.LoadAsync(arId).GetAwaiter().GetResult();
                ProjectionVersionManager manager;
                bool success = projectionVersionManagerEventStream.TryRestoreFromHistory<ProjectionVersionManager>(out manager);
                if (success)
                {
                    var live = manager.RootState().Versions.GetLive();
                    if (live is not null)
                        yield return live;
                }
            }
        }
    }

}
