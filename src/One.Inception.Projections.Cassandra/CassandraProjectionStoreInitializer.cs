using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace One.Inception.Projections.Cassandra;

public sealed class CassandraProjectionStoreInitializer : IInitializableProjectionStore
{
    static readonly ILogger logger = InceptionLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));
    private readonly ICassandraProjectionPartitionStoreSchema partitionsSchema;
    private readonly IProjectionStoreStorageManager projectionsSchemaNew;
    private readonly VersionedProjectionsNaming naming;

    public CassandraProjectionStoreInitializer(ICassandraProjectionPartitionStoreSchema partitionsSchema, VersionedProjectionsNaming naming, IProjectionStoreStorageManager projectionsSchemaNew)
    {
        this.naming = naming;
        this.partitionsSchema = partitionsSchema;
        this.projectionsSchemaNew = projectionsSchemaNew;
    }

    public async Task<bool> InitializeAsync(ProjectionVersion version)
    {
        try
        {
            await partitionsSchema.CreateProjectionPartitionsStorage(); // partitions

            string projectionColumnFamilyNew = naming.GetColumnFamilyNew(version);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projection Store] Initializing projection store with column family `{projectionColumnFamilyNew}`...", projectionColumnFamilyNew); // projections
            Task createProjectionStorageTaskNew = projectionsSchemaNew.CreateProjectionsStorageAsync(projectionColumnFamilyNew);
            await createProjectionStorageTaskNew.ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projection Store] Initialized projection store with column family `{projectionColumnFamilyNew}`", projectionColumnFamilyNew);

            return createProjectionStorageTaskNew.IsCompletedSuccessfully;
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to initialize projection version {version}", version)))
        {
            return false;
        }
    }
}

