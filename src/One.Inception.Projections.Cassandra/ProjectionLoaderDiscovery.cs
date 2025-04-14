using System;
using System.Collections.Generic;
using System.Linq;
using One.Inception.DangerZone;
using One.Inception.Discoveries;
using One.Inception.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace One.Inception.Projections.Cassandra;

public class ProjectionLoaderDiscovery : DiscoveryBase<IProjectionReader>
{
    protected override DiscoveryResult<IProjectionReader> DiscoverFromAssemblies(DiscoveryContext context)
    {
        return new DiscoveryResult<IProjectionReader>(GetModels(context), services => services
                                                                                            .AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>()
                                                                                            .AddOptions<TableRetentionOptions, TableRetentionOptionsProvider>());
    }

    IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
    {
        // settings
        var cassandraSettings = context.FindService<ICassandraProjectionStoreSettings>();
        foreach (Type setting in cassandraSettings)
        {
            yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
        }

        yield return new DiscoveredModel(typeof(CassandraProjectionStoreInitializer), typeof(CassandraProjectionStoreInitializer), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
        yield return new DiscoveredModel(typeof(IInitializableProjectionStore), typeof(CassandraProjectionStoreInitializer), ServiceLifetime.Singleton) { CanOverrideDefaults = true };

        yield return new DiscoveredModel(typeof(ProjectionFinder), typeof(ProjectionFinder), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchemaLegacy), typeof(CassandraProjectionStoreSchemaLegacy), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraProjectionPartitionStoreSchema), typeof(CassandraProjectionPartitionStoreSchema), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraProjectionPartitionStoreSchema), typeof(CassandraProjectionPartitionStoreSchema), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(IProjectionStoreStorageManager), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Singleton);

        // cassandra
        yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);

        // projections
        yield return new DiscoveredModel(typeof(IProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStore<>), typeof(CassandraProjectionStore<>), ServiceLifetime.Singleton);

        // data wiper
        yield return new DiscoveredModel(typeof(ProjectionsDataWiper), typeof(ProjectionsDataWiper), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(IDangerZone), typeof(ProjectionsDataWiper), ServiceLifetime.Singleton) { CanAddMultiple = true };

        // partitions store
        yield return new DiscoveredModel(typeof(IProjectionPartionsStore), typeof(CassandraProjectionPartitionsStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionPartitionsStore), typeof(CassandraProjectionPartitionsStore), ServiceLifetime.Singleton);

        // naming
        yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);

        var projectionTypes = context.FindService<IProjectionDefinition>().ToList();
        yield return new DiscoveredModel(typeof(ProjectionsProvider), provider => new ProjectionsProvider(projectionTypes), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

        yield return new DiscoveredModel(typeof(VersionedProjectionsNaming), typeof(VersionedProjectionsNaming), ServiceLifetime.Singleton);


        //yield return new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore), ServiceLifetime.Singleton);

        //yield return new DiscoveredModel(typeof(IProjectionTableRetentionStrategy), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);
        //yield return new DiscoveredModel(typeof(RetainOldProjectionRevisions), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);
    }
}

class CassandraReplicationStrategyFactory
{
    private readonly CassandraProviderOptions options;

    public CassandraReplicationStrategyFactory(IOptionsMonitor<CassandraProviderOptions> optionsMonitor)
    {
        this.options = optionsMonitor.CurrentValue;
    }

    internal ICassandraReplicationStrategy GetReplicationStrategy()
    {
        ICassandraReplicationStrategy replicationStrategy = null;
        if (options.ReplicationStrategy.Equals("simple", StringComparison.OrdinalIgnoreCase))
        {
            replicationStrategy = new SimpleReplicationStrategy(options.ReplicationFactor);
        }
        else if (options.ReplicationStrategy.Equals("network_topology", StringComparison.OrdinalIgnoreCase))
        {
            var settings = new List<NetworkTopologyReplicationStrategy.DataCenterSettings>();
            foreach (var datacenter in options.Datacenters)
            {
                var setting = new NetworkTopologyReplicationStrategy.DataCenterSettings(datacenter, options.ReplicationFactor);
                settings.Add(setting);
            }
            replicationStrategy = new NetworkTopologyReplicationStrategy(settings);
        }

        return replicationStrategy;
    }
}
