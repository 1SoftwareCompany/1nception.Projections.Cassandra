using One.Inception.Projections.Cassandra.Infrastructure;

namespace One.Inception.Projections.Cassandra;

public interface ICassandraProjectionStoreSettings
{
    ProjectionsProvider ProjectionsProvider { get; }
    ICassandraProvider CassandraProvider { get; }
    IProjectionPartionsStore Partititons { get; }
    ISerializer Serializer { get; }
    VersionedProjectionsNaming ProjectionsNamingStrategy { get; }
}
