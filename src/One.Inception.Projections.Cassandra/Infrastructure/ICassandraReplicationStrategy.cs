namespace One.Inception.Projections.Cassandra.Infrastructure;

public interface ICassandraReplicationStrategy
{
    string CreateKeySpaceTemplate(string keySpace);
}
