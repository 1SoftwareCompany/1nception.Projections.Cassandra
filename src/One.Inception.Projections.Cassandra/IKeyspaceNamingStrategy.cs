namespace One.Inception.Projections.Cassandra;

public interface IKeyspaceNamingStrategy
{
    string GetName(string baseConfigurationKeyspace);
}
