using System;
using One.Inception.MessageProcessing;

namespace One.Inception.Projections.Cassandra;

public sealed class KeyspacePerTenantKeyspace : IKeyspaceNamingStrategy
{
    private readonly IInceptionContextAccessor contextAccessor;

    public KeyspacePerTenantKeyspace(IInceptionContextAccessor contextAccessor)
    {
        this.contextAccessor = contextAccessor;
    }

    public string GetName(string baseConfigurationKeyspace)
    {
        string tenantPrefix = string.IsNullOrEmpty(contextAccessor.Context.Tenant) ? string.Empty : $"{contextAccessor.Context.Tenant}_";
        var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
        if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

        return keyspace;
    }
}
