﻿using Microsoft.Extensions.Configuration;
using System.Collections.Generic;

namespace One.Inception.Projections.Cassandra.Infrastructure;

public class CassandraProviderOptions
{
    public CassandraProviderOptions()
    {
        Datacenters = new List<string>();
    }

    public string ConnectionString { get; set; }

    public string ReplicationStrategy { get; set; } = "simple";

    public int ReplicationFactor { get; set; } = 1;

    public List<string> Datacenters { get; set; }

    public int MaxRequestsPerConnection { get; set; } = 4096;
}

public class CassandraProviderOptionsProvider : InceptionOptionsProviderBase<CassandraProviderOptions>
{
    public const string SettingKey = "inception:projections:cassandra";

    public CassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

    public override void Configure(CassandraProviderOptions options)
    {
        configuration.GetSection(SettingKey).Bind(options);
    }
}
