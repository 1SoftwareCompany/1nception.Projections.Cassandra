using Microsoft.Extensions.Configuration;

namespace One.Inception.Projections.Cassandra.Infrastructure;

public class TableRetentionOptionsProvider : InceptionOptionsProviderBase<TableRetentionOptions>
{
    public const string SettingKey = "inception:projections:cassandra:tableretention";

    public TableRetentionOptionsProvider(IConfiguration configuration) : base(configuration) { }

    public override void Configure(TableRetentionOptions options)
    {
        configuration.GetSection(SettingKey).Bind(options);
    }
}
