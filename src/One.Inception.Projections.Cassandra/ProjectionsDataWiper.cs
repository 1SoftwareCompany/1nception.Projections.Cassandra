using System;
using System.Threading.Tasks;
using Cassandra;
using One.Inception.DangerZone;
using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace One.Inception.Projections.Cassandra;

public class ProjectionsDataWiper : IDangerZone
{
    private readonly ICassandraProvider cassandraProvider;
    private readonly IInceptionContextAccessor inceptionContextAccessor;
    private readonly ILogger<ProjectionsDataWiper> logger;

    private DropKeyspaceQuery _dropKeyspaceQuery;

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

    public ProjectionsDataWiper(IInceptionContextAccessor inceptionContextAccessor, ICassandraProvider cassandraProvider, ILogger<ProjectionsDataWiper> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

        this.cassandraProvider = cassandraProvider;
        this.inceptionContextAccessor = inceptionContextAccessor;
        this.logger = logger;

        _dropKeyspaceQuery = new DropKeyspaceQuery(inceptionContextAccessor, cassandraProvider);
    }

    public async Task WipeDataAsync(string tenant)
    {
        try
        {
            if (tenant.Equals(inceptionContextAccessor.Context.Tenant, StringComparison.Ordinal) == false)
            {
                logger.LogError("Tenant mismatch. The tenant to be wiped is different from the current tenant.");
                return;
            }

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _dropKeyspaceQuery.PrepareWipeStatementAsync(session).ConfigureAwait(false);

            logger.LogInformation("Wiping projections data for tenant {tenant} query {query}", tenant, statement.QueryString);

            var bs = statement.Bind().SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);

            logger.LogInformation("Projections data wiped for tenant {tenant}", tenant);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to drop keyspace.");
            throw;
        }
    }

    class DropKeyspaceQuery : PreparedStatementCache
    {
        private const string Template = @"DROP KEYSPACE IF EXISTS {0};";

        public DropKeyspaceQuery(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }
}
