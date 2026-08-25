using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Logging;
using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;

namespace One.Inception.Projections.Cassandra;

public interface ICassandraProjectionPartitionStoreSchema
{
    Task CreateProjectionPartitionsStorage();
}

public class CassandraProjectionPartitionStoreSchema : ICassandraProjectionPartitionStoreSchema
{
    const string CreateProjectionPartionsTableTemplate = @"CREATE TABLE IF NOT EXISTS {0}.""{1}"" (pt text, id blob, pid bigint, PRIMARY KEY ((pt,id), pid)) WITH CLUSTERING ORDER BY (pid ASC)";
    const string PartionsTableName = "projection_partitions";

    private readonly ILogger<CassandraProjectionPartitionStoreSchema> logger;
    private readonly ICassandraProvider cassandraProvider;
    private readonly ICassandraReplicationStrategy replicationStrategy;
    private CreateTablePreparedStatement _createTablePreparedStatement;

    public CassandraProjectionPartitionStoreSchema(IInceptionContextAccessor inceptionContextAccessor, ICassandraProvider cassandraProvider, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProjectionPartitionStoreSchema> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

        this.cassandraProvider = cassandraProvider;
        this.replicationStrategy = replicationStrategy;
        this.logger = logger;

        _createTablePreparedStatement = new CreateTablePreparedStatement(inceptionContextAccessor, cassandraProvider);
    }

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

    public async Task CreateProjectionPartitionsStorage()
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);

        await CreateKeyspace(session).ConfigureAwait(false);

        string keyspace = cassandraProvider.GetKeyspace();
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[Projections] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", PartionsTableName, session.Cluster.AllHosts().First().Address, keyspace);

        long t0 = Stopwatch.GetTimestamp();
        PreparedStatement createEventsTableStatement = await _createTablePreparedStatement.PrepareStatementAsync(session, PartionsTableName);

        var rs = await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

        TimeSpan elapsed = Stopwatch.GetElapsedTime(t0);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[Projections] Created table `{tableName}` in keyspace `{keyspace}`...", PartionsTableName, keyspace);

        logger.LogInformation("[Projections] Created table `{tableName}`... Maybe?! Is schema in agreement = {isSchemaInAgreement}. Time elapsed : {timeForExecution}", PartionsTableName, rs?.Info?.IsSchemaInAgreement, elapsed);
    }

    public async Task CreateKeyspace(ISession session)
    {
        long t0 = Stopwatch.GetTimestamp();

        IStatement createTableStatement = await GetCreateKeySpaceQuery(session).ConfigureAwait(false);
        var rs = await session.ExecuteAsync(createTableStatement).ConfigureAwait(false);

        TimeSpan elapsed = Stopwatch.GetElapsedTime(t0);

        logger.LogInformation("[Projections] Created keyspace from partition store. Is schema in agreement = {isSchemaInAgreement}. Time elapsed : {timeForExecution}", rs?.Info?.IsSchemaInAgreement, elapsed);
    }

    private async Task<IStatement> GetCreateKeySpaceQuery(ISession session)
    {
        string keyspace = cassandraProvider.GetKeyspace();
        string createKeySpaceQueryTemplate = replicationStrategy.CreateKeySpaceTemplate(keyspace);
        PreparedStatement createEventsTableStatement = await session.PrepareAsync(createKeySpaceQueryTemplate).ConfigureAwait(false);
        createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

        return createEventsTableStatement.Bind();
    }

    class CreateTablePreparedStatement : PreparedStatementCache
    {
        public CreateTablePreparedStatement(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => CreateProjectionPartionsTableTemplate;
    }
}
