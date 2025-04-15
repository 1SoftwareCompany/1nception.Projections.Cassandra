using System;
using System.Threading.Tasks;
using Cassandra;
using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace One.Inception.Projections.Cassandra;

[Obsolete("This will only be used until only until delete is complete.")]
public class CassandraProjectionStoreSchemaLegacy // only until delete is complete
{
    private readonly ILogger<CassandraProjectionStoreSchemaLegacy> logger;
    private readonly ICassandraProvider cassandraProvider;
    private readonly ICassandraReplicationStrategy replicationStrategy;
    private readonly IInceptionContextAccessor _inceptionContextAccessor;

    const string DropQueryTemplate = @"DROP TABLE IF EXISTS {0}.""{1}"";";

    private DropProjectiondStatementLegacy _dropProjectiondStatementLegacy;

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();
    public async Task<string> GetKeypaceAsync()
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);
        return session.Keyspace;
    }

    /// <summary>
    /// Used for cassandra schema changes exclusively
    /// https://issues.apache.org/jira/browse/CASSANDRA-10699
    /// https://issues.apache.org/jira/browse/CASSANDRA-11429
    /// </summary>
    /// <param name="sessionForSchemaChanges"></param>
    public CassandraProjectionStoreSchemaLegacy(IInceptionContextAccessor contextAccessor, ICassandraProvider cassandraProvider, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProjectionStoreSchemaLegacy> logger)
    {
        if (ReferenceEquals(null, cassandraProvider)) throw new ArgumentNullException(nameof(cassandraProvider));
        this.cassandraProvider = cassandraProvider;
        this.replicationStrategy = replicationStrategy;
        this.logger = logger;

        this._inceptionContextAccessor = contextAccessor;

        _dropProjectiondStatementLegacy = new DropProjectiondStatementLegacy(contextAccessor, cassandraProvider);
    }

    public async Task DropTableAsync(string location)
    {
        if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

        ISession session = await GetSessionAsync().ConfigureAwait(false);
        PreparedStatement statement = await _dropProjectiondStatementLegacy.PrepareStatementAsync(session, location).ConfigureAwait(false);
        statement = statement.SetConsistencyLevel(ConsistencyLevel.All);
        await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);
    }

    public async Task CreateProjectionsStorageAsync(string location)
    {
       
    }

    private sealed class DropProjectiondStatementLegacy : PreparedStatementCache
    {
        public DropProjectiondStatementLegacy(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => DropQueryTemplate;
    }
}
