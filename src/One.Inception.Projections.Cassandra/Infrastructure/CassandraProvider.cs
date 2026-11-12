using One.Inception.MessageProcessing;
using One.Inception.Multitenancy;
using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Serialization;
using One.Inception.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;
using Cassandra.OpenTelemetry;

namespace One.Inception.Projections.Cassandra;

public class CassandraProvider : ICassandraProvider
{
    protected CassandraProviderOptions options;
    protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
    protected readonly IInitializer initializer;
    protected readonly ILogger<CassandraProvider> logger;

    protected ICluster cluster;
    protected ISession session;
    private readonly IInceptionContextAccessor contextAccessor;

    private string baseConfigurationKeyspace;
    public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null, IInceptionContextAccessor contextAccessor = null)
    {
        if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
        if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));

        this.options = optionsMonitor.CurrentValue;
        this.keyspaceNamingStrategy = keyspaceNamingStrategy;
        this.initializer = initializer;
        this.contextAccessor = contextAccessor;
        this.logger = logger;
    }

    private static SemaphoreSlim clusterThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

    public async Task<ICluster> GetClusterAsync()
    {
        if (cluster is null == false)
            return cluster;

        bool lockAccuired = false;
        try
        {
            lockAccuired = await clusterThreadGate.WaitAsync(30000).ConfigureAwait(false);
            if (lockAccuired == false)
                throw new TimeoutException("Unable to accquire lock for casandra cluster.");

            if (cluster is null == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = DataStax.Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = options.ConnectionString;

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                int ThirthySeconds = 1000 * 30;
                SocketOptions so = new SocketOptions();
                so.SetReadTimeoutMillis(ThirthySeconds);
                so.SetStreamMode(true);
                so.SetTcpNoDelay(true);

                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithSocketOptions(so)
                    .WithTypeSerializers(new TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithCompression(CompressionType.LZ4)
                    .WithMaxSchemaAgreementWaitSeconds(1200) // temp 20 min for seeing how much time it really takes
                    .WithPoolingOptions(new PoolingOptions()
                            .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                            .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                            .SetMaxRequestsPerConnection(options.MaxRequestsPerConnection))
                    .WithOpenTelemetryInstrumentation()
                    .Build();

                await cluster.RefreshSchemaAsync().ConfigureAwait(false);
            }
            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            return cluster;
        }
        finally
        {
            if (lockAccuired)
                clusterThreadGate?.Release();
        }
    }

    public virtual string GetKeyspace()
    {
        return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
    }

    private static SemaphoreSlim threadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

    public async Task<ISession> GetSessionAsync()
    {
        EnsureNormalAccess();
        ISession result = await GetSessionCoreAsync().ConfigureAwait(false);
        EnsureNormalAccess();
        return result;
    }

    private void EnsureNormalAccess()
    {
        InceptionContext context = contextAccessor?.Context;
        bool deleting = context?.IsTenantBeingDeleted == true;
        logger.LogTrace("Cassandra normal session check: Tenant={tenant}, HasContext={hasContext}, BeingDeleted={beingDeleted}, Allowed={allowed}",
            context?.Tenant, context != null, deleting, !deleting);
        if (deleting)
            throw new TenantBeingDeletedException(context.Tenant);
    }

    public Task<ISession> GetWipeSessionAsync()
    {
        logger.LogTrace("Cassandra wipe session requested: Tenant={tenant}, BeingDeleted={beingDeleted}. Bypassing normal access check; reusing shared session.", contextAccessor?.Context?.Tenant, contextAccessor?.Context?.IsTenantBeingDeleted);
        return GetSessionCoreAsync();
    }

    private async Task<ISession> GetSessionCoreAsync()
    {
        logger.LogTrace("Acquiring shared Cassandra session: Tenant={tenant}, CachedSessionAvailable={cachedSessionAvailable}", contextAccessor?.Context?.Tenant, session != null && !session.IsDisposed);
        bool lockAccuired = false;

        if (session is null || session.IsDisposed)
        {
            try
            {
                lockAccuired = await threadGate.WaitAsync(30000).ConfigureAwait(false);
                if (lockAccuired == false)
                    throw new TimeoutException("Unable to acquire lock for getting cassandra session.");

                if (session is null || session.IsDisposed)
                {
                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("Refreshing cassandra session...");

                    ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                    session = await cluster.ConnectAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (lockAccuired)
                    threadGate?.Release();
            }
        }

        return session;
    }
}

class ReadOnlyMemoryTypeSerializer : CustomTypeSerializer<ReadOnlyMemory<byte>>
{
    public ReadOnlyMemoryTypeSerializer() : base("it doesn't matter") { }

    public override ReadOnlyMemory<byte> Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        => buffer.AsMemory(offset, length); // we will never get here because the byte[] serializer kicks in

    public override byte[] Serialize(ushort protocolVersion, ReadOnlyMemory<byte> value) => value.ToArray();
}
