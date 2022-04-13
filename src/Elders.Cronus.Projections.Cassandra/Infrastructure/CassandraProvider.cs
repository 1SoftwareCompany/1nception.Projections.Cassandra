﻿using System;
using Cassandra;
using Microsoft.Extensions.Options;
using System.Threading;
using System.Threading.Tasks;
using DataStax = Cassandra;
using Microsoft.Extensions.Logging;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Cassandra;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        protected CassandraProviderOptions options;
        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly IInitializer initializer;
        protected readonly ILogger<CassandraProvider> logger;

        protected ICluster cluster;
        protected ISession session;

        private string baseConfigurationKeyspace;
        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.options = optionsMonitor.CurrentValue;
            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
            this.logger = logger;
        }

        public async Task<ICluster> GetClusterAsync()
        {
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

                cluster?.Shutdown(30000);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();

                await cluster.RefreshSchemaAsync().ConfigureAwait(false);
            }

            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        private static SemaphoreSlim threadGate = new SemaphoreSlim(1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

        public async Task<ISession> GetSessionAsync()
        {
            try
            {
                if (session is null || session.IsDisposed)
                {
                    await threadGate.WaitAsync(30000).ConfigureAwait(false);

                    try
                    {
                        if (session is null || session.IsDisposed)
                        {
                            logger.Info(() => "Refreshing cassandra session...");
                            try
                            {
                                ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                                session = await cluster.ConnectAsync(GetKeyspace()).ConfigureAwait(false);
                            }
                            catch (InvalidQueryException)
                            {
                                ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                                using (ISession schemaSession = await cluster.ConnectAsync().ConfigureAwait(false))
                                {
                                    string createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(GetKeyspace());
                                    IStatement createTableStatement = await GetKreateKeySpaceQuery(schemaSession).ConfigureAwait(false);
                                    await schemaSession.ExecuteAsync(createTableStatement).ConfigureAwait(false);
                                }

                                ICluster server = await GetClusterAsync().ConfigureAwait(false);
                                session = await server.ConnectAsync(GetKeyspace()).ConfigureAwait(false);
                            }
                        }
                    }
                    finally
                    {
                        threadGate?.Release();
                    }
                }
            }
            catch (ObjectDisposedException) { }


            return session;
        }

        private async Task<IStatement> GetKreateKeySpaceQuery(ISession schemaSession)
        {
            PreparedStatement createEventsTableStatement = await schemaSession.PrepareAsync(replicationStrategy.CreateKeySpaceTemplate(GetKeyspace())).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return createEventsTableStatement.Bind();
        }
    }

    class NoHintedHandOffRetryPolicy : IRetryPolicy
    {
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            if (nbRetry != 0)
                return RetryDecision.Rethrow();

            return receivedResponses >= requiredResponses && !dataRetrieved
                       ? RetryDecision.Retry(cl)
                       : RetryDecision.Rethrow();
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }
    }
}
