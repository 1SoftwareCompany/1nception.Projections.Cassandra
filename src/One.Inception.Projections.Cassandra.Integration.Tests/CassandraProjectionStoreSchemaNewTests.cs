using Cassandra;
using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace One.Inception.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionStoreSchemaNewTests
{
    ISession session;
    ICluster cluster;
    CassandraProjectionStoreSchema projectionStore;
    private Mock<IInceptionContextAccessor> contextAccessor;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        cluster = await cassandra.GetClusterAsync();

        contextAccessor = new Mock<IInceptionContextAccessor>();
        var serviceProviderMock = new Mock<IServiceProvider>();
        var cronusContext = new InceptionContext("test", serviceProviderMock.Object);
        contextAccessor.SetupProperty(x => x.Context, cronusContext);
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        projectionStore = new CassandraProjectionStoreSchema(contextAccessor.Object, cassandra, replicatoinStrategy, NullLogger<CassandraProjectionStoreSchema>.Instance);
    }

    [Test]
    public void CreateProjectionStorageNewAsync()
    {
        Assert.DoesNotThrowAsync(async () => await projectionStore.CreateProjectionsStorageAsync("tests"));

        var tables = cluster.Metadata.GetTables(session.Keyspace);
        Assert.That(tables, Contains.Item("tests"));
    }
}
