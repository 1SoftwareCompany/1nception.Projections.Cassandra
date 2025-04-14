using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using One.Inception.AutoUpdates;
using One.Inception.Projections.Cassandra.Infrastructure;
using One.Inception.MessageProcessing;
using Cassandra;
using System.Linq;
using System.Text;

namespace One.Inception.Projections.Cassandra;

public sealed class AutoUpdateToV12DeleteTable : IAutoUpdate
{
    private readonly ICassandraProvider _provider;
    private readonly ProjectionFinder _projectionFinder;
    private List<ProjectionVersion> liveOnlyProjections;
    private readonly ILogger<AutoUpdateToV12DeleteTable> _logger;
    private readonly CassandraProjectionStoreSchemaLegacy _oldSchema;
    private readonly VersionedProjectionsNaming _naming;
    private readonly IInceptionContextAccessor _inceptionContextAccessor;

    public AutoUpdateToV12DeleteTable(ICassandraProvider provider, ProjectionFinder projectionFinder, ILogger<AutoUpdateToV12DeleteTable> looger, CassandraProjectionStoreSchemaLegacy oldSchema, VersionedProjectionsNaming naming, IInceptionContextAccessor inceptionContextAccessor)
    {
        _provider = provider;
        _projectionFinder = projectionFinder;
        _logger = looger;
        _oldSchema = oldSchema;
        _naming = naming;
        _inceptionContextAccessor = inceptionContextAccessor;
    }

    public uint ExecutionSequence => 1;

    public string Name => "DeleteOldProjectionsAndCreateTheSameTableWithNewSchema";

    /// <summary>
    /// !!!!!!!! Make sure wherever we are updating you are loading from the correct `new` tables and they are done
    /// </summary>
    /// <returns></returns>
    public async Task<bool> ApplyAsync()
    {
        using (_logger.BeginScope(s => s.AddScope("tenant", _inceptionContextAccessor.Context.Tenant)))
        {
            ISession session = await _provider.GetSessionAsync().ConfigureAwait(false);
            List<ProjectionVersion> liveProjections = GetAllLiveVersions();

            foreach (ProjectionVersion live in liveProjections)
            {
                string originalLocation = _naming.GetColumnFamily(live);

                await _oldSchema.DropTableAsync(originalLocation);
                _logger.LogWarning("Deleted OLD ORIGINAL TABLE {live} dai boje...", live);

                _logger.LogInformation("-----------------FINISHED WITH {@live}", live);
            }

            _logger.LogInformation("-----------------ALL DONE WITH tenant {tenant}", _inceptionContextAccessor.Context.Tenant);
        }
        return true;
    }

    public List<ProjectionVersion> GetAllLiveVersions()
    {
        if (liveOnlyProjections is null || liveOnlyProjections.Count == 0)
        {
            var allProjections = _projectionFinder.GetProjectionVersionsToBootstrap();
            liveOnlyProjections = allProjections.Where(ver => ver.Status == ProjectionStatus.Live).ToList();

            var nonLiveProjections = allProjections.Except(liveOnlyProjections);
            if (nonLiveProjections.Any())
            {
                StringBuilder reporter = new StringBuilder();
                reporter.AppendLine("These are non live projections. Should i just delete them??????.");
                foreach (var projection in nonLiveProjections)
                {
                    reporter.AppendLine(projection.ToString());
                }
                _logger.LogWarning(reporter.ToString());
            }
        }

        return liveOnlyProjections;
    }
}
