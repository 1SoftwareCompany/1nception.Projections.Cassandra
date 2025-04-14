using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;

namespace One.Inception.Projections.Cassandra.PrepareStatements.New;

class PreparedStatementToGetProjectionNew : PreparedStatementCache
{
    public PreparedStatementToGetProjectionNew(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
    { }
    internal override string GetQueryTemplate() => QueriesConstants.GetTemplateQuery;
}
