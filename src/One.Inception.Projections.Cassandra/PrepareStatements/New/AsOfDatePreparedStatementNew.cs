using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;

namespace One.Inception.Projections.Cassandra.PrepareStatements.New;

class AsOfDatePreparedStatementNew : PreparedStatementCache
{
    public AsOfDatePreparedStatementNew(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
    { }
    internal override string GetQueryTemplate() => QueriesConstants.GetAsOfTemplateQuery;
}
