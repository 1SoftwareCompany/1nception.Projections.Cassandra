using One.Inception.MessageProcessing;
using One.Inception.Projections.Cassandra.Infrastructure;

namespace One.Inception.Projections.Cassandra.PrepareStatements.New;

class InsertPreparedStatementNew : PreparedStatementCache
{
    public InsertPreparedStatementNew(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
    { }
    internal override string GetQueryTemplate() => QueriesConstants.InsertTemplateQuery;
}
