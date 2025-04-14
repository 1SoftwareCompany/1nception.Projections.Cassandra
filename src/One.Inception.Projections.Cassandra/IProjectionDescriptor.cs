using System;

namespace One.Inception.Projections.Cassandra;

public interface IProjectionDescriptor /// TODO: use it to extend <see cref="CassandraProjectionStore.CalculatePartition"/>
{
    IComparable<long> GetPartition(IEvent @event);
}
