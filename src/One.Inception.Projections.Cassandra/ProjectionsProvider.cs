using System;
using System.Collections.Generic;

namespace One.Inception.Projections.Cassandra;

public class ProjectionsProvider
{
    private readonly IEnumerable<Type> projectionTypes;

    public ProjectionsProvider(IEnumerable<Type> projectionTypes)
    {
        this.projectionTypes = projectionTypes;
    }

    public IEnumerable<Type> GetProjections()
    {
        return projectionTypes;
    }
}
