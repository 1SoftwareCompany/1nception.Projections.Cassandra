﻿using System;
using System.Runtime.Serialization;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    [DataContract(Name = "4b1574d0-87b2-4469-bdf2-0fc89110b421")]
    public class EventOrigin
    {
        EventOrigin() { }

        public EventOrigin(string aggregateRootId, int aggregateRevision, int aggregateEventPosition, long timestamp)
        {
            if (ReferenceEquals(null, aggregateRootId)) throw new ArgumentNullException(nameof(aggregateRootId));
            if (aggregateRevision <= 0) throw new ArgumentException("Invalid revision", nameof(aggregateRevision));
            if (aggregateEventPosition < 0) throw new ArgumentException("Invalid event position", nameof(aggregateEventPosition));

            AggregateRootId = aggregateRootId;
            AggregateRevision = aggregateRevision;
            AggregateEventPosition = aggregateEventPosition;
            Timestamp = timestamp;
        }

        [DataMember(Order = 1)]
        public string AggregateRootId { get; private set; }

        [DataMember(Order = 2)]
        public int AggregateRevision { get; private set; }

        /// <summary>
        /// This is the position of the event insite a specific <see cref="AggregateRevision"/>
        /// </summary>
        [DataMember(Order = 3)]
        public int AggregateEventPosition { get; private set; }

        [DataMember(Order = 4)]
        public long Timestamp { get; set; }
    }
}
