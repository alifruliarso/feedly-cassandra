package com.feedly.cassandra.dao;

import java.util.Collection;

import me.prettyprint.hector.api.Keyspace;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

/**
 * provides a plugin to handle stale index values. Stale index values happen when an indexed value is updated. They are detected when an
 * index value is read and filter on read checks detect a mismatch between the indexed value and the actual row value. Typically strategies
 * will delete the indexed value.
 * 
 * @author kireet
 */
public interface IStaleIndexValueStrategy
{
    public void handle(EntityMetadata<?> entity, 
                       IndexMetadata index, 
                       Keyspace keyspace, 
                       Collection<StaleIndexValue> values);
}
