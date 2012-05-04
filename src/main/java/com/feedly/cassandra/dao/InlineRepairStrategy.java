package com.feedly.cassandra.dao;

import java.util.Collection;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

/**
 * corrects index "inline", i.e. during the find operation.
 * @author kireet
 */
public class InlineRepairStrategy implements IStaleIndexValueStrategy
{
    private static final Logger _logger = LoggerFactory.getLogger(InlineRepairStrategy.class.getName());
    
    private static final DynamicCompositeSerializer SER_COMPOSITE = new DynamicCompositeSerializer();
    
    @Override
    public void handle(EntityMetadata<?> entity, IndexMetadata index, Keyspace keyspace, Collection<StaleIndexValue> values)
    {
        try
        {
            Mutator<DynamicComposite> mutator = HFactory.createMutator(keyspace, SER_COMPOSITE);
            
            _logger.debug("deleting {} stale values from {}", values.size(), entity.getIndexFamilyName());
            for(StaleIndexValue value : values)
            {
                mutator.addDeletion(value.getRowKey(), entity.getIndexFamilyName(), value.getColumnName(), SER_COMPOSITE, value.getClock());
                _logger.trace("deleting stale value {}{}:{} ({})", new Object[] { entity.getIndexFamilyName(), value.getRowKey(), value.getColumnName(), value.getClock() });
            }
            
            mutator.execute();
        }
        catch(Exception ex)
        {
            _logger.error("Problem encountered while deleting stale index values, functionality should not be impacted", ex);
        }
    }
}
