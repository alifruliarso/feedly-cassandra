package com.feedly.cassandra.dao;

import java.util.Collection;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

public class InlineRepairStrategy implements IStaleIndexValueStrategy
{
    private static final Logger _logger = LoggerFactory.getLogger(InlineRepairStrategy.class.getName());
    
    private static final DynamicCompositeSerializer SER_COMPOSITE = new DynamicCompositeSerializer();
    
    private IKeyspaceFactory _keyspaceFactory;
    
    public void setKeyspaceFactory(IKeyspaceFactory keyspaceFactory)
    {
        _keyspaceFactory = keyspaceFactory;
    }

    @Override
    public void handle(EntityMetadata<?> entity, IndexMetadata index, Collection<StaleIndexValue> values)
    {
        try
        {
            Mutator<DynamicComposite> mutator = HFactory.createMutator(_keyspaceFactory.createKeyspace(), SER_COMPOSITE);
            
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
