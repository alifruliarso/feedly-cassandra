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

    private int _statsSize = MBeanUtils.DEFAULT_STATS_SIZE;
    private OperationStatistics _stats;
    
    public void setStatsSize(int s)
    {
        _statsSize = s;
    }
    
    public void init()
    {
        _stats = new OperationStatistics(_statsSize);
    }
    
    public OperationStatistics stats()
    {
        return _stats;
    }
    
    @Override
    public void handle(EntityMetadata<?> entity, IndexMetadata index, Keyspace keyspace, Collection<StaleIndexValue> values)
    {
        try
        {
            long startTime = System.nanoTime();
            Mutator<DynamicComposite> mutator = HFactory.createMutator(keyspace, SER_COMPOSITE);
            
            int size = values.size();
            _logger.debug("deleting {} stale values from {}", size, entity.getIndexFamilyName());
            for(StaleIndexValue value : values)
            {
                mutator.addDeletion(value.getRowKey(), entity.getIndexFamilyName(), value.getColumnName(), SER_COMPOSITE, value.getClock());
                _logger.trace("deleting stale value {}{}:{} ({})", new Object[] { entity.getIndexFamilyName(), value.getRowKey(), value.getColumnName(), value.getClock() });
            }
            
            mutator.execute();
            _stats.addRecentTiming(System.nanoTime() - startTime);
            _stats.incrNumOps(1);
            _stats.incrNumCassandraOps(size);
            _stats.incrNumCols(size);
            _stats.incrNumRows(size);
        }
        catch(Exception ex)
        {
            _logger.error("Problem encountered while deleting stale index values, functionality should not be impacted", ex);
        }
    }
}
