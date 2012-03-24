package com.feedly.cassandra.dao;

import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * Represents a stale index values. Stale values occur when an indexed property is updated.
 * 
 * @author kireet
 * @see IStaleIndexValueStrategy
 * @see OnlineRepairStrategy
 * @see OfflineRepairStrategy
 */
public class StaleIndexValue
{
    private final DynamicComposite _rowKey, _columnName;
    private final long _clock;

    StaleIndexValue(DynamicComposite rowKey, DynamicComposite column, long clock)
    {
        _rowKey = rowKey;
        _columnName = column;
        _clock = clock;
    }
    
    /**
     * get the key of the <b>index</b> row i.e. the "partition key"
     * @return
     */
    public DynamicComposite getRowKey()
    {
        return _rowKey;
    }

    /**
     * get the column name of the <b>index</b> column. This contains the stale index value as well as the row key.
     * @return
     */
    public DynamicComposite getColumnName()
    {
        return _columnName;
    }
    
    /**
     * The clock when the stale value occurred.
     * @return the clock
     */
    public long getClock()
    {
        return _clock;
    }
}
