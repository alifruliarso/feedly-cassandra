package com.feedly.cassandra.dao;

import me.prettyprint.hector.api.beans.DynamicComposite;

public class StaleIndexValue
{
    private final DynamicComposite _rowKey, _columnName;
    private final long _clock;

    public StaleIndexValue(DynamicComposite rowKey, DynamicComposite column, long clock)
    {
        _rowKey = rowKey;
        _columnName = column;
        _clock = clock;
    }
    
    public DynamicComposite getRowKey()
    {
        return _rowKey;
    }
    public DynamicComposite getColumnName()
    {
        return _columnName;
    }
    public long getClock()
    {
        return _clock;
    }
}
