package com.feedly.cassandra.dao;

import me.prettyprint.hector.api.beans.DynamicComposite;

public class RangeIndexQueryPartitionResult implements Cloneable
{
    private DynamicComposite _partitionKey;
    private DynamicComposite _startCol, _endCol;
    private boolean hasMore;
    
    public DynamicComposite getPartitionKey()
    {
        return _partitionKey;
    }
    public void setPartitionKey(DynamicComposite partitionKey)
    {
        _partitionKey = partitionKey;
    }
    public DynamicComposite getStartCol()
    {
        return _startCol;
    }
    public void setStartCol(DynamicComposite startCol)
    {
        _startCol = startCol;
    }
    public DynamicComposite getEndCol()
    {
        return _endCol;
    }
    public void setEndCol(DynamicComposite endCol)
    {
        _endCol = endCol;
    }
    public boolean hasMore()
    {
        return hasMore;
    }
    public void setHasMore(boolean hasMore)
    {
        this.hasMore = hasMore;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        RangeIndexQueryPartitionResult copy = (RangeIndexQueryPartitionResult) super.clone();
        copy._partitionKey = new DynamicComposite(_partitionKey);
        copy._startCol = new DynamicComposite(_startCol);
        copy._startCol.setEquality(_startCol.getEquality());
        copy._endCol = new DynamicComposite(_endCol);
        copy._endCol.setEquality(_endCol.getEquality());
        
        return copy;
    }
}
