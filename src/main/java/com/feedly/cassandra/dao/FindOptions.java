package com.feedly.cassandra.dao;

import java.util.Set;


public class FindOptions extends GetOptions implements Cloneable
{
    private static final int SIZE_UNLIMITED = Integer.MAX_VALUE;
    
    private int _maxRows = SIZE_UNLIMITED;
    
    public FindOptions()
    {
    }

    public FindOptions(Object startColumn, Object endColumn)
    {
        super(startColumn, endColumn);
    }

    public FindOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        super(includes, excludes);
    }

    public int getMaxRows()
    {
        return _maxRows;
    }
    
    public void setMaxRows(int maxRows)
    {
        if(maxRows <= 0)
            throw new IllegalArgumentException("must be positive: " + maxRows);
        _maxRows = maxRows;
    }
    
    public void setUnlimited()
    {
        _maxRows = SIZE_UNLIMITED;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }
}
