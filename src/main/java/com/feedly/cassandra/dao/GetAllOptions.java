package com.feedly.cassandra.dao;

import java.util.Set;


/**
 * Options when doing a mgetall.
 * @author kireet
 */
public class GetAllOptions extends GetOptions
{
    private static final int SIZE_UNLIMITED = Integer.MAX_VALUE;
    
    private int _maxRows = SIZE_UNLIMITED;
    
    /**
     * create options using default values.
     */
    public GetAllOptions()
    {
        
    }
    
    /**
     * create options, retrieving a range of columns.
     * 
     * @param startColumn the start column (inclusive)
     * @param endColumn the end column (inclusive
     * 
     * @see CollectionProperty
     */
    public GetAllOptions(Object startColumn, Object endColumn)
    {
        super(startColumn, endColumn);
    }

    /**
     * create options, retrieving a specific set of columns. Either includes or excludes 
     * should be null. Note that unmapped columns are always excluded when specifying columns.
     * 
     * @param includes the columns to include
     * @param excludes the columns to exclude.
     * 
     * @see CollectionProperty
     */
    public GetAllOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        super(includes, excludes);
    }
    
    
    /**
     * get the maximum rows to retrieve.
     * @return the max
     */
    public int getMaxRows()
    {
        return _maxRows;
    }
    
    /**
     * set the maximum rows to retrieve.
     * @param maxRows the max
     */
    public void setMaxRows(int maxRows)
    {
        if(maxRows <= 0)
            throw new IllegalArgumentException("must be positive: " + maxRows);
        _maxRows = maxRows;
    }
}
