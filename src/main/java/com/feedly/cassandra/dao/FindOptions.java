package com.feedly.cassandra.dao;

import java.util.Set;


/**
 * Options when doing a find.
 * @author kireet
 */
public class FindOptions extends GetOptions implements Cloneable
{
    private static final int SIZE_UNLIMITED = Integer.MAX_VALUE;
    
    private int _maxRows = SIZE_UNLIMITED;
    
    /**
     * create options using default values.
     */
    public FindOptions()
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
    public FindOptions(Object startColumn, Object endColumn)
    {
        super(startColumn, endColumn);
    }

    /**
     * create options, retrieving a specific set of columns. Either includes or excludes 
     * should be null.
     * 
     * @param includes the columns to include
     * @param excludes the columns to exclude.
     * 
     * @see CollectionProperty
     */
    public FindOptions(Set<? extends Object> includes, Set<String> excludes)
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
    
    /**
     * do not limit the number of rows to retrieve.
     */
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
