package com.feedly.cassandra.dao;


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
