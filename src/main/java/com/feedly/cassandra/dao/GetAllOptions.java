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
    private boolean _getNormalColumns = true;
    
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
    
    /**
     * set the get to retrieve normal columns. defaults to true.
     * <p>
     * Cassandra stores counters in a separate column family making it difficult to efficiently scan both the normal and counter column.
     * Therefore only normal or counter columns may be retrieved. To retrieve the rest of the row, update the retrieved rows using 
     * {@link CassandraDaoBase#mget(java.util.List, java.util.List, GetOptions). In certain cases both normal and counter columns will be 
     * returned but this should not be relied upon.
     *
     */
    public void setGetNormalColumns()
    {
        _getNormalColumns = true;
    }

    /**
     * set the get to retrieve counter columns. defaults to false. 
     * <p>
     * Cassandra stores counters in a separate column family making it difficult to efficiently scan both the normal and counter column. 
     * Therefore only normal or counter columns may be retrieved. To retrieve the rest of the row, update the retrieved rows using 
     * {@link CassandraDaoBase#mget(java.util.List, java.util.List, GetOptions). In certain cases both normal and counter columns will be 
     * returned but this should not be relied upon.
     * 
     */
    public void setGetCounterColumns()
    {
        _getNormalColumns = false;
    }

    /**
     * get if retrieving normal columns.
     * @return true if the retrieving normal columns
     */
    public boolean gettingNormalColumns()
    {
        return _getNormalColumns;
    }
    
    /**
     * get if retrieving counter columns.
     * @return true if the retrieving counter columns
     */
    public boolean gettingCounterColumns()
    {
        return !_getNormalColumns;
    }
}
