package com.feedly.cassandra.dao;

import java.util.Set;

/**
 * Options when doing a range find.
 * @author kireet
 */
public class FindBetweenOptions extends FindOptions implements Cloneable
{
    private EFindOrder _rowOrder = EFindOrder.NONE;
    
    /**
     * create options using default values.
     */
    public FindBetweenOptions()
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
    public FindBetweenOptions(Object startColumn, Object endColumn)
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
    public FindBetweenOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        super(includes, excludes);
    }
    
    /**
     * get the row order. This controls the sort order of the returned rows. 
     * @return the row order.
     */
    public EFindOrder getRowOrder()
    {
        return _rowOrder;
    }
    
    /**
     * set the row order. This controls the sort order of the returned rows. 
     * @param ordering
     */
    public void setRowOrder(EFindOrder ordering)
    {
        _rowOrder = ordering;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }
}
