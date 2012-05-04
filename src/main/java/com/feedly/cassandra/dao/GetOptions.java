package com.feedly.cassandra.dao;

import java.util.Set;

/**
 * Options when doing a get.
 * @author kireet
 */
public class GetOptions extends OptionsBase
{
    private Object _startColumn, _endColumn; //end is inclusive
    private Set<? extends Object> _includes;
    private Set<String> _excludes; //passing excludes will omit unmapped properties
    private EColumnFilterStrategy _columnFilterStrategy = EColumnFilterStrategy.UNFILTERED;
    
    /**
     * create options using default values.
     */
    public GetOptions()
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
    public GetOptions(Object startColumn, Object endColumn)
    {
        setStartColumn(startColumn);
        setEndColumn(endColumn);
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
    public GetOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        setIncludes(includes);
        setExcludes(excludes);
    }
    
    /**
     * fetch all columns in the row(s)
     */
    public void fetchAllColumns()
    {
        _startColumn = _endColumn = _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.UNFILTERED;
    }
    
    /**
     * get the start column (used when fetching a column range)
     * @return the start column (inclusive)
     */
    public Object getStartColumn()
    {
        return _startColumn;
    }
    
    /**
     * set the start column (used when fetching a column range)
     * @param startColumn the start column (inclusive)
     * @see CollectionProperty
     */
    public void setStartColumn(Object startColumn)
    {
        _startColumn = startColumn;
        _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.RANGE;
    }

    /**
     * get the end column (used when fetching a column range)
     * @return the end column (inclusive)
     */
    public Object getEndColumn()
    {
        return _endColumn;
    }
    
    /**
     * set the end column (used when fetching a column range)
     * @param endColumn the end column (inclusive)
     * @see CollectionProperty
     */
    public void setEndColumn(Object endColumn)
    {
        _endColumn = endColumn;
        _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.RANGE;
    }
    
    /**
     * Get the columns to fetch when fetching a set of columns, either includes or excludes should be set, not both.
     * @return the includes
     */
    public Set<? extends Object> getIncludes()
    {
        return _includes;
    }
    
    /**
     * Set the columns to fetch when fetching a set of columns, either includes or excludes should be set, not both.
     * @param includes - the includes
     * @see CollectionProperty
     */
    public void setIncludes(Set<? extends Object> includes)
    {
        _includes = includes;
        _startColumn = _endColumn = null;
        _columnFilterStrategy = EColumnFilterStrategy.INCLUDES;
    }
    
    /**
     * Get the columns to exclude when fetching a set of columns, either includes or excludes should be set, not both.
     * @return the includes
     */
    public Set<String> getExcludes()
    {
        return _excludes;
    }
    
    /**
     * Set the columns to exclude when fetching a set of columns, either includes or excludes should be set, not both. Note that unmapped
     * columns are always excluded when specifying columns.
     * @param includes - the includes
     * @see CollectionProperty
     */

    public void setExcludes(Set<String> excludes)
    {
        _excludes = excludes;
        _startColumn = _endColumn = null;
        _columnFilterStrategy = EColumnFilterStrategy.INCLUDES;
    }
    
    //convenience method to easily detech the column fetch option specified
    EColumnFilterStrategy getColumnFilterStrategy()
    {
        return _columnFilterStrategy;
    }
    
}
