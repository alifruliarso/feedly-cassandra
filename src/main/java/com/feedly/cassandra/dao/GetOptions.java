package com.feedly.cassandra.dao;

import java.util.Set;

public class GetOptions implements Cloneable
{
    private Object _startColumn, _endColumn; //end is inclusive
    private Set<? extends Object> _includes;
    private Set<String> _excludes; //passing excludes will omit unmapped properties
    private EColumnFilterStrategy _columnFilterStrategy = EColumnFilterStrategy.UNFILTERED;
    
    public GetOptions()
    {
        
    }
    
    public GetOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        setIncludes(includes);
        setExcludes(excludes);
    }
    
    public GetOptions(Object startColumn, Object endColumn)
    {
        setStartColumn(startColumn);
        setEndColumn(endColumn);
    }
    
    public void fetchAllColumns()
    {
        _startColumn = _endColumn = _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.UNFILTERED;
    }
    
    public Object getStartColumn()
    {
        return _startColumn;
    }
    
    public void setStartColumn(Object startColumn)
    {
        _startColumn = startColumn;
        _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.RANGE;
    }

    public Object getEndColumn()
    {
        return _endColumn;
    }
    
    public void setEndColumn(Object endColumn)
    {
        _endColumn = endColumn;
        _includes = _excludes = null;
        _columnFilterStrategy = EColumnFilterStrategy.RANGE;
    }
    
    public Set<? extends Object> getIncludes()
    {
        return _includes;
    }
    
    public void setIncludes(Set<? extends Object> includes)
    {
        _includes = includes;
        _startColumn = _endColumn = null;
        _columnFilterStrategy = EColumnFilterStrategy.INCLUDES;
    }
    
    public Set<String> getExcludes()
    {
        return _excludes;
    }

    public void setExcludes(Set<String> excludes)
    {
        _excludes = excludes;
        _startColumn = _endColumn = null;
        _columnFilterStrategy = EColumnFilterStrategy.INCLUDES;
    }
    
    EColumnFilterStrategy getColumnFilterStrategy()
    {
        return _columnFilterStrategy;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }
}
