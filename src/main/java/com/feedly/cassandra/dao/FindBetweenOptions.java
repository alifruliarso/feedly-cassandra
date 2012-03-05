package com.feedly.cassandra.dao;

import java.util.Set;

public class FindBetweenOptions extends FindOptions implements Cloneable
{
    private EFindOrder _rowOrder = EFindOrder.NONE;
    
    public FindBetweenOptions()
    {
        
    }

    public FindBetweenOptions(Object startColumn, Object endColumn)
    {
        super(startColumn, endColumn);
    }

    public FindBetweenOptions(Set<? extends Object> includes, Set<String> excludes)
    {
        super(includes, excludes);
    }
    
    public EFindOrder getRowOrder()
    {
        return _rowOrder;
    }
    
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
