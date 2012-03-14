package com.feedly.cassandra.dao;

import java.util.List;

//use for filter on read operations
class IndexedValue<V> 
{
    private final List<Object> _indexValues;
    private final V _value;
    
    public IndexedValue(List<Object> indexValues, V value)
    {
        _indexValues = indexValues;
        _value = value;
    }

    public List<Object> getIndexValues()
    {
        return _indexValues;
    }

    public V getValue()
    {
        return _value;
    }
}
