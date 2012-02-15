package com.feedly.cassandra.dao;

public class CollectionProperty implements Comparable<CollectionProperty>
{
    private String property;
    private Object key;
    
    public CollectionProperty(String property, Object key)
    {
        if(property == null || key == null)
            throw new IllegalArgumentException("null values not allowed");
        
        this.property = property;
        this.key = key;
    }
    
    public String getProperty()
    {
        return property;
    }
    public void setProperty(String property)
    {
        this.property = property;
    }
    public Object getKey()
    {
        return key;
    }
    public void setKey(Object key)
    {
        this.key = key;
    }

    @Override
    public int compareTo(CollectionProperty o)
    {
        return property.compareTo(o.property);
    }
}
