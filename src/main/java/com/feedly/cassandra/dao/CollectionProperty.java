package com.feedly.cassandra.dao;

/**
 * CollectionProperty objects are used when loading a subset of values from a collection stored within an entity. For Lists, the key should
 * be an Integer. For maps, it should be the map key. For embedded beans, it should be the <b>physical</b> column name.
 * 
 * @author kireet
 * @see ICassandraDao
 */
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
