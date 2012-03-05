package com.feedly.cassandra.dao;

import java.util.Collection;
import java.util.List;

/**
 * Manages object persistence in the Cassandra datastore. This object manages saving, loading (key lookup), and finding (secondary index 
 * lookup) objects in the database. Implementations should be thread-safe
 * 
 * @author kireet
 *
 *
 * @param <K> - the key type
 * @param <V> - the object type
 */
public interface ICassandraDao<K, V>
{
    public void put(V value);
    public void mput(Collection<V> values);

    public V get(K key);
    //passing in a value updates and returns the value
    public V get(K key, V value, GetOptions options); //end is inclusive

    public Collection<V> mget(Collection<K> keys);
    public List<V> mget(List<K> keys, List<V> values, GetOptions options); //end is inclusive
    
    /*
     * convenience methods that must find at most 1 value
     */
    public V find(V template); 
    public V find(V template, FindOptions options); 
    
    public Collection<V> mfind(V template);
    public Collection<V> mfind(V template, FindOptions options); 
    
    public Collection<V> mfindBetween(V startTemplate, V endTemplate);
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, FindBetweenOptions options); 
}
