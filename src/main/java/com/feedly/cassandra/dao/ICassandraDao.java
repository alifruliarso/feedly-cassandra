package com.feedly.cassandra.dao;

import java.util.Collection;
import java.util.List;
import java.util.Set;

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

    //passing in a value updates and returns the value
    public V get(K key, V value, Object start, Object end); //end is inclusive
    public V get(K key, V value, Set<? extends Object> includes, Set<String> excludes);//passing excludes will omit unmapped properties

    public List<V> mget(List<K> keys, List<V> values, Object start, Object end); //end is inclusive
    public List<V> mget(List<K> keys, List<V> values, Set<? extends Object> includes, Set<String> excludes);
    
    
    public V get(K key);
    public Collection<V> mget(Collection<K> keys);

    /*
     * convenience methods that must find at most 1 value
     */
    public V find(V template); 
    public V find(V template, Object start, Object end); //end is inclusive
    public V find(V template, Set<? extends Object> includes, Set<String> excludes);
    
    public Collection<V> mfind(V template);
    public Collection<V> mfind(V template, Object start, Object end); //end is inclusive
    public Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes);
}
