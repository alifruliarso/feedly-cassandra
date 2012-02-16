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
    public void save(V value);
    public void save(Collection<V> values);

    //passing in a value updates and returns the value
    public V loadPartial(K key, V value, Object start, Object end); //end is inclusive
    public V loadPartial(K key, V value, Set<? extends Object> includes, Set<String> excludes);//passing excludes will omit unmapped properties

    public List<V> bulkLoadPartial(List<K> keys, List<V> values, Object start, Object end); //end is inclusive
    public List<V> bulkLoadPartial(List<K> keys, List<V> values, Set<? extends Object> includes, Set<String> excludes);
    
    
    public V load(K key);
    public Collection<V> bulkLoad(Collection<K> keys);

    /*
     * convenience methods that must find at most 1 value
     */
    public V findByIndex(V template); 
    public V findByIndexPartial(V template, Object start, Object end); //end is inclusive
    public V findByIndexPartial(V template, Set<? extends Object> includes, Set<String> excludes);
    
    public Collection<V> bulkFindByIndex(V template);
    public Collection<V> bulkFindByIndexPartial(V template, Object start, Object end); //end is inclusive
    public Collection<V> bulkFindByIndexPartial(V template, Set<? extends Object> includes, Set<String> excludes);
}
