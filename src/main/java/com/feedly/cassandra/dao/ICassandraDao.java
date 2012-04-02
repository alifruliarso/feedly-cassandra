package com.feedly.cassandra.dao;

import java.util.Collection;
import java.util.List;

import com.feedly.cassandra.entity.enhance.EntityTransformerTask;

/**
 * Manages object persistence in the Cassandra datastore. This object manages saving, loading (key lookup), and finding (secondary index 
 * lookup) objects in the database. Implementations should be thread-safe. Entity classes should be enhanced using 
 * {@link EntityTransformerTask} at compile time.
 * 
 * @author kireet
 *
 *
 * @param <K> - the key type - this should match the type of the entity's row key
 * @param <V> - the entity type
 */
public interface ICassandraDao<K, V>
{
    /**
     * store the entity in cassandra. Only set properties are stored.
     * @param value the entity to store.
     */
    public void put(V value);

    /**
     * store multiple entities in cassandra. This is functionally equivalent to {@link #put(Object)}, the main difference will be improved
     * performance by batching the writes.
     * @param values the entities to store.
     */
    public void mput(Collection<V> values);

    /**
     * fetch an entity by row key, using default options
     * @param key the row key.
     * @return the entity, null if non-existent
     */
    public V get(K key);

    /**
     * fetch an entity by row key, using specified options
     * @param key the row key.
     * @param value a previous value. If non null this value is updated and returned. One can do this to do multiple column range gets.
     * @param options the get options.
     * @return the entity, null if non-existent
     */
    public V get(K key, V value, GetOptions options); //end is inclusive

    /**
     * fetch entities by row key, using default options. This is functionally equivalent to {@link #get(Object)}, the main difference will 
     * be improved performance by batching the reads.
     * @param keys the row keys
     * @return the entities.
     */
    public Collection<V> mget(Collection<K> keys);

    /**
     * fetch all entities by row key, using default options. Use with care when dealing with large column families.
     * 
     * @return the entities.
     */
    public Collection<V> mgetAll();

    /**
     * fetch all entities by row key, using specified options. Use with care when dealing with large column families.  
     * 
     * @param options the get options.
     * @return the entities.
     */
    public Collection<V> mgetAll(GetOptions options);


    /**
     * fetch entities by row key, using specified options. This is functionally equivalent to {@link #get(Object, Object, GetOptions)}, the
     *  main difference will be improved performance by batching the reads.
     * @param keys the row keys.
     * @param value a previous value. If non null this value is updated and returned. One can do this to do multiple column range mgets.
     * @param options the get options.
     * @return the entity, null if non-existent
     */
    public List<V> mget(List<K> keys, List<V> values, GetOptions options); //end is inclusive
    
    /**
     * Find an entity using a secondary index. 
     * @param template entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval.
     * @return The single value, or null if none exists.
     * @throws IllegalStateException if multiple values exist
     */
    public V find(V template); 

    /**
     * Find an entity using a secondary index, using the specified options. 
     * @param template entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval, although additional non indexed values may be set and used for filtering.
     * @return The single value, or null if none exists.
     * @throws IllegalStateException if multiple values exist
     */
    public V find(V template, FindOptions options); 
    
    /**
     * Find entities using a secondary index.
     * @param template entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval, although additional non indexed values may be set and used for filtering.
     * @return the values matching the template. The Collection may be lazy loaded, so {@link Collection#size()} should generally not be used 
     * unless the result size is known to be relatively small.
     */
    public Collection<V> mfind(V template);

    /**
     * Find entities using a secondary index, using the specified options.
     * @param template entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval, although additional non indexed values may be set and used for filtering.
     * @return the values matching the template. The Collection may be lazy loaded, so {@link Collection#size()} should generally not be used 
     * unless the result size is known to be relatively small.
     */
    public Collection<V> mfind(V template, FindOptions options); 
    
    /**
     * Find entities using a range of secondary index values. The start and end templates should generally have the same properties set so
     * an index can be chosen for retrieval. When calculating the range, the index columns are considered together, while non-indexed columns
     * are considered individually.
     * 
     * @param startTemplate entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * @param endTemplate entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval, although additional non indexed values may be set and used for filtering.
     * @return the values matching the template. The Collection may be lazy loaded, so {@link Collection#size()} should generally not be used 
     * unless the result size is known to be relatively small.
     */
    public Collection<V> mfindBetween(V startTemplate, V endTemplate);
    
    /**
     * Find entities using a range of secondary index values, using the specified options. The start and end templates should generally have 
     * the same properties set so an index can be chosen for retrieval. When calculating the range, the index columns are considered together, 
     * while non-indexed columns are considered individually.
     * 
     * @param startTemplate entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * @param endTemplate entity that fetched rows should match. The template should set values such that a single index can be chosen for 
     * retrieval, although additional non indexed values may be set and used for filtering.
     * @param options the options
     * @return the values matching the template. The Collection may be lazy loaded, so {@link Collection#size()} should generally not be used 
     * unless the result size is known to be relatively small.
     */
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, FindBetweenOptions options); 
    
    
    /**
     * Delete an entity.
     * @param key the key of the entity to delete.
     */
    public void delete(K key);
    
    /**
     * Delete multiple entities. This is functionally equivalent to {@link #delete(Object)}, the
     *  main difference will be improved performance by batching the deletes.
     *  
     * @param keys the keys of the entities to delete.
     */
    public void mdelete(Collection<K> keys);
}
