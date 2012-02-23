package com.feedly.cassandra.dao;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

public class CassandraDaoBase<K, V> implements ICassandraDao<K, V>
{
    private static final Logger _logger = LoggerFactory.getLogger(CassandraDaoBase.class.getName());
    
    static final int COL_RANGE_SIZE = 100;
    static final int ROW_RANGE_SIZE = 100;
    private final EntityMetadata<V> _entityMeta;
    private GetHelper<K, V> _getHelper;
    private FindHelper<K, V> _findHelper;
    private PutHelper<K, V> _putHelper;
    private IKeyspaceFactory _keyspaceFactory;
    private IStaleIndexValueStrategy _staleIndexValueStrategy;
    
    protected CassandraDaoBase()
    {
        this(null, null);
    }

    @SuppressWarnings("unchecked")
    protected CassandraDaoBase(Class<K> keyClass, Class<V> valueClass)
    {
        if(keyClass == null)
        {
            keyClass = (Class<K>) getGenericClass(0);

            if(keyClass == null)
                throw new IllegalStateException("could not determine key class");
        }

        if(valueClass == null)
        {
            valueClass = (Class<V>) getGenericClass(1);

            if(valueClass == null)
                throw new IllegalStateException("could not determine value class");
        }


        _entityMeta = new EntityMetadata<V>(valueClass);

        if(!_entityMeta.getKeyMetadata().getFieldType().equals(keyClass))
            throw new IllegalArgumentException(String.format("DAO/entity key mismatch: %s != %s",
                                                             keyClass.getName(),
                                                             _entityMeta.getKeyMetadata().getFieldType().getName()));



        _logger.info(getClass().getSimpleName(), new Object[] {"[", _entityMeta.getFamilyName(), "] -> ", _entityMeta.toString()});
    }

    public void setKeyspaceFactory(IKeyspaceFactory keyspaceFactory)
    {
        _keyspaceFactory = keyspaceFactory;
    }

    public void setStaleValueIndexStrategy(IStaleIndexValueStrategy strategy)
    {
        _staleIndexValueStrategy = strategy;
    }
    
    public void init()
    {
        if(_keyspaceFactory == null)
            throw new IllegalStateException("keyspace factory not set");
            
        if(_staleIndexValueStrategy == null)
        {
            _staleIndexValueStrategy = 
                    new IStaleIndexValueStrategy()
                    {
                        @Override
                        public void handle(EntityMetadata<?> entity, IndexMetadata index, Collection<StaleIndexValue> values)
                        {
                            _logger.warn("not handling {} stale values for {}", values.size(), entity.getFamilyName());
                        }
                
                    };
        }
        
        _getHelper = new GetHelper<K, V>(_entityMeta, _keyspaceFactory);
        _findHelper = new FindHelper<K, V>(_entityMeta, _keyspaceFactory, _staleIndexValueStrategy);
        _putHelper = new PutHelper<K, V>(_entityMeta, _keyspaceFactory);
    }
    
    private Class<?> getGenericClass(int idx)
    {
        Class<?> clazz = getClass();

        while(clazz != null)
        {
            if(clazz.getGenericSuperclass() instanceof ParameterizedType)
            {
                ParameterizedType t = (ParameterizedType) clazz.getGenericSuperclass();

                Type[] types = t.getActualTypeArguments();

                if(!(types[idx] instanceof Class<?>))
                    clazz = clazz.getSuperclass();
                else
                {
                    return (Class<?>) types[idx];
                }
            }
            else
                clazz = clazz.getSuperclass();
        }

        return null;
    }

    @Override
    public void put(V value)
    {
        _putHelper.put(value);
    }

    @Override
    public void mput(Collection<V> values)
    {
        _putHelper.mput(values);
    }

    @Override
    public V get(K key, V value, Object start, Object end)
    {
        return _getHelper.get(key, value, start, end);
    }

    @Override
    public V get(K key, V value, Set<? extends Object> includes, Set<String> excludes)
    {
        return _getHelper.get(key, value, includes, excludes);
    }

    @Override
    public List<V> mget(List<K> keys, List<V> values, Object start, Object end)
    {
        return _getHelper.mget(keys, values, start, end);
    }

    @Override
    public List<V> mget(List<K> keys, List<V> values, Set<? extends Object> includes, Set<String> excludes)
    {
        return _getHelper.mget(keys, values, includes, excludes);
    }

    @Override
    public V get(K key)
    {
        return _getHelper.get(key);
    }

    @Override
    public Collection<V> mget(Collection<K> keys)
    {
        return _getHelper.mget(keys);
    }

    
    @Override
    public V find(V template)
    {
        return _findHelper.find(template);
    }

    @Override
    public V find(V template, Object start, Object end)
    {
        return _findHelper.find(template, start, end);
    }

    @Override
    public V find(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        return _findHelper.find(template, includes, excludes);
    }

    @Override
    public Collection<V> mfind(V template)
    {
        return _findHelper.mfind(template);
    }

    @Override
    public Collection<V> mfind(V template, Object start, Object end)
    {
        return _findHelper.mfind(template, start, end);
    }

    @Override
    public Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        return _findHelper.mfind(template, includes, excludes);
    }

    @Override
    public Collection<V> mfindBetween(V startTemplate, V endTemplate)
    {
        return _findHelper.mfindBetween(startTemplate, endTemplate);
    }

    @Override
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, Object startColumn, Object endColumn)
    {
        return _findHelper.mfindBetween(startTemplate, endTemplate, startColumn, endColumn);
    }

    @Override
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, Set<? extends Object> includes, Set<String> excludes)
    {
        return _findHelper.mfindBetween(startTemplate, endTemplate, includes, excludes);
    }
}
