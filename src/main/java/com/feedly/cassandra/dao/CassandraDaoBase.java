package com.feedly.cassandra.dao;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

/**
 * This class serves as a foundation class for saving and retrieving entities in Cassandra. Subclasses can define additional business logic
 * related methods. Typically those methods then use functionality provided by this class.
 * 
 * @author kireet
 *
 * @param <K> the key type - should match the type of the Entity's row key field.
 * @param <V> the value type - the Entity itself.
 * 
 * @see ICassandraDao
 */
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

        if(!keyClassMatches(_entityMeta.getKeyMetadata().getFieldType(), keyClass))
            throw new IllegalArgumentException(String.format("DAO/entity key mismatch: %s != %s",
                                                             keyClass.getName(),
                                                             _entityMeta.getKeyMetadata().getFieldType().getName()));



        _logger.info("{} [{}]:\n{}", new Object[] {getClass().getSimpleName(), _entityMeta.getFamilyName(), _entityMeta.toString()});
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
    
    private boolean keyClassMatches(Class<?> fieldType, Class<?> keyType)
    {
        if(fieldType.equals(keyType))
            return true;

        if(fieldType.isPrimitive())
        {
            if(fieldType.equals(boolean.class))
                fieldType = Boolean.class;
            
            if(fieldType.equals(byte.class))
                fieldType = Byte.class;

            if(fieldType.equals(char.class))
                fieldType = Character.class;

            if(fieldType.equals(short.class))
                fieldType = Short.class;

            if(fieldType.equals(int.class))
                fieldType = Integer.class;

            if(fieldType.equals(long.class))
                fieldType = Long.class;

            if(fieldType.equals(float.class))
                fieldType = Float.class;

            if(fieldType.equals(double.class))
                fieldType = Double.class;

            return fieldType.equals(keyType);
        }

        return false;
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
    public final void put(V value)
    {
        _putHelper.put(value);
    }

    
    @Override
    public final void mput(Collection<V> values)
    {
        _putHelper.mput(values);
    }

    @Override
    public final V get(K key)
    {
        return get(key, null, null);
    }

    @Override
    public final V get(K key, V value, GetOptions options)
    {
        if(options == null)
            options = new GetOptions();
        
        return _getHelper.get(key, value, options);
    }

    @Override
    public Collection<V> mget(Collection<K> keys)
    {
        return _getHelper.mget(keys);
    }

    @Override
    public List<V> mget(List<K> keys, List<V> values, GetOptions options)
    {
        if(options == null)
            options = new GetOptions();
        
        return _getHelper.mget(keys, values, options);
    }

    @Override
    public V find(V template)
    {
        return find(template, null);
    }

    @Override
    public V find(V template, FindOptions options)
    {
        if(options == null)
            options = new FindOptions();
        return _findHelper.find(template, options);
    }

    
    public Collection<V> mfind(V template)
    {
        return mfind(template, null);
    }

    
    public Collection<V> mfind(V template, FindOptions options)
    {
        if(options == null)
            options = new FindOptions();
        return _findHelper.mfind(template, options);
    }

    public Collection<V> mfindBetween(V startTemplate, V endTemplate)
    {
        return mfindBetween(startTemplate, endTemplate, null);
    }

    
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, FindBetweenOptions options)
    {
        if(options == null)
            options = new FindBetweenOptions();
        
        return _findHelper.mfindBetween(startTemplate, endTemplate, options);
    }
}
