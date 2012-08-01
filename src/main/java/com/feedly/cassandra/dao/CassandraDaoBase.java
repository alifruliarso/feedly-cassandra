package com.feedly.cassandra.dao;

import java.lang.management.ManagementFactory;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.EConsistencyLevel;
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
    private final EConsistencyLevel _defaultConsistency;
    
    private GetHelper<K, V> _getHelper;
    private FindHelper<K, V> _findHelper;
    private PutHelper<K, V> _putHelper;
    private DeleteHelper<K, V> _deleteHelper;
    private IKeyspaceFactory _keyspaceFactory;
    private IStaleIndexValueStrategy _staleIndexValueStrategy;
    private int _statsSize = MBeanUtils.DEFAULT_STATS_SIZE;
    
    protected CassandraDaoBase()
    {
        this(null, null, null);
    }

    @SuppressWarnings("unchecked")
    protected CassandraDaoBase(Class<K> keyClass, Class<V> valueClass, EConsistencyLevel defaultConsistency)
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

        _defaultConsistency = defaultConsistency != null ? defaultConsistency : EConsistencyLevel.QUOROM;
        
        _entityMeta = new EntityMetadata<V>(valueClass);

        if(!keyClassMatches(_entityMeta.getKeyMetadata().getFieldType(), keyClass))
            throw new IllegalArgumentException(String.format("DAO/entity key mismatch: %s != %s",
                                                             keyClass.getName(),
                                                             _entityMeta.getKeyMetadata().getFieldType().getName()));



        _logger.info("{} [{}]:\n{}", new Object[] {getClass().getSimpleName(), _entityMeta.getFamilyName(), _entityMeta.toString()});
    }

    public void setStatsSize(int s)
    {
        _statsSize = s;
    }

    public void setKeyspaceFactory(IKeyspaceFactory keyspaceFactory)
    {
        _keyspaceFactory = keyspaceFactory;
    }

    public void setStaleValueIndexStrategy(IStaleIndexValueStrategy strategy)
    {
        _staleIndexValueStrategy = strategy;
    }
    
    public void destroy()
    {
        unregisterMBeans();
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
                        public void handle(EntityMetadata<?> entity, IndexMetadata index, Keyspace keyspace, Collection<StaleIndexValue> values)
                        {
                            _logger.warn("not handling {} stale values for {}", values.size(), entity.getFamilyName());
                        }
                    };
        }
        
        IKeyspaceFactory withDefault = 
            new IKeyspaceFactory()
            {
                @Override
                public Keyspace createKeyspace(EConsistencyLevel level)
                {
                    if(level == null)
                        level = _defaultConsistency;
                    
                    return _keyspaceFactory.createKeyspace(level);
                }
            };
        
        _getHelper = new GetHelper<K, V>(_entityMeta, withDefault, _statsSize);
        _findHelper = new FindHelper<K, V>(_entityMeta, withDefault, _staleIndexValueStrategy, _statsSize);
        _putHelper = new PutHelper<K, V>(_entityMeta, withDefault, _statsSize);
        _deleteHelper = new DeleteHelper<K, V>(_entityMeta, withDefault, _statsSize);
        registerMBeans();
    }

    
    private ObjectName mBeanName(String name) throws MalformedObjectNameException
    {
        return MBeanUtils.mBeanName(this, _entityMeta.getType().getSimpleName(), name);
    }
    
    private void registerMBeans()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try 
        {
            mbs.registerMBean(new OperationStatisticsMonitor(getStats()), mBeanName("getStats"));
            
            mbs.registerMBean(new OperationStatisticsMonitor(deleteStats()), mBeanName("deleteStats"));
            
            mbs.registerMBean(new OperationStatisticsMonitor(putStats()), mBeanName("putStats"));
            mbs.registerMBean(new OperationStatisticsMonitor(putIndexStats()), mBeanName("putIdxStats"));

            mbs.registerMBean(new OperationStatisticsMonitor(hashFindStats()), mBeanName("hashFindStats"));
            mbs.registerMBean(new OperationStatisticsMonitor(hashFindIndexStats()), mBeanName("hashFindIdxStats"));
            
            mbs.registerMBean(new OperationStatisticsMonitor(rangeFindStats()), mBeanName("rangeFindStats"));
            mbs.registerMBean(new OperationStatisticsMonitor(rangeFindIndexStats()), mBeanName("rangeFindIdxStats"));
            
            _logger.info("monitoring registration complete for {}", getClass().getSimpleName());
        } 
        catch(Exception e) 
        {
            _logger.warn("error registering mbeans", e);
        }
    }

    private void unregisterMBeans()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        
        try 
        {
            mbs.unregisterMBean(mBeanName("getStats"));

            mbs.unregisterMBean(mBeanName("deleteStats"));
            
            mbs.unregisterMBean(mBeanName("putStats"));
            mbs.unregisterMBean(mBeanName("putIdxStats"));

            mbs.unregisterMBean(mBeanName("hashFindStats"));
            mbs.unregisterMBean(mBeanName("hashFindIdxStats"));
            
            mbs.unregisterMBean(mBeanName("rangeFindStats"));
            mbs.unregisterMBean(mBeanName("rangeFindIdxStats"));

            _logger.info("monitoring unregistration complete for {}", getClass().getSimpleName());
        } 
        catch(Exception e) 
        {
            _logger.warn("error unregistering mbeans", e);
        }
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
        put(value, null);
    }

    @Override
    public final void put(V value, PutOptions options)
    {
        if(options == null)
            options = new PutOptions();
        
        _putHelper.put(value, options);
    }
    
    
    @Override
    public final void mput(Collection<V> values)
    {
        mput(values, null);
    }

    @Override
    public final void mput(Collection<V> values, PutOptions options)
    {
        if(options == null)
            options = new PutOptions();
        
        _putHelper.mput(values, options);
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
    public Collection<V> mgetAll()
    {
        return mgetAll(null);
    }

    @Override
    public Collection<V> mgetAll(GetAllOptions options)
    {
        if(options == null)
            options = new GetAllOptions();
        
        return _getHelper.mgetAll(options);
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

    @Override
    public void delete(K key)
    {
        delete(key, null);
    }

    @Override
    public void delete(K key, DeleteOptions options)
    {
        if(options == null)
            options = new DeleteOptions();
        _deleteHelper.delete(key, options);
    }
    
    @Override
    public void mdelete(Collection<K> keys)
    {
        mdelete(keys, null);
    }

    @Override
    public void mdelete(Collection<K> keys, DeleteOptions options)
    {
        if(options == null)
            options = new DeleteOptions();
        
        _deleteHelper.mdelete(keys, options);
    }
    
    public OperationStatistics getStats()
    {
        return _getHelper.stats();
    }

    public OperationStatistics deleteStats()
    {
        return _deleteHelper.stats();
    }

    public OperationStatistics putStats()
    {
        return _putHelper.stats();
    }
    
    public OperationStatistics putIndexStats()
    {
        return _putHelper.indexStats();
    }

    public OperationStatistics hashFindStats()
    {
        return _findHelper.hashFindStats();
    }
    
    public OperationStatistics hashFindIndexStats()
    {
        return _findHelper.hashFindIndexStats();
    }
    
    public OperationStatistics rangeFindStats()
    {
        return _findHelper.rangeFindStats();
    }
    
    public OperationStatistics rangeFindIndexStats()
    {
        return _findHelper.rangeFindIndexStats();
    }
}
