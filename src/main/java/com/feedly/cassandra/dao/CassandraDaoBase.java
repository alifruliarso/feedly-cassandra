package com.feedly.cassandra.dao;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.bean.BeanMetadata;
import com.feedly.cassandra.bean.BeanUtils;
import com.feedly.cassandra.bean.PropertyMetadata;
import com.feedly.cassandra.bean.enhance.ColumnFamilyTransformTask;
import com.feedly.cassandra.bean.enhance.IEnhancedBean;

public class CassandraDaoBase<K, V> implements ICassandraDao<K, V>
{
    private static final Logger _logger = LoggerFactory.getLogger(CassandraDaoBase.class.getName());
    
    private static final int COL_RANGE_SIZE = 100;
    private static final BytesArraySerializer SER_BYTES = BytesArraySerializer.get();
    private static final StringSerializer SER_STRING = StringSerializer.get();
    private static final DynamicCompositeSerializer SER_COMPOSITE = new DynamicCompositeSerializer();
    private final BeanMetadata<V> _valueMeta;
    private final String _columnFamily;
    private IKeyspaceFactory _keyspaceFactory;

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

        String colFamily = null;
        boolean forceCompositeColumns = false;
        for(Annotation anno : valueClass.getAnnotations())
        {
            if(anno.annotationType().equals(ColumnFamily.class))
            {
                ColumnFamily cf = (ColumnFamily) anno;
                colFamily = cf.name();
                if(colFamily.equals(""))
                    colFamily = valueClass.getSimpleName();

                forceCompositeColumns = cf.forceCompositeColumns();
                break;
            }
        }

        if(colFamily == null)
            throw new IllegalStateException(valueClass.getSimpleName() + " missing @ColumnFamily annotation");

        _valueMeta = new BeanMetadata<V>(valueClass, forceCompositeColumns);

        if(!_valueMeta.getKeyMetadata().getFieldType().equals(keyClass))
            throw new IllegalArgumentException(String.format("DAO/bean key mismatch: %s != %s",
                                                             keyClass.getName(),
                                                             _valueMeta.getKeyMetadata().getFieldType().getName()));


        _columnFamily = colFamily;

        _logger.info(getClass().getSimpleName(), new Object[] {"[", _columnFamily, "] -> ", _valueMeta.toString()});
    }

    public void setKeyspaceFactory(IKeyspaceFactory keyspaceFactory)
    {
        _keyspaceFactory = keyspaceFactory;
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

    private IEnhancedBean asBean(V value)
    {
        try
        {
            return (IEnhancedBean) value;
        }
        catch(ClassCastException cce)
        {
            throw new IllegalArgumentException(value.getClass().getSimpleName()
                    + " was not enhanced. Bean classes must be enhanced post compilation See " + ColumnFamilyTransformTask.class.getName());
        }
    }

    @Override
    public void save(V value)
    {
        save(Collections.singleton(value));
    }

    @Override
    public void save(Collection<V> values)
    {
        PropertyMetadata keyMeta = _valueMeta.getKeyMetadata();
        Mutator<byte[]> mutator = HFactory.createMutator(_keyspaceFactory.createKeyspace(), SER_BYTES);

        for(V value : values)
        {
            Object key = invokeGetter(keyMeta, value);
            byte[] keyBytes = serialize(key, false, keyMeta.getSerializer());

            _logger.debug("inserting {}[{}]", _valueMeta.getType().getSimpleName(), key);

            int colCnt = saveDirtyFields(key, keyBytes, value, mutator);
            colCnt += saveUnmappedFields(key, keyBytes, value, mutator);

            if(colCnt == 0)
                _logger.warn("no updates for ", key);
            
            _logger.debug("updated {} values for {}[{}]", new Object[] { colCnt, _valueMeta.getType().getSimpleName(), key });
        }
        
        mutator.execute();
        
        //do after execution
        for(V value : values)
        {
            IEnhancedBean bean = asBean(value);
            bean.getModifiedFields().clear();
            bean.setUnmappedFieldsModified(false);
        }
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int saveDirtyFields(Object key, byte[] keyBytes, V value, Mutator<byte[]> mutator)
    {
        List<PropertyMetadata> properties = _valueMeta.getProperties();

        IEnhancedBean bean = asBean(value);
        BitSet dirty = bean.getModifiedFields();
        int colCnt = 0;

        if(!dirty.isEmpty())
        {
            for(int i = dirty.nextSetBit(0); i >= 0; i = dirty.nextSetBit(i + 1))
            {
                PropertyMetadata colMeta = properties.get(i);

                if(colMeta.isCollection())
                {
                    if(colMeta.getFieldType().equals(List.class))
                        colCnt += saveListFields(key, keyBytes, colMeta, value, mutator);
                    else
                        colCnt += saveMapFields(key, keyBytes, colMeta, value, mutator);
                }
                else
                {
                    Object propVal = invokeGetter(colMeta, value);
                    
                    _logger.trace("{}[{}].{} = {}", new Object[] { _valueMeta.getType().getSimpleName(), key, colMeta.getName(), propVal});
                    
                    if(propVal != null)
                    {
                        HColumn column = HFactory.createColumn(colMeta.getPhysicalNameBytes(), propVal, SER_BYTES, (Serializer) colMeta.getSerializer());
                        mutator.addInsertion(keyBytes, _columnFamily, column);
                    }
                    else
                    {
                        mutator.addDeletion(keyBytes, _columnFamily, colMeta.getPhysicalNameBytes(), SER_BYTES);
                    }
                    
                    colCnt++;
                }
            }

        }

        return colCnt;
    }

    private int saveMapFields(Object key, byte[] keyBytes, PropertyMetadata colMeta, V value, Mutator<byte[]> mutator)
    {
        Map<?, ?> map = (Map<?,?>) invokeGetter(colMeta, value);
        
        if(map == null)
        {
            _logger.warn(String.format("{}[{}].{} null collections are ignored, to delete values, set individual keys with null values",
                                          _columnFamily,
                                          key,
                                          colMeta.getName()));
                          
            return 0;
        }
        

        for(Map.Entry<?, ?> entry : map.entrySet())
        {
            saveCollectionColumn(key, keyBytes, entry.getKey(), entry.getValue(), colMeta, mutator);
        }
        
        return map.size();  
    }

    private int saveListFields(Object key, byte[] keyBytes, PropertyMetadata colMeta, V value, Mutator<byte[]> mutator)
    {
        List<?> list = (List<?>) invokeGetter(colMeta, value);
        
        if(list == null)
        {
            _logger.warn("{}[{}].{} null collections are ignored, to delete values, set individual keys with null values",
                         new Object[] {_columnFamily, key, colMeta.getName()});
                          
            return 0;
        }
        
        int newIdx = 0;
        int size = list.size();
        
        
        int nullIdx = list.indexOf(null); 
        if(nullIdx >= 0)
        {
            Mutator<byte[]> delMutator = HFactory.createMutator(_keyspaceFactory.createKeyspace(), SER_BYTES);
            for(int i = nullIdx; i < size; i++)
            {
                saveCollectionColumn(key, keyBytes, i, null, colMeta, delMutator);
            }
            
            
            _logger.debug("{}[{}].{} list shortened, deleting %d entries (starting from first deleted entry)",
                          new Object[]{_columnFamily, key, colMeta.getName(), size - nullIdx});
            
            
            delMutator.execute();
        }

        
        for(int i = 0; i < size; i++) 
        {
            boolean added = saveCollectionColumn(key, keyBytes, newIdx, list.get(i), colMeta, mutator);
            if(added)
                newIdx++;
        }

        return size;  
    }

    private boolean saveCollectionColumn(Object key,
                                         byte[] keyBytes,
                                         Object propKey,
                                         Object propVal,
                                         PropertyMetadata colMeta,
                                         Mutator<byte[]> mutator)
    {
        _logger.trace("{}[{}].{}:{} = {}", new Object[] {_valueMeta.getType().getSimpleName(), key, colMeta.getName(), propKey, propVal});

        if(propKey == null)
        {
            throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s:null = %s. ensure keys are not null",
                                                             _columnFamily,
                                                             key,
                                                             colMeta.getName(),
                                                             propVal));
        }

        DynamicComposite colName = new DynamicComposite(colMeta.getName(), propKey);
        if(propVal == null)
        {
            mutator.addDeletion(keyBytes, _columnFamily, colName, SER_COMPOSITE);
        }
        else
        {
            byte[] propValBytes = serialize(propVal, false, colMeta.getSerializer());
            if(propValBytes == null)
            {
                throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s:%s = %s. ensure values can be serialized",
                                                                 _columnFamily,
                                                                 key,
                                                                 colMeta.getName(),
                                                                 propKey,
                                                                 propVal));
            }

            HColumn<DynamicComposite, byte[]> column = HFactory.createColumn(colName, propValBytes, SER_COMPOSITE, SER_BYTES);
            mutator.addInsertion(keyBytes, _columnFamily, column);
        }
        
        return propVal != null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int saveUnmappedFields(Object key, byte[] keyBytes, V value, Mutator<byte[]> mutator)
    {
        if(!asBean(value).getUnmappedFieldsModified())
            return 0;

        PropertyMetadata unmappedMeta = _valueMeta.getUnmappedHandler();

        if(unmappedMeta == null)
            return 0;
        
        Map<?, ?> unmapped = (Map) invokeGetter(unmappedMeta, value);
        
        if(unmapped == null)
            return 0;
            

        int colCnt = unmapped.size();

        Serializer<?> valueSer = unmappedMeta.getSerializer();
        for(Map.Entry<?, ?> entry : unmapped.entrySet())
        {

            Object colVal = entry.getValue();
            _logger.trace("{}[{}].{} = {}", new Object[] {_valueMeta.getType().getSimpleName(), key, entry.getKey(), colVal});

            if(!(entry.getKey() instanceof String))
                throw new IllegalArgumentException("only string keys supported for unmapped properties");
            
            byte[] colName = serialize(entry.getKey(), true, null);  
            
            if(colVal != null)
            {
                byte[] colValBytes = serialize(colVal, false, valueSer);
                
                if(colName == null || colValBytes == null)
                {
                    throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s = %s. ensure values are non-null and can be serialized",
                                                                     _columnFamily,
                                                                     key,
                                                                     entry.getKey(),
                                                                     colVal));
                }
                
                HColumn column = HFactory.createColumn(colName, colValBytes, SER_BYTES, SER_BYTES);
                mutator.addInsertion(keyBytes, _columnFamily, column);
            }
            else
            {
                if(colName == null)
                {
                    throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s = null. ensure values are non-null and can be serialized",
                                                                     _columnFamily,
                                                                     key,
                                                                     colName));
                }
                
                mutator.addDeletion(keyBytes, _columnFamily, colName, SER_BYTES);
            }
        }

        return colCnt;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private byte[] serialize(Object val, boolean isColName, Serializer serializer)
    {
        if(val == null)
        {
            if(isColName)
                throw new IllegalArgumentException("col name can not be null");
            else
                return null;
        }

        if(isColName && _valueMeta.useCompositeColumns())
            return SER_COMPOSITE.toBytes(new DynamicComposite(val));
        
        if(serializer == null)
            serializer = BeanUtils.getSerializer(val.getClass());
        
        if(serializer == null)
            throw new IllegalArgumentException("unable to serialize " + val);
        
        return serializer.toBytes(val);
    }

    public V load(K key)
    {
        return loadFromGet(key, null, null, null, null);
    }
    
    public V loadPartial(K key, V value, Object from, Object to)
    {
        byte[] fromBytes = null, toBytes = null;
        
        if(from instanceof CollectionProperty)
            fromBytes = collectionPropertyKey((CollectionProperty) from, ComponentEquality.EQUAL);
        else
            fromBytes = serialize(from, true, null);
        
        if(to instanceof CollectionProperty)
            toBytes = collectionPropertyKey((CollectionProperty) to, ComponentEquality.EQUAL);
        else
            toBytes = serialize(to, true, null);
        
        return loadFromGet(key, value, null, fromBytes, toBytes);
    }
    
    public V loadPartial(K key, V value, Set<? extends Object> includes, Set<String> excludes)
    {
        _logger.debug("loading {}[{}]", _columnFamily, key);
        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        List<byte[]> colNames = new ArrayList<byte[]>();
        List<PropertyMetadata> fullCollectionProperties = addPartialColumns(colNames, includes, excludes);

        value = loadFromGet(key, value, colNames, null, null);   

        if(fullCollectionProperties != null)
        {
            for(PropertyMetadata pm : fullCollectionProperties)
            {
                DynamicComposite dc = new DynamicComposite();
                dc.addComponent(0, pm.getName(), ComponentEquality.EQUAL);
                byte[] colBytes = SER_COMPOSITE.toBytes(dc);
                dc = new DynamicComposite();
                dc.addComponent(0, pm.getName(), ComponentEquality.GREATER_THAN_EQUAL); //little strange, this really means the first value greater than... 
                byte[] colBytesEnd = SER_COMPOSITE.toBytes(dc);
                
                value = loadFromGet(key, value, null, colBytes, colBytesEnd);
            }
        }
        
        return value;
    }

    public Collection<V> bulkLoad(Collection<K> keys)
    {
        return bulkLoadFromMultiGet(keys, null, null, null, null, false);
    }
    
    public List<V> bulkLoadPartial(List<K> keys, List<V> values, Object from, Object to)
    {
        if(values != null && keys.size() != values.size())
        {
            throw new IllegalArgumentException("key and value list must be same size");
        }
        
        byte[] fromBytes = null, toBytes = null;
        
        if(from instanceof CollectionProperty)
            fromBytes = collectionPropertyKey((CollectionProperty) from, ComponentEquality.EQUAL);
        else
            fromBytes = serialize(from, true, null);
        
        if(to instanceof CollectionProperty)
            toBytes = collectionPropertyKey((CollectionProperty) to, ComponentEquality.EQUAL);
        else
            toBytes = serialize(to, true, null);
        
        return bulkLoadFromMultiGet(keys, values, null, fromBytes, toBytes, true);
    }
    
    public List<V> bulkLoadPartial(List<K> keys, List<V> values, Set<? extends Object> includes, Set<String> excludes)
    {        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        List<byte[]> colNames = new ArrayList<byte[]>();
        List<PropertyMetadata> fullCollectionProperties = addPartialColumns(colNames, includes, excludes);

        values = bulkLoadFromMultiGet(keys, values, colNames, null, null, true);

        if(fullCollectionProperties != null)
        {
            for(PropertyMetadata pm : fullCollectionProperties)
            {
                DynamicComposite dc = new DynamicComposite();
                dc.addComponent(0, pm.getName(), ComponentEquality.EQUAL);
                byte[] colBytes = SER_COMPOSITE.toBytes(dc);
                
                dc = new DynamicComposite();
                dc.addComponent(0, pm.getName(), ComponentEquality.GREATER_THAN_EQUAL); //little strange, this really means the first value greater than... 
                byte[] colBytesEnd = SER_COMPOSITE.toBytes(dc);
                
                values = bulkLoadFromMultiGet(keys, values, null, colBytes, colBytesEnd, true);
            }
        }
        
        return values;
    }
    
    private Set<? extends Object> partialProperties(Set<? extends Object> includes, Set<String> excludes)
    {
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");

        if(includes == null && excludes == null)
            throw new IllegalArgumentException("either includes or excludes must be specified");
            
        
        if(includes != null)
        {
            if(includes.isEmpty())
                throw new IllegalArgumentException("at least one property must be included");
            
            return includes;
        }
        
        Set<Object> props = new HashSet<Object>();
        if(_valueMeta.getUnmappedHandler() == null)
        {
            for(String exclude : excludes)
            {
                if(_valueMeta.getProperty(exclude) == null)
                    throw new IllegalArgumentException("no such property " + exclude);
            }
        }

        for(PropertyMetadata pm : _valueMeta.getProperties())
        {
            if(!excludes.contains(pm.getName()))
                props.add(pm.getName());
        }
        
        return props;
    }
    

    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private V loadFromGet(K key, V value, List<byte[]> cols, byte[] from, byte[] to)
    {
        _logger.debug(String.format("loading {}[{}]", _columnFamily, key));

        PropertyMetadata keyMeta = _valueMeta.getKeyMetadata();
        byte[] keyBytes = ((Serializer) keyMeta.getSerializer()).toBytes(key);

        SliceQuery<byte[], byte[], byte[]> query = buildSliceQuery(keyBytes);

        if(cols != null)
            query.setColumnNames(cols.toArray(new byte[cols.size()][]));
        else
            query.setRange(from, to, false, COL_RANGE_SIZE);
        
        return fromColumnSlice(key, value, keyMeta, keyBytes, query, query.execute().get(), to);   
    }
    

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private List<V> bulkLoadFromMultiGet(Collection<K> keys, List<V> values, List<byte[]> colNames, byte[] first, byte[] last, boolean maintainOrder)
    {
        PropertyMetadata keyMeta = _valueMeta.getKeyMetadata();
        MultigetSliceQuery<byte[], byte[], byte[]> query = HFactory.createMultigetSliceQuery(_keyspaceFactory.createKeyspace(),
                                                                                             SER_BYTES,
                                                                                             SER_BYTES,
                                                                                             SER_BYTES);

        byte[][] keyBytes = new byte[keys.size()][];

        int i = 0;
        for(K key : keys)
        {
            _logger.debug("loading {}[{}]", _columnFamily, key);

            keyBytes[i] = ((Serializer) _valueMeta.getKeyMetadata().getSerializer()).toBytes(key);
            i++;
        }

        query.setKeys(keyBytes);
        query.setColumnFamily(_columnFamily);

        if(colNames != null)
            query.setColumnNames(colNames.toArray(new byte[colNames.size()][]));
        else
            query.setRange(first, last, false, COL_RANGE_SIZE);
            
        Rows<byte[], byte[], byte[]> rows = query.execute().get();

        Map<K, Integer> pos = null;
        
        if(maintainOrder)
        {
            pos = new HashMap<K, Integer>();
            for(i = keys.size() - 1; i >= 0; i--)
                pos.put( ((List<K>)keys).get(i), i);
            
        }

        values = values == null ? new ArrayList<V>(keys.size()) : values;

        for(Row<byte[], byte[], byte[]> row : rows)
        {
            K key = (K) ((Serializer) keyMeta.getSerializer()).fromBytes(row.getKey());

            V value = null;
            
            int idx = 0;
            if(maintainOrder)
            {
                idx = pos.get(key);
                
                for(i = values.size(); i <= idx; i++)
                    values.add(null);

                value = values.get(pos.get(key));
            }

            value = fromColumnSlice(key, value, keyMeta, row.getKey(), null, row.getColumnSlice(), last);

            if(value != null)
            {
                if(maintainOrder)
                    values.set(idx, value);
                else 
                    values.add(value);
            }
        }

        return values;
    }
    

    private List<PropertyMetadata> addPartialColumns(List<byte[]> colNames, Set<? extends Object> includes, Set<String> excludes)
    {
        List<PropertyMetadata> fullCollectionProperties = null;
        Set<? extends Object> partialProperties = partialProperties(includes, excludes);
        for(Object property : partialProperties)
        {
            byte[] colNameBytes = null;
            if(property instanceof String)
            {
                String strProp = (String) property;
                PropertyMetadata pm = _valueMeta.getProperty(strProp);
                if(_valueMeta.getUnmappedHandler() == null && pm == null)
                    throw new IllegalArgumentException("unrecognized property " + strProp);
                
                //collections need to be handled separately
                if(pm != null && pm.isCollection())
                {
                    if(fullCollectionProperties == null)
                        fullCollectionProperties = new ArrayList<PropertyMetadata>();
                    
                    fullCollectionProperties.add(pm);
                    continue; 
                }
                colNameBytes = pm != null ? pm.getPhysicalNameBytes() : serialize(property, true, null);
            }
            else
            {
                if(property instanceof CollectionProperty)
                {
                    CollectionProperty cp = (CollectionProperty) property;
                    colNameBytes = collectionPropertyKey(cp, ComponentEquality.EQUAL);
                }
                else if(_valueMeta.getUnmappedHandler() == null)
                    throw new IllegalArgumentException("property must be string, but encountered " + property);
                else
                    colNameBytes = serialize(property, true, null);
            }
            
            if(colNameBytes == null)
                throw new IllegalArgumentException("could not serialize " + property);
            
            colNames.add(colNameBytes);
        }        
        
        return fullCollectionProperties;
    }
    
    private byte[] collectionPropertyKey(CollectionProperty cp, ComponentEquality keyEq)
    {
        PropertyMetadata pm = _valueMeta.getProperty(cp.getProperty());
        
        if(pm == null || !pm.isCollection())
            throw new IllegalArgumentException("property " + cp.getProperty() + " is not a collection");

        if(pm.getFieldType().equals(List.class) && !(cp.getKey() instanceof Integer))
            throw new IllegalArgumentException("key for property" + cp.getProperty() + " must be an int");
        
        DynamicComposite dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL);
        Object colKey = cp.getKey();
        if(pm.getFieldType().equals(List.class))
            colKey = BigInteger.valueOf(((Integer) colKey).longValue());
        
        dc.addComponent(1, colKey, keyEq); 
        return SER_COMPOSITE.toBytes(dc);
    }
    
    //build value from set of columns, properly fetching additional columns if there are more columns than the range size.
    private V fromColumnSlice(K key, V value, 
                              PropertyMetadata keyMeta, byte[] keyBytes, 
                              SliceQuery<byte[], byte[], byte[]> query, ColumnSlice<byte[], byte[]> slice,
                              byte[] rangeEnd)
    {
        List<HColumn<byte[], byte[]>> columns = slice.getColumns();
        byte[] firstCol = null;
        boolean hasMore = true;
        while(hasMore)
        {
            value = loadValueProperties(key, value, keyMeta, columns);
         
            hasMore = columns.size() == COL_RANGE_SIZE;
            
            if(hasMore) //need to fetch more
            {
                if(query == null)
                    query = buildSliceQuery(keyBytes);

                firstCol = columns.get(COL_RANGE_SIZE - 1).getName();
                firstCol[firstCol.length-1]++; //next batch starts right after the current batch
                
                query.setRange(firstCol, rangeEnd, false, COL_RANGE_SIZE);
                columns = query.execute().get().getColumns();
            }
        }
        return value;
    }
    
    private SliceQuery<byte[], byte[], byte[]> buildSliceQuery(byte[] keyBytes)
    {
        SliceQuery<byte[], byte[], byte[]> query = HFactory.createSliceQuery(_keyspaceFactory.createKeyspace(), SER_BYTES, SER_BYTES, SER_BYTES);
        
        query.setKey(keyBytes);
        query.setColumnFamily(_columnFamily);

        return query;
    }
    
    @SuppressWarnings("unchecked")
    private V loadValueProperties(K key, V value, PropertyMetadata keyMeta, List<HColumn<byte[], byte[]>> columns)
    {
        if(columns.isEmpty())
            return null;

        try
        {
            if(value == null)
                value = _valueMeta.getType().newInstance();
        }
        catch(Exception ex)
        {
            throw new IllegalArgumentException("error instantiating value object of type " + _valueMeta.getClass(), ex);
        }

        
        Map<Object, Object> unmapped = null;
        Map<String, Object> collections = null; //cache the collections to avoid reflection invocations in loop
        if(_valueMeta.useCompositeColumns())
            collections = new HashMap<String, Object>();
        
        if(_valueMeta.getUnmappedHandler() != null)
            unmapped = (Map<Object, Object>) invokeGetter(_valueMeta.getUnmappedHandler(), value);
        
        int size = columns.size();
        for(int i = 0; i < size; i++)
        {
            HColumn<byte[], byte[]> col = columns.get(i);
            String pname = null;
            Object collectionKey = null;
            if(_valueMeta.useCompositeColumns())
            {
                DynamicComposite composite = SER_COMPOSITE.fromBytes(col.getName());
                pname = (String) composite.get(0);
                if(composite.size() > 1)
                    collectionKey = composite.get(1);
            }
            else
                pname = SER_STRING.fromBytes(col.getName());
            
            PropertyMetadata pm = _valueMeta.getPropertyByPhysicalName(pname);

            if(pm == null)
            {
                unmapped = loadUnmappedProperty(key, value, pname, col, unmapped);
            }
            else
            {
                if(collectionKey == null)
                {
                    loadProperty(key, value, col, pname, pm);
                }
                else
                {
                    if(pm.getFieldType().equals(List.class))
                    {
                        List<Object> l = (List<Object>) collections.get(pname);
                        if(l == null)
                        {
                            l = (List<Object>) invokeGetter(pm, value);
                            
                            if(l == null)
                            {
                                l = new ArrayList<Object>();
                                invokeSetter(pm, value, l);
                            }
                            collections.put(pname, l);
                        }
                        loadListProperty(key, value, col, pname, l, collectionKey, pm);
                    }
                    else
                    {
                        Map<Object, Object> m = (Map<Object, Object>) collections.get(pname);
                        if(m == null)
                        {
                            m = (Map<Object, Object>) invokeGetter(pm, value);
                            
                            if(m == null)
                            {
                                if(pm.getFieldType().equals(SortedMap.class))
                                    m = new TreeMap<Object, Object>();
                                else
                                    m = new HashMap<Object, Object>();
                                
                                invokeSetter(pm, value, m);
                            }
                            
                            collections.put(pname, m);
                        }
                        
                        loadMapProperty(key, value, col, pname, m, collectionKey, pm);
                    }
                }
            }
        }

        invokeSetter(keyMeta, value, key);

        IEnhancedBean bean = asBean(value);
        bean.getModifiedFields().clear();
        bean.setUnmappedFieldsModified(false);
        
        return value;
    }

    private void loadMapProperty(K key,
                                 V value,
                                 HColumn<byte[], byte[]> col,
                                 String pname,
                                 Map<Object, Object> map,
                                 Object collectionKey,
                                 PropertyMetadata pm)
    {
        Object pval = pm.getSerializer().fromByteBuffer(col.getValueBytes());
        
        map.put(collectionKey, pval); 
        _logger.debug("{}[{}].{}:{} = {}", new Object[] {_valueMeta.getType().getSimpleName(), key, pname, collectionKey, pval});
    }

    private void loadListProperty(K key,
                                  V value,
                                  HColumn<byte[], byte[]> col,
                                  String pname,
                                  List<Object> list,
                                  Object collectionKey,
                                  PropertyMetadata pm)
    {
        Object pval = pm.getSerializer().fromByteBuffer(col.getValueBytes());
        
        int idx = ((BigInteger) collectionKey).intValue();

        //columns should be loaded in order, but when loading partial values, null padding may be needed
        for(int i = list.size(); i < idx; i++)
            list.add(null);

        list.add(pval); 
            
        _logger.trace("{}[{}].{}:{} = {}", new Object[]{_valueMeta.getType().getSimpleName(), key, pname, collectionKey, pval});
    }

    private void loadProperty(K key, V value, HColumn<byte[], byte[]> col, String pname, PropertyMetadata pm)
    {
        Object pval = pm.getSerializer().fromBytes(col.getValue());
        invokeSetter(pm, value, pval);
        _logger.trace("{}[{}].{} = {}", new Object[]{_valueMeta.getType().getSimpleName(), key, pname, pval});
    }

    private Map<Object, Object> loadUnmappedProperty(K key,
                                                  V value,
                                                  String pname,
                                                  HColumn<byte[], byte[]> col,
                                                  Map<Object, Object> unmapped)
    {
        PropertyMetadata pm = _valueMeta.getUnmappedHandler();
        if(pm != null)
        {
            if(unmapped == null)
            {
                if(pm.getFieldType().equals(Map.class))
                    unmapped = new HashMap<Object, Object>();
                else
                    unmapped = new TreeMap<Object, Object>();
                
                invokeSetter(pm, value, unmapped);
            }
            
            Object pval = pm.getSerializer().fromByteBuffer(col.getValueBytes());
            if(pval != null)
            {
                unmapped.put(pname, pval);
                _logger.trace("{}[{}].{} = {}", new Object[] {_valueMeta.getType().getSimpleName(), key, pname, pval});
            }
            else
            {
                _logger.info("unrecognized value for {}[{}].{}, skipping", new Object[] { _valueMeta.getType().getSimpleName(), key, pname});
            }
        }
        else
        {
            _logger.info("unmapped value for {}[{}].{}, skipping", new Object[] { _valueMeta.getType().getSimpleName(), key, pname});
        }
        return unmapped;
    }

    private Object invokeGetter(PropertyMetadata pm, V obj)
    {
        try
        {
            return pm.getGetter().invoke(obj);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("unexpected error invoking " + pm.getGetter(), e);
        }
    }

    private void invokeSetter(PropertyMetadata pm, V obj, Object value)
    {
        try
        {
            pm.getSetter().invoke(obj, value);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("unexpected error invoking " + pm.getGetter(), e);
        }
    }
    
}
