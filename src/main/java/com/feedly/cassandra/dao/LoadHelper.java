package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.ByteIndicatorSerializer;
import com.feedly.cassandra.entity.EPropertyType;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.EntityMetadataBase;
import com.feedly.cassandra.entity.ListPropertyMetadata;
import com.feedly.cassandra.entity.MapPropertyMetadata;
import com.feedly.cassandra.entity.ObjectPropertyMetadata;
import com.feedly.cassandra.entity.PropertyMetadataBase;
import com.feedly.cassandra.entity.SimplePropertyMetadata;

abstract class LoadHelper<K,V> extends DaoHelperBase<K, V>
{

    LoadHelper(EntityMetadata<V> meta, IKeyspaceFactory factory)
    {
        super(meta, factory);
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> createUmappedHandlerMap(MapPropertyMetadata pm, Object entity)
    {
        Map<String, Object> unmapped = (Map<String, Object>) invokeGetter(pm, entity);

        if(unmapped == null)
        {
            if(pm.getFieldType().equals(Map.class))
                unmapped = new HashMap<String, Object>();
            else
                unmapped = new TreeMap<String, Object>();
        }
        
        return unmapped;
    }
    

    /**
     * load properties into an entity from a cassandra row's columns
     * @param key the row key
     * @param value an existing entity to update, if null is passed a new entity is created
     * @param keyMeta the key meta
     * @param columns the columns used for update
     * @return the entity
     */
    protected V loadValueProperties(K key, V value, SimplePropertyMetadata keyMeta, List<HColumn<byte[], byte[]>> columns)
    {
        if(columns.isEmpty())
            return value;

        try
        {
            if(value == null)
                value = _entityMeta.getType().newInstance();
        }
        catch(Exception ex)
        {
            throw new IllegalArgumentException("error instantiating value object of type " + _entityMeta.getClass(), ex);
        }

        
        Map<String, Object> unmapped = null;
        Map<String, Object> containers = null; //cache the containers to avoid reflection invocations in loop
        if(_entityMeta.useCompositeColumns())
            containers = new HashMap<String,Object>();
        if(_entityMeta.getUnmappedHandler() != null)
        {
            unmapped = createUmappedHandlerMap(_entityMeta.getUnmappedHandler(), value);
        }
        int size = columns.size();
        
        StringBuilder descriptor = null;
        if(_logger.isTraceEnabled())
        {
            descriptor = new StringBuilder();
            descriptor.append(_entityMeta.getType().getSimpleName());
            descriptor.append("[").append(key).append("]");
        }

        Set<Object> entities = new HashSet<Object>();
        for(int i = 0; i < size; i++)
        {
            HColumn<byte[], byte[]> col = columns.get(i);
            Object colName= _entityMeta.useCompositeColumns() ? SER_COMPOSITE.fromBytes(col.getName()) : SER_STRING.fromBytes(col.getName());

            loadValueProperty(descriptor, value, _entityMeta, 0, colName, col.getValue(), entities, unmapped, containers);
        }

        if(unmapped != null && !unmapped.isEmpty())
            invokeSetter(_entityMeta.getUnmappedHandler(), value, unmapped);

        invokeSetter(keyMeta, value, key);

        entities.add(value);
        resetEntities(entities);
        
        
        return value;
    }

    @SuppressWarnings("unchecked")
    private void loadValueProperty(StringBuilder descriptor,
                                   Object value,
                                   EntityMetadataBase<?> entityMeta,
                                   int colNameIdx,
                                   Object colName,
                                   byte[] colVal, 
                                   Set<Object> entities, 
                                   Map<String, Object> unmapped, 
                                   Map<String, Object> containers)
    {
        String pname = null;
        DynamicComposite compositeColName = null;
        if(colName instanceof DynamicComposite)
        {
            compositeColName = (DynamicComposite) colName;
            pname = (String) compositeColName.get(colNameIdx);
        }
        else
            pname = (String) colName;
        
        PropertyMetadataBase pm = entityMeta.getPropertyByPhysicalName(pname);

        if(pm == null)
        {
            if(entityMeta.getUnmappedHandler() != null)
            {
                if(unmapped == null)
                {
                    unmapped = createUmappedHandlerMap(entityMeta.getUnmappedHandler(), value);
                    invokeSetter(entityMeta.getUnmappedHandler(), value, unmapped);
                }
                
                loadUnmappedProperty(descriptor, entityMeta.getUnmappedHandler(), value, pname, colVal, unmapped);
            }
            else
                _logger.info("unmapped value for {}.{}, skipping", descriptor, pname);
        }
        else
        {
            EPropertyType t = pm.getPropertyType();
            if(t == EPropertyType.SIMPLE)
            {
                loadSimpleProperty(descriptor, value, colVal, pname, (SimplePropertyMetadata) pm);
            }
            else
            {
                if(descriptor != null)
                    descriptor.append(".").append(pm.getName());
                
                if(t == EPropertyType.MAP || t == EPropertyType.SORTED_MAP)
                {
                    Map<Object, Object> m = containers != null ? (Map<Object, Object>) containers.get(pname) : null;
                    if(m == null)
                    {
                        m = (Map<Object, Object>) invokeGetter(pm, value);
                        
                        if(m == null)
                        {
                            if(t == EPropertyType.SORTED_MAP)
                                m = new TreeMap<Object, Object>();
                            else
                                m = new HashMap<Object, Object>();
                            
                            invokeSetter(pm, value, m);
                        }
                        
                        if(containers != null)
                            containers.put(pname, m);
                    }
                    
                    loadMapProperty(descriptor, compositeColName, colVal, m, colNameIdx+1, (MapPropertyMetadata) pm, entities);
                }
                else if(t == EPropertyType.LIST)
                {
                    List<Object> l = containers != null ? (List<Object>) containers.get(pname) : null;
                    if(l == null)
                    {
                        l = (List<Object>) invokeGetter(pm, value);
                        
                        if(l == null)
                        {
                            l = new ArrayList<Object>();
                            invokeSetter(pm, value, l);
                        }
                        
                        if(containers != null)
                            containers.put(pname, l);
                    }
                    loadListProperty(descriptor, compositeColName, colVal, l, colNameIdx+1, (ListPropertyMetadata) pm, entities);
                }
                else //OBJECT
                {
                    ObjectPropertyMetadata opm = (ObjectPropertyMetadata) pm;
                    Object e = containers != null ? containers.get(pname) : null;
                    if(e == null)
                    {
                        e = invokeGetter(pm, value);
                        
                        if(e == null)
                        {
                            e = opm.newInstance();
                            invokeSetter(pm, value, e);
                            entities.add(e);
                        }
                        
                        if(containers != null)
                            containers.put(pname, e);
                    }
                    
                    loadValueProperty(descriptor, e, opm.getObjectMetadata(), colNameIdx+1, colName, colVal, entities, null, null);
                }
                
                if(descriptor != null)
                {
                    int len = descriptor.length();
                    descriptor.delete(len - (pm.getName().length() + 1), len);
                }
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private void loadMapProperty(StringBuilder descriptor, 
                                 DynamicComposite colName,
                                 byte[] colValue,
                                 Map<Object, Object> map,
                                 int colIdx,
                                 MapPropertyMetadata pm,
                                 Set<Object> entities)
    {
        SimplePropertyMetadata keyMeta = pm.getKeyPropertyMetadata();
        PropertyMetadataBase valueMeta = pm.getValuePropertyMetadata();
        Object key = colName.get(colIdx, keyMeta.getSerializer());
        
        if(descriptor != null)
        {
            descriptor.append(".").append(key);
        }

        EPropertyType t = valueMeta.getPropertyType();
        if(t == EPropertyType.SIMPLE)
        {
            Object pval = ((SimplePropertyMetadata) valueMeta).getSerializer().fromBytes(colValue);

            _logger.trace("{} = {}", descriptor, pval);
            map.put(key, pval); 
        }
        else if(t == EPropertyType.MAP || t == EPropertyType.SORTED_MAP)
        {
            if(descriptor != null)
                descriptor.append(".").append(key);
            
            Map<Object, Object> subMap = (Map<Object, Object>) map.get(key);
            if(subMap == null)
            {
                subMap = t == EPropertyType.MAP ? new HashMap<Object, Object>() : new TreeMap<Object, Object>();
                map.put(key, subMap);
            }
            
            loadMapProperty(descriptor, colName, colValue, subMap, colIdx+1, (MapPropertyMetadata) valueMeta, entities);
        }
        else if(t == EPropertyType.LIST)
        {
            List<Object> subList = (List<Object>) map.get(key);
            if(subList == null)
            {
                subList = new ArrayList<Object>();
                map.put(key, subList);
            }
            loadListProperty(descriptor, colName, colValue, subList, colIdx+1, (ListPropertyMetadata) valueMeta, entities);
            map.put(key, subList);
        }
        else //OBJECT
        {
            ObjectPropertyMetadata opm = (ObjectPropertyMetadata) valueMeta;
            Object e = map.get(key);
            if(e == null)
            {
                e = opm.newInstance();
                entities.add(e);
                map.put(key, e);
            }
            
            loadValueProperty(descriptor, e, opm.getObjectMetadata(), colIdx+1, colName, colValue, entities, null, null);
        }
        if(descriptor != null)
        {
            int len = descriptor.length();
            descriptor.delete(len - (String.valueOf(key).length() + 1), len);
        }

    }

    @SuppressWarnings("unchecked")
    private void loadListProperty(StringBuilder descriptor,
                                  DynamicComposite colName,
                                  byte[] colValue,
                                  List<Object> list,
                                  int colIdx,
                                  ListPropertyMetadata pm,
                                  Set<Object> entities)
    {
        PropertyMetadataBase elementMeta = pm.getElementPropertyMetadata();
        int idx = colName.get(colIdx, BigIntegerSerializer.get()).intValue();

        //columns should be loaded in order, but when loading partial values, null padding may be needed
        for(int i = list.size(); i < idx; i++)
            list.add(null);

        EPropertyType t = elementMeta.getPropertyType();
        if(descriptor != null)
            descriptor.append("[").append(idx).append("]");

        if(t == EPropertyType.SIMPLE)
        {
            Object pval = ((SimplePropertyMetadata) elementMeta).getSerializer().fromBytes(colValue);

            _logger.trace("{} = {}", descriptor, pval);
            list.add(pval); 
        }
        else if(t == EPropertyType.MAP || t == EPropertyType.SORTED_MAP)
        {
            Map<Object, Object> subMap; 
            if(list.size() > idx)
            {
                subMap = (Map<Object, Object>) list.get(idx);
                if(subMap == null)
                {
                    subMap = t == EPropertyType.MAP ? new HashMap<Object, Object>() : new TreeMap<Object, Object>();
                    list.set(idx, subMap);
                }
            }
            else
            {
                subMap = t == EPropertyType.MAP ? new HashMap<Object, Object>() : new TreeMap<Object, Object>();
                list.add(subMap);
            }
            loadMapProperty(descriptor, colName, colValue, subMap, colIdx+1, (MapPropertyMetadata) elementMeta, entities);
        }
        else if(t == EPropertyType.LIST)
        {
            List<Object> sublist; 
            if(list.size() > idx)
            {
                sublist = (List<Object>) list.get(idx);
                if(sublist == null)
                {
                    sublist = new ArrayList<Object>();
                    list.set(idx, sublist);
                }
            }
            else
            {
                sublist = new ArrayList<Object>();
                list.add(sublist);
            }
            
            loadListProperty(descriptor, colName, colValue, sublist, colIdx+1, (ListPropertyMetadata) elementMeta, entities);
        }
        else //OBJECT
        {
            ObjectPropertyMetadata opm = (ObjectPropertyMetadata) elementMeta;
            Object e;
            if(list.size() > idx)
            {
                e = list.get(idx);
                if(e == null)
                {
                    e = opm.newInstance();
                    list.set(idx, e);
                }
            }
            else
            {
                e = opm.newInstance();
                entities.add(e);
                list.add(e);
            }
            
            loadValueProperty(descriptor, e, opm.getObjectMetadata(), colIdx+1, colName, colValue, entities, null, null);
        }
        
        
        if(descriptor != null)
        {
            int len = descriptor.length();
            descriptor.delete(len - (String.valueOf(idx).length() + 2), len);
        }

    }

    private void loadSimpleProperty(StringBuilder descriptor, Object value, byte[] colVal, String pname, SimplePropertyMetadata pm)
    {
        Object pval = pm.getSerializer().fromBytes(colVal);
        invokeSetter(pm, value, pval);
        _logger.trace("{}.{} = {}", new Object[]{descriptor, pname, pval});
    }

    @SuppressWarnings("unchecked")
    private void loadUnmappedProperty(StringBuilder descriptor,
                                      MapPropertyMetadata pm,
                                      Object entity,
                                      String pname,
                                      byte[] colValue,
                                      Map<String, Object> unmapped)
    {
        Serializer<?> valueSer;
        
        if(pm.getValuePropertyMetadata().getPropertyType() == EPropertyType.SIMPLE)
            valueSer = ((SimplePropertyMetadata) pm.getValuePropertyMetadata()).getSerializer();
        else
            valueSer = ByteIndicatorSerializer.get();

        Object pval = valueSer.fromBytes(colValue);
        if(pval != null)
        {
            unmapped.put(pname, pval);
            _logger.trace("{}.{} = {}", new Object[] {descriptor, pname, pval});
        }
        else
        {
            _logger.info("unrecognized value for {}.{}, skipping", new Object[] { descriptor, pname});
        }
    }

    
    /**
     * build value from set of columns, properly fetching additional columns if there are more columns than the range size.
     * @param key the row key
     * @param value the value to update, if null is passed a new value is created
     * @param keyMeta 
     * @param keyBytes should match key param
     * @param query the query to use to get more columns
     * @param slice an already fetched slice
     * @param rangeEnd the last column to fetch
     * @return the updated entity
     */
    protected V fromColumnSlice(K key, V value, 
                              SimplePropertyMetadata keyMeta, byte[] keyBytes, 
                              SliceQuery<byte[], byte[], byte[]> query, ColumnSlice<byte[], byte[]> slice,
                              byte[] rangeEnd)
    {
        List<HColumn<byte[], byte[]>> columns = slice.getColumns();
        byte[] firstCol = null;
        boolean hasMore = true;
        while(hasMore)
        {
            value = loadValueProperties(key, value, keyMeta, columns);
         
            hasMore = columns.size() >= CassandraDaoBase.COL_RANGE_SIZE - 1; 
            
            if(hasMore) //need to fetch more
            {
                if(query == null)
                    query = buildSliceQuery(keyBytes);

                firstCol = columns.get(columns.size() - 1).getName();
                
                query.setRange(firstCol, rangeEnd, false, CassandraDaoBase.COL_RANGE_SIZE);
                columns = query.execute().get().getColumns();
                
                columns = columns.subList(1, columns.size()); //boundaries are inclusive, exclude previously processed column
            }
        }
        return value;
    }

    protected Set<? extends Object> partialProperties(Set<? extends Object> includes, Set<String> excludes)
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
        if(_entityMeta.getUnmappedHandler() == null)
        {
            for(String exclude : excludes)
            {
                if(_entityMeta.getProperty(exclude) == null)
                    throw new IllegalArgumentException("no such property " + exclude);
            }
        }

        for(PropertyMetadataBase pm : _entityMeta.getProperties())
        {
            if(!excludes.contains(pm.getName()))
                props.add(pm.getName());
        }
        
        return props;
    }
    
    /**
     * Determine the columns to include based on includes and excludes
     * @param colNames a list to be updated with all the simple property column names to be included
     * @param includes the columns to include
     * @param excludes the columns to exclude
     * @return the collection properties included
     */
    protected List<CollectionRange> derivePartialColumns(List<byte[]> colNames, Set<? extends Object> includes, Set<String> excludes)
    {
        List<CollectionRange> ranges = null;
        Set<? extends Object> partialProperties = partialProperties(includes, excludes);
        for(Object property : partialProperties)
        {
            byte[] colNameBytes = null;
            if(property instanceof String)
            {
                String strProp = (String) property;
                PropertyMetadataBase pm = _entityMeta.getProperty(strProp);
                if(_entityMeta.getUnmappedHandler() == null && pm == null)
                    throw new IllegalArgumentException("unrecognized property " + strProp);
                
                //collections need to be handled separately
                if(pm != null && !isSimpleProp(pm))
                {
                    if(ranges == null)
                        ranges = new ArrayList<CollectionRange>();
                    
                    ranges.add(new CollectionRange(pm));
                    continue; 
                }
                colNameBytes = pm != null ? pm.getPhysicalNameBytes() : serialize(property, true, null);
            }
            else
            {
                if(property instanceof CollectionProperty)
                {
                    CollectionProperty cp = (CollectionProperty) property;
                    PropertyMetadataBase pm = _entityMeta.getProperty(cp.getProperty());
                    
                    boolean rangeRequired = false;
                    if(pm.getPropertyType() == EPropertyType.OBJECT)
                        rangeRequired = true;
                    else if(pm.getPropertyType() == EPropertyType.LIST)
                        rangeRequired = ((ListPropertyMetadata) pm).getElementPropertyMetadata().getPropertyType() != EPropertyType.SIMPLE;
                    else if(pm.getPropertyType() == EPropertyType.MAP || pm.getPropertyType() == EPropertyType.SORTED_MAP)
                        rangeRequired = ((MapPropertyMetadata) pm).getValuePropertyMetadata().getPropertyType() != EPropertyType.SIMPLE;
                    
                    if(rangeRequired)
                    {
                        if(ranges == null)
                            ranges = new ArrayList<CollectionRange>();

                        ranges.add(new CollectionRange(pm, cp.getKey()));
                        continue;
                    }
                    
                    colNameBytes = collectionPropertyName(cp, ComponentEquality.EQUAL);
                }
                else if(_entityMeta.getUnmappedHandler() == null)
                    throw new IllegalArgumentException("property must be string, but encountered " + property);
                else
                    colNameBytes = serialize(property, true, null);
            }
            
            if(colNameBytes == null)
                throw new IllegalArgumentException("could not serialize " + property);
            
            colNames.add(colNameBytes);
        }        
        
        return ranges;
    }
    

    /**
     * bulk load values using a multi get slice query
     * @param keys the keys to fetch
     * @param values the values to update, if null a new list will be created
     * @param colNames the column names to fetch
     * @param first the first column (either colNames or first/last should be set)
     * @param last the last column
     * @param maintainOrder ensure the value list matches the key list by index
     * @return the updated values
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected List<V> bulkLoadFromMultiGet(Collection<K> keys, List<V> values, List<byte[]> colNames, byte[] first, byte[] last, boolean maintainOrder)
    {
        SimplePropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        MultigetSliceQuery<byte[], byte[], byte[]> query = HFactory.createMultigetSliceQuery(_keyspaceFactory.createKeyspace(),
                                                                                             SER_BYTES,
                                                                                             SER_BYTES,
                                                                                             SER_BYTES);

        byte[][] keyBytes = new byte[keys.size()][];

        int i = 0;
        for(K key : keys)
        {
            _logger.debug("loading {}[{}]", _entityMeta.getFamilyName(), key);

            keyBytes[i] = ((Serializer) _entityMeta.getKeyMetadata().getSerializer()).toBytes(key);
            i++;
        }

        query.setKeys(keyBytes);
        query.setColumnFamily(_entityMeta.getFamilyName());

        if(colNames != null)
            query.setColumnNames(colNames.toArray(new byte[colNames.size()][]));
        else
            query.setRange(first, last, false, CassandraDaoBase.COL_RANGE_SIZE);
            
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

    
    /**
     * fetch all the values for a collection property and update entities. 
     * @param keys the entity keys
     * @param values the values to update. if null a new list will be created
     * @param ranges the properties to fetch
     * @return the updated values
     */
    protected List<V> addCollectionRanges(List<K> keys, List<V> values, List<CollectionRange> ranges)
    {
        if(ranges != null)
        {
            for(CollectionRange r : ranges)
                values = bulkLoadFromMultiGet(keys, values, null, r.startBytes(), r.endBytes(), true);
        }

        return values;
    }
    
    /**
     * basic factory method to create a slice query based on the associated column family. 
     * @param keyBytes
     * @return
     */
    protected SliceQuery<byte[], byte[], byte[]> buildSliceQuery(byte[] keyBytes)
    {
        SliceQuery<byte[], byte[], byte[]> query = HFactory.createSliceQuery(_keyspaceFactory.createKeyspace(), SER_BYTES, SER_BYTES, SER_BYTES);
        
        query.setKey(keyBytes);
        query.setColumnFamily(_entityMeta.getFamilyName());

        return query;
    }
}
