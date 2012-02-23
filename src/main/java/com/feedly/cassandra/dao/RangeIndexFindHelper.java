package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;

public class RangeIndexFindHelper<K, V> extends LoadHelper<K, V>
{
    private static final Logger _logger = LoggerFactory.getLogger(RangeIndexFindHelper.class.getName());
    
    private final GetHelper<K, V> _getHelper;
    private final IStaleIndexValueStrategy _staleValueStrategy;
    RangeIndexFindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory, IStaleIndexValueStrategy staleValueStrategy)
    {
        super(meta, factory);
        _getHelper = new GetHelper<K, V>(meta, factory);
        _staleValueStrategy = staleValueStrategy;
    }

    private V uniqueValue(Collection<V> values)
    {
        if(values == null || values.isEmpty())
            return null;
        
        if(values.size() > 1)
            throw new IllegalStateException("non-unique value");
        
        return values.iterator().next();
    }
    
    public V find(V template, IndexMetadata index)
    {
        return uniqueValue(mfind(template, index));
    }
    

    public V find(V template, Set<? extends Object> includes, Set<String> excludes, IndexMetadata index)
    {
        return uniqueValue(mfind(template, includes, excludes, index));
    }

    public Collection<V> mfind(V template, IndexMetadata index)
    {
        Map<K, StaleIndexValue> keys = findKeys(template, template, index);
        Collection<V> values = _getHelper.mget(keys.keySet());
        filterValues(keys, values, new EqualityFilter(template), index); 
        
        return values;
    }
    

    @SuppressWarnings("unchecked")
    public Collection<V> mfind(V template, Object start, Object end, IndexMetadata index)
    {
        Set<String> indexed = new HashSet<String>();
        for(PropertyMetadata pm : index.getIndexedProperties())
            indexed.add(pm.getName());
        
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        List<V> values = mfind(template, indexed, null, index);
        
        List<K> filteredKeys = new ArrayList<K>(values.size());
        for(V v : values)
            filteredKeys.add((K) invokeGetter(keyMeta, v));
        
        return _getHelper.mget(filteredKeys, values, start, end);
    }

    public List<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes, IndexMetadata index)
    {
        Map<K, StaleIndexValue> keys = findKeys(template, template, index);
        
        Set<Object> partialProperties = new HashSet<Object>(partialProperties(includes, excludes));
        
        for(PropertyMetadata pm : index.getIndexedProperties())
            partialProperties.add(pm.getName());
        
        List<V> values = _getHelper.mget(new ArrayList<K>(keys.keySet()), null, partialProperties, null);
        
        filterValues(keys, values, new EqualityFilter(template), index); 
        
        return values;
    }

    public Collection<V> mfindBetween(V startTemplate, V endTemplate, IndexMetadata index)
    {
        Map<K, StaleIndexValue> keys = findKeys(startTemplate, endTemplate, index);
        Collection<V> values = _getHelper.mget(keys.keySet());
        filterValues(keys, values, new RangeFilter(startTemplate, endTemplate, index), index); 
        
        return values;
    }

    @SuppressWarnings("unchecked")
    public Collection<V> mfindBetween(V startTemplate, V endTemplate, Object startColumn, Object endColumn, IndexMetadata index)
    {
        Set<String> indexed = new HashSet<String>();
        for(PropertyMetadata pm : index.getIndexedProperties())
            indexed.add(pm.getName());
        
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        List<V> values = mfindBetween(startTemplate, endTemplate, indexed, null, index);
        
        List<K> filteredKeys = new ArrayList<K>(values.size());
        for(V v : values)
            filteredKeys.add((K) invokeGetter(keyMeta, v));
        
        return _getHelper.mget(filteredKeys, values, startColumn, endColumn);

    }

    public List<V> mfindBetween(V startTemplate, V endTemplate, Set<? extends Object> includes, Set<String> excludes, IndexMetadata index)
    {
        Map<K, StaleIndexValue> keys = findKeys(startTemplate, endTemplate, index);
        
        Set<Object> partialProperties = new HashSet<Object>(partialProperties(includes, excludes));
        
        //ensure properties needed for read filtering are read
        for(PropertyMetadata pm : index.getIndexedProperties())
            partialProperties.add(pm.getName());
        
        List<V> values = _getHelper.mget(new ArrayList<K>(keys.keySet()), null, partialProperties, null);
        
        filterValues(keys, values, new RangeFilter(startTemplate, endTemplate, index), index); 
        
        return values;

    }

    private void filterValues(Map<K, StaleIndexValue> indexValues, Collection<V> values, IValueFilter<V> filter, IndexMetadata index)
    {
        List<StaleIndexValue> filtered = null;
        
        Iterator<V> iter = values.iterator();
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        while(iter.hasNext())
        {
            V value  = iter.next();
            if(filter.isFiltered(value))
            {
                iter.remove();
                if(filtered == null)
                    filtered = new ArrayList<StaleIndexValue>();

                @SuppressWarnings("unchecked")
                K key = (K) invokeGetter(keyMeta, value);
                filtered.add(indexValues.get(key));

                _logger.debug("filtered {}[{}]", _entityMeta.getFamilyName(), key);
            }
        }
        
        if(filtered != null)
            _staleValueStrategy.handle(_entityMeta, index, filtered);
    }

    private List<Object> indexValues(V template, IndexMetadata index) 
    {
        BitSet dirty = asEntity(template).getModifiedFields();
        Set<PropertyMetadata> modified = new HashSet<PropertyMetadata>();
        List<Object> propValues = new ArrayList<Object>();
        for(int i = dirty.nextSetBit(0); i>= 0; i = dirty.nextSetBit(i+1))
            modified.add(_entityMeta.getProperties().get(i));
        
        for(PropertyMetadata pm : index.getIndexedProperties())
        {
            if(modified.contains(pm))
                propValues.add(invokeGetter(pm, template));
            else
                break;
        }
        
        return propValues;
    }
    
    /*
     * index column family structure
     * row key:   idx_id:partition key 
     * column:    index value:rowkey
     * value:     meaningless
     */
    @SuppressWarnings("unchecked")
    private Map<K, StaleIndexValue> findKeys(V startTemplate, V endTemplate, IndexMetadata index)
    {
        List<Object> startPropVals = indexValues(startTemplate, index);
        List<Object> endPropVals;
        List<? extends Object> indexPartitions;
        if(startTemplate == endTemplate)
        {
            endPropVals = startPropVals;
            indexPartitions = index.getIndexPartitioner().partitionValue(startPropVals);
        }
        else
        {
            endPropVals = indexValues(endTemplate, index);
            indexPartitions = index.getIndexPartitioner().partitionRange(startPropVals, endPropVals);            
        }
        
        List<DynamicComposite> rowKeys = new ArrayList<DynamicComposite>();
        for(Object partitionValue : indexPartitions)
        {
            DynamicComposite rowKey = new DynamicComposite();
            rowKey.add(index.id());
            rowKey.add(partitionValue);
            rowKeys.add(rowKey);
        }

        DynamicComposite startCol = new DynamicComposite(startPropVals);
        startCol.setEquality(ComponentEquality.EQUAL);
        DynamicComposite endCol = new DynamicComposite(endPropVals);
        endCol.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
        MultigetSliceQuery<DynamicComposite,DynamicComposite,byte[]> multiGetQuery =
                HFactory.createMultigetSliceQuery(_keyspaceFactory.createKeyspace(), SER_COMPOSITE, SER_COMPOSITE, SER_BYTES);

        SliceQuery<DynamicComposite,DynamicComposite,byte[]> query = null;

        multiGetQuery.setKeys(rowKeys);
        multiGetQuery.setColumnFamily(_entityMeta.getIndexFamilyName());
        
        Rows<DynamicComposite,DynamicComposite,byte[]> indexRows;
        Map<K, StaleIndexValue> keys = new HashMap<K, StaleIndexValue>();
        multiGetQuery.setRange(startCol, endCol, false, CassandraDaoBase.COL_RANGE_SIZE);
        indexRows = multiGetQuery.execute().get();
        for(Row<DynamicComposite, DynamicComposite, byte[]> row : indexRows)
        {
            List<HColumn<DynamicComposite,byte[]>> columns = row.getColumnSlice().getColumns();
            while(true)
            {
                for(HColumn<DynamicComposite, byte[]> col : columns)
                {
                    keys.put((K) col.getName().get(col.getName().size()-1), new StaleIndexValue(row.getKey(), col.getName(), col.getClock()));
                }
    
                if(columns.size() == CassandraDaoBase.COL_RANGE_SIZE)
                {
                    startCol = columns.get(CassandraDaoBase.COL_RANGE_SIZE - 1).getName();
                    if(query == null)
                    {
                        query = HFactory.createSliceQuery(_keyspaceFactory.createKeyspace(), SER_COMPOSITE, SER_COMPOSITE, SER_BYTES);
                    }
                    query.setKey(row.getKey());
                    query.setColumnFamily(_entityMeta.getIndexFamilyName());
                    query.setRange(startCol, endCol, false, CassandraDaoBase.COL_RANGE_SIZE);
                    columns = query.execute().get().getColumns();
                }
                else
                    break;
            }
        }
        
        if(_logger.isDebugEnabled())
        {
            if(startTemplate == endTemplate)
                _logger.debug("query {}, found {} keys", indexValuesToString(startTemplate, index), keys.size());
            else
                _logger.debug("range query {} - {}, found {} keys", 
                              new Object[] {indexValuesToString(startTemplate, index), indexValuesToString(endTemplate, index),  keys.size()});
        }
        
        return keys;
    }
    
    private String indexValuesToString(V template, IndexMetadata index)
    {
        BitSet dirty = asEntity(template).getModifiedFields();

        StringBuilder b = new StringBuilder();
        b.append(_entityMeta.getFamilyName()).append("[");

        boolean first = true;
        for(int i = dirty.nextSetBit(0); i>= 0; i = dirty.nextSetBit(i+1))
        {
            if(first)
                first = false;
            else
                b.append(" and ");
                
            PropertyMetadata pm =_entityMeta.getProperties().get(i); 
            b.append("@").append(pm.getName()).append(" = ").append(invokeGetter(pm, template));
        }

        return b.append("]").toString();
    }
    
    private interface IValueFilter<V>
    {
        public boolean isFiltered(V value);
    }
    
    private class EqualityFilter implements IValueFilter<V>
    {
        private final Map<PropertyMetadata, Object> _filter = new HashMap<PropertyMetadata, Object>();

        public EqualityFilter(V template)
        {
            BitSet dirty = asEntity(template).getModifiedFields();
            for(int i = dirty.nextSetBit (0); i>= 0; i = dirty.nextSetBit(i+1)) 
            {
                PropertyMetadata p = _entityMeta.getProperties().get(i);
                _filter.put(p, invokeGetter(p, template));
            }
        }
        
        public boolean isFiltered(V value)
        {
            for(Map.Entry<PropertyMetadata, Object> entry : _filter.entrySet())
            {
                if(!entry.getValue().equals(invokeGetter(entry.getKey(), value)))
                    return true;
            }

            return false;
        }
    }

    private class RangeFilter implements IValueFilter<V>
    {
        private final LinkedHashMap<PropertyMetadata, Object> _startIdxProps = new LinkedHashMap<PropertyMetadata, Object>();
        private final Map<PropertyMetadata, Object> _startProps = new HashMap<PropertyMetadata, Object>();
        private final LinkedHashMap<PropertyMetadata, Object> _endIdxProps = new LinkedHashMap<PropertyMetadata, Object>();
        private final Map<PropertyMetadata, Object> _endProps = new HashMap<PropertyMetadata, Object>();
        
        public RangeFilter(V startTemplate, V endTemplate, IndexMetadata idx)
        {
            toMap(startTemplate, _startIdxProps, _startProps, idx);
            toMap(endTemplate, _endIdxProps, _endProps, idx);
        }

        private void toMap(V template, 
                           LinkedHashMap<PropertyMetadata, Object> idxProps, 
                           Map<PropertyMetadata, Object> props, 
                           IndexMetadata idx)
        {
            BitSet dirty = asEntity(template).getModifiedFields();
            
            for(int i = dirty.nextSetBit (0); i>= 0; i = dirty.nextSetBit(i+1)) 
            {
                PropertyMetadata p = _entityMeta.getProperties().get(i);
                props.put(p, invokeGetter(p, template));
            }      
            
            boolean include = true;
            for(PropertyMetadata pm : idx.getIndexedProperties())
            {
                Object val = props.remove(pm);
                if(val == null)
                    include = false;
                
                if(include)
                    idxProps.put(pm, val);
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public boolean isFiltered(V value)
        {
            //for indexed properties, do comparison against entire index as one unit
            for(Map.Entry<PropertyMetadata, Object> entry : _startIdxProps.entrySet())
            {
                Comparable tVal = (Comparable) entry.getValue();
                Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value);
                
                int cmp = tVal.compareTo(vVal);
                if(cmp < 0)
                    break;
                
                if(cmp > 0)
                    return true;
            }

            //for other props, check current val is greater property by property
            for(Map.Entry<PropertyMetadata, Object> entry : _startProps.entrySet())
            {
                Comparable tVal = (Comparable) entry.getValue();
                Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value);
                
                if(tVal.compareTo(vVal) > 0)
                    return true;
            }
            
            
            for(Map.Entry<PropertyMetadata, Object> entry : _endIdxProps.entrySet())
            {
                Comparable tVal = (Comparable) entry.getValue();
                Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value);
                
                int cmp = tVal.compareTo(vVal);
                if(cmp > 0)
                    break;
                
                if(cmp < 0)
                    return true;
            }

            for(Map.Entry<PropertyMetadata, Object> entry : _endProps.entrySet())
            {
                Comparable tVal = (Comparable) entry.getValue();
                Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value);
                
                if(tVal.compareTo(vVal) < 0)
                    return true;
            }

            return false;
        }
    }
}
