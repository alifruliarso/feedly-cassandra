package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

public class FindHelper<K, V> extends LoadHelper<K, V>
{
    FindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory)
    {
        super(meta, factory);
    }

    private V uniqueValue(Collection<V> values)
    {
        if(values == null || values.isEmpty())
            return null;
        
        if(values.size() > 1)
            throw new IllegalStateException("non-unique value");
        
        return values.iterator().next();
    }
    
    public V find(V template)
    {
        return uniqueValue(mfind(template));
    }
    

    public V find(V template, Object start, Object end)
    {
        return uniqueValue(mfind(template, start, end));
    }

    public V find(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        return uniqueValue(mfind(template, includes, excludes));
    }

    public Collection<V> mfind(V template)
    {
        return bulkFindByIndexPartial(template, null, null, null);
    }


    public Collection<V> mfind(V template, Object start, Object end)
    {
        return bulkFindByIndexPartial(template, propertyName(start), propertyName(end), null);
    }
    
    @SuppressWarnings("unchecked")
    public Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        List<byte[]> colNames = new ArrayList<byte[]>();
        List<PropertyMetadata> fullCollectionProperties = derivePartialColumns(colNames, includes, excludes);

        List<V> values = bulkFindByIndexPartial(template, null, null, colNames);
        List<K> keys = new ArrayList<K>(values.size());
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        for(V v : values)
            keys.add((K) invokeGetter(keyMeta, v));
        
        return addFullCollectionProperties(keys, values, fullCollectionProperties);
    }


    @SuppressWarnings("unchecked")
    private List<V> bulkFindByIndexPartial(V template, byte[] startBytes, byte[] endBytes, List<byte[]> colNames)
    {
        IEnhancedEntity entity = asEntity(template);
        BitSet dirty = entity.getModifiedFields();
        List<PropertyMetadata> properties = _entityMeta.getProperties();
        for(int i = dirty.nextSetBit(0); i >= 0; i = dirty.nextSetBit(i + 1))
        {
            PropertyMetadata pm = properties.get(i);
            if(pm.isHashIndexed())
            {
                Object propVal = invokeGetter(pm, template);
                if(propVal != null)
                {
                    PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
                    IndexedSlicesQuery<byte[], byte[], byte[]> query = HFactory.createIndexedSlicesQuery(_keyspaceFactory.createKeyspace(), SER_BYTES, SER_BYTES, SER_BYTES);
                    query.setColumnFamily(_entityMeta.getFamilyName());
                    query.setRowCount(CassandraDaoBase.ROW_RANGE_SIZE);
                    query.addEqualsExpression(pm.getPhysicalNameBytes(), serialize(propVal, false, pm.getSerializer()));
                    
                    if(colNames != null)
                        query.setColumnNames(colNames);
                    else
                        query.setRange(startBytes, endBytes, false, CassandraDaoBase.COL_RANGE_SIZE);
                    
                    OrderedRows<byte[],byte[],byte[]> rows = query.execute().get();
                    
                    List<V> values = new ArrayList<V>();
                    boolean checkDuplicateKey = false;
                    while(true)
                    {
                        /*
                         * the last key of the previous range and the first key of the current range may overlap
                         */
                        for(Row<byte[], byte[], byte[]> row : rows)
                        {
                            K key = (K) keyMeta.getSerializer().fromBytes(row.getKey());
                            
                            if(checkDuplicateKey)
                            {
                                K lastKey = (K) invokeGetter(keyMeta, values.get(values.size()-1));
                                
                                if(key.equals(lastKey))
                                    continue;
                                
                                checkDuplicateKey = false;
                            }
                            
                            V value = fromColumnSlice(key, null, keyMeta, row.getKey(), null, row.getColumnSlice(), endBytes);
                            
                            if(value != null)
                                values.add(value);
                        }
                        
                        if(rows.getCount() == CassandraDaoBase.ROW_RANGE_SIZE)
                        {
                            query.setStartKey(rows.getList().get(CassandraDaoBase.ROW_RANGE_SIZE-1).getKey());
                            rows = query.execute().get();
                            checkDuplicateKey = true;
                        }
                        else 
                            break;
                    } 
                    
                    return values;
                }
            }
        }
        
        throw new IllegalArgumentException("no applicable index found.");
    }

}
