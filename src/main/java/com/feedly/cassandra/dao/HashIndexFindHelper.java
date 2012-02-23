package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;

public class HashIndexFindHelper<K, V> extends LoadHelper<K, V>
{
    HashIndexFindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory)
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
    
    public V find(V template, IndexMetadata index)
    {
        return uniqueValue(mfind(template, index));
    }
    

    public V find(V template, Object start, Object end, IndexMetadata index)
    {
        return uniqueValue(mfind(template, start, end, index));
    }

    public V find(V template, Set<? extends Object> includes, Set<String> excludes, IndexMetadata index)
    {
        return uniqueValue(mfind(template, includes, excludes, index));
    }

    public Collection<V> mfind(V template, IndexMetadata index)
    {
        return bulkFindByIndexPartial(template, null, null, null, index);
    }


    public Collection<V> mfind(V template, Object start, Object end, IndexMetadata index)
    {
        return bulkFindByIndexPartial(template, propertyName(start), propertyName(end), null, index);
    }
    
    @SuppressWarnings("unchecked")
    public Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes, IndexMetadata index)
    {
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        List<byte[]> colNames = new ArrayList<byte[]>();
        List<PropertyMetadata> fullCollectionProperties = derivePartialColumns(colNames, includes, excludes);

        List<V> values = bulkFindByIndexPartial(template, null, null, colNames, index);
        List<K> keys = new ArrayList<K>(values.size());
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        for(V v : values)
            keys.add((K) invokeGetter(keyMeta, v));
        
        return addFullCollectionProperties(keys, values, fullCollectionProperties);
    }


    @SuppressWarnings("unchecked")
    private List<V> bulkFindByIndexPartial(V template, byte[] startBytes, byte[] endBytes, List<byte[]> colNames, IndexMetadata index)
    {
        PropertyMetadata pm = index.getIndexedProperties().get(0); //must be exactly 1
                
        Object propVal = invokeGetter(pm, template);
        if(propVal == null)
            throw new IllegalArgumentException("null values not supported for hash indexes");
        
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
