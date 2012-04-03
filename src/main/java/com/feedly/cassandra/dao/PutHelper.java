package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.ByteIndicatorSerializer;
import com.feedly.cassandra.entity.EIndexType;
import com.feedly.cassandra.entity.EPropertyType;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.ListPropertyMetadata;
import com.feedly.cassandra.entity.MapPropertyMetadata;
import com.feedly.cassandra.entity.PropertyMetadataBase;
import com.feedly.cassandra.entity.SimplePropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

class PutHelper<K, V> extends DaoHelperBase<K, V>
{
    private static final LongSerializer SER_LONG = LongSerializer.get();

    private static final byte[] WAL_COL_NAME = StringSerializer.get().toBytes("rowkey"); 
    private static final byte[] IDX_COL_VAL = new byte[] {0}; 

    PutHelper(EntityMetadata<V> meta, IKeyspaceFactory factory)
    {
        super(meta, factory);
    }

    public void put(V value)
    {
        mput(Collections.singleton(value));
    }

    public void mput(Collection<V> values)
    {
        SimplePropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        Keyspace keyspace = _keyspaceFactory.createKeyspace();
        Mutator<byte[]> mutator = HFactory.createMutator(keyspace, SER_BYTES);
        Mutator<byte[]> walMutator = null;
        Mutator<byte[]> walCleanupMutator = null;
        boolean indexesUpdated = false;
        long clock = keyspace.createClock();
        byte[] nowBytes = SER_LONG.toBytes(clock);
        
        //prepare the operations...
        for(V value : values)
        {
            Object key = invokeGetter(keyMeta, value);
            byte[] keyBytes = serialize(key, false, keyMeta.getSerializer());

            
            _logger.debug("inserting {}[{}]", _entityMeta.getType().getSimpleName(), key);

            int[] colCnts = saveDirtyFields(key, keyBytes, value, clock, mutator);
            colCnts[0] += saveUnmappedFields(key, keyBytes, value, clock, mutator);

            if(colCnts[0] == 0)
                _logger.info("no updates for ", key);
            
            _logger.debug("updated {} values for {}[{}]", new Object[] { colCnts[0], _entityMeta.getType().getSimpleName(), key });
            
            if(colCnts[1] > 0)
            {
                _logger.debug("updated {} indexes for {}[{}]", new Object[] { colCnts[1], _entityMeta.getType().getSimpleName(), key });
                if(walMutator == null)
                {
                    walMutator = HFactory.createMutator(keyspace, SER_BYTES);
                    walCleanupMutator = HFactory.createMutator(keyspace, SER_BYTES);
                }
                indexesUpdated = true;
                HColumn<byte[], byte[]> column = HFactory.createColumn(WAL_COL_NAME, nowBytes, clock, SER_BYTES, SER_BYTES);
                walMutator.addInsertion(keyBytes, _entityMeta.getWalFamilyName(), column);
                walCleanupMutator.addDeletion(keyBytes, _entityMeta.getWalFamilyName(), WAL_COL_NAME, SER_BYTES, clock);
            }
        }
        

        /*
         * insert into WAL indicating index update about to happen, if something happens, the WAL row will indicate which rows 
         * need to be made consistent with its indexes
         */
        if(indexesUpdated)
            walMutator.execute();
            
        /*
         * execute the index and table updates
         */
        mutator.execute();
        
        /*
         * finally delete the WAL entries, no longer needed as mutation was successful
         */
        if(indexesUpdated)
            walCleanupMutator.execute();
        
        
        //do after execution
        for(V value : values)
        {
            IEnhancedEntity entity = asEntity(value);
            entity.getModifiedFields().clear();
            entity.setUnmappedFieldsModified(false);
        }
        
        _logger.info("inserted {} values into {}", values.size(), _entityMeta.getType().getSimpleName());

    }

    //rv[0] = total col cnt, rv[1] = range index update count
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int[] saveDirtyFields(Object key,
                                  byte[] keyBytes, 
                                  V value, 
                                  long clock,
                                  Mutator<byte[]> mutator)
    {
        List<PropertyMetadataBase> properties = _entityMeta.getProperties();

        IEnhancedEntity entity = asEntity(value);
        BitSet dirty = entity.getModifiedFields();
        int[] rv = new int[2];
        Set<IndexMetadata> affectedIndexes = new HashSet<IndexMetadata>();
        if(!dirty.isEmpty())
        {
            for(int i = dirty.nextSetBit(0); i >= 0; i = dirty.nextSetBit(i + 1))
            {
                PropertyMetadataBase colMeta = properties.get(i);

                if(isCollectionProp(colMeta))
                {
                    StringBuilder descriptor = null;
                    if(_logger.isTraceEnabled())
                    {
                        descriptor = new StringBuilder();
                        descriptor.append(_entityMeta.getFamilyName());
                        descriptor.append("[").append(key).append("]").append(".").append(colMeta.getName());
                    }
                    
                    DynamicComposite colName = new DynamicComposite(colMeta.getPhysicalName());

                    if(colMeta.getPropertyType() == EPropertyType.LIST)
                    {
                        List<?> list = (List<?>) invokeGetter(colMeta, value);
                        rv[0] += saveListFields(descriptor, keyBytes, colName, (ListPropertyMetadata) colMeta, list, clock, mutator);
                    }
                    else
                    {
                        Map<?, ?> map = (Map<?,?>) invokeGetter(colMeta, value);
                        rv[0] += saveMapFields(descriptor, keyBytes, colName, (MapPropertyMetadata) colMeta, map, clock, mutator);
                    }
                }
                else
                {
                    SimplePropertyMetadata spm = (SimplePropertyMetadata) colMeta;
                    Object propVal = invokeGetter(colMeta, value);
                    
                    _logger.trace("{}[{}].{} = {}", new Object[] { _entityMeta.getType().getSimpleName(), key, colMeta.getName(), propVal});
                    
                    if(propVal != null)
                    {
                        HColumn column = HFactory.createColumn(colMeta.getPhysicalNameBytes(), propVal, clock, SER_BYTES, (Serializer) spm.getSerializer());
                        mutator.addInsertion(keyBytes, _entityMeta.getFamilyName(), column);

                    }
                    else
                    {
                        mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colMeta.getPhysicalNameBytes(), SER_BYTES, clock);
                    }

                    for(IndexMetadata idxMeta : _entityMeta.getIndexes((SimplePropertyMetadata) colMeta))
                    {
                        if(idxMeta.getType() == EIndexType.RANGE && affectedIndexes.add(idxMeta))
                        {
                            addIndexWrite(key, value, dirty, idxMeta, clock, mutator);
                            rv[1]++;
                        }
                    }

                    rv[0]++;
                }
            }

        }

        return rv;
    }

    /*
     * index column family structure
     * row key:   idx_id:partition key 
     * column:    index value:rowkey
     * value:     meaningless
     */
    private void addIndexWrite(Object key, V value, BitSet dirty, IndexMetadata idxMeta, long clock, Mutator<byte[]> mutator)
    {
        List<Object> propVals = null;
        DynamicComposite colName = new DynamicComposite();
        boolean propertyNotSet = false;
        for(SimplePropertyMetadata pm : idxMeta.getIndexedProperties())
        {
            Object pval = invokeGetter(pm, value);
            if(pval != null)
            {
                if(propVals == null)
                    propVals = new ArrayList<Object>(idxMeta.getIndexedProperties().size());
                
                propVals.add(pval);
                colName.add(pval);
            }
            else if(!dirty.get(_entityMeta.getPropertyPosition(pm))) //prop value is null and not set
                propertyNotSet = true;
        }

        if(propVals == null) //no index property updated
            return;
        
        if(propertyNotSet) //must update none or all of a multi column index
            throw new IllegalArgumentException("cannot write a subset of columns to multi-column index: " + idxMeta);
        
        if(propVals.size() < idxMeta.getIndexedProperties().size()) //some index properties set to null -> entry is removed from index
            return;
        
        colName.add(key);
        
        HColumn<DynamicComposite, byte[]> column = HFactory.createColumn(colName, IDX_COL_VAL, clock, SER_COMPOSITE, SER_BYTES);
        
        DynamicComposite rowKey = new DynamicComposite(idxMeta.id());
        
        List<List<Object>> allPartitions = idxMeta.getIndexPartitioner().partitionValue(propVals);
        if(allPartitions.size() != 1)
            throw new IllegalStateException("expected single partition but encountered " + allPartitions.size());
            
        _logger.trace("writing to partition {}", allPartitions.get(0));
        for(Object partitionVal : allPartitions.get(0))
            rowKey.add(partitionVal);
        
        mutator.addInsertion(SER_COMPOSITE.toBytes(rowKey), _entityMeta.getIndexFamilyName(), column);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private int saveMapFields(StringBuilder descriptor,
                              byte[] keyBytes,
                              DynamicComposite colName,
                              MapPropertyMetadata colMeta, 
                              Map<?, ?> map, 
                              long clock, 
                              Mutator<byte[]> mutator)
    {
        if(map == null)
        {
            _logger.warn("{} null collections are ignored, to delete values, set individual keys with null values", 
                         descriptor == null ? "" : descriptor);
            return 0;
        }

        int cnt = 0;
        
        PropertyMetadataBase valuePropertyMeta = colMeta.getValuePropertyMetadata();
        EPropertyType t = valuePropertyMeta.getPropertyType();
        for(Map.Entry<?, ?> entry : map.entrySet())
        {
            String keyStr = null;
            if(descriptor != null)
            {
                keyStr = entry.getKey().toString();
                descriptor.append(".").append(keyStr);
            }

            if(t == EPropertyType.SIMPLE)
            {
                DynamicComposite dc = new DynamicComposite(colName);
                dc.addComponent(entry.getKey(), (Serializer) colMeta.getKeyPropertyMetadata().getSerializer());
                saveCollectionColumn(descriptor, keyBytes, dc, entry.getValue(), (SimplePropertyMetadata) valuePropertyMeta, clock, mutator);
                cnt++;
            }
            else if(t == EPropertyType.LIST)
            {
                colName.addComponent(entry.getKey(), (Serializer) colMeta.getKeyPropertyMetadata().getSerializer());
                cnt += saveListFields(descriptor, 
                                      keyBytes, 
                                      colName, 
                                      (ListPropertyMetadata) valuePropertyMeta, 
                                      (List<?>) entry.getValue(), 
                                      clock, 
                                      mutator);
                colName.remove(colName.size() - 1);
            }
            else if(t == EPropertyType.MAP || t == EPropertyType.SORTED_MAP)
            {
                Map<?, ?> subMap = (Map<?, ?>) entry.getValue();
                colName.addComponent(entry.getKey(), (Serializer) colMeta.getKeyPropertyMetadata().getSerializer());
                cnt += saveMapFields(descriptor, 
                                     keyBytes, 
                                     colName, 
                                     (MapPropertyMetadata) valuePropertyMeta, 
                                     subMap, 
                                     clock, 
                                     mutator);
                colName.remove(colName.size() - 1);
            }
            
            if(descriptor != null)
            {
                int len = descriptor.length();
                descriptor.delete(len - (1 + keyStr.length()), len);
            }
        }
        
        return cnt;  
    }

    private int saveListFields(StringBuilder descriptor, 
                               byte[] keyBytes, 
                               DynamicComposite colName, 
                               ListPropertyMetadata colMeta, 
                               List<?> list, 
                               long clock, 
                               Mutator<byte[]> mutator)
    {
        if(list == null)
        {
            _logger.warn("{} null collections are ignored, to delete values, set individual keys with null values",
                         descriptor == null ? "" : descriptor);
            return 0;
        }
        
        int size = list.size();
        
        int dbIdx = 0;

        PropertyMetadataBase elementPropertyMeta = colMeta.getElementPropertyMetadata();
        EPropertyType t = colMeta.getElementPropertyMetadata().getPropertyType();

        int cnt = 0;
        for(int i = 0; i < size; i++) 
        {
            String keyStr = null;
            if(descriptor != null)
            {
                keyStr = String.valueOf(i);                    
                descriptor.append("[").append(keyStr).append("]");
            }

            Object listVal = list.get(i);
            if(listVal != null)
            {
                if(t == EPropertyType.SIMPLE)
                {
                    DynamicComposite dc = new DynamicComposite(colName);
                    dc.add(dbIdx);
                    saveCollectionColumn(descriptor, keyBytes, dc, listVal, (SimplePropertyMetadata) elementPropertyMeta, clock, mutator);
                    cnt++;
                }
                else if(t == EPropertyType.LIST)
                {
                    colName.add(dbIdx);
                    cnt += saveListFields(descriptor, 
                                          keyBytes, 
                                          colName, 
                                          (ListPropertyMetadata) elementPropertyMeta, 
                                          (List<?>) listVal, 
                                          clock, 
                                          mutator);
                    colName.remove(colName.size() - 1);
                }
                else if(t == EPropertyType.MAP || t == EPropertyType.SORTED_MAP)
                {
                    colName.add(dbIdx);
                    cnt += saveMapFields(descriptor, 
                                         keyBytes, 
                                         colName, 
                                         (MapPropertyMetadata) elementPropertyMeta, 
                                         (Map<?,?>) listVal, 
                                         clock, 
                                         mutator);
                    colName.remove(colName.size() - 1);
                }
                dbIdx++;
            }
            
            if(descriptor != null)
            {
                int len = descriptor.length();
                descriptor.delete(len - (keyStr.length() + 2), len);
            }
        }

        if(dbIdx < size && colMeta.getElementPropertyMetadata() instanceof SimplePropertyMetadata)
        {
            _logger.debug("{}[{}] list shortened. deleting last {} entries",
                          new Object[]{descriptor, size - dbIdx});
            
            Mutator<byte[]> delMutator = HFactory.createMutator(_keyspaceFactory.createKeyspace(), SER_BYTES);
            for(int i = dbIdx; i < size; i++)
            {
                String keyStr = null;
                if(descriptor != null)
                {
                    keyStr = String.valueOf(i);                    
                    descriptor.append("[").append(keyStr).append("]");
                }
                
                DynamicComposite dc = new DynamicComposite(colName);
                dc.add(i);
                saveCollectionColumn(descriptor, 
                                     keyBytes, 
                                     dc,
                                     null, 
                                     (SimplePropertyMetadata) colMeta.getElementPropertyMetadata(), 
                                     clock-1,
                                     delMutator);

                if(descriptor != null)
                {
                    int len = descriptor.length();
                    descriptor.delete(len - (keyStr.length() + 2), len);
                }
            }

            delMutator.execute();
        }

        return cnt;  
    }

    private boolean saveCollectionColumn(StringBuilder descriptor,
                                         byte[] keyBytes,
                                         DynamicComposite colName,
                                         Object propVal,
                                         SimplePropertyMetadata colMeta,
                                         long clock,
                                         Mutator<byte[]> mutator)
    {
        _logger.trace("{} = {}", descriptor, propVal);

        if(propVal == null)
        {
            mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colName, SER_COMPOSITE, clock);
        }
        else
        {
            byte[] propValBytes = serialize(propVal, false, colMeta.getSerializer());
            if(propValBytes == null)
            {
                throw new IllegalArgumentException(String.format("problem serializing %s = %s. ensure values can be serialized",
                                                                 _entityMeta.getFamilyName(),
                                                                 descriptor,
                                                                 propVal));
            }

            HColumn<DynamicComposite, byte[]> column = HFactory.createColumn(colName, propValBytes, clock, SER_COMPOSITE, SER_BYTES);
            mutator.addInsertion(keyBytes, _entityMeta.getFamilyName(), column);
        }
        
        return propVal != null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int saveUnmappedFields(Object key, byte[] keyBytes, V value, long clock, Mutator<byte[]> mutator)
    {
        if(!asEntity(value).getUnmappedFieldsModified())
            return 0;

        MapPropertyMetadata unmappedMeta = _entityMeta.getUnmappedHandler();

        if(unmappedMeta == null)
            return 0;
        
        Map<?, ?> unmapped = (Map) invokeGetter(unmappedMeta, value);
        
        if(unmapped == null)
            return 0;
            

        int colCnt = unmapped.size();

        Serializer<?> valueSer;
        
        if(unmappedMeta.getValuePropertyMetadata().getPropertyType() == EPropertyType.SIMPLE)
            valueSer = ((SimplePropertyMetadata) unmappedMeta.getValuePropertyMetadata()).getSerializer();
        else
            valueSer = ByteIndicatorSerializer.get();
        for(Map.Entry<?, ?> entry : unmapped.entrySet())
        {

            Object colVal = entry.getValue();
            _logger.trace("{}[{}].{} = {}", new Object[] {_entityMeta.getType().getSimpleName(), key, entry.getKey(), colVal});

            if(!(entry.getKey() instanceof String))
                throw new IllegalArgumentException("only string keys supported for unmapped properties");
            
            byte[] colName = serialize(entry.getKey(), true, null);  
            
            if(colVal != null)
            {
                byte[] colValBytes = serialize(colVal, false, valueSer);
                
                if(colName == null || colValBytes == null)
                {
                    throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s = %s. ensure values are non-null and can be serialized",
                                                                     _entityMeta.getFamilyName(),
                                                                     key,
                                                                     entry.getKey(),
                                                                     colVal));
                }
                
                HColumn column = HFactory.createColumn(colName, colValBytes, clock, SER_BYTES, SER_BYTES);
                mutator.addInsertion(keyBytes, _entityMeta.getFamilyName(), column);
            }
            else
            {
                if(colName == null)
                {
                    throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s = null. ensure values are non-null and can be serialized",
                                                                     _entityMeta.getFamilyName(),
                                                                     key,
                                                                     colName));
                }
                
                mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colName, SER_BYTES, clock);
            }
        }

        return colCnt;
    }


}
