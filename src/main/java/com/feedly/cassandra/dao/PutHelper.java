package com.feedly.cassandra.dao;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

public class PutHelper<K, V> extends BaseDaoHelper<K, V>
{

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
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        Mutator<byte[]> mutator = HFactory.createMutator(_keyspaceFactory.createKeyspace(), SER_BYTES);

        for(V value : values)
        {
            Object key = invokeGetter(keyMeta, value);
            byte[] keyBytes = serialize(key, false, keyMeta.getSerializer());

            _logger.debug("inserting {}[{}]", _entityMeta.getType().getSimpleName(), key);

            int colCnt = saveDirtyFields(key, keyBytes, value, mutator);
            colCnt += saveUnmappedFields(key, keyBytes, value, mutator);

            if(colCnt == 0)
                _logger.warn("no updates for ", key);
            
            _logger.debug("updated {} values for {}[{}]", new Object[] { colCnt, _entityMeta.getType().getSimpleName(), key });
        }
        
        mutator.execute();
        
        //do after execution
        for(V value : values)
        {
            IEnhancedEntity entity = asEntity(value);
            entity.getModifiedFields().clear();
            entity.setUnmappedFieldsModified(false);
        }
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int saveDirtyFields(Object key, byte[] keyBytes, V value, Mutator<byte[]> mutator)
    {
        List<PropertyMetadata> properties = _entityMeta.getProperties();

        IEnhancedEntity entity = asEntity(value);
        BitSet dirty = entity.getModifiedFields();
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
                    
                    _logger.trace("{}[{}].{} = {}", new Object[] { _entityMeta.getType().getSimpleName(), key, colMeta.getName(), propVal});
                    
                    if(propVal != null)
                    {
                        HColumn column = HFactory.createColumn(colMeta.getPhysicalNameBytes(), propVal, SER_BYTES, (Serializer) colMeta.getSerializer());
                        mutator.addInsertion(keyBytes, _entityMeta.getFamilyName(), column);
                    }
                    else
                    {
                        mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colMeta.getPhysicalNameBytes(), SER_BYTES);
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
                                          _entityMeta.getFamilyName(),
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
                         new Object[] {_entityMeta.getFamilyName(), key, colMeta.getName()});
                          
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
                          new Object[]{_entityMeta.getFamilyName(), key, colMeta.getName(), size - nullIdx});
            
            
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
        _logger.trace("{}[{}].{}:{} = {}", new Object[] {_entityMeta.getType().getSimpleName(), key, colMeta.getName(), propKey, propVal});

        if(propKey == null)
        {
            throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s:null = %s. ensure keys are not null",
                                                             _entityMeta.getFamilyName(),
                                                             key,
                                                             colMeta.getName(),
                                                             propVal));
        }

        DynamicComposite colName = new DynamicComposite(colMeta.getName(), propKey);
        if(propVal == null)
        {
            mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colName, SER_COMPOSITE);
        }
        else
        {
            byte[] propValBytes = serialize(propVal, false, colMeta.getSerializer());
            if(propValBytes == null)
            {
                throw new IllegalArgumentException(String.format("problem serializing %s[%s].%s:%s = %s. ensure values can be serialized",
                                                                 _entityMeta.getFamilyName(),
                                                                 key,
                                                                 colMeta.getName(),
                                                                 propKey,
                                                                 propVal));
            }

            HColumn<DynamicComposite, byte[]> column = HFactory.createColumn(colName, propValBytes, SER_COMPOSITE, SER_BYTES);
            mutator.addInsertion(keyBytes, _entityMeta.getFamilyName(), column);
        }
        
        return propVal != null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private int saveUnmappedFields(Object key, byte[] keyBytes, V value, Mutator<byte[]> mutator)
    {
        if(!asEntity(value).getUnmappedFieldsModified())
            return 0;

        PropertyMetadata unmappedMeta = _entityMeta.getUnmappedHandler();

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
                
                HColumn column = HFactory.createColumn(colName, colValBytes, SER_BYTES, SER_BYTES);
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
                
                mutator.addDeletion(keyBytes, _entityMeta.getFamilyName(), colName, SER_BYTES);
            }
        }

        return colCnt;
    }


}
