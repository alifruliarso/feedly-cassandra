package com.feedly.cassandra.dao;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadataBase;
import com.feedly.cassandra.entity.SimplePropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

/**
 * used to perform filter-on-read logic during index retrievals. This class handles equality checks.
 * 
 * @author kireet
 *
 * @param <V> the entity type
 */
class EqualityValueFilter<V> implements IValueFilter<V>
{
    private final EntityMetadata<V> _entityMeta;
    private final Map<PropertyMetadataBase, Object> _propsFilter = new HashMap<PropertyMetadataBase, Object>();
    
    public EqualityValueFilter(EntityMetadata<V> meta, V template, IndexMetadata index)
    {
        _entityMeta = meta;
        BitSet dirty = ((IEnhancedEntity)template).getModifiedFields();
        Set<SimplePropertyMetadata> indexedProps = new HashSet<SimplePropertyMetadata>();
        
        for(int i = dirty.nextSetBit (0); i>= 0; i = dirty.nextSetBit(i+1)) 
        {
            PropertyMetadataBase p = _entityMeta.getProperties().get(i);
            
            if(!indexedProps.contains(p))
                _propsFilter.put(p, invokeGetter(p, template));
        }
        

        for(SimplePropertyMetadata pm : index.getIndexedProperties())
        {
            _propsFilter.remove(pm);
        }
    }
    
    public EFilterResult isFiltered(IndexedValue<V> value)
    {
        
        for(Map.Entry<PropertyMetadataBase, Object> entry : _propsFilter.entrySet())
        {
            Object pval = invokeGetter(entry.getKey(), value.getValue());
            if(!entry.getValue().equals(pval))
                return EFilterResult.FAIL;
        }

        return EFilterResult.PASS;
    }
    
    protected Object invokeGetter(PropertyMetadataBase pm, V obj)
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
}

