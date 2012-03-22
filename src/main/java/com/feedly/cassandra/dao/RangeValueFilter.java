package com.feedly.cassandra.dao;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

/**
 * used to perform filter-on-read logic during index retrievals. This class handles between checks.
 * 
 * @author kireet
 *
 * @param <V> the entity type
 */
class RangeValueFilter<V> implements IValueFilter<V>
{
    private final EntityMetadata<V> _entityMeta;
    private final Map<PropertyMetadata, Object> _startProps = new HashMap<PropertyMetadata, Object>();
    private final Map<PropertyMetadata, Object> _endProps = new HashMap<PropertyMetadata, Object>();
    
    public RangeValueFilter(EntityMetadata<V> meta, V startTemplate, V endTemplate, IndexMetadata idx)
    {
        _entityMeta = meta;
        toMap(startTemplate, _startProps, idx);
        toMap(endTemplate, _endProps, idx);
    }

    private void toMap(V template, 
                       Map<PropertyMetadata, Object> props, 
                       IndexMetadata idx)
    {
        BitSet dirty = ((IEnhancedEntity)template).getModifiedFields();
        
        for(int i = dirty.nextSetBit (0); i>= 0; i = dirty.nextSetBit(i+1)) 
        {
            PropertyMetadata p = _entityMeta.getProperties().get(i);
            props.put(p, invokeGetter(p, template));
        }      
        
        for(PropertyMetadata pm : idx.getIndexedProperties())
            props.remove(pm);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public EFilterResult isFiltered(IndexedValue<V> value)
    {
        /*
         * check current val is within range property by property
         */
        
        for(Map.Entry<PropertyMetadata, Object> entry : _startProps.entrySet())
        {
            Comparable tVal = (Comparable) entry.getValue();
            Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value.getValue());
            
            if(tVal.compareTo(vVal) > 0)
                return EFilterResult.FAIL;
        }
        
        
        for(Map.Entry<PropertyMetadata, Object> entry : _endProps.entrySet())
        {
            Comparable tVal = (Comparable) entry.getValue();
            Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value.getValue());
            
            if(tVal.compareTo(vVal) < 0)
                return EFilterResult.FAIL;
        }

        return EFilterResult.PASS;
    }
    
    protected Object invokeGetter(PropertyMetadata pm, V obj)
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
