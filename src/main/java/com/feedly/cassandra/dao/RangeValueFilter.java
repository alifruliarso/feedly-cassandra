package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

class RangeValueFilter<V> implements IValueFilter<V>
{
    private final EntityMetadata<V> _entityMeta;
    private final List<Object> _startIdxProps = new ArrayList<Object>();
    private final Map<PropertyMetadata, Object> _startProps = new HashMap<PropertyMetadata, Object>();
    private final List<Object> _endIdxProps = new ArrayList<Object>();
    private final Map<PropertyMetadata, Object> _endProps = new HashMap<PropertyMetadata, Object>();
    
    public RangeValueFilter(EntityMetadata<V> meta, V startTemplate, V endTemplate, IndexMetadata idx)
    {
        _entityMeta = meta;
        toMap(startTemplate, _startIdxProps, _startProps, idx);
        toMap(endTemplate, _endIdxProps, _endProps, idx);
    }

    private void toMap(V template, 
                       List<Object> idxVals, 
                       Map<PropertyMetadata, Object> props, 
                       IndexMetadata idx)
    {
        BitSet dirty = ((IEnhancedEntity)template).getModifiedFields();
        
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
                idxVals.add(val);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public EFilterResult isFiltered(IndexedValue<V> value)
    {
        List<Object> idxProps = value.getIndexValues();
        int numStartIdxFilterProps = _startIdxProps.size();
        int numEndIdxFilterProps = _endIdxProps.size();
        int numIdxProps = idxProps.size();
        
        if(numIdxProps < numStartIdxFilterProps || numIdxProps < numEndIdxFilterProps) //must be comparable to entire filter
            return EFilterResult.FAIL_STALE;
        
        //for indexed properties, do comparison against entire index as one unit
        for(int i = 0; i < numStartIdxFilterProps; i++)
        {
            Comparable tVal = (Comparable) _startIdxProps.get(i);
            Comparable vVal = (Comparable) idxProps.get(i);
            
            int cmp = tVal.compareTo(vVal);
            if(cmp < 0)
                break;
            
            if(cmp > 0)
                return EFilterResult.FAIL_STALE;
        }

        //for other props, check current val is greater property by property
        for(Map.Entry<PropertyMetadata, Object> entry : _startProps.entrySet())
        {
            Comparable tVal = (Comparable) entry.getValue();
            Comparable vVal = (Comparable) invokeGetter(entry.getKey(), value.getValue());
            
            if(tVal.compareTo(vVal) > 0)
                return EFilterResult.FAIL;
        }
        
        
        for(int i = 0; i < numEndIdxFilterProps; i++)
        {
            Comparable tVal = (Comparable) _endIdxProps.get(i);
            Comparable vVal = (Comparable) idxProps.get(i);
            
            int cmp = tVal.compareTo(vVal);
            if(cmp > 0)
                break;
            
            if(cmp < 0)
                return EFilterResult.FAIL_STALE;
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
