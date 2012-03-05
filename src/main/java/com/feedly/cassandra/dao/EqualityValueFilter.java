package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

class EqualityValueFilter<V> implements IValueFilter<V>
{
    private final EntityMetadata<V> _entityMeta;
    private final Map<PropertyMetadata, Object> _propsFilter = new HashMap<PropertyMetadata, Object>();
    private final List<Object> _indexFilter = new ArrayList<Object>();
    private final boolean _validateIdxVals;
    
    public EqualityValueFilter(EntityMetadata<V> meta, V template, boolean validateIdxVals, IndexMetadata index)
    {
        _entityMeta = meta;
        _validateIdxVals = validateIdxVals;
        BitSet dirty = ((IEnhancedEntity)template).getModifiedFields();
        Set<PropertyMetadata> indexedProps = new HashSet<PropertyMetadata>();
        
        for(PropertyMetadata pm : index.getIndexedProperties())
        {
            Object val = invokeGetter(pm, template);
            if(val != null)
                _indexFilter.add(val);
            else
                break;
            
            indexedProps.add(pm);
        }
        
        for(int i = dirty.nextSetBit (0); i>= 0; i = dirty.nextSetBit(i+1)) 
        {
            PropertyMetadata p = _entityMeta.getProperties().get(i);
            
            if(!indexedProps.contains(p))
                _propsFilter.put(p, invokeGetter(p, template));
        }
    }
    
    public EFilterResult isFiltered(IndexedValue<V> value)
    {
        
        if(_validateIdxVals)
        {
            List<Object> idxVals = value.getIndexValues();
            int numIdxFilterVals = _indexFilter.size();
            int numVals = idxVals.size();
            
            if(numVals < numIdxFilterVals) //value must match the entire filter but has null values
                return EFilterResult.FAIL_STALE;
            
            for(int i = 0; i < numIdxFilterVals; i++)
            {
                if(!idxVals.get(i).equals(_indexFilter.get(i)))
                    return EFilterResult.FAIL_STALE;
            }
        }
        
        for(Map.Entry<PropertyMetadata, Object> entry : _propsFilter.entrySet())
        {
            Object pval = invokeGetter(entry.getKey(), value.getValue());
            if(!entry.getValue().equals(pval))
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

