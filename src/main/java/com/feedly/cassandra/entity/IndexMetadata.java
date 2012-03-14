package com.feedly.cassandra.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.feedly.cassandra.IIndexRowPartitioner;

/**
 * Holds metadata for an entity index.
 * 
 * @author kireet
 */
public class IndexMetadata
{
    private final EIndexType _type;
    private final IIndexRowPartitioner _indexPartitioner;
    private final List<PropertyMetadata> _indexedProps;
    private final String _id;
    
    public IndexMetadata(String cfName,
                         List<PropertyMetadata> indexedProps,
                         IIndexRowPartitioner indexPartitioner,
                         EIndexType idxType) 
    {
        _type = idxType;
        _indexedProps = Collections.unmodifiableList(new ArrayList<PropertyMetadata>(indexedProps));
        _indexPartitioner = indexPartitioner;
        
        StringBuilder id = new StringBuilder();
        boolean first = true;
        for(PropertyMetadata pm : indexedProps)
        {
            if(first)
                first = false;
            else
                id.append(",");
            
            id.append(pm.getPhysicalName());
        }

        _id = id.toString();
    }
    
    public EIndexType getType()
    {
        return _type;
    }

    
    @Override
    public boolean equals(Object obj)
    {
        //type should always match, so don't check equality...
        if(obj instanceof IndexMetadata)
            return ((IndexMetadata) obj)._id.equals(_id);
        
        return false;
    }
    
    @Override
    public String toString()
    {
        return _id + "(" + _type + ")";
    }
    
    public String id()
    {
        return _id;
    }
    
    @Override
    public int hashCode()
    {
        return _id.hashCode();
    }

    public IIndexRowPartitioner getIndexPartitioner()
    {
        return _indexPartitioner;
    }

    public List<PropertyMetadata> getIndexedProperties()
    {
        return _indexedProps;
    }
}
