package com.feedly.cassandra.dao;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EIndexType;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;

@SuppressWarnings("unchecked")
public class FindHelper<K, V> extends LoadHelper<K, V>
{
    private final HashIndexFindHelper<K, V> _hashIndexFinder;
    private final RangeIndexFindHelper<K, V> _rangeIndexFinder;
    
    FindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory, IStaleIndexValueStrategy staleValueStrategy)
    {
        super(meta, factory);
        _hashIndexFinder = new HashIndexFindHelper<K, V>(meta, factory);
        _rangeIndexFinder = new RangeIndexFindHelper<K, V>(meta, factory, staleValueStrategy);
    }
    
    private IndexMetadata chooseIndex(boolean rangeOnly, V... templates) 
    {
        Set<PropertyMetadata> props = new HashSet<PropertyMetadata>();
        BitSet dirty = asEntity(templates[0]).getModifiedFields();
        
        if(templates.length > 1)
        {
            dirty = (BitSet) dirty.clone();
            for(int i = templates.length-1; i > 0; i--)
                dirty.and(asEntity(templates[i]).getModifiedFields());
        }
            
        for(int i = dirty.nextSetBit(0); i >= 0; i = dirty.nextSetBit(i + 1))
        {
            props.add(_entityMeta.getProperties().get(i));
        }
        
        if(props.isEmpty())
            throw new IllegalArgumentException("no properties set");
        
        IndexMetadata matching = null;
        int matchCnt = 0;
        for(IndexMetadata im : _entityMeta.getIndexes())
        {
            if(rangeOnly && im.getType() != EIndexType.RANGE)
                continue;
            
            if(props.equals(im.getIndexedProperties()))
                return im;

            int cnt = 0;
            for(PropertyMetadata indexedProp : im.getIndexedProperties())
            {
                if(props.contains(indexedProp))
                    cnt++;
                else
                    break;
            }

            if(cnt > matchCnt)
            {
                matchCnt = cnt;
                matching = im;
            }
            else if(cnt > 0 && cnt == matchCnt && im.getIndexedProperties().size() < matching.getIndexedProperties().size())
            {
                //smaller number of columns
                matching = im;
            }
        }
        
        if(matching != null)
            return matching;
        
        throw new IllegalStateException("no applicable index for properties " + props);
    }
    
    
    public V find(V template)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.find(template, index);
        }
        else 
        {
            return _rangeIndexFinder.find(template, index);
        }
    }
    

    public V find(V template, Object start, Object end)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.find(template, start, end, index);
        }
        
        throw new IllegalStateException(); //never happens
    }

    public V find(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.find(template, includes, excludes, index);
        }
        
        throw new IllegalStateException(); //never happens
    }

    public Collection<V> mfind(V template)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.mfind(template, index);
        }
        else 
        {
            return _rangeIndexFinder.mfind(template, index);
        }
    }


    public Collection<V> mfind(V template, Object start, Object end)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.mfind(template, start, end, index);
        }
        else 
        {
            return _rangeIndexFinder.mfind(template, start, end, index);
        }
        
    }
    
    public Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes)
    {
        IndexMetadata index = chooseIndex(false, template);
        if(index.getType() == EIndexType.HASH)
        {
            return _hashIndexFinder.mfind(template, includes, excludes, index);
        }
        else
        {
            return _rangeIndexFinder.mfind(template, includes, excludes, index);
        }
    }
    
    public Collection<V> mfindBetween(V startTemplate, V endTemplate)
    {
        IndexMetadata index = chooseIndex(true, startTemplate, endTemplate);

        return _rangeIndexFinder.mfindBetween(startTemplate, endTemplate, index);
    }

    public Collection<V> mfindBetween(V startTemplate, V endTemplate, Object startColumn, Object endColumn)
    {
        IndexMetadata index = chooseIndex(true, startTemplate, endTemplate);

        return _rangeIndexFinder.mfindBetween(startTemplate, endTemplate, startColumn, endColumn, index);
    }

    public Collection<V> mfindBetween(V startTemplate, V endTemplate, Set<? extends Object> includes, Set<String> excludes)
    {
        IndexMetadata index = chooseIndex(true, startTemplate, endTemplate);

        return _rangeIndexFinder.mfindBetween(startTemplate, endTemplate, includes, excludes, index);
    }
}
