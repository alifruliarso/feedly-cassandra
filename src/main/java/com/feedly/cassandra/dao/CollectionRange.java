package com.feedly.cassandra.dao;

import java.math.BigInteger;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import com.feedly.cassandra.entity.PropertyMetadataBase;

class CollectionRange
{
    private static final DynamicCompositeSerializer SER_COMPOSITE = DynamicCompositeSerializer.get();
    private final DynamicComposite _start, _end;
    private final PropertyMetadataBase _metadata;
    
    public CollectionRange(PropertyMetadataBase pm)
    {
        DynamicComposite dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL);
        _start = dc;
        dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.GREATER_THAN_EQUAL); 

        _end = dc;
        _metadata = pm;
    }

    public CollectionRange(PropertyMetadataBase pm, Object key)
    {
        if(key instanceof Integer)
            key = BigInteger.valueOf( ((Integer) key).longValue());
        
        DynamicComposite dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL);
        dc.addComponent(1, key, ComponentEquality.EQUAL);
        _start = dc;

        dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL); 
        dc.addComponent(1, key, ComponentEquality.GREATER_THAN_EQUAL);
        
        _end = dc;
        _metadata = pm;
    }
    
    public DynamicComposite start()
    {
        return _start;
    }
    
    public byte[] startBytes()
    {
        return SER_COMPOSITE.toBytes(_start);
    }
    
    public DynamicComposite end()
    {
        return _end;
    }

    public byte[] endBytes()
    {
        return SER_COMPOSITE.toBytes(_end);
    }
    
    public PropertyMetadataBase propertyMetadata()
    {
        return _metadata;
    }
}
