package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public class ListPropertyMetadata extends PropertyMetadataBase
{
    private final PropertyMetadataBase _elementPropertyMetadata;
    
    public ListPropertyMetadata(String name,
                                Type type,
                                Annotation[] annotations,
                                String physicalName,
                                int ttl,
                                Method getter,
                                Method setter)
    {
        super(name, List.class, annotations, physicalName, ttl, getter, setter, true, EPropertyType.LIST);
        
        if(type instanceof ParameterizedType)
        {
            ParameterizedType ptype = (ParameterizedType) type;
            if(((ParameterizedType) type).getActualTypeArguments().length != 1)
                throw new IllegalStateException("List types must have 1 generic argument");
            
            _elementPropertyMetadata = PropertyMetadataFactory.buildPropertyMetadata("", ptype.getActualTypeArguments()[0], null, physicalName, ttl, null, null, null, false);
        }
        else
        {
            throw new IllegalStateException("lists must be parameterized types: property " + physicalName + ", type " + type);
        }
    }

    public PropertyMetadataBase getElementPropertyMetadata()
    {
        return _elementPropertyMetadata;
    }
    
    public boolean hasCounter()
    {
        return _elementPropertyMetadata.hasCounter();
    }

    public boolean hasSimple()
    {
        return _elementPropertyMetadata.hasSimple();
    }
}
