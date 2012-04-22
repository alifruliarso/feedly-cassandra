package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;

public class MapPropertyMetadata extends PropertyMetadataBase
{
    private final SimplePropertyMetadata _keyPropertyMetadata;
    private final PropertyMetadataBase _valuePropertyMetadata;

    public MapPropertyMetadata(String name,
                               Type type,
                               Annotation[] annotations,
                               boolean isSorted,
                               String physicalName,
                               int ttl,
                               Method getter,
                               Method setter)
    {
        super(name, 
              isSorted ? SortedMap.class : Map.class, 
              annotations,
              physicalName,
              ttl,
              getter,
              setter, true, 
              isSorted ? EPropertyType.SORTED_MAP : EPropertyType.MAP);
        
        if(type instanceof ParameterizedType)
        {
            ParameterizedType ptype = (ParameterizedType) type;
            if(((ParameterizedType) type).getActualTypeArguments().length != 2)
                throw new IllegalStateException("Map types must have 2 generic argument");
            
            PropertyMetadataBase keyMeta = PropertyMetadataFactory.buildPropertyMetadata("", ptype.getActualTypeArguments()[0], null, physicalName, ttl, null, null, null, false);
            if(keyMeta.getPropertyType() != EPropertyType.SIMPLE)
                throw new IllegalStateException("maps key must be simple type: property " + physicalName + ", type " + keyMeta.getFieldType().getName());
            
            _keyPropertyMetadata = (SimplePropertyMetadata) keyMeta;
            _valuePropertyMetadata = PropertyMetadataFactory.buildPropertyMetadata("", ptype.getActualTypeArguments()[1], null, physicalName, ttl, null, null, null, false);
        }
        else
        {
            throw new IllegalStateException("maps must be parameterized types: property " + physicalName + ", type " + type);
        }
    }

    public SimplePropertyMetadata getKeyPropertyMetadata()
    {
        return _keyPropertyMetadata;
    }

    public PropertyMetadataBase getValuePropertyMetadata()
    {
        return _valuePropertyMetadata;
    }

}
