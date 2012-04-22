package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import me.prettyprint.hector.api.Serializer;

/**
 * Holds metadata for a primitive or enum entity property (column family column)
 * 
 * @author kireet
 */
public class SimplePropertyMetadata extends PropertyMetadataBase
{
    private final Serializer<?> _serializer;

    @SuppressWarnings("unchecked")
    public SimplePropertyMetadata(String name,
                                  Class<?> fieldClass,
                                  Annotation[] annotations,
                                  String physicalName,
                                  int ttl,
                                  Method getter,
                                  Method setter,
                                  Class<? extends Serializer<?>> serializerClass,
                                          boolean useCompositeKeySerializer)
    {
        super(name, fieldClass, annotations, physicalName, ttl, getter, setter, useCompositeKeySerializer, EPropertyType.SIMPLE);

        if(serializerClass != null)
            _serializer = getSerializerInstance(serializerClass);
//        else if(fieldClass.equals(Map.class) || fieldClass.equals(List.class) || fieldClass.equals(SortedMap.class))
//            _serializer = ByteIndicatorSerializer.get();
        else if(fieldClass.isEnum())
            _serializer = new EnumSerializer((Class<? extends Enum<?>>) fieldClass);
        else if(fieldClass.equals(Object.class) || fieldClass.equals(Number.class))
            _serializer = ByteIndicatorSerializer.get();
        else
            _serializer = EntityUtils.getSerializer(fieldClass);

        if(_serializer == null)
        {
            throw new IllegalArgumentException(name + ": invalid type. cannot serialize " + fieldClass.getName());
        }
    }

 
    
    private Serializer<?> getSerializerInstance(Class<? extends Serializer<?>> serializerClass)
    {
        try
        {
            Method m = serializerClass.getMethod("get");
            int mod = m.getModifiers();
            int reqFlags = Modifier.PUBLIC & Modifier.STATIC;
            if( (mod | reqFlags) == reqFlags)
            {
                return (Serializer<?>) m.invoke(null);
            }
        }
        catch(NoSuchMethodException ex)
        {
            //dropped
        }
        catch(Exception ex)
        {
            throw new RuntimeException("error creating serializer instance from get() factory method", ex);
        }
        
        try
        {
            return serializerClass.newInstance();
        }
        catch(Exception ex)
        {
            throw new RuntimeException("error creating serializer instance from constructor", ex);
        }
    }

    public Serializer<?> getSerializer()
    {
        return _serializer;
    }

}
