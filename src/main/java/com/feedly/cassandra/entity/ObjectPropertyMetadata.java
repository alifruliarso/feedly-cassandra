package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ObjectPropertyMetadata extends PropertyMetadataBase
{
    private final EmbeddedEntityMetadata _objectMetadata;
    private final Constructor _constructor;
    
    public ObjectPropertyMetadata(String name,
                                  Class<?> clazz,
                                  Annotation[] annotations,
                                  String physicalName,
                                  Method getter,
                                  Method setter)
    {
        super(name, clazz, annotations, physicalName, getter, setter, true, EPropertyType.OBJECT);
        _objectMetadata = new EmbeddedEntityMetadata(clazz);
        
        //make sure a no-arg constructor exists;
        try
        {
            _constructor = _objectMetadata.getType().getConstructor();
        }
        catch(Exception ex)
        {
            throw new IllegalStateException("no-arg constructor required, but was not found for property " + physicalName + ", class " + clazz.getName());
        }
    }

    public EmbeddedEntityMetadata<?> getObjectMetadata()
    {
        return _objectMetadata;
    }
    
    public Object newInstance()
    {
        try
        {
            return _constructor.newInstance();
        }
        catch(RuntimeException re)
        {
            throw re;
        }
        catch(Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
