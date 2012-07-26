package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;

import me.prettyprint.hector.api.Serializer;

import com.feedly.cassandra.dao.CounterColumn;

public class PropertyMetadataFactory
{
    public static final Set<Class<?>> PRIMITIVES;
    public static final Set<String> PRIMITIVE_STRINGS;

    static
    {
        Set<Class<?>> primitives = new HashSet<Class<?>>();

        primitives.add(boolean.class);
        primitives.add(Boolean.class);
        primitives.add(byte.class);
        primitives.add(byte.class);
        primitives.add(char.class);
        primitives.add(Character.class);
        primitives.add(Date.class);
        primitives.add(double.class);
        primitives.add(Double.class);
        primitives.add(float.class);
        primitives.add(Float.class);
        primitives.add(int.class);
        primitives.add(Integer.class);
        primitives.add(long.class);
        primitives.add(Long.class);
        primitives.add(short.class);
        primitives.add(Short.class);
        primitives.add(String.class);
        primitives.add(UUID.class);
        primitives.add(Object.class); //common super class for primitives
        primitives.add(Number.class); //common super class for primitives

        PRIMITIVES = Collections.unmodifiableSet(primitives);
        Set<String> strings = new HashSet<String>();
        for(Class<?> c : PRIMITIVES)
            strings.add(c.getName());
        
        PRIMITIVE_STRINGS = Collections.unmodifiableSet(strings);
    }

    public static SimplePropertyMetadata buildSimplePropertyMetadata(Field f,
                                                                     String physicalName,
                                                                     int ttl,
                                                                     Method getter,
                                                                     Method setter,
                                                                     Class<? extends Serializer<?>> serializerClass,
                                                                     boolean useCompositeKeySerializer)
    {
        PropertyMetadataBase pm = buildPropertyMetadata(f, physicalName, ttl, getter, setter, serializerClass, useCompositeKeySerializer);

        try
        {
            return (SimplePropertyMetadata) pm;
        }
        catch(ClassCastException cce)
        {
            throw new IllegalArgumentException("expected simple type for " + physicalName + " but encountered " + f.getClass());
        }
    }

    public static PropertyMetadataBase buildPropertyMetadata(Field f,
                                                             String physicalName,
                                                             int ttl,
                                                             Method getter,
                                                             Method setter,
                                                             Class<? extends Serializer<?>> serializerClass,
                                                             boolean useCompositeKeySerializer)
    {
        return buildPropertyMetadata(f.getName(), f.getGenericType(), f.getAnnotations(), physicalName, ttl, getter, setter, serializerClass, useCompositeKeySerializer);
    }


    public static PropertyMetadataBase buildPropertyMetadata(String name,
                                                             Type type,
                                                             Annotation[] annotations,
                                                             String physicalName,
                                                             int ttl,
                                                             Method getter,
                                                             Method setter,
                                                             Class<? extends Serializer<?>> serializerClass,
                                                             boolean useCompositeKeySerializer)
    {
        serializerClass = serializerClass(serializerClass);
        
        if(type instanceof Class)
        {
            Class<?> clazz = (Class<?>) type;
            if(isSimpleType(clazz) || clazz.equals(CounterColumn.class))
                return new SimplePropertyMetadata(name, clazz, annotations, physicalName, ttl, getter, setter, serializerClass, useCompositeKeySerializer);
            else 
            {
                return new ObjectPropertyMetadata(name, clazz, annotations, physicalName, ttl, getter, setter);
            }
        }
        else if(type instanceof ParameterizedType)
        {
            ParameterizedType ptype = (ParameterizedType) type;
            Type rawType = ptype.getRawType();
            if(Map.class.equals(rawType) || SortedMap.class.equals(rawType))
            {
                return new MapPropertyMetadata(name, ptype, annotations, SortedMap.class.equals(rawType), physicalName, ttl, getter, setter);
            }
            else if(List.class.equals(rawType))
            {
                return new ListPropertyMetadata(name, ptype, annotations, physicalName, ttl, getter, setter);
            }
            else
            {
                throw new IllegalArgumentException("collection type " + rawType + " not allowed, expected Map, SortedMap, or List.");
            }
        }

        throw new IllegalArgumentException("type " + type + " not allowed");
    }

    private static Class<? extends Serializer<?>> serializerClass(Class<? extends Serializer<?>> value)
    {
        return Serializer.class.equals(value) ? null : value;
    }

    public static boolean isSimpleType(Field f)
    {
        return isSimpleType(f.getType());
    }

    public static boolean isPrimitiveType(String s)
    {
        return PRIMITIVE_STRINGS.contains(s);
    }
    
    public static boolean isSimpleType(Class<?> clazz)
    {
        if(clazz.isEnum())
            return true;
        
        return PRIMITIVES.contains(clazz);
    }
    
}
