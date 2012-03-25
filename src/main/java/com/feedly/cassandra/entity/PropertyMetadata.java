package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * Holds metadata for a entity property (column family column)
 * 
 * @author kireet
 */
public class PropertyMetadata implements Comparable<PropertyMetadata>
{
    private final byte[] _physicalNameBytes;
    private final String _name;
    private final String _physicalName;
    private final Method _getter;
    private final Method _setter;
    private final Set<Annotation> _annotations;
    private final Class<?> _fieldClass;
    private final Serializer<?> _serializer;
    private final boolean _isCollection;
    
    @SuppressWarnings("unchecked")
    public PropertyMetadata(Field field,
                            String physicalName,
                            Method getter,
                            Method setter,
                            Class<? extends Serializer<?>> serializerClass,
                            boolean useCompositeKeySerializer)
    {
        _name = field.getName();
        _physicalName = physicalName;
        
        byte[] physNameBytes = null;
        
        if(physicalName != null)
        {
            if(useCompositeKeySerializer)
            {
                DynamicComposite dc = new DynamicComposite();
                dc.add(physicalName);
                physNameBytes = new DynamicCompositeSerializer().toBytes(dc);
            }
            else
                physNameBytes = StringSerializer.get().toBytes(physicalName);
        }
                
        _physicalNameBytes = physNameBytes;
        _getter = getter;
        _setter = setter;
        _fieldClass = field.getType();
        _isCollection = Map.class.equals(_fieldClass) || SortedMap.class.equals(_fieldClass) || List.class.equals(_fieldClass);
        
        Set<Annotation> annos = new TreeSet<Annotation>(
                new Comparator<Annotation>()
                {
                    @Override
                    public int compare(Annotation o1, Annotation o2)
                    {
                        return o1.annotationType().getSimpleName().compareTo(o2.annotationType().getSimpleName());
                    }
                    
                });
        
        for(Annotation a : field.getAnnotations())
            annos.add(a);
            
        _annotations = Collections.unmodifiableSet(annos);
        
        if(serializerClass != null)
            _serializer = getSerializerInstance(serializerClass);
        else if(_fieldClass.equals(Map.class) || _fieldClass.equals(List.class) || _fieldClass.equals(SortedMap.class))
            _serializer = ByteIndicatorSerializer.get();
        else if(_fieldClass.isEnum())
            _serializer = new EnumSerializer((Class<? extends Enum<?>>) _fieldClass);
        else
            _serializer = EntityUtils.getSerializer(_fieldClass);

        if(!Map.class.equals(_fieldClass) &&
                !SortedMap.class.equals(_fieldClass) &&
                !List.class.equals(_fieldClass) && 
                _serializer == null)
        {
            if(Map.class.isAssignableFrom(_fieldClass) || List.class.isAssignableFrom(_fieldClass))
                throw new IllegalArgumentException(field.getName() + ": collection types must be Map, SortedMap, or List, not sub interfaces or classes.");
                
            throw new IllegalArgumentException(field.getName() + ": invalid type. cannot serialize " + _fieldClass.getName());
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
    
    public String getName()
    {
        return _name;
    }

    public byte[] getPhysicalNameBytes()
    {
        return _physicalNameBytes;
    }
    
    public String getPhysicalName()
    {
        return _physicalName;
    }
    
    public Class<?> getFieldType()
    {
        return _fieldClass;
    }

    public Method getGetter()
    {
        return _getter;
    }

    public Method getSetter()
    {
        return _setter;
    }

    public Set<Annotation> getAnnotations()
    {
        return _annotations;
    }

    public boolean isCollection()
    {
        return _isCollection;
    }
    
    @Override
    public int compareTo(PropertyMetadata o)
    {
        return _name.compareTo(o._name);
    }
    
    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof PropertyMetadata)
            return ((PropertyMetadata) obj)._name.equals(_name);
        
        return false;
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        for(Annotation a : _annotations)
            b.append("@").append(a.annotationType().getSimpleName()).append(" ");
        
        b.append(_getter.getReturnType().getSimpleName()).append(" ");
        b.append(_name);
        
        return b.toString();
    }
}
