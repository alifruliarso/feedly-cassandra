package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * Base class for entity property metadata (column family column).
 * 
 * @author kireet
 */public abstract class PropertyMetadataBase implements Comparable<PropertyMetadataBase>
{
    private final EPropertyType _propertyType;
    private final byte[] _physicalNameBytes;
    private final String _name;
    private final String _physicalName;
    private final Method _getter;
    private final Method _setter;
    private final Set<Annotation> _annotations;
    private final Class<?> _fieldClass;
    private final int _ttl;
    
    public PropertyMetadataBase(String name,
                                Class<?> clazz,
                                Annotation[] annotations,
                                String physicalName,
                                int ttl,
                                Method getter,
                                Method setter,
                                boolean useCompositeKeySerializer,
                                EPropertyType propertyType)
    {
        _name = name;
        _physicalName = physicalName;
        _propertyType = propertyType;
        _ttl = ttl;
        if(_ttl == 0)
            throw new IllegalArgumentException("TTL must be positive (>= 1 second) or unset");
        
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
        _fieldClass = clazz;
        
        Set<Annotation> annos = new TreeSet<Annotation>(
                new Comparator<Annotation>()
                {
                    @Override
                    public int compare(Annotation o1, Annotation o2)
                    {
                        return o1.annotationType().getSimpleName().compareTo(o2.annotationType().getSimpleName());
                    }
                    
                });
        
        if(annotations != null)
        {
            for(Annotation a : annotations)
                annos.add(a);
        }
            
        _annotations = Collections.unmodifiableSet(annos);
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

    public boolean isTtlSet() 
    {
        return _ttl > 0;
    }
    
    public int ttl() //in seconds
    {
        return _ttl;
    }
    
    @Override
    public int compareTo(PropertyMetadataBase o)
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
        if(obj != null && getClass().equals(obj.getClass()))
            return ((PropertyMetadataBase) obj)._name.equals(_name);
        
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

    
    public EPropertyType getPropertyType()
    {
        return _propertyType;
    }
}
