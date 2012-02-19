package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import me.prettyprint.hector.api.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.anno.UnmappedColumnHandler;

public class EntityMetadata<V>
{
    private static final Logger _logger = LoggerFactory.getLogger(EntityMetadata.class.getName());
    
    private final Map<String, PropertyMetadata> _propsByName, _propsByPhysicalName;
    private final List<PropertyMetadata> _props;
    private final Map<String, Set<PropertyMetadata>> _propsByAnno;
    private final Set<Annotation> _annotations;
    private final Class<V> _clazz;
    private final PropertyMetadata _keyMeta;
    private final PropertyMetadata _unmappedHandler;
    private final boolean _useCompositeColumns;
    
    static final Set<Class<?>> ALLOWED_TYPES;

    static
    {
        HashSet<Class<?>> allowedTypes = new HashSet<Class<?>>();  
        allowedTypes.add(String.class);
        allowedTypes.add(Boolean.class);
        allowedTypes.add(boolean.class);
        
        ALLOWED_TYPES = Collections.unmodifiableSet(allowedTypes);
    }

    public Set<Class<?>> getAllowedTypes()
    {
        return ALLOWED_TYPES;
    }
    
    public EntityMetadata(Class<V> beanClass, boolean forceCompositeColumns)
    {
        _clazz = beanClass;
        Set<Annotation> beanAnnos = new HashSet<Annotation>();
        for(Annotation a : beanClass.getAnnotations())
            beanAnnos.add(a);
        
        _annotations = Collections.unmodifiableSet(beanAnnos);
        
        Map<String, PropertyMetadata> props = new TreeMap<String, PropertyMetadata>(); 
        Map<String, PropertyMetadata>  propsByPhysical = new TreeMap<String, PropertyMetadata>();
        Map<String, Set<PropertyMetadata>> propsByAnno = new TreeMap<String, Set<PropertyMetadata>>();
        PropertyMetadata keyMeta = null, unmappedHandler = null;
        boolean hasCollections = false;

        for(Field f : beanClass.getDeclaredFields())
        {
            if(f.isAnnotationPresent(Column.class) && 
                    (f.getType().equals(Map.class) || f.getType().equals(List.class) || f.getType().equals(SortedMap.class)))
            {
                hasCollections = true;
                break;
            }
        }
        
        _useCompositeColumns = hasCollections || forceCompositeColumns;

        for(Field f : beanClass.getDeclaredFields())
        {
            Method getter = getGetter(f);
            Method setter = getSetter(f);
            if(getter != null && setter != null)
            {
                if(f.isAnnotationPresent(RowKey.class))
                {
                    if(keyMeta != null)
                        throw new IllegalArgumentException("@RowKey may only be used on one field.");

                    RowKey anno = f.getAnnotation(RowKey.class);
                    keyMeta = new PropertyMetadata(f, null, false, false, getter, setter, serializerClass(anno.value()),  false);
                }

                if(f.isAnnotationPresent(UnmappedColumnHandler.class))
                {
                    if(unmappedHandler != null)
                        throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on one field.");

                    if(!Map.class.equals(f.getType()) && !SortedMap.class.equals(f.getType()))
                        throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on a Map or SortedMap, not sub-interfaces or classes.");
                    
                    UnmappedColumnHandler anno = f.getAnnotation(UnmappedColumnHandler.class);
                    unmappedHandler = new PropertyMetadata(f, null, false, false, getter, setter, serializerClass(anno.value()), _useCompositeColumns);
                }

                
                if(f.isAnnotationPresent(Column.class))
                {
                    Column anno = f.getAnnotation(Column.class);
                    String col = anno.col();
                    if(col.equals(""))
                        col = f.getName();
                    

                    PropertyMetadata pm = new PropertyMetadata(f, col, anno.hashIndexed(), anno.rangeIndexed(), getter, setter, serializerClass(anno.serializer()), _useCompositeColumns);
                    props.put(f.getName(), pm);
                    propsByPhysical.put(col, pm);

                    if(f.isAnnotationPresent(UnmappedColumnHandler.class))
                    {
                        if(keyMeta != null)
                            throw new IllegalArgumentException(f.getName() + ": @UnmappedColumnHandler should not also be annotated as a mapped @Column.");

                        keyMeta = new PropertyMetadata(f, null, false, false, getter, setter, serializerClass(anno.serializer()), _useCompositeColumns);
                    }

                    for(Annotation a : f.getDeclaredAnnotations())
                    {
                        Set<PropertyMetadata> annos = propsByAnno.get(a.annotationType().getName());
                        if(annos == null)
                        {
                            annos = new TreeSet<PropertyMetadata>();
                            propsByAnno.put(a.annotationType().getName(),  annos);
                        }
                        annos.add(pm);
                    }
                }
            }
        }
        
        _unmappedHandler = unmappedHandler;
        
        if(keyMeta == null)
            throw new IllegalArgumentException("missing @RowKey annotated field");

        if(propsByAnno.get(RowKey.class.getName()) != null)
            _logger.warn(keyMeta.getName(), ": key property is also stored in a column");

        _keyMeta = keyMeta;
        
        for(Entry<String, Set<PropertyMetadata>> annos : propsByAnno.entrySet())
            annos.setValue(Collections.unmodifiableSet(annos.getValue()));
        
        _propsByAnno = Collections.unmodifiableMap(propsByAnno);
        _propsByName = Collections.unmodifiableMap(props);
        _propsByPhysicalName = Collections.unmodifiableMap(propsByPhysical);

        List<PropertyMetadata> sorted = new ArrayList<PropertyMetadata>(props.values());
        Collections.sort(sorted);
        _props = Collections.unmodifiableList(sorted);
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Class<? extends Serializer<?>> serializerClass(Class<? extends Serializer> value)
    {
        return (Class<? extends Serializer<?>>) (value.equals(Serializer.class) ? null : value);
    }

    private Method getSetter(Field prop)
    {
        String name = "set" + Character.toUpperCase(prop.getName().charAt(0)) + prop.getName().substring(1);
        Method setter = null;

        try
        {
            setter = _clazz.getMethod(name, prop.getType());
            
            if(!EntityUtils.isValidSetter(setter))
                return null;
            
            return setter;
        }
        catch(NoSuchMethodException ex)
        {
            _logger.trace("no setter  ", new Object[] { name, "(", prop.getType().getSimpleName(), "). excluding"});
            
            return null;
        }

    }
    
    private Method getGetter(Field prop)
    {
        String name = "get" + Character.toUpperCase(prop.getName().charAt(0)) + prop.getName().substring(1);
        Method getter = null;

        try
        {
            getter = _clazz.getMethod(name);
            
            if(!getter.getReturnType().equals(prop.getType()) || !EntityUtils.isValidGetter(getter))
                return null;
            
            return getter;
        }
        catch(NoSuchMethodException ex)
        {
            _logger.trace("no setter  ", new Object[] {name, "(", prop.getType().getSimpleName(), "). excluding"});
            
            return null;
        }
    }
    
    public Set<Annotation> getAnnotations()
    {
        return _annotations;
    }
    
    public PropertyMetadata getProperty(String name)
    {
        return _propsByName.get(name);
    }

    public PropertyMetadata getPropertyByPhysicalName(String pname)
    {
        return _propsByPhysicalName.get(pname);
    }
    
    public List<PropertyMetadata> getProperties()
    {
        return _props;
    }
  
    
    public Set<PropertyMetadata> getAnnotatedProperties(Class<? extends Annotation> annoType)
    {
        Set<PropertyMetadata> rv = _propsByAnno.get(annoType.getName());
        
        return rv != null ? rv : Collections.<PropertyMetadata>emptySet();
    }

    public PropertyMetadata getKeyMetadata()
    {
        return _keyMeta;
    }

    public PropertyMetadata getUnmappedHandler()
    {
        return _unmappedHandler;
    }
    
    public boolean useCompositeColumns()
    {
        return _useCompositeColumns;
    }
    
    public Class<V> getType()
    {
        return _clazz;
    }
    
    @Override
    public int hashCode()
    {
        return _clazz.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof EntityMetadata<?>)
            return _clazz.equals(((EntityMetadata<?>) obj)._clazz);
            
        return false;
    }
    
    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        for(Annotation a : _annotations)
            b.append("@").append(a.annotationType().getSimpleName()).append(" ");
        
        b.append(_clazz.getSimpleName());
        
        for(PropertyMetadata pm : _props)
            b.append("\n\t+ ").append(pm);
        
        return b.toString();
    }
    
}
