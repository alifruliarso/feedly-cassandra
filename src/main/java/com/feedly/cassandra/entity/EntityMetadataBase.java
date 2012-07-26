package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import me.prettyprint.hector.api.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.UnmappedColumnHandler;

/**
 * This class holds metadata for a given entity including key, property and index information.
 * 
 * @author kireet
 * 
 * @param <V> the entity type
 */
public class EntityMetadataBase<V>
{
    private static final Logger _logger = LoggerFactory.getLogger(EntityMetadataBase.class.getName());
    
    private final Map<String, PropertyMetadataBase> _propsByName, _propsByPhysicalName;
    private final List<PropertyMetadataBase> _props;
    private final Map<String, Set<PropertyMetadataBase>> _propsByAnno;
    private final Class<V> _clazz;
    private final Map<PropertyMetadataBase, Integer> _propPositions;
    private final boolean _useCompositeColumns;
    private final MapPropertyMetadata _unmappedHandler;
    private final boolean _hasCounterCols, _hasNormalCols;

    @SuppressWarnings("unchecked")
    public EntityMetadataBase(Class<V> clazz, boolean useCompositeColumns, int ttl, boolean overrideTtl)
    {
        _clazz = clazz;
        _useCompositeColumns = useCompositeColumns;
        
        Map<String, PropertyMetadataBase> props = new TreeMap<String, PropertyMetadataBase>(); 
        Map<String, PropertyMetadataBase>  propsByPhysical = new TreeMap<String, PropertyMetadataBase>();
        Map<String, Set<PropertyMetadataBase>> propsByAnno = new TreeMap<String, Set<PropertyMetadataBase>>();
        MapPropertyMetadata unmappedHandler = null;
        boolean hasCounters = false, hasNormalColumns = false;
        
        for(Field f : clazz.getDeclaredFields())
        {
            Method getter = getGetter(f);
            Method setter = getSetter(f);

            if(f.isAnnotationPresent(UnmappedColumnHandler.class))
            {
                if(unmappedHandler != null)
                    throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on one field.");

                if(!Map.class.equals(f.getType()) && !SortedMap.class.equals(f.getType()))
                    throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on a Map or SortedMap, not sub-interfaces or classes.");

                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@UnmappedColumnHandler field must have valid getter and setter.");

                UnmappedColumnHandler anno = f.getAnnotation(UnmappedColumnHandler.class);
                unmappedHandler = (MapPropertyMetadata) PropertyMetadataFactory.buildPropertyMetadata(f, null, -1, getter, setter, (Class<? extends Serializer<?>>) anno.value(), useCompositeColumns());
            }

            if(f.isAnnotationPresent(Column.class))
            {
                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@Column field must have valid getter and setter.");

                Column anno = f.getAnnotation(Column.class);
                String col = anno.name();
                if(col.equals(""))
                    col = f.getName();
                
                if(anno.hashIndexed() && anno.rangeIndexed())
                    throw new IllegalStateException(f.getName() + ": property can be range or hash indexed, not both");
                
                int colTtl = anno.ttl() > 0 ? (int) TimeUnit.SECONDS.convert(anno.ttl(), anno.ttlUnit()) : -1;

                if(overrideTtl && ttl > 0) //if override and entity level ttl is set
                    colTtl = ttl;
                else if(!overrideTtl && colTtl < 0) //if not overriding and prop level ttl is not set
                    colTtl = ttl;
                
                PropertyMetadataBase pm = 
                        PropertyMetadataFactory.buildPropertyMetadata(f, col, colTtl, getter, setter, 
                                                                      (Class<? extends Serializer<?>>) anno.serializer(), useCompositeColumns);
                
                hasCounters = hasCounters || pm.hasCounter();
                hasNormalColumns = hasNormalColumns || pm.hasSimple();
                props.put(f.getName(), pm);
                if(propsByPhysical.put(col, pm) != null)
                    throw new IllegalStateException(f.getName() + ": physical column name must be unique - " + col);

                for(Annotation a : f.getDeclaredAnnotations())
                {
                    Set<PropertyMetadataBase> annos = propsByAnno.get(a.annotationType().getName());
                    if(annos == null)
                    {
                        annos = new TreeSet<PropertyMetadataBase>();
                        propsByAnno.put(a.annotationType().getName(),  annos);
                    }
                    annos.add(pm);
                }
            }
        }

        _hasCounterCols = hasCounters;
        _hasNormalCols = hasNormalColumns;
        _unmappedHandler = unmappedHandler;

        for(Entry<String, Set<PropertyMetadataBase>> annos : propsByAnno.entrySet())
            annos.setValue(Collections.unmodifiableSet(annos.getValue()));
        
        _propsByAnno = Collections.unmodifiableMap(propsByAnno);
        _propsByName = Collections.unmodifiableMap(props);
        _propsByPhysicalName = Collections.unmodifiableMap(propsByPhysical);

        List<PropertyMetadataBase> sorted = new ArrayList<PropertyMetadataBase>(props.values());
        Collections.sort(sorted);
        _props = Collections.unmodifiableList(sorted);
        
        Map<PropertyMetadataBase, Integer> positions = new HashMap<PropertyMetadataBase, Integer>();
        for(int i = sorted.size() - 1; i >=0; i--)
            positions.put(sorted.get(i), i);
        
        _propPositions = Collections.unmodifiableMap(positions);
    }

    protected Method getSetter(Field prop)
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
            if(!prop.getName().startsWith("__"))
                _logger.trace(prop.getName() + " no setter {} ({}). excluding", name, prop.getType().getSimpleName());
            
            return null;
        }

    }
    
    protected Method getGetter(Field prop)
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
            if(!prop.getName().startsWith("__"))
                _logger.trace(prop.getName() + "no getter {}({}). excluding", name, prop.getType().getSimpleName());
            
            return null;
        }
    }
    
    public final PropertyMetadataBase getProperty(String name)
    {
        return _propsByName.get(name);
    }

    public final int getPropertyPosition(SimplePropertyMetadata pm)
    {
        return _propPositions.get(pm);
    }
    
    public final PropertyMetadataBase getPropertyByPhysicalName(String pname)
    {
        return _propsByPhysicalName.get(pname);
    }
    
    public final List<PropertyMetadataBase> getProperties()
    {
        return _props;
    }
  
    public final Set<PropertyMetadataBase> getAnnotatedProperties(Class<? extends Annotation> annoType)
    {
        Set<PropertyMetadataBase> rv = _propsByAnno.get(annoType.getName());
        
        return rv != null ? rv : Collections.<PropertyMetadataBase>emptySet();
    }
    
    public final Class<V> getType()
    {
        return _clazz;
    }
    
    public boolean useCompositeColumns()
    {
        return _useCompositeColumns;
    }

    public MapPropertyMetadata getUnmappedHandler()
    {
        return _unmappedHandler;
    }
    

    public boolean hasCounterColumns()
    {
        return _hasCounterCols;
    }

    public boolean hasNormalColumns()
    {
        return _hasNormalCols;
    }
    
    @Override
    public final int hashCode()
    {
        return _clazz.hashCode();
    }
    
    @Override
    public final boolean equals(Object obj)
    {
        if(obj instanceof EntityMetadataBase<?>)
            return _clazz.equals(((EntityMetadataBase<?>) obj)._clazz);
            
        return false;
    }
    
    @Override
    public final String toString()
    {
        StringBuilder b = new StringBuilder();
        
        b.append(_clazz.getSimpleName());
        
        for(PropertyMetadataBase pm : _props)
            b.append("\n\t+ ").append(pm);
        
        return b.toString();
    }
    
}
