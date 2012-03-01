package com.feedly.cassandra.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

import com.feedly.cassandra.IIndexRowPartitioner;
import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.Index;
import com.feedly.cassandra.anno.Indexes;
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
    private final String _familyName;
    private final String _walFamilyName;
    private final String _idxFamilyName;
    private final List<IndexMetadata> _indexes;
    private final Map<PropertyMetadata, List<IndexMetadata>> _indexesByProp;
    
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
    
    public EntityMetadata(Class<V> clazz)
    {
        _clazz = clazz;
        ColumnFamily familyAnno = clazz.getAnnotation(ColumnFamily.class);

        _familyName = familyAnno.name();
        _walFamilyName = familyAnno.name() + "_idxwal";
        _idxFamilyName = familyAnno.name() + "_idx";

        Set<Annotation> beanAnnos = new HashSet<Annotation>();
        for(Annotation a : clazz.getAnnotations())
            beanAnnos.add(a);
        
        _annotations = Collections.unmodifiableSet(beanAnnos);
        
        Map<String, PropertyMetadata> props = new TreeMap<String, PropertyMetadata>(); 
        Map<String, PropertyMetadata>  propsByPhysical = new TreeMap<String, PropertyMetadata>();
        Map<String, Set<PropertyMetadata>> propsByAnno = new TreeMap<String, Set<PropertyMetadata>>();
        List<IndexMetadata> indexes = new ArrayList<IndexMetadata>();
        Map<PropertyMetadata, List<IndexMetadata>> indexesByProp = new HashMap<PropertyMetadata, List<IndexMetadata>>();
        PropertyMetadata keyMeta = null, unmappedHandler = null;
        boolean hasCollections = false;

        for(Field f : clazz.getDeclaredFields())
        {
            if(f.isAnnotationPresent(Column.class) && isCollectionType(f))
            {
                hasCollections = true;
                break;
            }
        }
        
        _useCompositeColumns = hasCollections || familyAnno.forceCompositeColumns();

        for(Field f : clazz.getDeclaredFields())
        {
            Method getter = getGetter(f);
            Method setter = getSetter(f);
            if(f.isAnnotationPresent(RowKey.class))
            {
                if(keyMeta != null)
                    throw new IllegalArgumentException("@RowKey may only be used on one field.");

                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@RowKey field must have valid getter and setter.");

                RowKey anno = f.getAnnotation(RowKey.class);
                keyMeta = new PropertyMetadata(f, null, getter, setter, serializerClass(anno.value()),  false);
            }

            if(f.isAnnotationPresent(UnmappedColumnHandler.class))
            {
                if(unmappedHandler != null)
                    throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on one field.");

                if(!Map.class.equals(f.getType()) && !SortedMap.class.equals(f.getType()))
                    throw new IllegalArgumentException("@UnmappedColumnHandler may only be used on a Map or SortedMap, not sub-interfaces or classes.");

                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@UnmappedColumnHandler field must have valid getter and setter.");

                UnmappedColumnHandler anno = f.getAnnotation(UnmappedColumnHandler.class);
                unmappedHandler = new PropertyMetadata(f, null, getter, setter, serializerClass(anno.value()), _useCompositeColumns);
            }

            
            if(f.isAnnotationPresent(Column.class))
            {
                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@Column field must have valid getter and setter.");

                Column anno = f.getAnnotation(Column.class);
                String col = anno.col();
                if(col.equals(""))
                    col = f.getName();
                
                if(anno.hashIndexed() && anno.rangeIndexed())
                    throw new IllegalStateException(f.getName() + ": property can be range or hash indexed, not both");
                
                PropertyMetadata pm = new PropertyMetadata(f, col, getter, setter, serializerClass(anno.serializer()), _useCompositeColumns);
                props.put(f.getName(), pm);
                if(propsByPhysical.put(col, pm) != null)
                    throw new IllegalStateException(f.getName() + ": physical column name must be unique - " + col);

                if(anno.hashIndexed() || anno.rangeIndexed())
                {
                    if(isCollectionType(f))
                        throw new IllegalStateException(f.getName() + ": collection property cannot be indexed");
                    
                    if(anno.hashIndexed() && anno.rangeIndexed())
                        throw new IllegalArgumentException(f.getName() + ": cannot be both hash and range indexed, select one or the other.");
                    

                    IndexMetadata idxMeta = new IndexMetadata(familyAnno.name(),
                                                Collections.singletonList(pm), 
                                                createPartitioner(anno.rangeIndexPartitioner()), 
                                                anno.hashIndexed() ? EIndexType.HASH : EIndexType.RANGE);
                    indexes.add(idxMeta);
                    List<IndexMetadata> l = indexesByProp.get(pm);
                    if(l == null)
                    {
                        l = new ArrayList<IndexMetadata>();
                        indexesByProp.put(pm, l);
                    }
                    l.add(idxMeta);
                }

                if(f.isAnnotationPresent(UnmappedColumnHandler.class))
                {
                    if(keyMeta != null)
                        throw new IllegalArgumentException(f.getName() + ": @UnmappedColumnHandler should not also be annotated as a mapped @Column.");

                    keyMeta = new PropertyMetadata(f, null, getter, setter, serializerClass(anno.serializer()), _useCompositeColumns);
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
        
        /*
         * now include indexes declared at the class level. Usually these are indexes that are on multiple columns
         */
        Index idxAnno = clazz.getAnnotation(Index.class);
        Indexes idxArrAnno = clazz.getAnnotation(Indexes.class);
        Index[] allIdxAnnos;
        if(idxAnno == null)
        {
            allIdxAnnos = idxArrAnno == null ? new Index[0] : idxArrAnno.value(); 
        }
        else
        {
            if(idxArrAnno == null)
                allIdxAnnos = new Index[] {idxAnno};
            else
            {
                allIdxAnnos = new Index[idxArrAnno.value().length+1];
                System.arraycopy(idxArrAnno.value(), 0, allIdxAnnos, 0, idxArrAnno.value().length);
                allIdxAnnos[allIdxAnnos.length-1] = idxAnno;
            }
        }
        
        for(Index anno : allIdxAnnos)
        {
            List<PropertyMetadata> l = new ArrayList<PropertyMetadata>();
            if(anno.props() == null || anno.props().length == 0)
                throw new IllegalStateException("no properties referenced in index: ");
                
            for(String prop : anno.props())
            {
                PropertyMetadata p = _propsByName.get(prop);
                if(prop == null)
                    throw new IllegalStateException("unrecognized property referenced in index: " + prop);
                
                if(l.contains(p))
                    throw new IllegalStateException("duplicate property referenced in index: " + prop);
                
                l.add(p);
            }
            
            IndexMetadata im = new IndexMetadata(familyAnno.name(), l, createPartitioner(anno.partitioner()), EIndexType.RANGE);
            indexes.add(im);
            for(PropertyMetadata p : l)
            {
                List<IndexMetadata> pl = indexesByProp.get(p);
                if(pl == null)
                {
                    pl = new ArrayList<IndexMetadata>();
                    indexesByProp.put(p, pl);
                }
                
                pl.add(im);
            }
        }
        
        
        Set<String> indexIds = new HashSet<String>();
        for(IndexMetadata m : indexes)
        {
            if(!indexIds.add(m.id()))
                throw new IllegalStateException("duplicate index " + m);
        }
        _indexes = Collections.unmodifiableList(indexes);
        for(Entry<PropertyMetadata, List<IndexMetadata>> entry : indexesByProp.entrySet())
            entry.setValue(Collections.unmodifiableList(entry.getValue()));
        
        _indexesByProp = indexesByProp;
    }

    private IIndexRowPartitioner createPartitioner(Class<? extends IIndexRowPartitioner> clazz)
    {
        try
        {
            return clazz.newInstance();
        }
        catch(Exception ex)
        {
            throw new IllegalStateException("no no-arg constructor for partitioner " + clazz.getName());
        }
    }

    private boolean isCollectionType(Field f)
    {
        return f.getType().equals(Map.class) || f.getType().equals(List.class) || f.getType().equals(SortedMap.class);
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
  
    public List<IndexMetadata> getIndexes()
    {
        return _indexes;
    }
    
    public boolean isIndexed(PropertyMetadata pm)
    {
        return _indexesByProp.containsKey(pm);
    }
    
    public List<IndexMetadata> getIndexes(PropertyMetadata pm)
    {
        List<IndexMetadata> l = _indexesByProp.get(pm);
        return l == null ? Collections.<IndexMetadata>emptyList() : l;
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
    
    public String getFamilyName()
    {
        return _familyName;
    }
    
    public String getWalFamilyName()
    {
        return _walFamilyName;
    }
    
    public String getIndexFamilyName()
    {
        return _idxFamilyName;
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
