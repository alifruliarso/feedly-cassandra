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
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.serializers.StringSerializer;
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
import com.feedly.cassandra.dao.CounterColumn;

/**
 * This class holds metadata for a given entity including key, property and index information.
 * 
 * @author kireet
 * 
 * @param <V> the entity type
 */
public class EntityMetadata<V> extends EntityMetadataBase<V>
{
    private static final Logger _logger = LoggerFactory.getLogger(EntityMetadata.class.getName());
    
    private final Set<Annotation> _annotations;
    private final SimplePropertyMetadata _keyMeta;
    private final String _familyName;
    private final byte[] _familyNameBytes;
    private final String _idxFamilyName;
    private final String _counterFamilyName;
    private final List<IndexMetadata> _indexes;
    private final Map<SimplePropertyMetadata, List<IndexMetadata>> _indexesByProp;

    @SuppressWarnings("unchecked")
    public EntityMetadata(Class<V> clazz)
    {
        super(clazz, 
              areCompositeColsForced(clazz),
              ttlValue(clazz),
              false);

        ColumnFamily familyAnno = clazz.getAnnotation(ColumnFamily.class);

        _familyName = familyAnno.name();
        _familyNameBytes = StringSerializer.get().toBytes(_familyName);
        _idxFamilyName = familyAnno.name() + "_idx";
        _counterFamilyName = familyAnno.name() + "_cntr";
        
        Set<Annotation> beanAnnos = new HashSet<Annotation>();
        for(Annotation a : clazz.getAnnotations())
            beanAnnos.add(a);
        
        _annotations = Collections.unmodifiableSet(beanAnnos);
        
        List<IndexMetadata> indexes = new ArrayList<IndexMetadata>();
        Map<SimplePropertyMetadata, List<IndexMetadata>> indexesByProp = new HashMap<SimplePropertyMetadata, List<IndexMetadata>>();
        SimplePropertyMetadata keyMeta = null;
        
        for(Field f : clazz.getDeclaredFields())
        {
            if(f.isAnnotationPresent(RowKey.class))
            {
                Method getter = getGetter(f);
                Method setter = getSetter(f);
                if(keyMeta != null)
                    throw new IllegalArgumentException("@RowKey may only be used on one field.");

                if(getter == null || setter == null)
                    throw new IllegalArgumentException("@RowKey field must have valid getter and setter.");

                if(!PropertyMetadataFactory.isSimpleType(f))
                    throw new IllegalArgumentException("@RowKey may only be used on a simple type, not custom types or collections.");
                
                RowKey anno = f.getAnnotation(RowKey.class);
                keyMeta = PropertyMetadataFactory.buildSimplePropertyMetadata(f, null, -1, getter, setter, (Class<? extends Serializer<?>>) anno.value(), false);
            }

            if(f.isAnnotationPresent(Column.class))
            {
                Column anno = f.getAnnotation(Column.class);
                String col = anno.name();
                if(col.equals(""))
                    col = f.getName();
                
                if(anno.hashIndexed() && anno.rangeIndexed())
                    throw new IllegalStateException(f.getName() + ": property can be range or hash indexed, not both");

                PropertyMetadataBase pm = getProperty(f.getName());
                if(anno.hashIndexed() || anno.rangeIndexed())
                {
                    if(pm.getPropertyType() != EPropertyType.SIMPLE)
                        throw new IllegalStateException(f.getName() + ": property cannot be indexed, not a simple type: " + pm.getPropertyType());
                    
                    if(anno.hashIndexed() && anno.rangeIndexed())
                        throw new IllegalArgumentException(f.getName() + ": cannot be both hash and range indexed, select one or the other.");
                    

                    IndexMetadata idxMeta = new IndexMetadata(familyAnno.name(),
                                                Collections.singletonList( (SimplePropertyMetadata) pm), 
                                                createPartitioner(anno.rangeIndexPartitioner()), 
                                                anno.hashIndexed() ? EIndexType.HASH : EIndexType.RANGE);
                    indexes.add(idxMeta);
                    List<IndexMetadata> l = indexesByProp.get(pm);
                    if(l == null)
                    {
                        l = new ArrayList<IndexMetadata>();
                        indexesByProp.put((SimplePropertyMetadata) pm, l);
                    }
                    l.add(idxMeta);
                }

                if(f.isAnnotationPresent(UnmappedColumnHandler.class))
                {
                    throw new IllegalArgumentException(f.getName() + ": @UnmappedColumnHandler should not also be annotated as a mapped @Column.");
                }
            }
        }
        
        if(keyMeta == null)
            throw new IllegalArgumentException("missing @RowKey annotated field");

        if(!getAnnotatedProperties(RowKey.class).isEmpty())
            _logger.warn(keyMeta.getName(), ": key property is also stored in a column");

        _keyMeta = keyMeta;
        
        /*
         * include indexes declared at the class level. Usually these are indexes that are on multiple columns
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
            List<SimplePropertyMetadata> l = new ArrayList<SimplePropertyMetadata>();
            if(anno.props() == null || anno.props().length == 0)
                throw new IllegalStateException("no properties referenced in index: ");
                
            for(String prop : anno.props())
            {
                PropertyMetadataBase p = getProperty(prop);

                if(prop == null)
                    throw new IllegalStateException("unrecognized property referenced in index: " + prop);
                
                if(l.contains(p))
                    throw new IllegalStateException("duplicate property referenced in index: " + prop);

                if(p.getPropertyType() != EPropertyType.SIMPLE)
                    throw new IllegalStateException("non primitive or enum property referenced in index: " + prop);
                
                l.add((SimplePropertyMetadata) p);
            }
            
            IndexMetadata im = new IndexMetadata(familyAnno.name(), l, createPartitioner(anno.partitioner()), EIndexType.RANGE);
            indexes.add(im);
            for(SimplePropertyMetadata p : l)
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
        for(Entry<SimplePropertyMetadata, List<IndexMetadata>> entry : indexesByProp.entrySet())
            entry.setValue(Collections.unmodifiableList(entry.getValue()));
        
        _indexesByProp = indexesByProp;
    }

    private static int ttlValue(Class<?> clazz)
    {
        ColumnFamily familyAnno = clazz.getAnnotation(ColumnFamily.class);
        if(familyAnno == null)
            throw new NullPointerException("@ColumnFamily annotation not set.");
        
        if(familyAnno.ttl() < 0)
            return -1;
        
        int ttl = (int) TimeUnit.SECONDS.convert(familyAnno.ttl(), familyAnno.ttlUnit());
        
        if(ttl == 0)
            throw new IllegalArgumentException("TTL must be positive (>= 1 second) or unset");

        return ttl;
    }

    private static boolean areCompositeColsForced(Class<?> clazz)
    {
        ColumnFamily familyAnno = clazz.getAnnotation(ColumnFamily.class);
        if(familyAnno == null)
            throw new NullPointerException("@ColumnFamily annotation not set.");
        
        
        return familyAnno.forceCompositeColumns() || !onlySimpleTypes(clazz);
    }

    private static boolean onlySimpleTypes(Class<?> clazz)
    {
        for(Field f : clazz.getDeclaredFields())
        {
            if(f.isAnnotationPresent(Column.class) && !PropertyMetadataFactory.isSimpleType(f) && !f.getType().equals(CounterColumn.class))
                return false;
        }
        
        return true;
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

    public Set<Annotation> getAnnotations()
    {
        return _annotations;
    }
    
    public List<IndexMetadata> getIndexes()
    {
        return _indexes;
    }
    
    public boolean isIndexed(SimplePropertyMetadata pm)
    {
        return _indexesByProp.containsKey(pm);
    }
    
    public List<IndexMetadata> getIndexes(SimplePropertyMetadata pm)
    {
        List<IndexMetadata> l = _indexesByProp.get(pm);
        return l == null ? Collections.<IndexMetadata>emptyList() : l;
    }
    
    public SimplePropertyMetadata getKeyMetadata()
    {
        return _keyMeta;
    }
    
    public String getFamilyName()
    {
        return _familyName;
    }
    
    public byte[] getFamilyNameBytes()
    {
        return _familyNameBytes;
    }
    
    public String getIndexFamilyName()
    {
        return _idxFamilyName;
    }

    public String getCounterFamilyName()
    {
        return _counterFamilyName;
    }
}
