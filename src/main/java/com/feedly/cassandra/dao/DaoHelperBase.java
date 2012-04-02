package com.feedly.cassandra.dao;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EPropertyType;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.EntityUtils;
import com.feedly.cassandra.entity.PropertyMetadataBase;
import com.feedly.cassandra.entity.enhance.EntityTransformerTask;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

abstract class DaoHelperBase<K,V>
{
    protected final Logger _logger = LoggerFactory.getLogger(getClass().getName());

    protected static final BytesArraySerializer SER_BYTES = BytesArraySerializer.get();
    protected static final StringSerializer SER_STRING = StringSerializer.get();
    protected static final DynamicCompositeSerializer SER_COMPOSITE = new DynamicCompositeSerializer();

    private static final Set<EPropertyType> COLLECTION_PROPS = 
            Collections.unmodifiableSet(new HashSet<EPropertyType>(Arrays.asList(new EPropertyType[] {EPropertyType.LIST, 
                    EPropertyType.MAP, 
                    EPropertyType.SORTED_MAP})));

    protected final EntityMetadata<V> _entityMeta;
    protected IKeyspaceFactory _keyspaceFactory;

    DaoHelperBase(EntityMetadata<V> meta, IKeyspaceFactory factory)
    {
        _entityMeta = meta;
        _keyspaceFactory = factory;
    }
    
    protected static boolean isCollectionProp(PropertyMetadataBase pmb)
    {
        return COLLECTION_PROPS.contains(pmb.getPropertyType());
    }
    
    /**
     * generate a property name for a collection property
     * @param cp
     * @param keyEq
     * @return
     */
    protected byte[] collectionPropertyName(CollectionProperty cp, ComponentEquality keyEq)
    {
        PropertyMetadataBase pm = _entityMeta.getProperty(cp.getProperty());
        
        if(pm == null || !isCollectionProp(pm))
            throw new IllegalArgumentException("property " + cp.getProperty() + " is not a collection");

        if(pm.getPropertyType() == EPropertyType.LIST && !(cp.getKey() instanceof Integer))
            throw new IllegalArgumentException("key for property" + cp.getProperty() + " must be an int");
        
        DynamicComposite dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL);
        Object colKey = cp.getKey();
        if(pm.getFieldType().equals(List.class))
            colKey = BigInteger.valueOf(((Integer) colKey).longValue());
        
        dc.addComponent(1, colKey, keyEq); 
        return SER_COMPOSITE.toBytes(dc);
    }
    
    
    /**
     * generate a property name (column name)
     * @param from the from value, can be a {@link CollectionProperty}
     * @return the key
     */
    protected byte[] propertyName(Object from)
    {
        if(from instanceof CollectionProperty)
            return collectionPropertyName((CollectionProperty) from, ComponentEquality.EQUAL);
        else
            return serialize(from, true, null);
    }
    
    /**
     * helper to serialize values
     * @param val the value
     * @param isColName is the value a column name
     * @param serializer the serializer to use, if null is passed one will try to be inferred
     * @return the serialized byte[]
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected byte[] serialize(Object val, boolean isColName, Serializer serializer)
    {
        if(val == null)
        {
            if(isColName)
                throw new IllegalArgumentException("col name can not be null");
            else
                return null;
        }

        if(isColName && _entityMeta.useCompositeColumns())
            return SER_COMPOSITE.toBytes(new DynamicComposite(val));
        
        if(serializer == null)
            serializer = EntityUtils.getSerializer(val.getClass());
        
        if(serializer == null)
            throw new IllegalArgumentException("unable to serialize " + val);
        
        return serializer.toBytes(val);
    }
    

    protected IEnhancedEntity asEntity(V value)
    {
        try
        {
            return (IEnhancedEntity) value;
        }
        catch(ClassCastException cce)
        {
            throw new IllegalArgumentException(value.getClass().getSimpleName()
                    + " was not enhanced. Entity classes must be enhanced post compilation See " + EntityTransformerTask.class.getName());
        }
    }
    
    protected Object invokeGetter(PropertyMetadataBase pm, V obj)
    {
        try
        {
            return pm.getGetter().invoke(obj);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("unexpected error invoking " + pm.getGetter(), e);
        }
    }

    protected void invokeSetter(PropertyMetadataBase pm, V obj, Object value)
    {
        try
        {
            pm.getSetter().invoke(obj, value);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("unexpected error invoking " + pm.getGetter(), e);
        }
    }
    
}
