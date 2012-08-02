package com.feedly.cassandra.dao;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
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
    protected static final DynamicCompositeSerializer SER_DYNAMIC_COMPOSITE = new DynamicCompositeSerializer();
    protected static final CompositeSerializer SER_COMPOSITE = new CompositeSerializer();
    protected static final LongSerializer SER_LONG = LongSerializer.get();

    protected final EntityMetadata<V> _entityMeta;
    protected final IKeyspaceFactory _keyspaceFactory;
    protected final OperationStatistics _stats;
    protected final int _statsSize;
    
    DaoHelperBase(EntityMetadata<V> meta, IKeyspaceFactory factory, int statsSize)
    {
        _entityMeta = meta;
        _keyspaceFactory = factory;
        _stats = new OperationStatistics(statsSize);
        _statsSize = statsSize;
    }
    
    protected static boolean isSimpleProp(PropertyMetadataBase pmb)
    {
        return pmb.getPropertyType() == EPropertyType.SIMPLE;
    }
    
    public OperationStatistics stats()
    {
        return _stats;
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
        
        if(pm == null || isSimpleProp(pm))
            throw new IllegalArgumentException("property " + cp.getProperty() + " is not a collection");

        if(pm.getPropertyType() == EPropertyType.LIST && !(cp.getKey() instanceof Integer))
            throw new IllegalArgumentException("key for property" + cp.getProperty() + " must be an int");
        
        DynamicComposite dc = new DynamicComposite();
        dc.addComponent(0, pm.getPhysicalName(), ComponentEquality.EQUAL);
        Object colKey = cp.getKey();
        if(pm.getFieldType().equals(List.class))
            colKey = BigInteger.valueOf(((Integer) colKey).longValue());
        
        dc.addComponent(1, colKey, keyEq); 
        return SER_DYNAMIC_COMPOSITE.toBytes(dc);
    }
    
    
    /**
     * generate a property name (column name)
     * @param from the from value, can be a {@link CollectionProperty}
     * @param eq 
     * @return the key
     */
    protected byte[] propertyName(Object from, ComponentEquality eq)
    {
        if(from instanceof CollectionProperty)
            return collectionPropertyName((CollectionProperty) from, eq);
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
            return SER_DYNAMIC_COMPOSITE.toBytes(new DynamicComposite(val));
        
        if(serializer == null)
            serializer = EntityUtils.getSerializer(val.getClass());
        
        if(serializer == null)
            throw new IllegalArgumentException("unable to serialize " + val);
        
        return serializer.toBytes(val);
    }
    

    protected IEnhancedEntity asEntity(Object value)
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
    

    protected void resetEntities(Collection<?> entities)
    {
        for(Object value : entities)
        {
            IEnhancedEntity entity = asEntity(value);
            entity.getModifiedFields().clear();
            entity.setUnmappedFieldsModified(false);
        }
    }
    
    
    protected Object invokeGetter(PropertyMetadataBase pm, Object obj)
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

    protected void invokeSetter(PropertyMetadataBase pm, Object obj, Object propertyValue)
    {
        try
        {
            pm.getSetter().invoke(obj, propertyValue);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("unexpected error invoking " + pm.getSetter() + " with " + propertyValue, e);
        }
    }
    
}
