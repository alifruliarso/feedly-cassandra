package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import me.prettyprint.hector.api.Serializer;

import com.feedly.cassandra.bean.ByteIndicatorSerializer;


/**
 * This annotation marks fields to be saved in cassandra. The field should be accompanied by typical public getter and setter methods. 
 * @author kireet
 * @see ColumnFamily
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column
{
    /**
     * The physical column name to use in cassandra.
     * @return the column name
     */
    public String col() default "";
    
    /**
     * the serializer to use for this property. Using the default value indicates an appropriate serializer should be detected at runtime.
     * For collections, the default serializer is {@link ByteIndicatorSerializer}.
     * @return the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends Serializer> serializer() default Serializer.class;
    
    /**
     * Should this column be indexed using native cassandra secondary indexes?
     * @return true if the column should be indexed
     */
    public boolean indexed() default false;
}
