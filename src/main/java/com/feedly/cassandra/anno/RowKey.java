package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import me.prettyprint.hector.api.Serializer;

/**
 * This annotation marks the field to 
 * @author kireet
 * @see ColumnFamily
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RowKey
{
    
    /**
     * the serializer to use for this property. Using the default value indicates an appropriate serializer should be detected at runtime.
     * @return the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Serializer> value() default Serializer.class;
}
