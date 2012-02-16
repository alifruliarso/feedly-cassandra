package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;

import com.feedly.cassandra.entity.ByteIndicatorSerializer;


/**
 * Marker for {@link Map} field to store unmapped column values. Only string keys are supported. Unlike mapped columns, all unmapped 
 * properties will be written and read on every database operation. To avoid saves, set the handler field to null before saving.
 * 
 * @author kireet
 * @see ColumnFamily
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface UnmappedColumnHandler
{

    /**
     * the serializer to use for this property. Using the default value indicates {@link ByteIndicatorSerializer} should be used.
     * @return the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends Serializer> value() default Serializer.class;

}
