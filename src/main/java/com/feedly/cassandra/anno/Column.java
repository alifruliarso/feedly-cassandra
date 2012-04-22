package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import me.prettyprint.hector.api.Serializer;

import com.feedly.cassandra.IIndexRowPartitioner;
import com.feedly.cassandra.SingleIndexRowPartitioner;
import com.feedly.cassandra.entity.ByteIndicatorSerializer;


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
     * The physical column name to use in cassandra, defaults to the field name. Typically a very short name is used to save storage space.
     * @return the column name
     */
    public String name() default "";
    
    /**
     * the serializer to use for this property. Using the default value indicates an appropriate serializer should be detected at runtime.
     * For collections, the default serializer is {@link ByteIndicatorSerializer}.
     * @return the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends Serializer> serializer() default Serializer.class;
    
    /**
     * Should this column be hashed indexed? Hash indexes provide unique lookup capability but not range searches. If range searches are
     * not required, using a hash index is preferacble as native cassandra secondary indexes may be used.
     * @return true if the column should be hash indexed
     */
    public boolean hashIndexed() default false;

    /**
     * Should this column be range indexed? Range indexes provide the ability to do range lookups as opposed to hash indexes which only allow
     * exact value lookups. The downside of range indexes is write performance is significantly worse as custom built indexes must be used,
     * resulting in multiple cassandra operations per save. 
     * 
     * @return true if the column should be range indexed
     */
    public boolean rangeIndexed() default false;
    
    /**
     * Partitions index columns into rows. This is neeeded for column families that have the potential to get extremely large. Cassandra
     * allows up to 2 billion columns per row, so in most cases the default implementation (that places all values in a single row) should
     * suffice.
     * 
     * @return the partitioner.
     * @see SingleIndexRowPartitioner
     */
    public Class<? extends IIndexRowPartitioner> rangeIndexPartitioner() default SingleIndexRowPartitioner.class;
    
    /**
     * The time to live for this column. For collections, the time to live applies to each entry in the collection, not the entire collection
     * itself. TTL values for collections and embedded properties override any TTL values specified in the embedded objects. The granularity 
     * and minimum interval for TTLs is 1 second.
     * 
     * @return the time to live duration
     */
    public int ttl() default -1;
    
    /**
     * the time unit for the ttl interval, defaults to seconds.
     * @return the time unit
     */
    public TimeUnit ttlUnit() default TimeUnit.SECONDS;
}
