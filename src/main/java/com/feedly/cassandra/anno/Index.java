package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.feedly.cassandra.IIndexRowPartitioner;
import com.feedly.cassandra.SingleIndexRowPartitioner;

/**
 * This index can be placed on classes to declare an index. Usually this is used to declare multi column indexes while single column
 * indexes are defined in the @Column annotation on the field itself.
 * 
 * @author kireet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Index
{
    /**
     * the property names of the index. note that order is significant there.
     * @return the properties.
     */
    String[] props();
    
    /**
     * Partitions index columns into rows. This is neeeded for column families that have the potential to get extremely large. Cassandra
     * allows up to 2 billion columns per row, so in most cases the default implementation (that places all values in a single row) should
     * suffice.
     * @return the partitioner.
     * 
     * @see SingleIndexRowPartitioner
     */
    Class<? extends IIndexRowPartitioner> partitioner() default SingleIndexRowPartitioner.class;
}
