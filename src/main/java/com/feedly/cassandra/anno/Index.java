package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.feedly.cassandra.IIndexRowPartitioner;
import com.feedly.cassandra.SingleIndexRowPartitioner;

//class level annotation, can be used to create multi-column indexes
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Index
{
    String[] props();
    
    @SuppressWarnings("rawtypes")
    Class<? extends IIndexRowPartitioner> partitioner() default SingleIndexRowPartitioner.class;
}
