package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation marks classes mapped to Cassandra column families. It should be used in conjunction with other field annotations within
 * this package.
 *  
 * @author kireet
 * @see Column
 * @see RowKey
 * @see UnmappedColumnHandler
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ColumnFamily
{
    
    /**
     * the column family name
     * @return the column family name, no value indicates the class's simple name should be used
     */
    String name() default "";
    
    /**
     * the library requires column families to use composite columns when embedding collections, otherwise string column names can be
     * used. However, once string columns are used, composite columns may not be introduced later or vice versa. Thus this flag can be
     * used for backward compatibility when removing all collections, or also to allow for the future possibility of including embedded
     * collections.
     * 
     * @return true if using compression
     */
    boolean forceCompositeColumns() default false;

    /**
     * should this column family be compressed within cassandra?
     * @return true if using compression
     */
    boolean compressed() default true;

    /**
     * The compression algorithm to use
     * @return the algorithm, passed thru to cassandra, currently "SnappyCompressor" and "DeflateCompressor" are supported
     */
    String compressionAlgo() default "SnappyCompressor";
    
    
    /**
     * The compression chunk length, in KB
     * @return the chunk length
     */
    int compressionChunkLength() default 64;
    
}
