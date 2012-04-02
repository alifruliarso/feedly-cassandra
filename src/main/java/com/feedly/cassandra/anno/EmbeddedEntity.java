package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation marks classes that are used as embedded entities within column families.
 *  
 * @author kireet
 * @see ColumnFamily
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EmbeddedEntity
{
}
