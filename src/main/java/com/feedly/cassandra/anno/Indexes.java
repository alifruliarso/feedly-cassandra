package com.feedly.cassandra.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to define multiple indexes at the class level.
 * @author kireet
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Indexes
{
    /**
     * The indexes, order is insignificant here.
     * @return the indexes.
     */
    public Index[] value();
}
