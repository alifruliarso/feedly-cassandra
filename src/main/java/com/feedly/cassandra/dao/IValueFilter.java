package com.feedly.cassandra.dao;

/**
 * Used to perform filter on read operations when retrieving indexed values.
 * 
 * @author kireet
 *
 * @param <V> - the entity type
 */
interface IValueFilter<V>
{
    /**
     * test if the value should be filtered (excluded). Values may be excluded when the index is stale (index value does not match row) or
     * the row does not match the non-indexed values of the template.
     * @param value the entity
     * @return the filter result
     */
    public EFilterResult isFiltered(IndexedValue<V> value);
}