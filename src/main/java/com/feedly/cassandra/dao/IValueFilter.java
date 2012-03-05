package com.feedly.cassandra.dao;

interface IValueFilter<V>
{
    public EFilterResult isFiltered(IndexedValue<V> value);
}