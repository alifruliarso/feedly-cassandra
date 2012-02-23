package com.feedly.cassandra.dao;

import java.util.Collection;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

public interface IStaleIndexValueStrategy
{
    public void handle(EntityMetadata<?> entity, IndexMetadata index, Collection<StaleIndexValue> values);
}
