package com.feedly.cassandra.entity;


public class EmbeddedEntityMetadata<V> extends EntityMetadataBase<V>
{
    public EmbeddedEntityMetadata(Class<V> clazz, int ttl)
    {
        super(clazz, true, ttl);
    }
}
