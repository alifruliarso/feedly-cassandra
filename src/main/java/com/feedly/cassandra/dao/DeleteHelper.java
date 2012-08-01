package com.feedly.cassandra.dao;

import java.util.Collection;
import java.util.Collections;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.SimplePropertyMetadata;

class DeleteHelper<K, V> extends DaoHelperBase<K, V>
{

    DeleteHelper(EntityMetadata<V> meta, IKeyspaceFactory factory, int statsSize)
    {
        super(meta, factory, statsSize);
    }

    public void delete(K key, DeleteOptions options)
    {
        mdelete(Collections.singleton(key), options);
    }

    //for now, not cleaning up indexes, it's assumed subsequent finds will eventually clean up stale entries
    public void mdelete(Collection<K> keys, DeleteOptions options)
    {
        long startTime = System.nanoTime();
        SimplePropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        Keyspace keyspace = _keyspaceFactory.createKeyspace(options.getConsistencyLevel());
        Mutator<byte[]> mutator = HFactory.createMutator(keyspace, SER_BYTES);
        
        for(K key : keys)
        {
            byte[] keyBytes = serialize(key, false, keyMeta.getSerializer());
            
            _logger.debug("deleting {}[{}]", _entityMeta.getType().getSimpleName(), key);

            if(_entityMeta.hasNormalColumns())
                mutator.addDeletion(keyBytes, _entityMeta.getFamilyName());
            
            if(_entityMeta.hasCounterColumns())
                mutator.addDeletion(keyBytes, _entityMeta.getCounterFamilyName());
        }

        /*
         * execute the deletions
         */
        mutator.execute();

        int size = keys.size();
        _stats.addRecentTiming(System.nanoTime() - startTime);
        _stats.incrNumOps(1);
        
        if(_entityMeta.hasNormalColumns())
            _stats.incrNumCassandraOps(size);
        if(_entityMeta.hasCounterColumns())
            _stats.incrNumCassandraOps(size);

        _stats.incrNumRows(size);
        
        _logger.debug("deleted up to {} values from {}", size, _entityMeta.getType().getSimpleName());
    }
    
}
