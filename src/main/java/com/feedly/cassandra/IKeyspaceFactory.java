package com.feedly.cassandra;

import me.prettyprint.hector.api.Keyspace;

public interface IKeyspaceFactory
{
    Keyspace createKeyspace(EConsistencyLevel level);
}
