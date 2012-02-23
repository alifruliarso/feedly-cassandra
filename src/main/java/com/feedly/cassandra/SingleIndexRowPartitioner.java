package com.feedly.cassandra;

import java.util.Collections;
import java.util.List;

public class SingleIndexRowPartitioner implements IIndexRowPartitioner
{
    private final Byte VAL = new Byte((byte)0);
    private final List<Byte> VAL_ARRAY = Collections.singletonList(VAL);
    
    @Override
    public List<Byte> partitionValue(List<Object> value)
    {
        return VAL_ARRAY;
    }

    @Override
    public List<Byte> partitionRange(List<Object> start, List<Object> end)
    {
        return VAL_ARRAY;
    }
}
