package com.feedly.cassandra;

import java.util.Collections;
import java.util.List;

/**
 * The default index partitioner that places all index values in a single partition.
 * 
 * @author kireet
 */
public class SingleIndexRowPartitioner implements IIndexRowPartitioner
{
    private final Byte VAL = new Byte((byte)0);
    private final List<List<Object>> VAL_ARRAY = Collections.singletonList(Collections.<Object>singletonList(VAL));
    
    @Override
    public List<List<Object>> partitionValue(List<Object> value)
    {
        return VAL_ARRAY;
    }

    @Override
    public List<List<Object>> partitionRange(List<Object> start, List<Object> end)
    {
        return VAL_ARRAY;
    }
}
