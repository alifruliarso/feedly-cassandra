package com.feedly.cassandra;

import java.util.List;

public interface IIndexRowPartitioner
{
    public List<? extends Object> partitionValue(List<Object> idxValue);
    
    public List<? extends Object> partitionRange(List<Object> startIdxValues, List<Object> endIdxValues);
}
