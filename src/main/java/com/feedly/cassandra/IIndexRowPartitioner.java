package com.feedly.cassandra;

import java.util.List;

public interface IIndexRowPartitioner
{
    public List<List<Object>> partitionValue(List<Object> idxValue);
    
    public List<List<Object>> partitionRange(List<Object> startIdxValues, List<Object> endIdxValues);
}
