package com.feedly.cassandra;

import java.util.List;

/**
 * Partitions index columns into rows. This is neeeded for column families that have the potential to get extremely large. Cassandra
 * allows up to 2 billion columns per row, so in most cases the default implementation (that places all values in a single row) should
 * suffice.
 * 
 * @return the partitioner.
 * @see SingleIndexRowPartitioner
 */
public interface IIndexRowPartitioner
{
    /**
     * Return the partition keys where the index value may be located. If the idxValue parameter contains values for all of the index properties
     * a single element list must be returned. When partial (leading) values are provided, the returned list can be of variable size.
     * 
     * @param idxValue the index values
     * @return A list of partition row keys, represented as a List&lt;Object>. The list is convereted to a composite type and used as a row key.
     */
    public List<List<Object>> partitionValue(List<Object> idxValue);
    
    /**
     * Return the partition keys where the index values may be located. All potential keys needed to satisfy the range must be returned.
     *  
     * @param startIdxValue the index values representing the start (least value) of the range (inclusive)
     * @param endIdxValue  the index values representing the end (greatest value) of the range (inclusive)
     * @return A list of partition row keys, represented as a List&lt;Object>. The list is convereted to a composite type and used as a row key.
     */
    public List<List<Object>> partitionRange(List<Object> startIdxValues, List<Object> endIdxValues);
}
