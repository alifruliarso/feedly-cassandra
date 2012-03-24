package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.List;

class RangeIndexQueryResult<K> implements Cloneable
{
    List<K> _currentKeys = new ArrayList<K>();
    List<StaleIndexValue> _currentValues = new ArrayList<StaleIndexValue>();
    List<RangeIndexQueryPartitionResult> _partitionResults = new ArrayList<RangeIndexQueryPartitionResult>();
    private int _nextPartitionIdx; //index of partition that potentially has more results left to fetch
    
    public List<StaleIndexValue> getCurrentValues()
    {
        return _currentValues;
    }
    public void setCurrentValues(List<StaleIndexValue> values)
    {
        _currentValues = values;
    }
    public List<K> getCurrentKeys()
    {
        return _currentKeys;
    }
    public void setCurrentKeys(List<K> keys)
    {
        _currentKeys = keys;
    }

    public List<RangeIndexQueryPartitionResult> getPartitionResults()
    {
        return _partitionResults;
    }
    public void setPartitionResults(List<RangeIndexQueryPartitionResult> partitionResults)
    {
        _partitionResults = partitionResults;
    }
    public int getPartitionPos()
    {
        return _nextPartitionIdx;
    }
    public void setPartitionPos(int partitionPos)
    {
        _nextPartitionIdx = partitionPos;
    }
    
    public boolean hasMore()
    {
        return _nextPartitionIdx < _partitionResults.size();
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        @SuppressWarnings("unchecked")
        RangeIndexQueryResult<K> copy = (RangeIndexQueryResult<K>) super.clone();
        copy._currentKeys = new ArrayList<K>(_currentKeys);
        copy._currentValues = new ArrayList<StaleIndexValue>(_currentValues);
        copy._partitionResults = new ArrayList<RangeIndexQueryPartitionResult>();
        for(RangeIndexQueryPartitionResult r : _partitionResults)
            copy._partitionResults.add((RangeIndexQueryPartitionResult) r.clone());
        
        return copy;
    }
}
