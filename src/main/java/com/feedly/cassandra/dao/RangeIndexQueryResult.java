package com.feedly.cassandra.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RangeIndexQueryResult<K> implements Cloneable
{
    private List<K> _currentKeys = new ArrayList<K>(); //not really sorted, just reliably ordered
    private Map<K, List<StaleIndexValue>> _currentValues = new HashMap<K, List<StaleIndexValue>>();
    private List<RangeIndexQueryPartitionResult> _partitionResults = new ArrayList<RangeIndexQueryPartitionResult>();
    private int _nextPartitionIdx; //index of partition that potentially has more results left to fetch
    
    public List<K> getCurrentKeys()
    {
        return _currentKeys;
    }

    public void setCurrentKeys(List<K> currentKeys)
    {
        _currentKeys = currentKeys;
    }
    public Map<K, List<StaleIndexValue>> getCurrentValues()
    {
        return _currentValues;
    }
    
    public void setCurrentValues(Map<K, List<StaleIndexValue>> values)
    {
        _currentValues = values;
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

    
    public void add(K key, StaleIndexValue value)
    {
        _currentKeys.add(key);
        List<StaleIndexValue> l = _currentValues.get(key);
        if(l == null)
        {
            l = new ArrayList<StaleIndexValue>();
            _currentValues.put(key, l);
        }
        
        l.add(value);
    }

    public void clearCurrent()
    {
        _currentKeys.clear();
        _currentValues.clear();
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
        copy._currentValues = new HashMap<K, List<StaleIndexValue>>();
        
        for(Map.Entry<K, List<StaleIndexValue>> entry : _currentValues.entrySet())
            copy._currentValues.put(entry.getKey(), new ArrayList<StaleIndexValue>(entry.getValue()));
        
        copy._partitionResults = new ArrayList<RangeIndexQueryPartitionResult>();
        for(RangeIndexQueryPartitionResult r : _partitionResults)
            copy._partitionResults.add((RangeIndexQueryPartitionResult) r.clone());
        
        return copy;
    }
}
