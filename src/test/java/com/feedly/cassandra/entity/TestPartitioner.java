package com.feedly.cassandra.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.feedly.cassandra.IIndexRowPartitioner;

public class TestPartitioner implements IIndexRowPartitioner
{
    private static List<List<List<Object>>> _audit = new ArrayList<List<List<Object>>>();
    private static List<List<List<Object>>> _rangeAudit = new ArrayList<List<List<Object>>>();
    
    public static List<List<List<Object>>> partitionHistory()
    {
        return _audit;
    }

    public static List<List<List<Object>>> rangePartitionHistory()
    {
        return _rangeAudit;
    }
    
    @Override
    public List<List<Object>> partitionValue(List<Object> idxValue)
    {
        Object first = idxValue.get(0);
        List<List<Object>> rv = Collections.singletonList(Collections.singletonList(first)); 
        _audit.add(rv);
        return rv;
    }

    @Override
    public List<List<Object>> partitionRange(List<Object> startIdxValues, List<Object> endIdxValues)
    {
        Object start = startIdxValues.get(0);
        Object end = endIdxValues.get(0);
        
        List<List<Object>> rv = range(start, end);
        _rangeAudit.add(rv);
        
        return rv;
    }

    private List<List<Object>> range(Object start, Object end)
    {
        List<List<Object>> range = new ArrayList<List<Object>>();
        if(start instanceof String)
        {
            char first = ((String) start).charAt(0);
            char last = ((String) end).charAt(1);
            
            for(char c = first; c <= last; c++)
                range.add(Collections.<Object>singletonList(c));
        }
        else if(start instanceof Number)
        {
            long first = ((Number) start).longValue();
            long last = ((Number) end).longValue();
            
            for(long c = first; c <= last; c++)
                range.add(Collections.<Object>singletonList(c));
        }
        else
        {
            range.add(Collections.<Object>singletonList(new Byte((byte)0)));
        }
        
        
        return range;
    }

}
