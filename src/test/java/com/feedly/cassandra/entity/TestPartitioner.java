package com.feedly.cassandra.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.feedly.cassandra.IIndexRowPartitioner;

public class TestPartitioner implements IIndexRowPartitioner
{
    private static List<List<Object>> _audit = new ArrayList<List<Object>>();
    private static List<List<Object>> _rangeAudit = new ArrayList<List<Object>>();
    
    public static List<List<Object>> partitionHistory()
    {
        return _audit;
    }

    public static List<List<Object>> rangePartitionHistory()
    {
        return _rangeAudit;
    }
    
    @Override
    public List<? extends Object> partitionValue(List<Object> idxValue)
    {
        Object first = idxValue.get(0);
        List<Object> rv = Collections.singletonList(first); 
        _audit.add(rv);
        return rv;
    }

    @Override
    public List<? extends Object> partitionRange(List<Object> startIdxValues, List<Object> endIdxValues)
    {
        Object start = startIdxValues.get(0);
        Object end = endIdxValues.get(0);
        
        List<Object> rv = range(start, end);
        _rangeAudit.add(rv);
        
        return rv;
    }

    private List<Object> range(Object start, Object end)
    {
        List<Object> range = new ArrayList<Object>();
        if(start instanceof String)
        {
            char first = ((String) start).charAt(0);
            char last = ((String) end).charAt(1);
            
            for(char c = first; c <= last; c++)
                range.add(c);
        }
        else if(start instanceof Number)
        {
            long first = ((Number) start).longValue();
            long last = ((Number) end).longValue();
            
            for(long c = first; c <= last; c++)
                range.add(c);
        }
        else
        {
            range.add(new Byte((byte)0));
        }
        
        
        return range;
    }

}
