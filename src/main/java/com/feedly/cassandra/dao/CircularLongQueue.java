package com.feedly.cassandra.dao;

public class CircularLongQueue
{
    private final int _capacity;
    private final long[] _timings;
    private int _timingsIdx;
    private int _numTimings;

    public CircularLongQueue(int capacity)
    {
        _timings = new long[capacity];
        _capacity = capacity;
    }
    
    public void reset()
    {
        synchronized(_timings)
        {
            _timingsIdx = 0;
            _numTimings = 0;
        }
    }
    
    public void add(long timing)
    {
        synchronized(_timings)
        {
            if(_timingsIdx == _capacity)
                _timingsIdx = 0;
            
            _timings[_timingsIdx++] = timing;
            
            if(_numTimings < _capacity)
                _numTimings++;
        }
    }

    public long[] timings()
    {
        synchronized(_timings)
        {
            long[] copy = new long[_numTimings];
            long[] timings = _timings;
            int maxIdx = _timingsIdx - 1;
            for(int i = copy.length - 1; i >= 0; i--)
            {
                int idx = maxIdx - i;
                if(idx < 0)
                    idx += _capacity;
                copy[i] = timings[idx]; 
            }

            return copy;
        }

    }
}
