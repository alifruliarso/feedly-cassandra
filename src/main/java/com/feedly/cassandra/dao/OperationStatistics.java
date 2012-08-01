package com.feedly.cassandra.dao;

import java.util.concurrent.atomic.AtomicLong;

class OperationStatistics
{
    private final AtomicLong _numCassandraOps = new AtomicLong();
    private final AtomicLong _numOps = new AtomicLong();
    private final AtomicLong _numRows = new AtomicLong();
    private final AtomicLong _numCols = new AtomicLong();
    private final AtomicLong _totalTime = new AtomicLong();
    private final CircularLongQueue _timings;
    
    public OperationStatistics(int timingsSize)
    {
        _timings = new CircularLongQueue(timingsSize);
    }
    
    public void reset()
    {
        _numCassandraOps.set(0);
        _numOps.set(0);
        _numRows.set(0);
        _numCols.set(0);
        _totalTime.set(0);
        _timings.reset();
    }

    public void addRecentTiming(long measurement)
    {
        _totalTime.addAndGet(measurement);
        _timings.add(measurement);
    }
    
    public void incrNumRows(long incr)
    {
        _numRows.addAndGet(incr);
    }
    
    public void incrNumCols(long incr)
    {
        _numCols.addAndGet(incr);
    }
    
    public void incrNumCassandraOps(long incr)
    {
        _numCassandraOps.addAndGet(incr);
    }
    
    public void incrNumOps(long incr)
    {
        _numOps.addAndGet(incr);
    }

    public long getNumCassandraOps()
    {
        return _numCassandraOps.get();
    }
    
    public long getNumOps()
    {
        return _numOps.get();
    }

    public long getNumCols()
    {
        return _numCols.get();
    }

    public long getNumRows()
    {
        return _numRows.get();
    }

    public long getTotalTime()
    {
        return _totalTime.get();
    }
    
    public long[] getRecentTimings()
    {
        return _timings.timings();
    }
}
