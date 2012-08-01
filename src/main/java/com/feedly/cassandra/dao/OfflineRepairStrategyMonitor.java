package com.feedly.cassandra.dao;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class OfflineRepairStrategyMonitor extends OperationStatisticsMonitor implements OfflineRepairStrategyMonitorMBean 
{
    private final AtomicLong _numDropped = new AtomicLong(); 
    private final Queue<?> _executionQueue;
    
    public OfflineRepairStrategyMonitor(OperationStatistics stats, Queue<?> executionQueue)
    {
        super(stats);
        _executionQueue = executionQueue;
    }

    @Override
    public int getQueueSize()
    {
        return _executionQueue.size();
    }

    @Override
    public long getNumDroppedRepairs()
    {
        return _numDropped.get();
    }

    @Override
    public void reset()
    {
        super.reset();
        _numDropped.set(0);
    }
    
    public void rejectedExecution()
    {
        _numDropped.incrementAndGet();
    }

}
