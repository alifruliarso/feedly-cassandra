package com.feedly.cassandra.dao;

public interface OfflineRepairStrategyMonitorMBean extends OperationStatisticsMonitorMBean
{
    public int getQueueSize();
    public long getNumDroppedRepairs();
}
