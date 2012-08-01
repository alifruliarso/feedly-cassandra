package com.feedly.cassandra.dao;

public interface OperationStatisticsMonitorMBean
{
    public long getNumOperations();
    public long getNumCassandraOperations();
    public long getRowCount();
    public long getColumnCount();
    
    public String[] getTimings();
    public String getRecentMinTime();
    public String getRecentMaxTime();
    public String getRecentAvgTime();
    public String getRecentStdDev();
    public String getTotalTime();
    
    public void reset();
}
