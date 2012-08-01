package com.feedly.cassandra.dao;

import java.util.concurrent.TimeUnit;

public class OperationStatisticsMonitor implements OperationStatisticsMonitorMBean
{
    private final OperationStatistics _stats;

    public OperationStatisticsMonitor(OperationStatistics stats)
    {
        _stats = stats;
    }
    
    @Override
    public void reset()
    {
        _stats.reset();
    }
    
    @Override
    public long getNumOperations()
    {
        return _stats.getNumOps();
    }

    @Override
    public long getNumCassandraOperations()
    {
        return _stats.getNumCassandraOps();
    }

    @Override
    public long getRowCount()
    {
        return _stats.getNumRows();
    }

    @Override
    public long getColumnCount()
    {
        return _stats.getNumCols();
    }

    @Override
    public String[] getTimings()
    {
        long[] timings = _stats.getRecentTimings();

        String[] reportedTimings = new String[timings.length];
        
        for(int i = timings.length - 1; i >= 0; i--)
            reportedTimings[i] = toReportedTime(timings[i]);
        
        return reportedTimings;
    }

    
    String toReportedTime(long val)
    {
        if(val >= 1e9)
        {
            double seconds = ((double) val)/1e9;
            
            if(seconds >= 3600)
                return seconds/3600 + " hours";
            if(seconds >= 60)
                return seconds/60 + " minutes";

            return  seconds + " seconds";
        }
        else if (val >= 1e6)
            return ((double) val)/1e6 + " milliseconds";
        else if (val >= 1e3)
            return ((double) val)/1e3 + " microseconds";
        
        return val + " nanoseconds";
    }
    
    String toReportedTime(double val)
    {
        if(val >= 1e9)
        {
            double seconds = val/1e9;
            
            if(seconds >= 3600)
                return seconds/3600 + " hours";
            if(seconds >= 60)
                return seconds/60 + " minutes";
            
            return  seconds + " seconds";
        }
        else if (val >= 1e6)
            return val/1e6 + " milliseconds";
        else if (val >= 1e3)
            return val/1e3 + " microseconds";
        
        return val + " nanoseconds";
    }

    public static void main(String[] args) throws Throwable
    {
        OperationStatisticsMonitor o = new OperationStatisticsMonitor(new OperationStatistics(10));
        
        System.out.println(o.toReportedTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.MICROSECONDS)));
        System.out.println(o.toReportedTime((double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.MICROSECONDS)));

        System.out.println(o.toReportedTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS)));
        System.out.println(o.toReportedTime((double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS)));
        
        System.out.println(o.toReportedTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)));
        System.out.println(o.toReportedTime((double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)));

        System.out.println(o.toReportedTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES)));
        System.out.println(o.toReportedTime((double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES)));
        
        System.out.println(o.toReportedTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.HOURS)));
        System.out.println(o.toReportedTime((double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.HOURS)));
        
    }
    
    
    @Override
    public String getRecentMinTime()
    {
        long[] timings = _stats.getRecentTimings();

        if(timings.length == 0)
            return "none";

        long m = Long.MAX_VALUE;
         for(long t : timings)
             m = Math.min(m, t);

         return toReportedTime(m);
    }

    @Override
    public String getRecentMaxTime()
    {
        long[] timings = _stats.getRecentTimings();

        if(timings.length == 0)
            return "none";
        
        long m = Long.MIN_VALUE;
        for(long t : timings)
            m = Math.max(m, t);

        return toReportedTime(m);
    }

    @Override
    public String getRecentAvgTime()
    {
        long total = 0;
        long[] timings = _stats.getRecentTimings();
        
        if(timings.length == 0)
            return "none";
        
        for(long t : timings)
            total += t;

        return toReportedTime( ((double) total)/timings.length );
    }

    @Override
    public String getRecentStdDev()
    {
        long total = 0;
        long[] timings = _stats.getRecentTimings();
        
        if(timings.length == 0)
            return "none";
        
        for(long t : timings)
            total += t;
        
        double avg = ((double) total)/timings.length;
        double sqVar = 0;
        
        for(long t : timings)
            sqVar += Math.pow(t - avg, 2);
        
        return toReportedTime(Math.sqrt(sqVar/timings.length));
    }

    @Override
    public String getTotalTime()
    {
        return toReportedTime(_stats.getTotalTime());
    }
    
}
