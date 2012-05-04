package com.feedly.cassandra.dao;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;

/**
 * corrects index "inline", i.e. asynchronously. A thread pool is configured to handle the updates. If the thread pool gets overwhelmed,
 * updates are simply dropped.
 * 
 * @author kireet
 */
public class OfflineRepairStrategy implements IStaleIndexValueStrategy
{
    private static final Logger _logger = LoggerFactory.getLogger(OfflineRepairStrategy.class.getName());
    private static final AtomicInteger _threadId = new AtomicInteger();
    private InlineRepairStrategy _strategy;
    private int _threadCount = 1;
    private int _queueSize = 1000;
    private ThreadPoolExecutor _executor;
    
    
    public void init()
    {
        _strategy = new InlineRepairStrategy();
        _executor = new ThreadPoolExecutor(1, _threadCount, 10, TimeUnit.SECONDS,
                                           new LinkedBlockingQueue<Runnable>(_queueSize),
                                           new ThreadFactory()
                                           {
                                               public Thread newThread(Runnable r)
                                               {
                                                   return new Thread(r, "offline-repair-strategy-" + _threadId.incrementAndGet());
                                               }
                                           },
                                           new RejectedExecutionHandler()
                                           {
                                               public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
                                               {
                                                   _logger.warn("repair queue full, dropping update");
                                               }
                                           });
    }
    
    public void destroy()
    {
        _executor.shutdown();
        try
        {
            _executor.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch(Exception ex)
        {
            _logger.warn("repair queue not emptied after 60 seconds, continuing system shutdown");
            _executor.shutdownNow();
        }
    }

    /**
     * set the thread pool size.
     * @param threadCount
     */
    public void setThreadCount(int threadCount)
    {
        _threadCount = threadCount;
    }

    /**
     * set the thread pool queue size. If the queue is full, repair operations are dropped.
     * @param queueSize
     */
    public void setQueueSize(int queueSize)
    {
        _queueSize = queueSize;
    }


    @Override
    public void handle(final EntityMetadata<?> entity, final IndexMetadata index, final Keyspace keyspace, final Collection<StaleIndexValue> values)
    {
        try
        {
            _executor.submit(
                             new Runnable()
                             {
                                 @Override
                                 public void run()
                                 {
                                     _strategy.handle(entity, index, keyspace, values);
                                 }
                             });
        }
        catch(RejectedExecutionException ree)
        {
            _logger.warn("system shutting down, dropping repair update");
        }
    }
}
