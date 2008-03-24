/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Mar 24, 2008
 */

package com.bigdata.service;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.QueueLengthTask;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class encapsulates for {@link IBigdataClient} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBigdataClient implements IBigdataClient {

    
    /**
     * The properties specified to the ctor.
     */
    protected final Properties properties;

    /**
     * The federation and <code>null</code> iff not connected.
     */
    protected IBigdataFederation fed = null;

    /**
     * Used to run application tasks.
     */
    private final ThreadPoolExecutor threadPool;

    /**
     * Used to sample and report on the queue associated with the
     * {@link #threadPool}.
     */
    protected final ScheduledExecutorService sampleService = Executors
            .newSingleThreadScheduledExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
    
    private final int defaultRangeQueryCapacity;
    private final boolean batchApiOnly;
    private final int maxStaleLocatorRetries; 
    private final int maxParallelTasksPerRequest; 
    
    /*
     * IBigdataClient API.
     */

    public ExecutorService getThreadPool() {
        
        assertConnected();
        
        return threadPool;
        
    }

    public int getDefaultRangeQueryCapacity() {
        
        return defaultRangeQueryCapacity;
        
    }
    
    public boolean getBatchApiOnly() {
        
        return batchApiOnly;
        
    }
    
    public int getMaxStaleLocatorRetries() {
        
        return maxStaleLocatorRetries;
        
    }
    
    public int getMaxParallelTasksPerRequest() {
        
        return maxParallelTasksPerRequest;
        
    }
    
    protected void assertConnected() {
        
        if (fed == null)
            throw new IllegalStateException("Not connected");
        
    }
    
    /**
     * 
     * @param properties
     *            See {@link IBigdataClient.Options}
     */
    protected AbstractBigdataClient(Properties properties) {
        
        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;
        
        // client thread pool setup.
        {
         
            final int nthreads = Integer.parseInt(properties.getProperty(
                    Options.CLIENT_THREAD_POOL_SIZE,
                    Options.DEFAULT_CLIENT_THREAD_POOL_SIZE));

            log.info(Options.CLIENT_THREAD_POOL_SIZE + "=" + nthreads);

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    nthreads, DaemonThreadFactory.defaultThreadFactory());
            
        }

        // maxStaleLocatorRetries
        {
            
            maxStaleLocatorRetries = Integer.parseInt(properties.getProperty(
                    Options.CLIENT_MAX_STALE_LOCATOR_RETRIES,
                    Options.DEFAULT_CLIENT_MAX_STALE_LOCATOR_RETRIES));

            log.info(Options.CLIENT_MAX_STALE_LOCATOR_RETRIES + "=" + maxStaleLocatorRetries);

            if (maxStaleLocatorRetries < 0) {

                throw new RuntimeException(Options.CLIENT_MAX_STALE_LOCATOR_RETRIES
                        + " must be non-negative");

            }
            
        }

        // maxParallelTasksPerRequest
        {
            
            maxParallelTasksPerRequest = Integer
                    .parseInt(properties
                            .getProperty(
                                    Options.CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST,
                                    Options.DEFAULT_CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST));

            log.info(Options.CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST + "="
                    + maxParallelTasksPerRequest);

            if (maxParallelTasksPerRequest <= 0) {

                throw new RuntimeException(
                        Options.CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST
                                + " must be positive");

            }
            
        }
        
        /*
         * Setup sampling and reporting on the client's thread pool.
         * 
         * @todo report to the load balancer.
         * 
         * @todo config. See ConcurrencyManager, which also setups up some queue
         * monitors.
         */
        {

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            QueueLengthTask queueLengthTask = new QueueLengthTask(
                    "clientThreadPool", threadPool);

            sampleService.scheduleWithFixedDelay(queueLengthTask, initialDelay,
                    delay, unit);
        }

        // defaultRangeQueryCapacity
        {
         
            defaultRangeQueryCapacity = Integer.parseInt(properties
                    .getProperty(Options.CLIENT_RANGE_QUERY_CAPACITY,
                            Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));

            log.info(Options.CLIENT_RANGE_QUERY_CAPACITY + "="
                    + defaultRangeQueryCapacity);
            
        }

        // batchApiOnly
        {
            
            batchApiOnly = Boolean.valueOf(properties.getProperty(
                    Options.CLIENT_BATCH_API_ONLY,
                    Options.DEFAULT_CLIENT_BATCH_API_ONLY));

            log.info(Options.CLIENT_BATCH_API_ONLY + "=" + batchApiOnly);
            
        }

    }
    
    /**
     * Note: concrete implementations MUST extend this method to either
     * disconnect from the remote federation or close the embedded federation
     * and then clear the {@link #fed} reference so that the client is no longer
     * "connected" to the federation.
     */
    public void shutdown() {

        final long begin = System.currentTimeMillis();
        
        log.info("begin");

        if (fed != null) {

            // allow client requests to finish normally.
            threadPool.shutdown();
            
            try {
            
                if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    
                    log.warn("Timeout awaiting thread pool termination.");
                    
                }
   
            } catch (InterruptedException e) {
                
                log.warn("Interrupted awaiting thread pool termination.", e);
                
            }
            
        }

        log.info("done: elapsed="+(System.currentTimeMillis()-begin));
        
    }

    /**
     * Note: concrete implementations MUST extend this method to either
     * disconnect from the remote federation or close the embedded federation
     * and then clear the {@link #fed} reference so that the client is no longer
     * "connected" to the federation.
     */
    public void shutdownNow() {
        
        final long begin = System.currentTimeMillis();
        
        log.info("begin");
        
        if(fed != null) {

            // stop client requests.
            threadPool.shutdownNow();
            
        }

        log.info("done: elapsed="+(System.currentTimeMillis()-begin));
        
    }
    
}
