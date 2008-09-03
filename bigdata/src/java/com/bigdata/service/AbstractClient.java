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
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.Banner;

/**
 * Abstract base class for {@link IBigdataClient} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractClient implements IBigdataClient {
    
    protected static final Logger log = Logger.getLogger(IBigdataClient.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * The properties specified to the ctor.
     */
    private final Properties properties;
    
    public Properties getProperties() {
        
        return new Properties( properties );
        
    }
    
    private final UUID clientUUID;
    
    final public UUID getClientUUID() {
        
        return clientUUID;
        
    }
    
    private final int defaultRangeQueryCapacity;
    private final boolean batchApiOnly;
    private final int threadPoolSize;
    private final int maxStaleLocatorRetries; 
    private final int maxParallelTasksPerRequest;
    private final long taskTimeout;
    private final int indexCacheCapacity;
    private final long tempStoreMaxExtent;
    
    /*
     * IBigdataClient API.
     */

    public int getThreadPoolSize() {
        
        return threadPoolSize;
        
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
    
    public long getTaskTimeout() {
        
        return taskTimeout;
        
    }
    
    public int getIndexCacheCapacity() {
        
        return indexCacheCapacity;
        
    }
    
    public long getTempStoreMaxExtent() {
        
        return tempStoreMaxExtent;
        
    }
    
    /**
     * 
     * @param properties
     *            See {@link IBigdataClient.Options}
     */
    protected AbstractClient(Properties properties) {
        
        // show the copyright banner during statup.
        Banner.banner();

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

        this.clientUUID = UUID.randomUUID();
        
        // client thread pool setup.
        {
         
            threadPoolSize = Integer.parseInt(properties.getProperty(
                    Options.CLIENT_THREAD_POOL_SIZE,
                    Options.DEFAULT_CLIENT_THREAD_POOL_SIZE));

            if (log.isInfoEnabled())
                log
                        .info(Options.CLIENT_THREAD_POOL_SIZE + "="
                                + threadPoolSize);

        }

        // maxStaleLocatorRetries
        {
            
            maxStaleLocatorRetries = Integer.parseInt(properties.getProperty(
                    Options.CLIENT_MAX_STALE_LOCATOR_RETRIES,
                    Options.DEFAULT_CLIENT_MAX_STALE_LOCATOR_RETRIES));

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_MAX_STALE_LOCATOR_RETRIES + "="
                        + maxStaleLocatorRetries);

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

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST + "="
                        + maxParallelTasksPerRequest);

            if (maxParallelTasksPerRequest <= 0) {

                throw new RuntimeException(
                        Options.CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST
                                + " must be positive");

            }
            
        }

        // task timeout
        {
         
            taskTimeout = Long.parseLong(properties.getProperty(
                    Options.CLIENT_TASK_TIMEOUT,
                    Options.DEFAULT_CLIENT_TASK_TIMEOUT));

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_TASK_TIMEOUT + "=" + taskTimeout);

        }

        // defaultRangeQueryCapacity
        {
         
            defaultRangeQueryCapacity = Integer.parseInt(properties
                    .getProperty(Options.CLIENT_RANGE_QUERY_CAPACITY,
                            Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_RANGE_QUERY_CAPACITY + "="
                        + defaultRangeQueryCapacity);
            
        }

        // batchApiOnly
        {
            
            batchApiOnly = Boolean.valueOf(properties.getProperty(
                    Options.CLIENT_BATCH_API_ONLY,
                    Options.DEFAULT_CLIENT_BATCH_API_ONLY));

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_BATCH_API_ONLY + "=" + batchApiOnly);
            
        }
        
        // indexCacheCapacity
        {

            indexCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.CLIENT_INDEX_CACHE_CAPACITY,
                    Options.DEFAULT_CLIENT_INDEX_CACHE_CAPACITY));

            if (log.isInfoEnabled())
                log.info(Options.CLIENT_INDEX_CACHE_CAPACITY + "="
                        + indexCacheCapacity);

            if (indexCacheCapacity <= 0)
                throw new RuntimeException(Options.CLIENT_INDEX_CACHE_CAPACITY
                        + " must be positive");

        }

        // tempStoreMaxExtent
        {

            tempStoreMaxExtent = Long.parseLong(properties.getProperty(
                    Options.TEMP_STORE_MAX_EXTENT,
                    Options.DEFAULT_TEMP_STORE_MAX_EXTENT));

            if (log.isInfoEnabled())
                log.info(Options.TEMP_STORE_MAX_EXTENT + "="
                        + tempStoreMaxExtent);

            if (tempStoreMaxExtent < 0)
                throw new RuntimeException(Options.TEMP_STORE_MAX_EXTENT
                        + " must be non-negative");

        }
        
    }
    
}
