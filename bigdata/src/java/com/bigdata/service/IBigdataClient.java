/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.util.Properties;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.query.QueryUtil;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.eval.ProgramTask;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;

/**
 * Interface for clients of a {@link IBigdataFederation}.
 * <p>
 * An application uses a {@link IBigdataClient} to connect to an
 * {@link IBigdataFederation}. Once connected, the application uses the
 * {@link IBigdataFederation} for operations against a given federation.
 * <p>
 * An application can read and write on multiple federations by creating an
 * {@link IBigdataClient} for each federation to which it needs to establish a
 * connection. In this manner, an application can connect to federations that
 * are deployed using different service discovery frameworks. However, precisely
 * how the client is configured to identify the federation depends on the
 * specific service discovery framework, including any protocol options or
 * security measures, with which that federation was deployed. Likewise, the
 * services within a given federation only see those services which belong to
 * that federation. Therefore federation to federation data transfers MUST go
 * through a client.
 * <p>
 * Applications normally work with scale-out indices using the methods defined
 * by {@link IBigdataFederation} to register, drop, or access indices.
 * <p>
 * An application may use an {@link ITransactionManagerService} if needs to use
 * transactions as opposed to unisolated reads and writes. When the client
 * requests a transaction, the transaction manager responds with a long integer
 * containing the transaction identifier - this is simply the unique start time
 * assigned to that transaction by the transaction manager. The client then
 * provides that transaction identifier for operations that are isolated within
 * the transaction. When the client is done with the transaction, it must use
 * the transaction manager to either abort or commit the transaction.
 * (Transactions that fail to progress may be eventually aborted.)
 * <p>
 * When using unisolated operations, the client simply specifies
 * {@link ITx#UNISOLATED} as the timestamp for its operations.
 * 
 * @see ClientIndexView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the client or service.
 */
public interface IBigdataClient<T> {

    /**
     * Connect to a bigdata federation. If the client is already connected, then
     * the existing connection is returned.
     * 
     * @return The object used to access the federation services.
     * 
     * @throws RuntimeException
     *             if the connection can not be established.
     */
    public IBigdataFederation<T> connect();

    /**
     * Return the connected federation,
     * 
     * @throws IllegalStateException
     *             if the client is not connected.
     */
    public IBigdataFederation<T> getFederation();

    /**
     * Disconnect from the bigdata federation.
     * <p>
     * Normal shutdown allows any existing client requests to federation
     * services to complete but does not schedule new requests, and then
     * terminates any background processing that is being performed on the
     * behalf of the client (service discovery, etc).
     * <p>
     * Immediate shutdown terminates any client requests to federation services,
     * and then terminate any background processing that is being performed on
     * the behalf of the client (service discovery, etc).
     * <p>
     * Note: Immediate shutdown can cause odd exceptions to be logged. Normal
     * shutdown is recommended unless there is a reason to force immediate
     * shutdown.
     * 
     * @param immediateShutdown
     *            When <code>true</code> an immediate shutdown will be
     *            performed as described above. Otherwise a normal shutdown will
     *            be performed.
     */
    public void disconnect(boolean immediateShutdown);

    /**
     * Return <code>true</code> iff the client is connected to a federation.
     */
    public boolean isConnected();

    /**
     * The configured #of threads in the client's thread pool.
     * 
     * @see Options#CLIENT_THREAD_POOL_SIZE
     */
    public int getThreadPoolSize();

    /**
     * The default capacity when a client issues a range query request.
     * 
     * @see Options#CLIENT_RANGE_QUERY_CAPACITY
     */
    public int getDefaultRangeQueryCapacity();

    /**
     * When <code>true</code> requests for non-batch API operations will throw
     * exceptions.
     * 
     * @see Options#CLIENT_BATCH_API_ONLY
     */
    public boolean getBatchApiOnly();

    /**
     * The maximum #of retries when an operation results in a
     * {@link StaleLocatorException}.
     * 
     * @see Options#CLIENT_MAX_STALE_LOCATOR_RETRIES
     */
    public int getMaxStaleLocatorRetries();

    /**
     * The maximum #of tasks that may be submitted in parallel for a single user
     * request.
     * 
     * @see Options#CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST
     */
    public int getMaxParallelTasksPerRequest();

    /**
     * The timeout in milliseconds for a task submitted to an
     * {@link IDataService}.
     * 
     * @see Options#CLIENT_TASK_TIMEOUT
     */
    public long getTaskTimeout();

    /**
     * The capacity of the client's {@link IIndex} proxy cache.
     * 
     * @see Options#CLIENT_INDEX_CACHE_CAPACITY
     */
    public int getIndexCacheCapacity();

    /**
     * The timeout in milliseconds for stale entries in the client's
     * {@link IIndex} proxy cache.
     * 
     * @see Options#CLIENT_INDEX_CACHE_TIMEOUT
     */
    public long getIndexCacheTimeout();

    /**
     * <code>true</code> iff performance counters will be collected for the
     * platform on which the client is running.
     * 
     * @see Options#COLLECT_PLATFORM_STATISTICS
     */
    public boolean getCollectPlatformStatistics();

    /**
     * <code>true</code> iff statistics will be collected for work queues.
     * 
     * @see Options#COLLECT_QUEUE_STATISTICS
     */
    public boolean getCollectQueueStatistics();

    /**
     * The port on which the optional httpd service will be run. The httpd
     * service exposes statistics about the client, its work queues, the
     * platform on which it is running, etc.
     * 
     * @see Options#HTTPD_PORT
     */
    public int getHttpdPort();
    
    /**
     * An object wrapping the properties used to configure the client.
     */
    public Properties getProperties();
    
    /**
     * Configuration options for {@link IBigdataClient}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * The #of threads in the client thread pool -or- ZERO (0) if the size
         * of the thread pool is not fixed (default is <code>0</code>). The
         * thread pool is used to parallelize requests issued by the client.
         * <p>
         * Note: It is possible for the client to deadlock if the size of the
         * thread pool is limited. At least some sources of deadlock have been
         * eliminated (retries after a {@link StaleLocatorException} are now run
         * in the caller's thread) but as of <code>5/14/08</code> it is clear
         * that there is at least one source of deadlock remaining so the
         * default value of <code>0</code> is advised.
         */
        String CLIENT_THREAD_POOL_SIZE = IBigdataClient.class.getName()
                + ".threadPoolSize";

        String DEFAULT_CLIENT_THREAD_POOL_SIZE = "0";

        /**
         * The maximum #of times that a client will retry an operation which
         * resulted in a {@link StaleLocatorException} (default
         * {@value #DEFAULT_CLIENT_MAX_STALE_LOCATOR_RETRIES}).
         * <p>
         * Note: The {@link StaleLocatorException} is thrown when a split, join,
         * or move results in one or more new index partitions which replace the
         * index partition addressed by the client.
         * <p>
         * This value needs to be relatively large if when we are aggressively
         * driving journal overflows and index partitions splits during the
         * "young" phase of a data service or scale-out index since a LOT of
         * index partition splits and moves will result.
         * <p>
         * For mature data services and scale-out indices a retry will normally
         * succeed.
         */
        String CLIENT_MAX_STALE_LOCATOR_RETRIES = IBigdataClient.class
                .getName()
                + ".maxStaleLocatorRetries";

        String DEFAULT_CLIENT_MAX_STALE_LOCATOR_RETRIES = "100";

        /**
         * The maximum #of tasks that will be created and submitted in parallel
         * for a single application request (default
         * {@value #DEFAULT_CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST}). Multiple
         * tasks are created for an application request whenever that request
         * spans more than a single index partition. This limit prevents
         * operations which span a very large #of index partitions from creating
         * and submitting all of their tasks at once and thereby effectively
         * blocking other client operations until the tasks have completed.
         * Instead, this application request generates at most this many tasks
         * at a time and new tasks will not be created for that request until
         * the previous set of tasks for the request have completed.
         * 
         * @todo use for {@link ProgramTask} for parallel rule evaluation?
         */
        String CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST = IBigdataClient.class
                .getName()
                + ".maxParallelTasksPerRequest";

        String DEFAULT_CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST = "100";

        /**
         * The timeout in milliseconds for a task submitting to an
         * {@link IDataService} (default {@value #DEFAULT_CLIENT_TASK_TIMEOUT}).
         * <p>
         * Note: Use {@value Long#MAX_VALUE} for NO timeout (the maximum value
         * for a {@link Long}).
         */
        String CLIENT_TASK_TIMEOUT = IBigdataClient.class.getName()
                + "taskTimeout";
        
        /**
         * The default timeout in milliseconds.
         * 
         * @see #CLIENT_TASK_TIMEOUT
         */
        String DEFAULT_CLIENT_TASK_TIMEOUT = ""+Long.MAX_VALUE;
//        String DEFAULT_CLIENT_TASK_TIMEOUT = ""+20*1000L;
        
        /**
         * The default capacity used when a client issues a range query request
         * (default {@value #DEFAULT_CLIENT_RANGE_QUERY_CAPACITY}).
         * 
         * @todo use on {@link IAccessPath}s for the chunk size?
         * 
         * @see IBigdataClient#getDefaultRangeQueryCapacity()
         */
        String CLIENT_RANGE_QUERY_CAPACITY = IBigdataClient.class.getName()
                + ".rangeIteratorCapacity";

        String DEFAULT_CLIENT_RANGE_QUERY_CAPACITY = "10000";

        /**
         * A boolean property which controls whether or not the non-batch API
         * will log errors complete with stack traces (default
         * {@value #DEFAULT_CLIENT_BATCH_API_ONLY}). This may be used to
         * locating code that needs to be re-written to use
         * {@link IIndexProcedure}s in order to obtain high performance.
         */
        String CLIENT_BATCH_API_ONLY = IBigdataClient.class.getName()
                + ".batchOnly";

        String DEFAULT_CLIENT_BATCH_API_ONLY = "false";

        /**
         * The capacity of the {@link HardReferenceQueue} backing the
         * {@link IResourceLocator} maintained by the {@link IBigdataClient}.
         * The capacity of this cache indirectly controls how many
         * {@link ILocatableResource}s the {@link IBigdataClient} will hold
         * open.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an {@link ILocatableResource} becomes weakly reachable,
         * the JVM will eventually GC the object. Since objects which are
         * strongly reachable are never cleared, this provides our guarantee
         * that resources are never closed if they are in use.
         * 
         * @see #DEFAULT_LOCATOR_CACHE_CAPACITY
         */
        String CLIENT_LOCATOR_CACHE_CAPACITY = IBigdataClient.class.getName()
                + ".locatorCacheCapacity";

        String DEFAULT_CLIENT_LOCATOR_CACHE_CAPACITY = "20";
        
        /**
         * The timeout in milliseconds for stale entries in the
         * {@link IResourceLocator} cache -or- ZERO (0) to disable the timeout
         * (default {@value #DEFAULT_LOCATOR_CACHE_TIMEOUT}). When this timeout
         * expires, the reference for the entry in the backing
         * {@link HardReferenceQueue} will be cleared. Note that the entry will
         * remain in the {@link IResourceLocator} cache regardless as long as it
         * is strongly reachable.
         */
        String CLIENT_LOCATOR_CACHE_TIMEOUT = IBigdataClient.class.getName()
                + ".locatorCacheTimeout";

        String DEFAULT_CLIENT_LOCATOR_CACHE_TIMEOUT = "" + (60 * 1000);

        /**
         * The capacity of the LRU cache of {@link IIndex} proxies held by the
         * client (default {@value #DEFAULT_CLIENT_INDEX_CACHE_CAPACITY}). The
         * capacity of this cache indirectly controls how long an {@link IIndex}
         * proxy will be cached. The main reason for keeping an {@link IIndex}
         * in the cache is to reuse its buffers if another request arrives
         * "soon" for that {@link IIndex}.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an {@link IIndex} proxy becomes weakly reachable, the
         * JVM will eventually GC the {@link IIndex}, thereby releasing all
         * resources associated with it.
         * 
         * @see #DEFAULT_CLIENT_INDEX_CACHE_CAPACITY
         */
        String CLIENT_INDEX_CACHE_CAPACITY = IBigdataClient.class.getName()
                + ".indexCacheCapacity";

        /**
         * The default for the {@link #CLIENT_INDEX_CACHE_CAPACITY} option.
         */
        String DEFAULT_CLIENT_INDEX_CACHE_CAPACITY = "20";

        /**
         * The time in milliseconds before an entry in the clients index cache
         * will be cleared from the backing {@link HardReferenceQueue} (default
         * {@value #DEFAULT_INDEX_CACHE_TIMEOUT}). This property controls how
         * long the client's index cache will retain an {@link IIndex} which has
         * not been recently used. This is in contrast to the cache capacity.
         */
        String CLIENT_INDEX_CACHE_TIMEOUT = IBigdataClient.class.getName()
                + ".indexCacheTimeout";

        String DEFAULT_CLIENT_INDEX_CACHE_TIMEOUT = ""+(60*1000); // One minute.

        // Now handled by TemporaryStoreFactory.Options.
//        /**
//         * The maximum extent for a {@link TemporaryStore} before a new
//         * {@link TemporaryStore} will be created by
//         * {@link IIndexStore#getTempStore()} for an {@link IBigdataClient}
//         * (default {@value #DEFAULT_TEMP_STORE_MAXIMUM_EXTENT}).
//         */
//        String TEMP_STORE_MAXIMUM_EXTENT = IBigdataClient.class.getName()
//                + ".tempStore.maximumExtent";
//
//        String DEFAULT_TEMP_STORE_MAXIMUM_EXTENT = "" + (5 * Bytes.gigabyte);
        
        /**
         * Boolean option for the collection of statistics from the underlying
         * operating system (default
         * {@value #DEFAULT_COLLECT_PLATFORM_STATISTICS}).
         * 
         * @see AbstractStatisticsCollector#newInstance(Properties)
         */
        String COLLECT_PLATFORM_STATISTICS = IBigdataClient.class.getName()
                + ".collectPlatformStatistics";

        String DEFAULT_COLLECT_PLATFORM_STATISTICS = "true"; 

        /**
         * Boolean option for the collection of statistics from the various
         * queues using to run tasks (default
         * {@link #DEFAULT_COLLECT_QUEUE_STATISTICS}).
         * 
         * @see ThreadPoolExecutorStatisticsTask
         */
        String COLLECT_QUEUE_STATISTICS = IBigdataClient.class.getName()
                + ".collectQueueStatistics";

        String DEFAULT_COLLECT_QUEUE_STATISTICS = "true";

		/**
		 * The delay between reports of performance counters to the
		 * {@link ILoadBalancerService} in milliseconds (
		 * {@value #DEFAULT_REPORT_DELAY}). When ZERO (0L), performance counter
		 * reporting will be disabled.
		 * 
		 * @see #DEFAULT_REPORT_DELAY
		 */
        String REPORT_DELAY = IBigdataClient.class.getName() + ".reportDelay";

        /**
         * The default {@link #REPORT_DELAY}.
         */
        String DEFAULT_REPORT_DELAY = ""+(60*1000);

		/**
		 * When <code>true</code>, all collected performance counters are
		 * reported (default {@value #DEFAULT_REPORT_ALL)}. Otherwise only the
		 * {@link QueryUtil#getRequiredPerformanceCountersFilter()} will be
		 * reported. Reporting all performance counters is useful when
		 * diagnosing the services in a cluster. However, only a small number of
		 * performance counters are actually necessary for the functioning of
		 * the {@link ILoadBalancerService}.
		 */
		String REPORT_ALL = IBigdataClient.class.getName() + ".reportAll";

		String DEFAULT_REPORT_ALL = "false";
    
        /**
         * Integer option specifies the port on which an httpd service will be
         * started that exposes the {@link CounterSet} for the client (default
         * {@value #DEFAULT_HTTPD_PORT}). When ZERO (0), a random port will be
         * used. The httpd service may be disabled by specifying <code>-1</code>
         * as the port.
         * <p>
         * Note: The httpd service for the {@link LoadBalancerService} is
         * normally run on a known port in order to make it easy to locate that
         * service, e.g., port 80, 8000 or 8080, etc. This MUST be overridden for
         * the {@link LoadBalancerService} it its configuration since
         * {@link #DEFAULT_HTTPD_PORT} will otherwise cause a random port to be
         * assigned.
         */
        String HTTPD_PORT = IBigdataClient.class.getName() + ".httpdPort";

        /**
         * The default http service port is ZERO (0), which means that a random
         * port will be chosen.
         */
        String DEFAULT_HTTPD_PORT = "0";
        
    };

}
