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
 * Created on Jul 10, 2008
 */

package com.bigdata.relation;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.rules.FastClosure;
import com.bigdata.rdf.rules.FullClosure;
import com.bigdata.rdf.rules.RuleFastClosure5;
import com.bigdata.rdf.rules.RuleFastClosure6;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.ProgramTask;
import com.bigdata.service.IBigdataFederation;

/**
 * Base class for locatable resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <E>
 */
abstract public class AbstractResource<E> implements IMutableResource<E> {

    protected final transient static Logger log = Logger.getLogger(AbstractResource.class);

    final private IIndexManager indexManager;
    
    final private String namespace;

    final private String containerNamespace;

    final private long timestamp;
    
    final private Properties properties;
    
    final private int chunkOfChunksCapacity;
    final private int chunkCapacity;
    final private long chunkTimeout;
    private final int fullyBufferedReadThreshold;
    private final boolean forceSerialExecution;
    private final int maxParallelSubqueries;
//    private final boolean nestedSubquery;

    /**
     * The capacity of the buffers accumulating chunks from concurrent producers.
     * 
     * @see Options#CHUNK_OF_CHUNKS_CAPACITY
     * 
     * @deprecated by {@link BOp} annotations.
     */
    final public int getChunkOfChunksCapacity() {
        
        return chunkOfChunksCapacity;
        
    }

    /**
     * The target chunk size.
     * 
     * @see Options#CHUNK_CAPACITY
     * 
     * @deprecated by {@link BOp} annotations.
     */
    final public int getChunkCapacity() {
        
        return chunkCapacity;
        
    }
    
    /**
     * The timeout in milliseconds that the {@link BlockingBuffer} will wait for
     * another chunk to combine with the current chunk before returning the
     * current chunk. This may be ZERO (0) to disable the chunk combiner.
     * 
     * @see Options#CHUNK_TIMEOUT
     * 
     * @deprecated by {@link BOp} annotations.
     */
    public final long getChunkTimeout() {

        return chunkTimeout;

    }

    /**
     * If the estimated range count for an
     * {@link IAccessPath#iterator(int, int)} is LTE this threshold then do a
     * fully buffered (synchronous) read. Otherwise we will do an asynchronous
     * read.
     * 
     * @see Options#FULLY_BUFFERED_READ_THRESHOLD
     * 
     * @deprecated by {@link BOp} annotations.
     */
    public int getFullyBufferedReadThreshold() {

        return fullyBufferedReadThreshold;
        
    }
    
    /**
     * When <code>true</code>, rule sets will be forced to execute
     * sequentially even when they are not flagged as a sequential program.
     * 
     * @see Options#FORCE_SERIAL_EXECUTION
     * 
     * @deprecated by {@link BOp} annotations.
     */
    public boolean isForceSerialExecution() {

        return forceSerialExecution;
        
    }

    /**
     * The maximum #of subqueries for the first join dimension that will be
     * issued in parallel. Use ZERO(0) to avoid submitting tasks to the
     * {@link ExecutorService} entirely and ONE (1) to submit a single task at a
     * time to the {@link ExecutorService}.
     * 
     * @see Options#MAX_PARALLEL_SUBQUERIES
     * 
     * @deprecated by {@link BOp} annotations.
     */
    public int getMaxParallelSubqueries() {

        return maxParallelSubqueries;
        
    }

//    /**
//     * When <code>true</code> the {@link NestedSubqueryWithJoinThreadsTask} is
//     * applied. Otherwise the {@link JoinMasterTask} is applied.
//     * 
//     * @see Options#NESTED_SUBQUERY
//     * 
//     * @deprecated by {@link BOp} annotations and the pipeline join, which
//     *             always does better than the older nested subquery evaluation
//     *             logic.
//     */
//    public boolean isNestedSubquery() {
//
////        return false;
//        return nestedSubquery;
//        
//    }
    
    /**
     * Options for locatable resources.
     * 
     * @todo most of these options effect asynchronous iterators, access path
     *       behavior, and join behavior. these are general features for bigdata
     *       resources, but some of the code to support this stuff is still
     *       local to the RDF module. That can be fixed using an abstract base
     *       class for {@link IJoinNexus} and {@link IJoinNexusFactory}.
     * 
     * @todo some of these defaults need to be re-examined. see notes in the
     *       javadoc below.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
        
        /**
         * <p>
         * Set the maximum #of chunks from concurrent producers that can be
         * buffered before an {@link IBuffer} containing chunks of
         * {@link ISolution}s would block (default
         * {@link #DEFAULT_CHUNK_OF_CHUNKS_CAPACITY}). This is used to
         * provision a {@link BlockingQueue} for {@link BlockingBuffer}. A
         * value of ZERO(0) indicates that a {@link SynchronousQueue} should be
         * used instead. The best value may be more than the #of concurrent
         * producers if the producers are generating small chunks, e.g., because
         * there are few solutions for a join subquery.
         * </p>
         * @deprecated by {@link BOp} annotations.
         */
        String CHUNK_OF_CHUNKS_CAPACITY = BlockingBuffer.class.getName()
                + ".chunkOfChunksCapacity";

        /**
         * Default for {@link #CHUNK_OF_CHUNKS_CAPACITY}
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_CHUNK_OF_CHUNKS_CAPACITY = "10"; // was 1000

        /**
         * <p>
         * Sets the capacity of the {@link IBuffer}s used to accumulate a chunk
         * when evaluating rules, etc (default {@value #CHUNK_CAPACITY}). Note
         * that many processes use a {@link BlockingBuffer} to accumulate
         * "chunks of chunks".
         * </p>
         * 
         * @see #CHUNK_OF_CHUNKS_CAPACITY
         * @deprecated by {@link BOp} annotations.
         */
        String CHUNK_CAPACITY = IBuffer.class.getName() + ".chunkCapacity";

        /**
         * Default for {@link #CHUNK_CAPACITY}
         * <p>
         * Note: This used to be 20k, but chunks of chunks works better than
         * just a large chunk.
         * 
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_CHUNK_CAPACITY = "1000"; // was 100

        /**
         * The timeout in milliseconds that the {@link BlockingBuffer} will wait
         * for another chunk to combine with the current chunk before returning
         * the current chunk (default {@link #DEFAULT_CHUNK_TIMEOUT}). This may
         * be ZERO (0) to disable the chunk combiner.
         * 
         * @deprecated by {@link BOp} annotations.
         */
        String CHUNK_TIMEOUT = BlockingBuffer.class.getName() + ".chunkTimeout";

        /**
         * The default for {@link #CHUNK_TIMEOUT}.
         * 
         * @todo this is probably much larger than we want. Try 10ms.
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_CHUNK_TIMEOUT = "10"; // was 1000

        /**
         * If the estimated rangeCount for an
         * {@link AccessPath#iterator()} is LTE this threshold then use
         * a fully buffered (synchronous) iterator. Otherwise use an
         * asynchronous iterator whose capacity is governed by
         * {@link #CHUNK_OF_CHUNKS_CAPACITY}.
         * 
         * @deprecated by {@link BOp} annotations.
         */
        String FULLY_BUFFERED_READ_THRESHOLD = AccessPath.class
                .getName()
                + ".fullyBufferedReadThreadshold";

        /**
         * Default for {@link #FULLY_BUFFERED_READ_THRESHOLD}
         * 
         * @todo figure out how good this value is.
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_FULLY_BUFFERED_READ_THRESHOLD = ""+20*Bytes.kilobyte32;

        /**
         * When <code>true</code> ({@value #DEFAULT_FORCE_SERIAL_EXECUTION}),
         * rule sets will be forced to execute sequentially even when they are
         * not flagged as a sequential program.
         * 
         * @todo The following discussion applies to the
         *       {@link AbstractTripleStore}. and should be relocated.
         *       <P>
         *       The {@link #CLOSURE_CLASS} option defaults to
         *       {@link FastClosure}, which has very little possible parallelism
         *       (it is mostly a sequential program by nature). For that reason,
         *       {@link #FORCE_SERIAL_EXECUTION} defaults to <code>false</code>
         *       since the overhead of parallel execution is more likely to
         *       lower the observed performance with such limited possible
         *       parallelism. However, when using {@link FullClosure} the
         *       benefits of parallelism MAY justify its overhead.
         *       <p>
         *       The following data are for LUBM datasets.
         * 
         *       <pre>
         * U1  Fast Serial   : closure =  2250ms; 2765, 2499, 2530
         * U1  Fast Parallel : closure =  2579ms; 2514, 2594
         * U1  Full Serial   : closure = 10437ms.
         * U1  Full Parallel : closure = 10843ms.
         * 
         * U10 Fast Serial   : closure = 41203ms, 39171ms (38594, 35360 when running in caller's thread rather than on the executorService).
         * U10 Fast Parallel : closure = 30722ms. 
         * U10 Full Serial   : closure = 108110ms.
         * U10 Full Parallel : closure = 248550ms.
         * </pre>
         * 
         *       Note that the only rules in the fast closure program that have
         *       potential parallelism are {@link RuleFastClosure5} and
         *       {@link RuleFastClosure6} and these rules are not being
         *       triggered by these datasets, so there is in fact NO potential
         *       parallelism (in the data) for these datasets.
         *       <p>
         *       It is possible that a machine with more cores would perform
         *       better under the "full" closure program with parallel rule
         *       execution (these data were collected on a laptop with 2 cores)
         *       since performance tends to be CPU bound for small data sets.
         *       However, the benefit of the "fast" closure program is so large
         *       that there is little reason to consider parallel rule execution
         *       for the "full" closure program.
         * 
         * @todo collect new timings for this option. The LUBM performance has
         *       basically doubled since these data were collected. Look further
         *       into ways in which overhead might be reduced for rule
         *       parallelism and also for when rule parallelism is not enabled.
         * 
         * @todo rename as parallel_rule_execution.
         * @deprecated by {@link BOp} annotations.
         */
        String FORCE_SERIAL_EXECUTION = ProgramTask.class.getName()
                + ".forceSerialExecution";

        /**
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_FORCE_SERIAL_EXECUTION = "true";

        /**
         * The maximum #of subqueries for the first join dimension that will be
         * issued in parallel. Use ZERO(0) to avoid submitting tasks to the
         * {@link ExecutorService} entirely and ONE (1) to submit a single task
         * at a time to the {@link ExecutorService}.
         * 
         * @deprecated by {@link BOp} annotations.
         */
        String MAX_PARALLEL_SUBQUERIES = ProgramTask.class.getName()
                + ".maxParallelSubqueries";

        /**
         * @deprecated by {@link BOp} annotations.
         */
        String DEFAULT_MAX_PARALLEL_SUBQUERIES = "5";

//        /**
//         * Boolean option controls the JOIN evaluation strategy. When
//         * <code>true</code>, {@link NestedSubqueryWithJoinThreadsTask} is used
//         * to compute joins. When <code>false</code>, {@link JoinMasterTask} is
//         * used instead (aka pipeline joins).
//         * <p>
//         * Note: The default depends on the deployment mode. Nested subquery
//         * joins are somewhat faster for local data (temporary stores, journals,
//         * and a federation that does not support scale-out). However, pipeline
//         * joins are MUCH faster for scale-out so they are used by default
//         * whenever {@link IBigdataFederation#isScaleOut()} reports
//         * <code>true</code>.
//         * <p>
//         * Note: Cold query performance for complex high volume queries appears
//         * to be better for the pipeline join, so it may make sense to use the
//         * pipeline join even for local data.
//         * 
//         * @deprecated The {@link NestedSubqueryWithJoinThreadsTask} is much
//         *             slower than the pipeline join algorithm, even for a
//         *             single machine.
//         */
//        String NESTED_SUBQUERY = DefaultRuleTaskFactory.class.getName()
//                + ".nestedSubquery";
        
//        /** 
//         * @todo option to specify the class that will serve as the
//         *       {@link IRuleTaskFactory} - basically, this is how you choose
//         *       the join strategy. however, {@link DefaultRuleTaskFactory}
//         *       needs to be refactored in order to make this choice by
//         *       {@link Class} rather than by the object's state.  Also note
//         *       that the pipeline join may be better off with maxParallel=0.
//         */
//        String RULE_TASK_FACTORY_CLASS = "ruleTaskFactoryClass";
//
//        String DEFAULT_RULE_TASK_FACTORY_CLASS = DefaultRuleTaskFactory.class.getName();
        
    }
    
    /**
     * 
     */
    protected AbstractResource(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        if (timestamp == null)
            throw new IllegalArgumentException();

        if (properties == null)
            throw new IllegalArgumentException();

        // Note: Bound before we lookup property values!
        this.indexManager = indexManager;

        // Note: Bound before we lookup property values!
        this.namespace = namespace;

        {
            String val = properties.getProperty(RelationSchema.CONTAINER);

            this.containerNamespace = val;

        }

        this.timestamp = timestamp;

        this.properties = properties;

        properties.setProperty(RelationSchema.NAMESPACE, namespace);

        properties.setProperty(RelationSchema.CLASS, getClass().getName());

        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace + ", timestamp=" + timestamp
                    + ", container=" + containerNamespace + ", indexManager="
                    + indexManager);

        }

        forceSerialExecution = Boolean.parseBoolean(getProperty(
                Options.FORCE_SERIAL_EXECUTION,
                Options.DEFAULT_FORCE_SERIAL_EXECUTION));

        maxParallelSubqueries = getProperty(Options.MAX_PARALLEL_SUBQUERIES,
                Options.DEFAULT_MAX_PARALLEL_SUBQUERIES,
                IntegerValidator.GTE_ZERO);

        /*
         * Note: The pipeline join is flat out better all around.
         */
//        final boolean pipelineIsBetter = (indexManager instanceof IBigdataFederation && ((IBigdataFederation) indexManager)
//                .isScaleOut());
//        
//        nestedSubquery = Boolean.parseBoolean(getProperty(
//                Options.NESTED_SUBQUERY, "false"));
//        Boolean
//                        .toString(!pipelineIsBetter)));

        chunkOfChunksCapacity = getProperty(Options.CHUNK_OF_CHUNKS_CAPACITY,
                Options.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY,
                IntegerValidator.GT_ZERO);

        chunkCapacity = getProperty(Options.CHUNK_CAPACITY,
                Options.DEFAULT_CHUNK_CAPACITY, IntegerValidator.GT_ZERO);

        chunkTimeout = getProperty(Options.CHUNK_TIMEOUT,
                Options.DEFAULT_CHUNK_TIMEOUT, LongValidator.GTE_ZERO);

        fullyBufferedReadThreshold = getProperty(
                Options.FULLY_BUFFERED_READ_THRESHOLD,
                Options.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD,
                IntegerValidator.GT_ZERO);

    }
    
    public final String getNamespace() {
        
        return namespace;
        
    }

    public final String getContainerNamespace() {
        
        return containerNamespace;
        
    }

    /**
     * Return the container.
     * 
     * @return The container -or- <code>null</code> if there is no container.
     */
    public ILocatableResource getContainer() {

        if (container == null) {

            synchronized (this) {

                if (container == null) {

                    if (getContainerNamespace() != null) {

                        if (log.isInfoEnabled()) {

                            log.info("resolving container: "
                                    + getContainerNamespace());

                        }

                        container = getIndexManager()
                                .getResourceLocator()
                                .locate(getContainerNamespace(), getTimestamp());

                    }

                }

            }
            
        }

        return container;
        
    }
    private volatile ILocatableResource container;
    
    public final long getTimestamp() {
        
        return timestamp;
        
    }

    /**
     * Return an object wrapping the properties specified to the ctor.
     */
    public final Properties getProperties() {
        
        return new Properties(properties);
        
    }

    /**
     * Return the object used to locate indices, relations, and relation
     * containers and to execute operations on those resources.
     * <p>
     * Note: For scale-out, this is <em>always</em> the federation's index
     * manager NOT the data service local index manager. This is an artifact of
     * how we resolve the metadata for the relation against the global row
     * store.
     * 
     * @return The {@link IIndexManager}.
     * 
     * @todo If we support the notion of a "relation shard" then this could
     *       become the shard's data service local index manager in that
     *       instance but, regardless, we would need a means to resolve the
     *       metadata for the relation against the federation's index manager
     */
    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }
    
    final public ExecutorService getExecutorService() {
        
        return indexManager.getExecutorService();
        
    }
    
    /**
     * The class name, timestamp and namespace for the relation view.
     */
    public String toString() {
        
        return getClass().getSimpleName() + "{timestamp=" + timestamp
                + ", namespace=" + namespace + ", container=" + containerNamespace
                + ", indexManager=" + indexManager + "}";
    
    }

    /**
     * The default implementation only logs the event.
     */
    public AbstractResource<E> init() {

    	if (log.isInfoEnabled())
			log.info(toString());
    	
    	return this;
    	
	}

    /**
     * 
     * @todo Lock service supporting shared locks, leases and lease renewal,
     *       escalation of shared locks to exclusive locks, deadlock detection,
     *       and possibly a resource hierarchy. Leases should be Callable
     *       objects that are submitted by the client to its executor service so
     *       that they will renew automatically until cancelled (and will cancel
     *       automatically if not renewed).
     *       <P>
     *       There is existing code that could be adapted for this purpose. It
     *       might have to be adapted to support lock escalation (shared to
     *       exclusive), a resource hierarchy, and a delay queue to cancel
     *       leases that are not renewed. It would have to be wrapped up as a
     *       low-latency service and made available via the
     *       {@link IBigdataFederation}. It also needs to use a weak reference
     *       cache for the collection of resource queues so that they are GC'd
     *       rather than growing as new resources are locked and never
     *       shrinking.
     *       <p>
     *       If we require pre-declaration of locks, then we do not need the
     *       dependency graph since deadlocks can only arise with 2PL.
     *       <p>
     *       Since the service is remote it should use {@link UUID}s to
     *       identify the lock owner(s).
     *       <p>
     *       The lock service would be used to bracket operations such as
     *       relation {@link #create()} and {@link #destroy()} and would be used
     *       to prevent those operations while a lease is held by concurrent
     *       processes with a shared lock.
     *       <p>
     *       Add ctor flag to create iff not found?
     *       <p>
     *       There needs to be a lock protocol for subclasses so that they can
     *       ensure that they are the only task running create (across the
     *       federation) and so that they can release the lock when they are
     *       done. The lock can be per the notes above, but the protocol with
     *       the subclass will require some coordinating methods.
     *       <p>
     *       Full transactions are another way to solve this problem.
     */
    public void create() {
        
        if (log.isInfoEnabled())
            log.info(toString());
    
        /*
         * Convert the Properties to a Map.
         */
        final Map<String, Object> map = new HashMap<String, Object>();
        {

            final Enumeration<? extends Object> e = properties.propertyNames();

            while (e.hasMoreElements()) {

                final Object key = e.nextElement();

//                if (!(key instanceof String)) {
//
//                    log.warn("Will not store non-String key: " + key);
//
//                    continue;
//
//                }

                final String name = (String) key;

                map.put(name, properties.getProperty(name));

            }

        }
    
        // Write the map on the row store.
        final Map afterMap = indexManager.getGlobalRowStore().write(
                RelationSchema.INSTANCE, map);
        
        if(log.isDebugEnabled()) {
            
            log.debug("Properties after write: "+afterMap);
            
        }
        
        /*
         * Add this instance to the locator cache.
         * 
         * Note: Normally, the instances are created by the locator cache
         * itself. In general the only the the application creates an instance
         * directly is when it is going to attempt to create the relation. This
         * takes advantage of that pattern to notify the locator that it should
         * cache this instance.
         */
        
        ((DefaultResourceLocator) getIndexManager().getResourceLocator())
                .putInstance(this);

    }

    public void destroy() {

        if (log.isInfoEnabled())
            log.info(toString());

        // Delete the entry for this relation from the row store.
        indexManager.getGlobalRowStore().delete(RelationSchema.INSTANCE,
                namespace);

    }

    /**
     * Acquires an exclusive lock for the {@link #getNamespace()}.
     * 
     * @return the lock.
     * 
     * @throws RuntimeException
     *             if anything goes wrong.
     * 
     * @see IResourceLockService
     */
    protected IResourceLock acquireExclusiveLock() {

        return indexManager.getResourceLockService().acquireLock(getNamespace());

    }

    /**
     * Release the lock.
     * 
     * @param resourceLock
     *            The lock.
     */
    protected void unlock(final IResourceLock resourceLock) {
        
        resourceLock.unlock();
        
    }
    
    /**
     * Resolve the property value using the {@link IIndexManager}, the
     * namespace of the resource, and the {@link Properties} instance to be
     * tested as hidden parameters.
     * 
     * @param globalName
     *            The global property name.
     * @param defaultValue
     *            The default.
     * 
     * @return The resolved property value.
     * 
     * @see Configuration
     */
    protected String getProperty(final String localName,
            final String defaultValue) {

        return Configuration.getProperty(indexManager, properties, namespace,
                localName, defaultValue);

    }

    /**
     * Resolves, parses, and validates the property value.
     * 
     * @param name
     *            The property name.
     * @param defaultValue
     *            The default value.
     * @return
     */
    protected <T> T getProperty(final String name, final String defaultValue,
            final IValidator<T> validator) {

        return Configuration.getProperty(indexManager, properties, namespace,
                name, defaultValue, validator);

    }

}
