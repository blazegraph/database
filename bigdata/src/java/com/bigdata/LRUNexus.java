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
 * Created on Sep 8, 2009
 */

package com.bigdata;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.cache.BCHMGlobalLRU;
import com.bigdata.cache.BCHMGlobalLRU2;
import com.bigdata.cache.HardReferenceGlobalLRU;
import com.bigdata.cache.HardReferenceGlobalLRURecycler;
import com.bigdata.cache.HardReferenceGlobalLRURecyclerExplicitDeleteRequired;
import com.bigdata.cache.IGlobalLRU;
import com.bigdata.cache.WeakReferenceGlobalLRU;
import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Static singleton factory used to configure the record level cache behavior
 * for bigdata within the current JVM. The configuration is specified using
 * system properties defined by {@link Options}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME LRUNexus : writes MUST must be "isolated" until the commit.
 *          Isolated indices MUST have their own cache backed by the shared LRU
 *          (actually, they are on the shared temporary store so that helps).
 *          Unisolated indices SHOULD have their own cache backed by the shared
 *          LRU. At commit, any records in the "isolated" cache for a B+Tree
 *          should be putAll() onto the unisolated cache for the backing store.
 *          This way, we do not need to do anything if there is an abort().
 *          <p>
 *          There are two quick fixes: (1) Disable the Global LRU; and (2)
 *          discard the cache if there is an abort on a store. The latter is
 *          pretty easy since we only have one store with abort semantics, which
 *          is the {@link AbstractJournal}, so that is how this is being handled
 *          right now by {@link AbstractJournal#abort()}.
 *          <p>
 *          An optimization would essentially isolate the writes on the cache
 *          per BTree or between commits. At the commit point, the written
 *          records would be migrated into the "committed" cache for the store.
 *          The caller would read on the uncommitted cache, which would read
 *          through to the "committed" cache. This would prevent incorrect reads
 *          without requiring us to throw away valid records in the cache. This
 *          could be a significant performance gain if aborts are common on a
 *          machine with a lot of RAM.
 * 
 * @todo Test w/ G1 <code>-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC</code>
 *       <p>
 *       G1 appears faster for query, but somewhat slower for load. This is
 *       probably related to the increased memory demand during load (more of
 *       the data winds up buffered). G1 might work for both use cases with a
 *       smaller portion of the heap given over to buffers.
 *       <p>
 *       G1 can also trip a crash, at least during load. There is a Sun incident
 *       ID# 1609804 for this.
 * 
 * @todo Look into the memory pool threshold notification mechanism. See
 *       {@link ManagementFactory#getMemoryPoolMXBeans()} and
 *       {@link MemoryPoolMXBean}. TonyP suggests that tracking the old
 *       generation occupancy may be a better metric (more stable). The tricky
 *       part is to identify which pool(s?) correspond(s) to the old generation.
 *       Once that is done, the idea is to set a notification threshold using
 *       {@link MemoryPoolMXBean#setUsageThreshold(long)} and to only clear
 *       references from the tail of the global LRU when we have exceeded that
 *       threshold. Reading the javadoc, it seems that threshold notification
 *       would probably come after a (full) GC. The goal would have to be
 *       something like reducing the bytesInMemory to some percentage of its
 *       value at threshold notification (e.g., 80%). Since we can't directly
 *       control that and the feedback from the JVM is only at full GC
 *       intervals, we need to simply discard some percentage of the references
 *       from the tail of the global LRU. We could actually adjust the desired
 *       #of references on the LRU if that metric appears to be relatively
 *       stable. However, note that the average #of bytes per reference and the
 *       average #of instances of a reference on the LRU are not necessarily
 *       stable values. We could also examine the recordCount (total cache size
 *       across all caches). If weak references are cleared on an ongoing basis
 *       rather than during the full GC mark phase, then that will be very close
 *       to the real hard reference count.
 * 
 * @todo Does it make sense to both buffer the index segment nodes region and
 *       buffer the nodes and leaves? [buffering the nodes region is an option.]
 * 
 * @todo Note that a r/w store will require an approach in which addresses are
 *       PURGED from the store's cache during the commit protocol. That might be
 *       handled at the tx layer.
 * 
 * @todo Better ergonomics! Perhaps keep some minimum amount for the JVM and
 *       then set a trigger on the GC time and if it crosses 5-10% of the CPU
 *       time for the application, then reduce the maximum bytes allowed for the
 *       global LRU buffer.
 * 
 * @see Options
 */
public class LRUNexus {

    protected static final transient Logger log = Logger
            .getLogger(LRUNexus.class);

    /**
     * These options are MUST BE specified as <em>ENVIRONMENT</em> variables on
     * the command line when you start the JVM. The options control the
     * existence of and behavior of the {@link LRUNexus#INSTANCE}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * Option may be used to disable the {@link LRUNexus#INSTANCE}.
         */
        String ENABLED = LRUNexus.class.getName() + ".enabled";

        String DEFAULT_ENABLED = "true";

        /**
         * The maximum heap capacity as a percentage of the JVM heap expressed
         * as a value in <code>[0.0:1.0]</code>. This is used IFF
         * {@link #MAX_HEAP} is not specified or is ZERO (0), which is its
         * default value. If both options are zero, then the maximum heap is
         * understood to be zero and the {@link LRUNexus#INSTANCE} will be
         * disabled.
         */
        String PERCENT_HEAP = LRUNexus.class.getName() + ".percentHeap";

        /**
         * The default ({@value #DEFAULT_PERCENT_HEAP}) is a bit conservative.
         * It is designed to leave some room for application data objects and
         * GC. You may be able to get away with significantly more on machines
         * with large RAM, or just specify the buffer heap size directly using
         * {@link #MAX_HEAP}.
         */ 
        String DEFAULT_PERCENT_HEAP = ".1";

        /**
         * This option overrides {@link #PERCENT_HEAP} and directly specifies
         * the maximum capacity of the {@link LRUNexus#INSTANCE} in bytes. If
         * both options are zero, then the maximum heap is understood to be zero
         * and the {@link LRUNexus#INSTANCE} will be disabled. Legal examples
         * include:
         * 
         * <pre>
         * 30000000
         * 400m
         * 2Gb
         * </pre>
         * 
         * @see BytesUtil#getByteCount(String)
         */
        String MAX_HEAP = LRUNexus.class.getName() + ".maxHeap";

        String DEFAULT_MAX_HEAP = "0";

        /**
         * The percent of the maximum bytes which the LRU may buffer to be
         * cleared from the LRU when evicting the LRU entry (default
         * {@value #DEFAULT_PERCENT_CLEARED}). This parameter provides some
         * "batching" of evictions but is not used by all {@link IGlobalLRU}
         * implementations.
         */
        String PERCENT_CLEARED = LRUNexus.class.getName() + ".percentCleared";

        String DEFAULT_PERCENT_CLEARED = ".01";

        /**
         * The name of {@link IGlobalLRU} implementation class.
         * 
         * @see #DEFAULT_CLASS
         */
        String CLASS = LRUNexus.class.getName() + ".class";

        /**
         * The default {@link IGlobalLRU} implementation class (
         * {@value #DEFAULT_CLASS}).
         * 
         * FIXME The {@link HardReferenceGlobalLRURecycler} has less throughput
         * than the {@link HardReferenceGlobalLRU} but I want to test the
         * {@link HardReferenceGlobalLRU} more throughly on high throughput
         * cluster data loads to make sure that it is performing correctly.
         * <p>
         * Note: It is also possible that the {@link HardReferenceGlobalLRU}
         * causes problems with the tenured generation since the Entry instances
         * are always new, but they could last quite a while before eviction
         * from the LRU position if there is a large heap.
         * <p>
         * Scale-out should use the
         * {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired} to avoid
         * giving away the cached index segment records when an index segment
         * store is closed by a timeout.
         * <p>
         * For scale-up, the {@link HardReferenceGlobalLRURecycler} is fine. We
         * are not re-opening stores all the time so the weak value reference
         * semantics of that class do not cause a problem.
         * <p>
         * The recently written {@link BCHMGlobalLRU2} implementation should be
         * ideal for both scale-out and scale-up once it has been tested more
         * throughly. Even better would be a LIRS access policy for that class.
         */
        String DEFAULT_CLASS = HardReferenceGlobalLRURecycler.class.getName();
//        String DEFAULT_CLASS = BCHMGlobalLRU2.class.getName();

        /**
         * The load factor for the cache instances.
         */
        String LOAD_FACTOR = LRUNexus.class.getName() + ".loadFactor";

        String DEFAULT_LOAD_FACTOR = ".75";

        /**
         * The concurrency level for the backing hash map(s). This property is
         * not understood by all implementations. A value of ZERO (0) is
         * interpreted in a special manner by {@link BCHMGlobalLRU2}.
         */
        String CONCURRENCY_LEVEL = LRUNexus.class.getName()
                + ".concurrencyLevel";

        String DEFAULT_CONCURRENCY_LEVEL = "16";

        /**
         * When <code>true</code> the cache will use true <em>per-thread</em>
         * buffers to absorb access policy updates. When <code>false</code>, the
         * cache will use striped locks protecting a fixed array of buffers.
         * This property is not understood by all implementations.
         * */
        String THREAD_LOCAL_BUFFERS = LRUNexus.class.getName()
                + ".threadLocalBuffers";

        String DEFAULT_THREAD_LOCAL_BUFFERS = "false";
        
        /**
         * The initial capacity for the cache instances.
         */
        String INITIAL_CAPACITY = LRUNexus.class.getName() + ".initialCapacity";

        String DEFAULT_INITIAL_CAPACITY = "16";

        /**
         * The limiting cache capacity across all cache instances.
         * 
         * @see #DEFAULT_LIMITING_CAPACITY
         * 
         * @deprecated At the moment, this option is only understood by the
         *             {@link BCHMGlobalLRU}. The option may or may not survive
         *             as that feature is alpha.
         */
        String LIMITING_CAPACITY = LRUNexus.class.getName()+".limitingCapacity";

        String DEFAULT_LIMITING_CAPACITY = "" + (2 * Bytes.megabyte);

        /**
         * The capacity of the thread-local buffer used to amortize the cost of
         * updating the access policy. This option is only understood by select
         * {@link IGlobalLRU} implementations.
         */
        String THREAD_LOCAL_BUFFER_CAPACITY = LRUNexus.class.getName()
                + ".threadLocalBufferCapacity";

        String DEFAULT_THREAD_LOCAL_BUFFER_CAPACITY = "128";

        /**
         * The access policy (LIRS, LRU, etc). At the moment, this option is
         * only understood by the {@link BCHMGlobalLRU}.
         * 
         * @see #DEFAULT_ACCESS_POLICY_ENUM
         */
        String ACCESS_POLICY = LRUNexus.class.getName() + ".accessPolicy";
        
        String DEFAULT_ACCESS_POLICY = AccessPolicyEnum.LRU.toString();

        /**
         * The minimum #of per-{@link IRawStore} cache instances that will be
         * retained by hard references when using an {@link IGlobalLRU} based on
         * a weak value hash map such as {@link WeakReferenceGlobalLRU}. This
         * controls the size of a hard reference ring buffer backing a weak
         * value hash map. The actual number of cache instances will be less if
         * fewer stores have been opened or if open stores have been
         * {@link IRawStore#deleteResources() destroyed}. More cache instances
         * will exist if there are hard references to more {@link IRawStore}
         * instances.
         */
        String MIN_CACHE_SET_SIZE = LRUNexus.class.getName()+".minCacheSetSize";
        
        String DEFAULT_MIN_CACHE_SET_SIZE = "5";

        /**
         * When <code>true</code>, the {@link IndexSegmentBuilder} will
         * pre-populate the {@link IGlobalLRU} cache with the nodes and leaves
         * of the new index segment during the build or merge operation (default
         * {@value #DEFAULT_INDEX_SEGMENT_BUILD_POPULATES_CACHE}).
         */
        String INDEX_SEGMENT_BUILD_POPULATES_CACHE = LRUNexus.class.getName()
                + ".indexSegmentBuildPopulatesCache";

        /**
         * FIXME Verify that [true] is the right behavior for scale-out. This is
         * being tested in combination with the
         * {@link HardReferenceGlobalLRURecyclerExplicitDeleteRequired}.
         */
        String DEFAULT_INDEX_SEGMENT_BUILD_POPULATES_CACHE = "true";

    }


    /**
     * Global instance.
     * <p>
     * Note: A <a href="http://bugs.sun.com/view_bug.do?bug_id=6880903">Sun G1
     * bug in JDK 1.6.0_16</a> provides a false estimate of the available
     * memory.
     * 
     * @see Options
     */
    public static final IGlobalLRU<Long, Object> INSTANCE;

    /**
     * The access policy. Not all {@link IGlobalLRU} implementations support
     * multiple access policies. Check the specific implementation to see which
     * policies it supports.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static enum AccessPolicyEnum {

        /**
         * Least-recently used access policy.
         */
        LRU,

        /**
         * An access policy designed to avoid displacement of frequently used
         * cache entries by scans of infrequently used items.
         * 
         * @see <a
         *      href="http://portal.acm.org/citation.cfm?doid=511334.511340">LIRS:
         *      an efficient low inter-reference recency set replacement policy
         *      to improve buffer cache performance</a> and <a
         *      href="http://www.ece.eng.wayne.edu/~sjiang/Projects/LIRS/sig02.ppt"
         *      >LIRS : An Efficient Replacement Policy to Improve Buffer Cache
         *      Performance.</a>
         */
        LIRS;
        
        private AccessPolicyEnum() {
        }
        
    }
    
    /**
     * A class which reflects the configuration {@link Options}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class CacheSettings {

        /**
         * @see Options#ENABLED
         */
        public final boolean enabled;

        /**
         * @see Options#INDEX_SEGMENT_BUILD_POPULATES_CACHE
         */
        public final boolean indexSegmentBuildPopulatesCache;

        /**
         * The {@link IGlobalLRU} implementation class.
         * 
         * @see Options#CLASS
         */
        public final Class<? extends IGlobalLRU> cls;

        /**
         * The load factor for the backing hash map(s).
         * 
         * @see Options#LOAD_FACTOR
         */
        public final float loadFactor;

        /**
         * The concurrency level for the backing hash map(s). This property is
         * not understood by all implementations.
         * 
         * @see Options#CONCURRENCY_LEVEL
         */
        public final int concurrencyLevel;

        /**
         * When <code>true</code> the cache will use true <em>per-thread</em>
         * buffers to absorb access policy updates. When <code>false</code>, the
         * cache will use striped locks protecting a fixed array of buffers.
         * This property is not understood by all implementations.
         * 
         * @see Options#THREAD_LOCAL_BUFFERS
         */
        public final boolean threadLocalBuffers;

        /**
         * The initial capacity for the backing {@link ILRUCache} hash map for
         * each {@link IRawStore}.
         * 
         * @see Options#INITIAL_CAPACITY
         */
        public final int initialCacheCapacity;

        /**
         * The limiting cache capacity across all cache instances.
         * 
         * @see Options#LIMITING_CAPACITY
         */
        public final int limitingCacheCapacity;

        /**
         * The percentage of the JVM heap to use for bigdata buffers.
         * 
         * @see Options#PERCENT_HEAP
         */
        public final float percentHeap;

        /**
         * The maximum heap size in bytes (optional).
         * 
         * @see Options#MAX_HEAP
         */
        public final long maxHeap;

        /**
         * The maximum bytesInMemory to retain across the caches. This is
         * computed based on {@link #percentHeap} and {@link #maxHeap} and
         * represents the actual limit which will be imposed on the shared LRU.
         * 
         * @see Options#MAX_HEAP
         * @see Options#PERCENT_HEAP
         */
        public final long maximumBytesInMemory;

        /**
         * @see Options#PERCENT_CLEARED
         */
        public final double percentCleared;
        
        /**
         * {@link #percentCleared} TIMES {@link #maximumBytesInMemory}.
         */
        public final long minCleared;
        
        /**
         * The minimum #of caches to keep open for an {@link IGlobalLRU} based
         * on a weak value hash map.
         * 
         * @see Options#MIN_CACHE_SET_SIZE
         */
        public final int minCacheSetSize;

        /**
         * @see WeakReferenceGlobalLRU
         */
        public final int queueCapacity;
        
        /**
         * @see WeakReferenceGlobalLRU 
         */
        public final int nscan;
        
        /**
         * The capacity of the thread-local buffer used to amortize the cost of
         * updating the access policy. This option is only recognized by some
         * {@link IGlobalLRU} implementations.
         * 
         * @see BCHMGlobalLRU2
         * 
         * @see Options#THREAD_LOCAL_BUFFER_CAPACITY
         */
        public final int threadLocalBufferCapacity;

        /**
         * The access policy algorithm (LRU, LIRS, etc).
         * 
         * @see Options#ACCESS_POLICY
         */
        public final AccessPolicyEnum accessPolicy;

        /**
         * Parses the {@link Options} found in the caller's {@link Properties}
         * to populate the fields of this {@link CacheSettings} object.
         * 
         * @param properties
         *            The properties.
         * @throws ClassNotFoundException
         */
        public CacheSettings(final Properties properties)
                throws ClassNotFoundException {

            if (properties == null)
                throw new IllegalArgumentException();
            
            enabled = Boolean.valueOf(properties.getProperty(
                    Options.ENABLED, Options.DEFAULT_ENABLED));

            indexSegmentBuildPopulatesCache = Boolean.valueOf(properties.getProperty(
                    Options.INDEX_SEGMENT_BUILD_POPULATES_CACHE,
                    Options.DEFAULT_INDEX_SEGMENT_BUILD_POPULATES_CACHE));

            cls = (Class<? extends IGlobalLRU>) LRUNexus.class
                        .forName(properties.getProperty(Options.CLASS,
                                Options.DEFAULT_CLASS));

            final boolean validClass = IGlobalLRU.class.isAssignableFrom(cls);
            
            if (!validClass) {

                throw new RuntimeException("Class does not implement "
                        + IGlobalLRU.class.getName() + " : class=" + cls);

            }
            
            loadFactor = Float.valueOf(properties.getProperty(
                    Options.LOAD_FACTOR, Options.DEFAULT_LOAD_FACTOR));

            concurrencyLevel = Integer.valueOf(properties.getProperty(
                    Options.CONCURRENCY_LEVEL, Options.DEFAULT_CONCURRENCY_LEVEL));

            threadLocalBuffers = Boolean.valueOf(properties.getProperty(
                    Options.THREAD_LOCAL_BUFFERS,
                    Options.DEFAULT_THREAD_LOCAL_BUFFERS));

            initialCacheCapacity = Integer.valueOf(System
                    .getProperty(Options.INITIAL_CAPACITY,
                            Options.DEFAULT_INITIAL_CAPACITY));

            limitingCacheCapacity = Integer.valueOf(System
                    .getProperty(Options.LIMITING_CAPACITY,
                            Options.DEFAULT_LIMITING_CAPACITY));

            threadLocalBufferCapacity = Integer.valueOf(properties.getProperty(
                    Options.THREAD_LOCAL_BUFFER_CAPACITY,
                    Options.DEFAULT_THREAD_LOCAL_BUFFER_CAPACITY));

            accessPolicy = AccessPolicyEnum.valueOf(properties.getProperty(
                    Options.ACCESS_POLICY, Options.DEFAULT_ACCESS_POLICY));
            
            percentHeap = Float.valueOf(properties.getProperty(
                    Options.PERCENT_HEAP, Options.DEFAULT_PERCENT_HEAP));

            if (percentHeap < 0f || percentHeap > 1f) {

                throw new IllegalArgumentException(Options.PERCENT_HEAP
                        + " : must be in [0:1].");

            }

            maxHeap = BytesUtil.getByteCount(properties.getProperty(
                    Options.MAX_HEAP, Options.DEFAULT_MAX_HEAP));

            if (maxHeap < 0)
                throw new IllegalArgumentException(Options.MAX_HEAP
                        + "="
                        + properties.getProperty(Options.MAX_HEAP,
                                Options.DEFAULT_MAX_HEAP));

            if (maxHeap == 0 && percentHeap != 0f) {
                // compute based on the percentage of the heap.
                maximumBytesInMemory = (long) (Runtime.getRuntime()
                        .maxMemory() * percentHeap);
            } else if (maxHeap != 0) {
                // directly given.
                maximumBytesInMemory = maxHeap;
            } else {
                // disabled.
                maximumBytesInMemory = 0L;
            }

            percentCleared = Double.valueOf(properties.getProperty(
                    Options.PERCENT_CLEARED, Options.DEFAULT_PERCENT_CLEARED));

            if (percentCleared < 0f || percentCleared > 1f) {

                throw new IllegalArgumentException(Options.PERCENT_CLEARED
                        + " : must be in [0:1].");

            }

            minCleared = (long) (percentCleared * maximumBytesInMemory);
            
            minCacheSetSize = Integer.valueOf(properties.getProperty(
                    Options.MIN_CACHE_SET_SIZE,
                    Options.DEFAULT_MIN_CACHE_SET_SIZE));

            /*
             * Note: Values below this point are specific to the
             * WeakReferenceGlobalLRU.
             */
            
            /*
             * Estimate of the average record size.
             * 
             * Note: 1024 is not a bad value for a WORM journal, but 4096 or
             * 8192 are better values for the RW store and the index segment
             * files.
             */
            // The average record size.
            final int baseAverageRecordSize = 1024;

            final int averageRecordSize = (int) (baseAverageRecordSize * (Integer
                    .valueOf(IndexMetadata.Options.DEFAULT_BTREE_BRANCHING_FACTOR) / 32.));

            /*
             * The target capacity for that expected record size.
             * 
             * Note: This parameter can get you into trouble with too much GC if
             * too much gets buffered on the queue (this is the reasons this LRU
             * implementation is not recommended!)
             * 
             * 4x may be a bit aggressive. Try 3x.
             * 
             * TestTripleStoreLoadRateLocal: 4x yields 38s GC time with 1G heap.
             * 
             * TestTripleStoreLoadRateLocal: 3x yields 36s GC time with 1G heap.
             */
            final long maximumQueueCapacityEstimate = maximumBytesInMemory
                    / averageRecordSize * 2;

            if (BigdataStatics.debug)
                System.err.println(//
                        "averageRecordSize="
                                + averageRecordSize//
                                + ", maximumQueueCapacityEstimate="
                                + maximumQueueCapacityEstimate//
                        );

            if (true) {

                queueCapacity = (int) Math.min(Integer.MAX_VALUE,
                        maximumQueueCapacityEstimate);

            } else if (maximumBytesInMemory < Bytes.gigabyte * 2) {

                // capacity is no more than X
                queueCapacity = (int) Math.min(
                        maximumQueueCapacityEstimate, 200000/*
                                                             * 200k
                                                             */);

            } else {

                // capacity is no more than Y
                queueCapacity = (int) Math.min(
                        maximumQueueCapacityEstimate, 1000000/*
                                                              * 1M
                                                              */);

            }
            
            nscan = 20;
//            Integer.valueOf(properties.getProperty(Options.NSCAN,
//                    Options.DEFAULT_NSCAN));

        }

        /**
         * Create a new {@link IGlobalLRU} instance from the
         * {@link CacheSettings}. The {@link IGlobalLRU} MUST define a public
         * constructor with the following method signature.
         * 
         * <pre>
         * public FooGlobalLRU(CacheSettings)
         * </pre>
         * 
         * @return The new instance -or- <code>null</code> if the cache is
         *         disabled.
         * 
         * @throws NoSuchMethodException
         * @throws SecurityException
         * @throws InvocationTargetException
         * @throws IllegalAccessException
         * @throws InstantiationException
         * @throws IllegalArgumentException
         * @throws UnsupportedOperationException
         *             if something is not supported....
         * 
         * @todo Instead of returning <code>null</code> if the cache is not
         *       enabled or if something goes wrong we could return a
         *       NOPGlobalLRU. That could simplify conditional logic. The
         *       implementation would have to support per-store caches but would
         *       not retain any records in those caches.
         */
        public IGlobalLRU<Long, Object> newInstance() throws SecurityException,
                NoSuchMethodException, IllegalArgumentException,
                InstantiationException, IllegalAccessException,
                InvocationTargetException {

            if (enabled) {

                if (maximumBytesInMemory > 0) {

                    final Constructor<?> ctor = cls
                            .getConstructor(new Class[] { CacheSettings.class });

                    return (IGlobalLRU<Long, Object>) ctor
                            .newInstance(new Object[] { this });

                }

            }

            // Not enabled.
            return null;

        }

        public String toString() {

            return super.toString()
                    + "{"//
                    + "maxPercent=" + percentHeap//
                    + ", maxHeap=" + maxHeap//
                    + ", maximumBytesInMemory=" + maximumBytesInMemory//
                    + ", percentCleared=" + percentCleared//
                    + ", minCleared=" + minCleared//
                    + ", maxMemory=" + Runtime.getRuntime().maxMemory()//
                    + ", loadFactor=" + loadFactor// 
                    + ", concurrencyLevel=" + concurrencyLevel// 
                    + ", threadLocalBuffers=" + threadLocalBuffers// 
                    + ", threadLocalBufferCapacity=" + threadLocalBufferCapacity// 
                    + ", initialCacheCapacity=" + initialCacheCapacity//
                    + ", limitingCacheCapacity=" + limitingCacheCapacity//
                    + ", minCacheSetSize=" + minCacheSetSize//
                    + ", queueCapacity=" + queueCapacity//
                    + ", nscan=" + nscan//
                    + ", cls=" + cls.getName()//
                    + ", indexSegmentBuildPopulatesCache=" + indexSegmentBuildPopulatesCache + //
                    "}";

        }

    }

    /**
     * The configuration in use.
     */
    private static final CacheSettings settings;

    static {

        IGlobalLRU<Long, Object> tmp = null;
        CacheSettings s = null;

        try {

            // parse the options.
            s = new CacheSettings(System.getProperties());

            if (BigdataStatics.debug || log.isInfoEnabled()) {

                final String msg = s.enabled ? s.toString()
                        : "LRUNexus is disabled";

                if (BigdataStatics.debug)
                    System.err.println(msg);

                if (log.isInfoEnabled())
                    log.info(msg);

            }

            // create the cache object.
            tmp = s.newInstance();
            
        } catch (Throwable t) {

            log.error("LRUNexus disabled", t);

        } finally {

            // Note: MAY be null.
            INSTANCE = tmp;

            // Note: MAY be null.
            settings = s;
            
        }

    }

    /**
     * Return <code>true</code> if the {@link IndexSegmentBuilder} will populate
     * the {@link IGlobalLRU} with records for the new {@link IndexSegment}
     * during the build.
     * 
     * @see Options#INDEX_SEGMENT_BUILD_POPULATES_CACHE
     */
    public static final boolean getIndexSegmentBuildPopulatesCache() {

        return settings != null && settings.indexSegmentBuildPopulatesCache;

    }
    
    /**
     * Factory returns the {@link ILRUCache} for the store iff the
     * {@link LRUNexus} is enabled.
     * 
     * @param store
     *            The store.
     * 
     * @return The cache for that store if the {@link LRUNexus} is enabled and
     *         otherwise <code>null</code>.
     * 
     * @throws IllegalArgumentException
     *             if the store is <code>null</code>.
     */
    public static ILRUCache<Long, Object> getCache(final IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();

        if (INSTANCE == null)
            return null;

        final IAddressManager am;

        if (store instanceof AbstractJournal) {

            /*
             * This avoids hard reference to the journal (it winds up using a
             * clone of the address manager instead).
             */
            
            am = ((IBufferStrategy) ((AbstractJournal) store)
                    .getBufferStrategy()).getAddressManager();

        } else if (store instanceof TemporaryRawStore) {

            /*
             * This avoids using a hard reference to the temporary store (it
             * basically clones the address manager instead).
             */
            
            am = new WormAddressManager(((TemporaryRawStore) store)
                    .getOffsetBits());

        } else if (store instanceof AbstractRawStore) {

            /*
             * Note: this covers the IndexSegmentStore.
             */
            am = ((AbstractRawStore) store).getAddressManager();

        } else {

            // @todo which cases come though here? SimpleMemoryStore,
            // SimpleFileStore,
            am = null;

        }

        if (am instanceof IRawStore) {

            /*
             * This would cause the IRawStore to be retained by a hard
             * reference!
             */

            throw new AssertionError(am.getClass().getName() + " implements "
                    + IRawStore.class.getName());

        }

        return INSTANCE.getCache(store.getUUID(), am);
        
    }

    /**
     * Command line utility may be used to confirm the environment settings.
     * 
     * @param args
     *            Ignored. All parameters are specified either in the
     *            environment or using JVM
     *            <code>-Dcom.bigdata.LRUNexus.foo=bar</code> arguments on the
     *            command line.
     *            
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws ClassNotFoundException {

        System.out.println(new CacheSettings(System.getProperties()).toString());
        
    }
    
}
