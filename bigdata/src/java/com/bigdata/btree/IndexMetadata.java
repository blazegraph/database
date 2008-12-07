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
package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.compression.PrefixSerializer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerRangeValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IConflictResolver;
import com.bigdata.journal.IIndexManager;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.SparseRowStore;

/**
 * <p>
 * The persistent and mostly immutable metadata for a {@link AbstractBTree}.
 * This class allows you to configured several very important aspects of the
 * B+Tree behavior. Read on.
 * </p>
 * <p>
 * An instance of this class is required in order to create a {@link BTree} or
 * {@link IndexSegment}. Further, when registering a scale-out index you will
 * first create an instance of this class that will serve as the metadata
 * <i>template</i> for all index resources which are part of that scale-out
 * index.
 * </p>
 * <h2>Delete markers, version timestamps, and isolatable indices</h2>
 * <p>
 * By default a {@link BTree} does not maintain delete markers and a request to
 * delete an entry under a key will cause that entry to be removed from the live
 * version of the index. However, such indices do not support "overflow" (they
 * can not be evicted onto read-only {@link IndexSegment}s) and as such they do
 * not support scale-out).
 * </p>
 * <p>
 * The {@link SparseRowStore} handles a "delete" of a property value by writing
 * a <code>null</code> value under a key and does NOT require the use of index
 * entry delete markers, even in scale-out deployments. A compacting merge of a
 * {@link SparseRowStore} applies a history policy based on a consideration of
 * the timestamped property values, including values bound to a
 * <code>null</code>.
 * </p>
 * <p>
 * Delete markers combined with an ordered set of index resources is sufficient
 * to support all features of range-partitioned indices, including compacting
 * merge. Given three index resources {A,B,C} for a single index partition, the
 * order over the resources gives us the guarentee that any index entry in A
 * will be more recent than any index enty in B or C. So when reading a fused
 * view we always stop once we have an index entry for a key, even if that entry
 * has the deleted flag set.
 * </p>
 * <p>
 * Delete markers occupy very little space in the leaf data structure (one bit
 * each), however when they are used a deleted index entry is NOT removed from
 * the index. Instead, the key remains in the leaf paired to a delete bit and a
 * <code>null</code> value (or simply elided). These "deleted" entries can
 * only be removed from the index by a compacting merge. When transactional
 * isolation is used, the criteria for removing deleted entries are stronger -
 * they must no longer be visible to any active or allowable transaction as
 * determined by the transaction manager, see below for more on this.
 * </p>
 * <p>
 * Transaction isolation requires delete markers <i>plus</i> version
 * timestamps. The version timestamps in the unisolated index (the live index)
 * give the timestamp of the commit during which the index entry was last
 * updated. The timestamp in the write set of transaction is copy from the index
 * view corresponding to the ground state of the transaction the first time that
 * index entry is overwritten within that transaction (there is a special case
 * when the index entry was not pre-existing - we assign the start time of the
 * transaction in that case so when we validate we are only concerned that the
 * entry is either not found (never written) or that the entry exists with the
 * same timestamp - other conditions are write-write conflicts). On commit we
 * write the commit time on the updated index entries in the unisolated index.
 * </p>
 * <h2>History policies and timestamps</h2>
 * <p>
 * There are in fact two kinds of timestamps in use - isolatable indices place a
 * timestamp on the index entry itself while the {@link SparseRowStore} places a
 * timestamp in the <i>key</i>. One consequence of this is that it is very
 * efficient to read historical data from the {@link SparseRowStore} since the
 * data are right there in key order. On the other hand, reading historical data
 * from an isolatable index requires reading from each historical commit state
 * of that index which is not interest (this is NOT efficient). This is why the
 * {@link SparseRowStore} design places timestamps in the key - so that the
 * application can efficiently read both the current and historical property
 * values within a logical row.
 * </p>
 * <p>
 * Regardless of whether the timestamp is on the index entry (as it always is
 * for isolatable indices) or in the key ({@link SparseRowStore}), the
 * existence of timestamps makes it possible for an application to specify a
 * history policy governing when property values will be deleted.
 * </p>
 * <p>
 * When an index participates in transactions the transaction manager manages
 * the life cycle of overwritten and deleted index entries (and the resources on
 * which the indices exist). This is done by preserving such data until no
 * transaction exists that can read from those resources. Unless an immortal
 * store is desired, the "purge time" is set at a time no more recent than the
 * earliest fully isolated transaction (either a read-only tx as of the start
 * time of the tx or a read-write tx as of its start time). The role of a
 * "history policy" with transactions is therefore how much history to buffer
 * between the earliest running tx and the choosen "purge time". When the
 * transaction manager updates the "purge time" it notifies the journal/data
 * services. Resources having no data later than the purge time may be deleted
 * and SHOULD NOT be carried forward when building new index segments.
 * </p>
 * <p>
 * History policies for non-transactional indices are somewhat different. An
 * scale-out index without timestamps will buffer historical data only until the
 * next compacting merge of a given index partition. The compacting merge uses
 * the fused view of the resources comprising the index partition and only
 * writes out the undeleted index entries.
 * </p>
 * <p>
 * If an application instead chooses to use timestamps in a non-transactional
 * index then (a) timestamps must be assigned by either the client or the data
 * service; and (b) applications can specify a history policy where data older
 * than a threshold time (but not #of versions) will be eradicated. This
 * approach is possible, but not well-supported in the higher level APIs.
 * </p>
 * <p>
 * The {@link SparseRowStore} design is more flexible since it allows (a) fast
 * access to historical property values for the same "row"; and (b) history
 * policies that may be specified in terms of the #of versions, the age of a
 * datum, and that can keep at least N versions of a datum. This last point is
 * quite important as it allows you to retain the entirety of the most current
 * revision of the logical row, even when some datums are much older than
 * others.
 * </p>
 * 
 * <h2>Serialization</h2>
 * 
 * <p>
 * Note: Derived classes SHOULD extend the {@link Externalizable} interface and
 * explicitly manage serialization versions so that their metadata may evolve in
 * a backward compatible manner.
 * </p>
 * 
 * @todo Make sure that metadata for index partition "zones" propagates with the
 *       partition metadata so that appropriate policies are enforcable locally
 *       (access control, latency requirements, replication, purging of
 *       historical deleted versions, etc).
 * 
 * @todo add optional property containing IndexMetadata to be used as of the
 *       next overflow so that people can swap out key and value serializers and
 *       the like during overflow operations. the conops is that you map the new
 *       metadata over the index partitions and either lazily or eagerly
 *       overflow is triggered and the resulting {@link BTree} and
 *       {@link IndexSegment} objects will begin to use the new key / value
 *       serializers, etc. while the existing objects will still have their old
 *       key/val serializers and therefore can still be read.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexMetadata implements Serializable, Externalizable, Cloneable,
        IKeyBuilderFactory {

    private static final long serialVersionUID = 4370669592664382720L;
    
    /**
     * Options and their defaults for the {@link com.bigdata.btree} package and
     * the {@link BTree} and {@link IndexSegment} classes. Options that apply
     * equally to views and {@link AbstractBTree}s are in the package
     * namespace, such as whether or not a bloom filter is enabled. Options that
     * apply to all {@link AbstractBTree}s are specified within that namespace
     * while those that are specific to {@link BTree} or {@link IndexSegment}
     * are located within their respective class namespaces. Some properties,
     * such as the branchingFactor, are defined for both the {@link BTree} and
     * the {@link IndexSegment} because their defaults tend to be different when
     * an {@link IndexSegment} is generated from an {@link BTree}.
     * 
     * @todo It should be possible to specify the key compression (serializer)
     *       for nodes and leaves and the value compression (serializer) via
     *       this interface. This is easy enough if there is a standard factory
     *       interface, since we can specify the class name, and more difficult
     *       if we need to create an instance.
     *       <p>
     *       Note: The basic pattern here is using the class name, having a
     *       default instance of the class (or a factory for that instance), and
     *       then being able to override properties for that instance. Beans
     *       stuff really, just simpler.
     * 
     * @todo it should be possible to specify the overflow handler and its
     *       properties via options (as you can with beans or jini
     *       configurations).
     * 
     * @todo it should be possible to specify a different split handler and its
     *       properties via options (as you can with beans or jini
     *       configurations).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /*
         * Constants.
         */
        
        /**
         * The minimum allowed branching factor (3). The branching factor may be
         * odd or even.
         */
        int MIN_BRANCHING_FACTOR = 3;

        /**
         * A reasonable maximum branching factor for a {@link BTree}.
         */
        int MAX_BTREE_BRANCHING_FACTOR = 1024;

        /**
         * A reasonable maximum branching factor for an {@link IndexSegment}.
         */
        int MAX_INDEX_SEGMENT_BRANCHING_FACTOR = 10240;
        
        /**
         * The minimum write retention queue capacity is two (2) in order to
         * avoid cache evictions of the leaves participating in a split.
         */
        int MIN_WRITE_RETENTION_QUEUE_CAPACITY = 2;
        
        /**
         * A reasonable maximum write retention queue capacity.
         */
        int MAX_WRITE_RETENTION_QUEUE_CAPACITY = 2000;
        
         /*
         * Options that apply to FusedViews as well as to AbstractBTrees.
         * 
         * Note: These options are in the package namespace.
         */
        
        /**
         * Optional property controls whether or not a bloom filter is
         * maintained (default {@value #DEFAULT_BLOOM_FILTER}). When enabled,
         * the bloom filter is effective up to ~ 2M entries per index
         * (partition). For scale-up, the bloom filter is automatically disabled
         * after its error rate would be too large given the #of index enties.
         * For scale-out, as the index grows we keep splitting it into more and
         * more index partitions, and those index partitions are comprised of
         * both views of one or more {@link AbstractBTree}s. While the mutable
         * {@link BTree}s might occasionally grow to large to support a bloom
         * filter, data is periodically migrated onto immutable
         * {@link IndexSegment}s which have perfect fit bloom filters. This
         * means that the bloom filter scales-out, but not up.
         * 
         * @see BloomFilterFactory#DEFAULT
         * 
         * @see #DEFAULT_BLOOM_FILTER
         */
        String BLOOM_FILTER = com.bigdata.btree.BTree.class.getPackage()
                .getName()
                + ".bloomFilter";
        
        String DEFAULT_BLOOM_FILTER = "false";
        
        /**
         * The name of an optional property whose value identifies the data
         * service on which the initial index partition of a scale-out index
         * will be created. The value may be the {@link UUID} of that data
         * service (this is unambiguous) of the name associated with the data
         * service (it is up to the administrator to not assign the same name to
         * different data service instances and an arbitrary instance having the
         * desired name will be used if more than one instance is assigned the
         * same name). The default behavior is to select a data service using
         * the load balancer, which is done automatically if
         * {@link IndexMetadata#getInitialDataServiceUUID()} returns
         * <code>null</code>.
         */
        // note: property applies to views so namespace is the package.
        String INITIAL_DATA_SERVICE = com.bigdata.btree.BTree.class
                .getPackage().getName()
                + ".initialDataService";

        /**
         * The capacity of the hard reference queue used to defer the eviction
         * of dirty nodes (nodes or leaves).
         * <p>
         * The purpose of this queue is to defer eviction of dirty nodes and
         * leaves. Once a node falls off the write retention queue it is checked
         * to see if it is dirty. If it is dirty, then it is serialized and
         * persisted on the backing store. If the write retention queue capacity
         * is set to a large value (say, GTE 1000), then that will will increase
         * the commit latency and have a negative effect on the overall
         * performance. Too small a value will mean that nodes that are
         * undergoing mutation will be serialized and persisted prematurely
         * leading to excessive writes on the backing store. For append-only
         * stores, this directly contributes to what are effectively redundent
         * and thereafter unreachable copies of the intermediate state of nodes
         * as only nodes that can be reached by navigation from a
         * {@link Checkpoint} will ever be read again. The value
         * <code>500</code> appears to be a good default. While it is possible
         * that some workloads could benefit from a larger value, this leads to
         * higher commit latency and can therefore have a broad impact on
         * performance.
         * 
         * @todo For historical reasons, the write rentention queue is used for
         *       both {@link BTree} and {@link IndexSegment}. However, the code
         *       should be modified such that the write retention queue is ONLY
         *       use for mutable {@link BTree}s and the read-rentention queue
         *       should be either provisioned dynamically or separately for
         *       {@link IndexSegment}s and {@link BTree}s. (The
         *       read-rentention queue would be used for both read-only and
         *       mutable {@link BTree}s.)
         */
        String WRITE_RETENTION_QUEUE_CAPACITY = com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".writeRetentionQueue.capacity";

        /**
         * The #of entries on the write retention queue that will be scanned for
         * a match before a new reference is appended to the queue. This trades
         * off the cost of scanning entries on the queue, which is handled by
         * the queue itself, against the cost of queue churn. Note that queue
         * eviction drives IOs required to write the leaves on the store, but
         * incremental writes occur iff the {@link AbstractNode#referenceCount}
         * is zero and the node or leaf is dirty.
         */
        String WRITE_RETENTION_QUEUE_SCAN = com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".writeRetentionQueue.scan";

        /**
         * The capacity of the hard reference queue used to retain recently used
         * nodes (or leaves). When zero (0), this queue is disabled.
         * <p>
         * The read retention queue complements the write retention queue. The
         * latter has a strong buffering effect, but we can not increase the
         * size of the write retention queue without bound as that will increase
         * the commit latency. However, the read retention queue can be quite
         * large and will "simply" buffer "recently" used nodes and leaves in
         * memory. This can have a huge effect, especially when a complex
         * high-level query would otherwise thrash the disk as nodes that are
         * required for query processing fall off of the write retention queue
         * and get garbage collected. The pragmatic upper bound for this
         * probably depends on the index workload. At some point, you will stop
         * seeing an increase in performance as a function of the read retention
         * queue for a given workload. The larger the read retention queue, the
         * more burden the index can impose on the heap. However, copy-on-write
         * explicitly clears all references in a node so the JVM can collect the
         * data for nodes that are no longer part of the index before they fall
         * off of the queue even if it can not collect the node reference
         * itself.
         * 
         * @todo The size of the read rentention queue should be set dynamically
         *       as a function of the depth of the BTree (or the #of nodes and
         *       leaves), the branching factor, and the RAM available to the
         *       HOST (w/o swapping) and to the JVM. For a mutable {@link BTree}
         *       the depth changes only slowly, but the other factors are always
         *       changing. Regardless, changing the read-retention queue size is
         *       never a problem as cleared references will never cause a
         *       strongly reachable node to be released.
         *       <p>
         *       To avoid needless effort, there should be a minimum queue
         *       capacity that is used up to depth=2/3. If the queue capacity is
         *       set to n=~5-10% of the maximum possible #of nodes in a btree of
         *       a given depth, then we can compute the capacity dynamically
         *       based on that parameter. And of course it can be easily
         *       provisioned when the BTree is {@link #reopen()}ed.
         */
        String READ_RETENTION_QUEUE_CAPACITY = com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".readRetentionQueue.capacity";

        /**
         * The #of entries on the hard reference queue that will be scanned for a
         * match before a new reference is appended to the queue. This trades off
         * the cost of scanning entries on the queue, which is handled by the queue
         * itself, against the cost of queue churn.
         */
        String READ_RETENTION_QUEUE_SCAN = com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".readRetentionQueue.scan"; 

        String DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY = "500";

        String DEFAULT_WRITE_RETENTION_QUEUE_SCAN = "20";

        /**
         * @todo very different defaults make sense for scale-up vs scale-out
         *       unless the actual capacity is determined dynamically.
         *       <p>
         *       It might make sense to give an "importance" factor to each
         *       index so that performance critical indices can buffer more data
         *       even when the host has less RAM available.
         */
        String DEFAULT_READ_RETENTION_QUEUE_CAPACITY = "10000";

        String DEFAULT_READ_RETENTION_QUEUE_SCAN = "20";

        /*
         * Options that are valid for any AbstractBTree but which are not
         * defined for a FusedView.
         * 
         * Note: These options are in the AbstractBTree namespace.
         */
        
        /*
         * Options that are specific to BTree.
         * 
         * Note: These options are in the BTree namespace.
         */
        
        /**
         * The name of a class derived from {@link BTree} that will be used to
         * re-load the index. Note that index partitions are in general views
         * (of one or more resources). Therefore only unpartitioned indices can
         * be meaningfully specialized solely in terms of the {@link BTree} base
         * class.
         * 
         * @todo in order to provide a similar specialization mechanism for
         *       scale-out indices you would need to specify the class name for
         *       the {@link IndexSegment} and the {@link FusedView}. You might
         *       also need to override the {@link Checkpoint} class - for
         *       example the {@link MetadataIndex} does this.
         */
        String BTREE_CLASS_NAME = BTree.class.getName()+".className";
        
        /**
         * The name of an optional property whose value specifies the branching
         * factor for a mutable {@link BTree}.
         * 
         * @see #DEFAULT_BTREE_BRANCHING_FACTOR
         * @see #INDEX_SEGMENT_BRANCHING_FACTOR
         */
        String BTREE_BRANCHING_FACTOR = BTree.class.getName()+".branchingFactor";
        
        /**
         * The default branching factor for a mutable {@link BTree}.
         * 
         * @todo Performance is best for up to at least a 4M triple RDF dataset for
         *       load, closure and query at m=256, but thereafter performance begins
         *       to drag. Reconsider once I get rid of the
         *       {@link ImmutableKeyBuffer} and other cruft that is driving GC.
         */
        String DEFAULT_BTREE_BRANCHING_FACTOR = "32"; //"256"

        /*
         * Options that are specific to IndexSegment.
         * 
         * Note: These options are in the IndexSegment namespace.
         */
        
        /**
         * The name of the property whose value specifies the branching factory
         * for an immutable {@link IndexSegment}.
         */
        String INDEX_SEGMENT_BRANCHING_FACTOR = IndexSegment.class
                .getName()
                + ".branchingFactor";

        /**
         * The default branching factor for an {@link IndexSegment}.
         * 
         * @todo experiment with performance for various values in {128:4096}
         *       for commonly used scale-out indices.
         */
        String DEFAULT_INDEX_SEGMENT_BRANCHING_FACTOR = "512";

        /**
         * When <code>true</code> an attempt will be made to fully buffer the
         * nodes (but not the leaves) of the {@link IndexSegment} (default
         * {@value #DEFAULT_INDEX_SEGMENT_BUFFER_NODES}). The nodes in the
         * {@link IndexSegment} are serialized in a contiguous region by the
         * {@link IndexSegmentBuilder}. That region may be fully buffered when
         * the {@link IndexSegment} is opened, in which case queries against the
         * {@link IndexSegment} will incur NO disk hits for the nodes and only
         * one disk hit per visited leaf.
         * <p>
         * Note: The nodes are read into a buffer allocated from the
         * {@link DirectBufferPool}. If the size of the nodes region in the
         * {@link IndexSegmentStore} file exceeds the capacity of the buffers
         * managed by the {@link DirectBufferPool}, then the nodes WILL NOT be
         * buffered. The {@link DirectBufferPool} is used both for efficiency
         * and because a bug dealing with temporary direct buffers would
         * otherwise cause the C heap to be exhausted!
         * 
         * @see #DEFAULT_INDEX_SEGMENT_BUFFER_NODES
         */
        String INDEX_SEGMENT_BUFFER_NODES = IndexSegment.class.getName()
                + ".bufferNodes";
        
        /**
         * @see #BUFFER_NODES
         */
        String DEFAULT_INDEX_SEGMENT_BUFFER_NODES = "false";
     
        /**
         * The size of the LRU cache backing the weak reference cache for leaves
         * (default {@value #DEFAULT_LEAF_CACHE_SIZE}).
         * <p>
         * While the {@link AbstractBTree} already provides caching for nodes
         * and leaves based on navigation down the hierarchy from the root node,
         * the {@link IndexSegment} uses an additional leaf cache to optimize
         * access to leaves based on the double-linked list connecting the
         * leaves.
         * <p>
         * A larger value will tend to retain leaves longer at the expense of
         * consuming more RAM when many parts of the {@link IndexSegment} are
         * hot.
         * 
         * @todo The {@link #READ_RETENTION_QUEUE_CAPACITY} for index segments
         *       should probably be much smaller than for a mutable btree for
         *       several reasons. First, the {@link IndexSegment}'s iterators
         *       DO NOT use navigation of the nodes from the root for prior/next
         *       operations. Second, it is possible to fully buffer the nodes
         *       region for an {@link IndexSegment}. Third, the branching
         *       factor for an {@link IndexSegment} is normally larger and the
         *       effect of the read-retention queue capacity and the leaf cache
         *       size are related to the branching factor.
         * 
         * @todo Use a timeout and a {@link ConcurrentWeakValueCache} for this
         *       cache?
         * 
         * @todo allow zero (0) to disable the cache?
         */
        String INDEX_SEGMENT_LEAF_CACHE_CAPACITY = IndexSegment.class.getName()
                + ".leafCacheCapacity";
        
        /**
         * 
         * @see #INDEX_SEGMENT_LEAF_CACHE_CAPACITY
         */
        String DEFAULT_INDEX_SEGMENT_LEAF_CACHE_CAPACITY = "100";
        
        /*
         * Split handler properties.
         * 
         * @see DefaultSplitHandler
         * 
         * Note: Use these settings to trigger splits sooner and thus enter the
         * more interesting regions of the phase space more quickly BUT DO NOT
         * use these settings for deployment!
         * 
         * final int minimumEntryCount = 1 * Bytes.kilobyte32; (or 10k)
         * 
         * final int entryCountPerSplit = 5 * Bytes.megabyte32; (or 50k)
         */
        
        String SPLIT_HANDLER_MIN_ENTRY_COUNT = DefaultSplitHandler.class
                .getName()
                + ".minimumEntryCount";

        String SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT = DefaultSplitHandler.class
                .getName()
                + ".entryCountPerSplit";

        String SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER = DefaultSplitHandler.class
                .getName()
                + ".overCapacityMultiplier";

        String SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER = DefaultSplitHandler.class
                .getName()
                + ".underCapacityMultiplier";

        String SPLIT_HANDLER_SAMPLE_RATE = DefaultSplitHandler.class.getName()
                + ".sampleRate";

        String DEFAULT_SPLIT_HANDLER_MIN_ENTRY_COUNT = ""
                + (500 * Bytes.kilobyte32);

        String DEFAULT_SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT = ""
                + (1 * Bytes.megabyte32);

        String DEFAULT_SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER = "1.5";

        String DEFAULT_SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER = ".75";

        String DEFAULT_SPLIT_HANDLER_SAMPLE_RATE = "20"; 

    }
    
    /**
     * Address that can be used to read this metadata record from the store.
     * <p>
     * Note: This is not persisted since we do not have the address until after
     * we have written out the state of this record. However the value is
     * written into each {@link Checkpoint} record.
     */
    private transient /*final*/ long addrMetadata;

    /**
     * Address that can be used to read this metadata record from the store.
     * <p>
     * Note: This is not a persistent property. However the value is set when
     * the metadata record is read from, or written on, the store. It is zero
     * when you {@link #clone()} a metadata record until it's been written onto
     * the store.
     */
    final public long getMetadataAddr() {
        
        return addrMetadata;
        
    }
    
    /**
     * The {@link UUID} of the {@link DataService} on which the first partition
     * of the scale-out index should be created. This is a purely transient
     * property and will be <code>null</code> unless either explicitly set or
     * set using {@value Options#INITIAL_DATA_SERVICE}. This property is only
     * set by the ctor(s) that are used to create a new {@link IndexMetadata}
     * instance, so no additional lookups are performed during de-serialization.
     * 
     * @see Options#INITIAL_DATA_SERVICE
     */
    public UUID getInitialDataServiceUUID() {
        
        return initialDataServiceUUID;
        
    }
    public void setInitialDataServiceUUID(UUID uuid) {
        
        initialDataServiceUUID = uuid;
        
    }
    private transient UUID initialDataServiceUUID;

    /*
     * @todo consider allowing distinct values for the branching factor (already
     * done), the class name, and possibly some other properties (record
     * compression, checksum) for the index segments vs the mutable btrees.
     */

    private UUID indexUUID;
    private String name;
    private int branchingFactor;
    private int writeRetentionQueueCapacity;
    private int writeRetentionQueueScan;
    private int readRetentionQueueCapacity;
    private int readRetentionQueueScan;
    private LocalPartitionMetadata pmd;
    private String btreeClassName;
    private String checkpointClassName;
    private IAddressSerializer addrSer;
    private IDataSerializer nodeKeySer;
    private ITupleSerializer tupleSer;
    private IConflictResolver conflictResolver;
    private boolean deleteMarkers;
    private boolean versionTimestamps;
    private BloomFilterFactory bloomFilterFactory;
    private IOverflowHandler overflowHandler;
    private ISplitHandler splitHandler;
//    private Object historyPolicy;

    /* 
     * IndexSegment fields.
     */
    
    private int indexSegmentBranchingFactor;
    private boolean indexSegmentBufferNodes;
    private int indexSegmentLeafCacheCapacity;

    /**
     * The unique identifier for the (scale-out) index whose data is stored in
     * this B+Tree data structure.
     * <p>
     * Note: When using a scale-out index the same <i>indexUUID</i> MUST be
     * assigned to each mutable and immutable B+Tree having data for any
     * partition of that scale-out index. This makes it possible to work
     * backwards from the B+Tree data structures and identify the index to which
     * they belong.
     */
    public final UUID getIndexUUID() {return indexUUID;}

    /**
     * The name associated with the index -or- <code>null</code> iff the index
     * is not named (internal indices are generally not named while application
     * indices are always named).
     * <p>
     * Note: When the index is a scale-out index, this is the name of the
     * scale-out index NOT the name under which an index partition is
     * registered.
     * <p>
     * Note: When the index is a metadata index, then this is the name of the
     * metadata index itself NOT the name of the managed scale-out index.
     */
    public final String getName() {return name;}
    
    /**
     * The branching factor for a mutable {@link BTree}. The branching factor
     * is the #of children in a node or values in a leaf and must be an integer
     * greater than or equal to three (3). Larger branching factors result in
     * trees with fewer levels. However there is a point of diminishing returns
     * at which the amount of copying performed to move the data around in the
     * nodes and leaves exceeds the performance gain from having fewer levels.
     * The branching factor for the read-only {@link IndexSegment}s is
     * generally much larger in order to reduce the number of disk seeks.
     */
    public final int getBranchingFactor() {
    
        return branchingFactor;
        
    }
    
    /**
     * The branching factor used when building an {@link IndexSegment} (default
     * is 4096). Index segments are read-only B+Tree resources. The are built
     * using a bulk index build procedure and typically have a much higher
     * branching factor than the corresponding mutable index on the journal.
     * <p>
     * Note: the value of this property will determine the branching factor of
     * the {@link IndexSegment}. When the {@link IndexSegment} is built, it
     * will be given a {@link #clone()} of this {@link IndexMetadata} and the
     * actual branching factor for the {@link IndexSegment} be set on the
     * {@link #getBranchingFactor()} at that time.
     * <p>
     * Note: a branching factor of 256 for an index segment and split limits of
     * (1M,5M) imply an average B+Tree height of 1.5 to 1.8. With a 10ms seek
     * time and NO CACHE that is between 15 and 18ms average seek time.
     * <p>
     * Note: a branching factor of 512 for an index segment and split limits of
     * (1M,5M) imply an average B+Tree height of 1.2 to 1.5. With a 10ms seek
     * time and NO CACHE that is between 12 and 15ms average seek time.
     * <p>
     * Note: the actual size of the index segment of course depends heavily on
     * (a) whether or now block references are being stored since the referenced
     * blocks are also stored in the index segment; (b) the size of the keys and
     * values stored in the index; and (c) the key, value, and record
     * compression options in use.
     */
    public final int getIndexSegmentBranchingFactor() {

        return indexSegmentBranchingFactor;
        
    }
    
    /**
     * Return <code>true</code> iff the nodes region for the
     * {@link IndexSegment} should be fully buffered by the
     * {@link IndexSegmentStore}.
     * 
     * @see Options#INDEX_DEFAULT_SEGMENT_BUFFER_NODES
     */
    public final boolean getIndexSegmentBufferNodes() {
        
        return indexSegmentBufferNodes;
        
    }

    public final void setIndexSegmentBufferNodes(boolean newValue) {
        
        this.indexSegmentBufferNodes = newValue;
        
    }
    
    /**
     * Return the capacity of the LRU cache of leaves for an
     * {@link IndexSegment}.
     * 
     * @see Options#INDEX_SEGMENT_LEAF_CACHE_CAPACITY
     */
    public final int getIndexSegmentLeafCacheCapacity() {
        
        return indexSegmentLeafCacheCapacity;
        
    }

    public final void setIndexSegmentLeafCacheCapacity(final int newValue) {
        
        if (newValue <= 0) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.indexSegmentLeafCacheCapacity = newValue;
        
    }
    
    /**
     * @see Options#WRITE_RETENTION_QUEUE_CAPACITY
     */
    public final int getWriteRetentionQueueCapacity() {
        
        return writeRetentionQueueCapacity;
        
    }

    public final void setWriteRetentionQueueCapacity(int v) {
        
        this.writeRetentionQueueCapacity = v;
        
    }

    /**
     * @see Options#WRITE_RETENTION_QUEUE_SCAN
     */
    public final int getWriteRetentionQueueScan() {
        
        return writeRetentionQueueScan;
        
    }
    
    public final void setWriteRetentionQueueScan(int v) {
        
        this.writeRetentionQueueScan = v;
        
    }

    /**
     * @see Options#READ_RETENTION_QUEUE_CAPACITY
     */
    public final int getReadRetentionQueueCapacity() {
        
        return readRetentionQueueCapacity;
        
    }
    
    public final void setReadRetentionQueueCapacity(int v) {
        
        this.readRetentionQueueCapacity = v;
        
    }

    /**
     * @see Options#READ_RETENTION_QUEUE_SCAN
     */
    public final int getReadRetentionQueueScan() {
        
        return readRetentionQueueScan;
        
    }
    
    public final void setReadRetentionQueueScan(int v) {
        
        this.readRetentionQueueScan = v;
        
    }

    /**
     * When non-<code>null</code>, this is the description of the view of
     * this index partition. This will be <code>null</code> iff the
     * {@link BTree} is not part of a scale-out index. This is updated when the
     * view composition for the index partition is changed.
     */
    public final LocalPartitionMetadata getPartitionMetadata() {
    
        return pmd;
        
    }
    
    /**
     * The name of a class derived from {@link BTree} that will be used to
     * re-load the index. Note that index partitions are in general views (of
     * one or more resources). Therefore only unpartitioned indices can be
     * meaningfully specialized solely in terms of the {@link BTree} base class.
     * 
     * @see Options#BTREE_CLASS_NAME
     */
    public final String getBTreeClassName() {

        return btreeClassName;

    }

    /**
     * The name of the {@link Checkpoint} class used by the index. This may be
     * overriden to store additional state with each {@link Checkpoint} record.
     */
    public final String getCheckpointClassName() {

        return checkpointClassName;
        
    }

    public final void setCheckpointClassName(final String className) {
        
        if (className == null)
            throw new IllegalArgumentException();

        this.checkpointClassName = className;
        
    }

    /**
     * Object used to (de-)serialize the addresses of the children of a node.
     */
    public final IAddressSerializer getAddressSerializer() {return addrSer;}
    
    /**
     * Object used to (de-)serialize/(de-)compress the keys in a node.
     * <p>
     * Note: The keys for nodes are separator keys for the leaves. Since they
     * are choosen to be the minimum length separator keys dynamically when a
     * leaf is split or joined the keys in the node typically DO NOT conform to
     * application expectations and are normally assigned a different serializer
     * for that reason.
     * <p>
     * Note: This handles the "serialization" of the <code>byte[][]</code>
     * containing all of the keys for some node of the index. As such it may be
     * used to provide compression across the already serialized node for the
     * leaf.
     * 
     * @see #getTupleSerializer()
     */
    public final IDataSerializer getNodeKeySerializer() {return nodeKeySer;}
    
    /**
     * The object used to form unsigned byte[] keys from Java objects, to
     * (de-)serialize Java object stored in the index, and to (de-)compress the
     * keys and values when stored in a leaf or {@link ResultSet}.
     * <p>
     * Note: If you change this value in a manner that is not backward
     * compatable once entries have been written on the index then you may be
     * unable to any read data already written.
     */
    public final ITupleSerializer getTupleSerializer() {return tupleSer;}
    
    /**
     * The optional object for handling write-write conflicts.
     * <p>
     * The concurrency control strategy detects write-write conflict resolution
     * during backward validation. If a write-write conflict is detected and a
     * conflict resolver is defined, then the conflict resolver is expected to
     * make a best attempt using data type specific rules to reconcile the state
     * for two versions of the same persistent identifier. If the conflict can
     * not be resolved, then validation will fail. State-based conflict
     * resolution when combined with validation (aka optimistic locking) is
     * capable of validating the greatest number of interleavings of
     * transactions (aka serialization orders).
     * 
     * @return The conflict resolver to be applied during validation or
     *         <code>null</code> iff no conflict resolution will be performed.
     */
    public final IConflictResolver getConflictResolver() {return conflictResolver;}
    
    /**
     * When <code>true</code> the index will write a delete marker when an
     * attempt is made to delete the entry under a key. Delete markers will be
     * retained until a compacting merge of an index partition. When
     * <code>false</code> the index entry will be removed from the index
     * immediately.
     * <p>
     * Delete markers MUST be enabled to use scale-out indices. Index partition
     * views depend on an ordered array of {@link AbstractBTree}s. The presence
     * of a delete marker provides an indication of a deleted index entry and is
     * used to prevent reading of index entries for the same key which might
     * exist in an earlier {@link AbstractBTree} which is part of the same index
     * partition view.
     * <p>
     * Delete markers MUST be enabled for transaction support where they play a
     * similar role recording within the write set of the transaction the fact
     * that an index entry has been deleted.
     */
    public final boolean getDeleteMarkers() {return deleteMarkers;}
    
    public final void setDeleteMarkers(boolean deleteMarkers) {

        this.deleteMarkers = deleteMarkers;
        
    }

    /**
     * When <code>true</code> the index will maintain a per-index entry
     * timestamp. The primary use of this is in support of transactional
     * isolation.
     */
    public final boolean getVersionTimestamps() {return versionTimestamps;}
    
    public final void setVersionTimestamps(boolean versionTimestamps) {
        
        this.versionTimestamps = versionTimestamps;
        
    }

    /**
     * True iff the index supports transactional isolation (both delete markers
     * and version timestamps are required).
     */
    public final boolean isIsolatable() {
        
        return deleteMarkers && versionTimestamps;
        
    }

    public void setIsolatable(boolean isolatable) {

        setDeleteMarkers(isolatable);
        
        setVersionTimestamps(isolatable);
        
    }

    public void setPartitionMetadata(LocalPartitionMetadata pmd) {
        
        this.pmd = pmd;
        
    }

    public void setAddressSerializer(IAddressSerializer addrSer) {
        
        if (addrSer == null)
            throw new IllegalArgumentException();
        
        this.addrSer = addrSer;
        
    }

    public void setNodeKeySerializer(IDataSerializer nodeKeySer) {
        
        if (nodeKeySer == null)
            throw new IllegalArgumentException();
        
        this.nodeKeySer = nodeKeySer;
        
    }

    public void setTupleSerializer(ITupleSerializer tupleSer) {

        if (tupleSer == null)
            throw new IllegalArgumentException();

        this.tupleSer = tupleSer;
        
    }

    /**
     * The branching factor MAY NOT be changed once an {@link AbstractBTree}
     * object has been created.
     * 
     * @param branchingFactor
     */
    public void setBranchingFactor(int branchingFactor) {
        
        if(branchingFactor < Options.MIN_BRANCHING_FACTOR) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.branchingFactor = branchingFactor;
        
    }

    public void setIndexSegmentBranchingFactor(int branchingFactor) {

        if(branchingFactor < Options.MIN_BRANCHING_FACTOR) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.indexSegmentBranchingFactor = branchingFactor;
        
    }

    public void setBTreeClassName(final String className) {

        if (className == null)
            throw new IllegalArgumentException();

        this.btreeClassName = className;
        
    }

    public void setConflictResolver(IConflictResolver conflictResolver) {

        this.conflictResolver = conflictResolver;
        
    }

    /**
     * Return the bloom filter factory.
     * <p>
     * Bloom filters provide fast rejection for point tests in a space efficient
     * manner with a configurable probability of a false positive. Since the
     * bloom filter does not give positive results with 100% certainity, the
     * index is tested iff the bloom filter states that the key exists.
     * <p>
     * Note: Bloom filters are NOT enabled by default since point tests are not
     * a bottleneck (or even used) for some indices. Also, when multiple indices
     * represent different access paths for the same information, you only need
     * a bloom filter on one of those indices.
     * 
     * @return Return the object that will be used to configure an optional
     *         bloom filter for a {@link BTree} or {@link IndexSegment}. When
     *         <code>null</code> the index WILL NOT use a bloom filter.
     * 
     * @see BloomFilterFactory
     * @see BloomFilterFactory#DEFAULT
     */
    public BloomFilterFactory getBloomFilterFactory() {
        
        return bloomFilterFactory;
        
    }
    
    /**
     * Set the bloom filter factory.
     * <p>
     * Bloom filters provide fast rejection for point tests in a space efficient
     * manner with a configurable probability of a false positive. Since the
     * bloom filter does not give positive results with 100% certainity, the
     * index is tested iff the bloom filter states that the key exists.
     * 
     * @param bloomFilterFactory
     *            The new value (may be null).
     * 
     * @see BloomFilterFactory#DEFAULT
     */
    public void setBloomFilterFactory(BloomFilterFactory bloomFilterFactory) {
        
        this.bloomFilterFactory = bloomFilterFactory;
        
    }
    
    /**
     * An optional object that may be used to inspect, and possibly operate on,
     * each index entry as it is copied into an {@link IndexSegment}.
     */
    public IOverflowHandler getOverflowHandler() {
        
        return overflowHandler;
        
    }

    public void setOverflowHandler(IOverflowHandler overflowHandler) {
        
        this.overflowHandler = overflowHandler;
        
    }

    /**
     * Object which decides whether and where to split an index partition into 2
     * or more index partitions. The default is a {@link DefaultSplitHandler}
     * using some reasonable values for its properties. You can override this
     * property in order to tune or constrain the manner in which index split
     * points are choosen.
     * 
     * @return The {@link ISplitHandler}. If <code>null</code> then index
     *         splits will be disabled.
     */
    public ISplitHandler getSplitHandler() {

        return splitHandler;
        
    }
    
    public void setSplitHandler(ISplitHandler splitHandler) {
        
        this.splitHandler = splitHandler;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public IndexMetadata() {
        
    }
    
    /**
     * Constructor used to configure a new <em>unnamed</em> B+Tree. The index
     * UUID is set to the given value and all other fields are defaulted as
     * explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
     * defaults may be overriden using the various setter methods, but some
     * values can not be safely overriden after the index is in use.
     * 
     * @param indexUUID
     *            The indexUUID.
     * 
     * @throws IllegalArgumentException
     *             if the indexUUID is <code>null</code>.
     */
    public IndexMetadata(final UUID indexUUID) {

        this(null, indexUUID);
        
    }
        
    /**
     * Constructor used to configure a new <em>named</em> B+Tree. The index
     * UUID is set to the given value and all other fields are defaulted as
     * explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
     * defaults may be overriden using the various setter methods, but some
     * values can not be safely overriden after the index is in use.
     * 
     * @param name
     *            The index name. When this is a scale-out index, the same
     *            <i>name</i> is specified for each index resource. However
     *            they will be registered on the journal under different names
     *            depending on the index partition to which they belong.
     * 
     * @param indexUUID
     *            The indexUUID. The same index UUID MUST be used for all
     *            component indices in a scale-out index.
     * 
     * @throws IllegalArgumentException
     *             if the indexUUID is <code>null</code>.
     */
    public IndexMetadata(final String name, final UUID indexUUID) {

        this(null, System.getProperties(), name, indexUUID);
    
    }

    /**
     * Constructor used to configure a new <em>named</em> B+Tree. The index
     * UUID is set to the given value and all other fields are defaulted as
     * explained at {@link #getProperty(Properties, String, String, String)}.
     * Those defaults may be overriden using the various setter methods.
     * 
     * @param indexManager
     *            Optional. When given and when the {@link IIndexManager} is a
     *            scale-out {@link IBigdataFederation}, this object will be
     *            used to interpret the {@link Options#INITIAL_DATA_SERVICE}
     *            property.
     * @param properties
     *            Properties object used to overriden the default values for
     *            this {@link IndexMetadata} instance.
     * @param namespace
     *            The index name. When this is a scale-out index, the same
     *            <i>name</i> is specified for each index resource. However
     *            they will be registered on the journal under different names
     *            depending on the index partition to which they belong.
     * @param indexUUID
     *            The indexUUID. The same index UUID MUST be used for all
     *            component indices in a scale-out index.
     * 
     * @throws IllegalArgumentException
     *             if <i>properties</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>indexUUID</i> is <code>null</code>.
     */
    public IndexMetadata(final IIndexManager indexManager,
            final Properties properties, final String namespace,
            final UUID indexUUID) {

        if (indexUUID == null)
            throw new IllegalArgumentException();
        
        this.name = namespace;
        
        this.indexUUID = indexUUID;
        
        this.branchingFactor = getProperty(indexManager, properties, namespace,
                Options.BTREE_BRANCHING_FACTOR,
                Options.DEFAULT_BTREE_BRANCHING_FACTOR,
                new IntegerRangeValidator(Options.MIN_BRANCHING_FACTOR,
                        Options.MAX_BTREE_BRANCHING_FACTOR));

        this.indexSegmentBranchingFactor = getProperty(indexManager,
                properties, namespace, Options.INDEX_SEGMENT_BRANCHING_FACTOR,
                Options.DEFAULT_INDEX_SEGMENT_BRANCHING_FACTOR,
                new IntegerRangeValidator(Options.MIN_BRANCHING_FACTOR,
                        Options.MAX_INDEX_SEGMENT_BRANCHING_FACTOR));

        this.writeRetentionQueueCapacity = getProperty(indexManager,
                properties, namespace, Options.WRITE_RETENTION_QUEUE_CAPACITY,
                Options.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY,
                new IntegerRangeValidator(Options.MIN_WRITE_RETENTION_QUEUE_CAPACITY,
                        Options.MAX_WRITE_RETENTION_QUEUE_CAPACITY));

        this.writeRetentionQueueScan = getProperty(indexManager,
                properties, namespace, Options.WRITE_RETENTION_QUEUE_SCAN,
                Options.DEFAULT_WRITE_RETENTION_QUEUE_SCAN,
                IntegerValidator.GTE_ZERO);

        this.readRetentionQueueCapacity = getProperty(indexManager,
                properties, namespace, Options.READ_RETENTION_QUEUE_CAPACITY,
                Options.DEFAULT_READ_RETENTION_QUEUE_CAPACITY,
                IntegerValidator.GTE_ZERO);

        this.readRetentionQueueScan = getProperty(indexManager,
                properties, namespace, Options.READ_RETENTION_QUEUE_SCAN,
                Options.DEFAULT_READ_RETENTION_QUEUE_SCAN,
                IntegerValidator.GTE_ZERO);

        this.indexSegmentBufferNodes = Boolean.parseBoolean(getProperty(
                indexManager, properties, namespace,
                Options.INDEX_SEGMENT_BUFFER_NODES,
                Options.DEFAULT_INDEX_SEGMENT_BUFFER_NODES));

        this.indexSegmentLeafCacheCapacity = getProperty(indexManager,
                properties, namespace,
                Options.INDEX_SEGMENT_LEAF_CACHE_CAPACITY,
                Options.DEFAULT_INDEX_SEGMENT_LEAF_CACHE_CAPACITY,
                IntegerValidator.GT_ZERO);

        // Note: default assumes NOT an index partition.
        this.pmd = null;
        
        this.btreeClassName = getProperty(indexManager, properties, namespace,
                Options.BTREE_CLASS_NAME, BTree.class.getName().toString());

        this.checkpointClassName = Checkpoint.class.getName();
        
        /*
         * FIXME Experiment with performance using a packed address serializer
         * (or a nibble-based one). This effects how we serialize the child
         * addresses for a Node. (PackedAddressSerialized appears to be broken
         * - see the NodeSerialier test suite).
         */
        this.addrSer = AddressSerializer.INSTANCE;
        
//        this.nodeKeySer = SimplePrefixSerializer.INSTANCE;
        this.nodeKeySer = PrefixSerializer.INSTANCE;
        
        this.tupleSer = DefaultTupleSerializer.newInstance();

        this.conflictResolver = null;
        
        this.deleteMarkers = false;
        
        this.versionTimestamps = false;

        // optional bloom filter setup.
        final boolean bloomFilter = Boolean.parseBoolean(getProperty(
                indexManager, properties, namespace, Options.BLOOM_FILTER,
                Options.DEFAULT_BLOOM_FILTER));
        
        this.bloomFilterFactory = bloomFilter ? BloomFilterFactory.DEFAULT
                : null;
  
        // Note: by default there is no overflow handler.
        this.overflowHandler = null;

        // split handler setup (used iff scale-out).
        {
            
            final int minimumEntryCount = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.SPLIT_HANDLER_MIN_ENTRY_COUNT,
                    Options.DEFAULT_SPLIT_HANDLER_MIN_ENTRY_COUNT));

            final int entryCountPerSplit = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT,
                    Options.DEFAULT_SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT));

            final double overCapacityMultiplier = Double.parseDouble(getProperty(
                    indexManager, properties, namespace,
                    Options.SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER,
                    Options.DEFAULT_SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER));

            final double underCapacityMultiplier = Double.parseDouble(getProperty(
                    indexManager, properties, namespace,
                    Options.SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER,
                    Options.DEFAULT_SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER));

            final int sampleRate = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.SPLIT_HANDLER_SAMPLE_RATE,
                    Options.DEFAULT_SPLIT_HANDLER_SAMPLE_RATE));

            this.splitHandler = new DefaultSplitHandler(//
                    minimumEntryCount, //
                    entryCountPerSplit, //
                    overCapacityMultiplier, //
                    underCapacityMultiplier, //
                    sampleRate //
            );
        
        }

    }

    /**
     * Write out the metadata record for the btree on the store and return the
     * address.
     * 
     * @param store
     *            The store on which the metadata record is being written.
     * 
     * @return The address of the metadata record is set on this object as a
     *         side effect.
     * 
     * @throws IllegalStateException
     *             if the record has already been written on the store.
     * @throws IllegalStateException
     *             if the {@link #indexUUID} field is <code>null</code> - this
     *             generally indicates that you used the de-serialization
     *             constructor rather than one of the constructor variants that
     *             accept the required UUID parameter.
     */
    public void write(final IRawStore store) {

        if (addrMetadata != 0L) {

            throw new IllegalStateException("Already written.");
            
        }

        if (indexUUID == null) {
            
            throw new IllegalStateException("No indexUUID : wrong constructor?");
            
        }

        // write on the store, setting address as side-effect.
        this.addrMetadata = store.write(ByteBuffer.wrap(SerializerUtil
                .serialize(this)));

    }

    /**
     * Read the metadata record from the store.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the metadata record.
     * 
     * @return the metadata record. The address from which it was loaded is set
     *         on the metadata record as a side-effect.
     */
    public static IndexMetadata read(final IRawStore store, final long addr) {
        
        final IndexMetadata metadata = (IndexMetadata) SerializerUtil
                .deserialize(store.read(addr));
        
        // save the address from which the metadata record was loaded.
        metadata.addrMetadata = addr;
        
        return metadata;
        
    }

    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();

        // transient
        sb.append("addrMetadata=" + addrMetadata);

        // persistent
        sb.append(", name=" + (name == null ? "N/A" : name));
        sb.append(", indexUUID=" + indexUUID);
        sb.append(", branchingFactor=" + branchingFactor);
        sb.append(", indexSegmentBranchingFactor=" + indexSegmentBranchingFactor);
        sb.append(", pmd=" + pmd);
        sb.append(", class=" + btreeClassName);
        sb.append(", checkpointClass=" + checkpointClassName);
        sb.append(", childAddrSerializer=" + addrSer.getClass().getName());
        sb.append(", nodeKeySerializer=" + nodeKeySer.getClass().getName());
        sb.append(", tupleSerializer=" + tupleSer.getClass().getName());
        sb.append(", conflictResolver="
                + (conflictResolver == null ? "N/A" : conflictResolver
                        .getClass().getName()));
        sb.append(", deleteMarkers=" + deleteMarkers);
        sb.append(", versionTimestamps=" + versionTimestamps);
        sb.append(", isolatable=" + isIsolatable());
        sb.append(", bloomFilterFactory=" + (bloomFilterFactory == null ? "N/A"
                : bloomFilterFactory.toString())); 
        sb.append(", overflowHandler="
                + (overflowHandler == null ? "N/A" : overflowHandler.getClass()
                        .getName()));
        sb.append(", splitHandler="
                + (splitHandler == null ? "N/A" : splitHandler.getClass()
                        .getName()));

        return sb.toString();
        
    }

    /**
     * The initial version.
     */
    private static transient final int VERSION0 = 0x0;
    
    /**
     * @todo review generated record for compactness.
     */
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final int version = (int) LongPacker.unpackLong(in);

        if (version != VERSION0) {

            throw new IOException("Unknown version: version=" + version);

        }

        final boolean hasName = in.readBoolean();

        if (hasName) {

            name = in.readUTF();

        }
        
        indexUUID = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);

        branchingFactor = (int)LongPacker.unpackLong(in);

        writeRetentionQueueCapacity = (int)LongPacker.unpackLong(in);
        
        writeRetentionQueueScan = (int)LongPacker.unpackLong(in);
        
        readRetentionQueueCapacity = (int)LongPacker.unpackLong(in);
        
        readRetentionQueueScan = (int)LongPacker.unpackLong(in);

        pmd = (LocalPartitionMetadata)in.readObject();
        
        btreeClassName = in.readUTF();
        
        checkpointClassName = in.readUTF();
        
        addrSer = (IAddressSerializer)in.readObject();
        
        nodeKeySer = (IDataSerializer)in.readObject();

        tupleSer = (ITupleSerializer)in.readObject();
        
        conflictResolver = (IConflictResolver)in.readObject();
        
        deleteMarkers = in.readBoolean();
        
        versionTimestamps = in.readBoolean();

        bloomFilterFactory = (BloomFilterFactory) in.readObject();

        overflowHandler = (IOverflowHandler)in.readObject();

        splitHandler = (ISplitHandler)in.readObject();

        /*
         * IndexSegment.
         */

        indexSegmentBranchingFactor = (int)LongPacker.unpackLong(in);

        indexSegmentLeafCacheCapacity = (int)LongPacker.unpackLong(in);
        
        indexSegmentBufferNodes = in.readBoolean();

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        LongPacker.packLong(out, VERSION0);

        // hasName?
        out.writeBoolean(name != null ? true : false);
        
        // the name
        if (name != null) {

            out.writeUTF(name);
            
        }
        
        out.writeLong(indexUUID.getMostSignificantBits());
        
        out.writeLong(indexUUID.getLeastSignificantBits());
        
        LongPacker.packLong(out, branchingFactor);

        LongPacker.packLong(out, writeRetentionQueueCapacity);

        LongPacker.packLong(out, writeRetentionQueueScan);
        
        LongPacker.packLong(out, readRetentionQueueCapacity);
       
        LongPacker.packLong(out, readRetentionQueueScan);

        out.writeObject(pmd);
        
        out.writeUTF(btreeClassName);

        out.writeUTF(checkpointClassName);
        
        out.writeObject(addrSer);
        
        out.writeObject(nodeKeySer);

        out.writeObject(tupleSer);
        
        out.writeObject(conflictResolver);

        out.writeBoolean(deleteMarkers);
        
        out.writeBoolean(versionTimestamps);

        out.writeObject(bloomFilterFactory);
        
        out.writeObject(overflowHandler);

        out.writeObject(splitHandler);

        /*
         * IndexSegment.
         */

        LongPacker.packLong(out, indexSegmentBranchingFactor);

        LongPacker.packLong(out, indexSegmentLeafCacheCapacity);
        
        out.writeBoolean(indexSegmentBufferNodes);
        
    }

    /**
     * Makes a copy of the persistent data, clearing the 
     */
    public IndexMetadata clone() {
        
        try {

            final IndexMetadata copy = (IndexMetadata) super.clone();
            
            copy.addrMetadata = 0L;
            
            return copy;
            
        } catch (CloneNotSupportedException e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Create an initial {@link Checkpoint} for a new {@link BTree} described by
     * this metadata record.
     * <p>
     * The caller is responsible for writing the {@link Checkpoint} record onto
     * the store.
     * <p>
     * The class identified by {@link #getCheckpointClassName()} MUST declare a
     * public constructor with the following method signature
     * 
     * <pre>
     *  ...( IndexMetadata metadata )
     * </pre>
     * 
     * @return The {@link Checkpoint}.
     */
    final public Checkpoint firstCheckpoint() {

        try {
            
            Class cl = Class.forName(getCheckpointClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor.
             */
            
            Constructor ctor = cl.getConstructor(new Class[] {
                    IndexMetadata.class//
                    });

            Checkpoint checkpoint = (Checkpoint) ctor.newInstance(new Object[] { //
                    this //
                    });
            
            return checkpoint;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }

    }
    
    /**
     * Variant used when an index overflows onto a new backing store.
     * <p>
     * The caller is responsible for writing the {@link Checkpoint} record onto
     * the store.
     * <p>
     * The class identified by {@link #getCheckpointClassName()} MUST declare a
     * public constructor with the following method signature
     * 
     * <pre>
     *  ...( IndexMetadata metadata, Checkpoint oldCheckpoint )
     * </pre>
     * 
     * @param oldCheckpoint
     *            The last checkpoint for the index of the old backing store.
     * 
     * @return The first {@link Checkpoint} for the index on the new backing
     *         store.
     * 
     * @throws IllegalArgumentException
     *             if the oldCheckpoint is <code>null</code>.
     */
    final public Checkpoint overflowCheckpoint(Checkpoint oldCheckpoint) {
       
        if (oldCheckpoint == null) {
         
            throw new IllegalArgumentException();
            
        }
        
        try {
            
            Class cl = Class.forName(getCheckpointClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor.
             */
            
            Constructor ctor = cl.getConstructor(new Class[] {
                    IndexMetadata.class, //
                    Checkpoint.class//
                    });

            Checkpoint checkpoint = (Checkpoint) ctor.newInstance(new Object[] { //
                    this, //
                    oldCheckpoint//
                    });
            
            // sanity check makes sure the counter is propagated to the new store.
            assert checkpoint.getCounter() == oldCheckpoint.getCounter();
            
            return checkpoint;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Create a {@link Checkpoint} for a {@link BTree}.
     * <p>
     * The caller is responsible for writing the {@link Checkpoint} record onto
     * the store.
     * <p>
     * The class identified by {@link #getCheckpointClassName()} MUST declare a
     * public constructor with the following method signature
     * 
     * <pre>
     *   ...( BTree btree )
     * </pre>
     * 
     * @param btree
     *            The {@link BTree}.
     * 
     * @return The {@link Checkpoint}.
     */
    final public Checkpoint newCheckpoint(BTree btree) {
        
        try {
            
            Class cl = Class.forName(getCheckpointClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor.
             */
            
            Constructor ctor = cl.getConstructor(new Class[] {
                    BTree.class //
                    });

            Checkpoint checkpoint = (Checkpoint) ctor.newInstance(new Object[] { //
                    btree //
                    });
            
            return checkpoint;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * <p>
     * Factory for thread-safe {@link IKeyBuilder} objects for use by
     * {@link ITupleSerializer#serializeKey(Object)} and possibly others.
     * </p>
     * <p>
     * Note: A mutable B+Tree is always single-threaded. However, read-only
     * B+Trees allow concurrent readers. Therefore, thread-safety requirement is
     * <em>safe for either a single writers -or- for concurrent readers</em>.
     * </p>
     * <p>
     * Note: If you change this value in a manner that is not backward
     * compatable once entries have been written on the index then you may be
     * unable to any read data already written.
     * </p>
     * <p>
     * Note: This method delegates to {@link ITupleSerializer#getKeyBuilder()}.
     * This {@link IKeyBuilder} SHOULD be used to form all keys for <i>this</i>
     * index. This is critical for indices that have Unicode data in their
     * application keys as the formation of Unicode sort keys from Unicode data
     * depends on the {@link IKeyBuilderFactory}. If you use a locally
     * configured {@link IKeyBuilder} then your Unicode keys will be encoded
     * based on the {@link Locale} configured for the JVM NOT the factory
     * specified for <i>this</i> index.
     * </p>
     */
    public IKeyBuilder getKeyBuilder() {

        return getTupleSerializer().getKeyBuilder();
        
    }
    
    /**
     * @see Configuration#getProperty(IIndexManager, Properties, String, String,
     *      String)
     */
    protected String getProperty(final IIndexManager indexManager,
            final Properties properties, final String namespace,
            final String globalName, final String defaultValue) {

        return Configuration.getProperty(indexManager, properties, namespace,
                globalName, defaultValue);

    }

    /**
     * @see Configuration#getProperty(IIndexManager, Properties, String, String,
     *      String, IValidator)
     */
    protected <E> E getProperty(final IIndexManager indexManager,
            final Properties properties, final String namespace,
            final String globalName, final String defaultValue,
            IValidator<E> validator) {

        return Configuration.getProperty(indexManager, properties, namespace,
                globalName, defaultValue, validator);

    }

}
