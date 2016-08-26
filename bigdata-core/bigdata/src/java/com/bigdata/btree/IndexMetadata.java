/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.isolation.IConflictResolver;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder.DefaultFrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoderDupKeys;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.view.FusedView;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerRangeValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.LongPacker;
import com.bigdata.io.SerializerUtil;
import com.bigdata.io.compression.IRecordCompressorFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.OverflowManager;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ndx.pipeline.AbstractSubtask;
import com.bigdata.sparse.SparseRowStore;

/**
 * <p>
 * The persistent and mostly immutable metadata for a {@link AbstractBTree}.
 * This class allows you to configured several very important aspects of the
 * B+Tree (and other persistence capable data structures) behavior. Read on.
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
 * order over the resources gives us the guarantee that any index entry in A
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
 * between the earliest running tx and the chosen "purge time". When the
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
    
    private static final transient Logger log = Logger
            .getLogger(IndexMetadata.class);

    /**
     * Options and their defaults for the {@link com.bigdata.btree} package and
     * the {@link BTree} and {@link IndexSegment} classes. Options that apply
     * equally to views and {@link AbstractBTree}s are in the package namespace,
     * such as whether or not a bloom filter is enabled. Options that apply to
     * all {@link AbstractBTree}s are specified within that namespace while
     * those that are specific to {@link BTree} or {@link IndexSegment} are
     * located within their respective class namespaces. Some properties, such
     * as the branchingFactor, are defined for both the {@link BTree} and the
     * {@link IndexSegment} because their defaults tend to be different when an
     * {@link IndexSegment} is generated from an {@link BTree}.
     * 
     * @todo It should be possible to specify the key, value, and node/leaf
     *       coders via this interface. This is easy enough if there is a
     *       standard factory interface, since we can specify the class name,
     *       and more difficult if we need to create an instance.
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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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
        int MAX_BTREE_BRANCHING_FACTOR = 4196;

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
         * A large maximum write retention queue capacity. A reasonable value
         * with a large heap is generally in 4000 to 8000, depending on the
         * branching factor. The impact on the JVM heap is a function of both
         * the write retention queue capacity and the B+Tree branching factor.
         * Larger values are of benefit if you are doing sustained writes on the
         * index and have a large java heap (and even then, GC will probably
         * prevent values larger than 10000 from being useful).
         */
        int MAX_WRITE_RETENTION_QUEUE_CAPACITY = 50000;
        
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
         * after its error rate would be too large given the #of index entries.
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
        String BLOOM_FILTER = (com.bigdata.btree.BTree.class.getPackage()
                .getName()
                + ".bloomFilter").intern();
        
        String DEFAULT_BLOOM_FILTER = "false";

		/**
		 * When raw record support is enabled for the index, this is the maximum
		 * length of an index value which will be stored within a leaf before it
		 * is automatically promoted to a raw record reference on the backing
		 * store (default {@value #DEFAULT_MAX_REC_LEN}).
		 * 
		 * @see IndexMetadata#getRawRecords()
		 * @see IndexMetadata#getMaxRecLen()
		 */
		String MAX_REC_LEN = (com.bigdata.btree.BTree.class.getPackage()
				.getName() + ".maxRecLen").intern();

		String DEFAULT_MAX_REC_LEN = "256";
        
        /**
         * The name of an optional property whose value identifies the data
         * service on which the initial index partition of a scale-out index
         * will be created. The value may be the {@link UUID} of that data
         * service (this is unambiguous) of the name associated with the data
         * service (it is up to the administrator to not assign the same name to
         * different data service instances and an arbitrary instance having the
         * desired name will be used if more than one instance is assigned the
         * same name). The default behavior is to select a data service using
         * the load balancer, which is done automatically by
         * {@link IBigdataFederation#registerIndex(IndexMetadata, UUID)} if
         * {@link IndexMetadata#getInitialDataServiceUUID()} returns
         * <code>null</code>.
         */
        // note: property applies to views so namespace is the package.
        String INITIAL_DATA_SERVICE = com.bigdata.btree.BTree.class
                .getPackage().getName()
                + ".initialDataService";

        /**
         * The capacity of the hard reference queue used to retain recently
         * touched nodes (nodes or leaves) and to defer the eviction of dirty
         * nodes (nodes or leaves).
         * <p>
         * The purpose of this queue is to retain recently touched nodes and
         * leaves and to defer eviction of dirty nodes and leaves in case they
         * will be modified again soon. Once a node falls off the write
         * retention queue it is checked to see if it is dirty. If it is dirty,
         * then it is serialized and persisted on the backing store. If the
         * write retention queue capacity is set to a large value (say, GTE
         * 1000), then that will will increase the commit latency and have a
         * negative effect on the overall performance. Too small a value will
         * mean that nodes that are undergoing mutation will be serialized and
         * persisted prematurely leading to excessive writes on the backing
         * store. For append-only stores, this directly contributes to what are
         * effectively redundant and thereafter unreachable copies of the
         * intermediate state of nodes as only nodes that can be reached by
         * navigation from a {@link Checkpoint} will ever be read again. The
         * value <code>500</code> appears to be a good default. While it is
         * possible that some workloads could benefit from a larger value, this
         * leads to higher commit latency and can therefore have a broad impact
         * on performance.
         * <p>
         * Note: The write retention queue is used for both {@link BTree} and
         * {@link IndexSegment}. Any touched node or leaf is placed onto this
         * queue. As nodes and leaves are evicted from this queue, they are then
         * placed onto the optional read-retention queue.
         * <p>
         * The default value is a function of the JVM heap size. For small
         * heaps, it is {@value #DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY}. For
         * larger heaps the value may be 8000 (1G), or 20000 (10G). These larger
         * defaults are heuristics. Values larger than 8000 benefit the size of
         * disk of the journal, while values up to 8000 can also improve
         * throughput dramatically. Larger values are ONLY useful if the
         * application is performing sustained writes on the index (hundreds of
         * thousands to millions of records).
         */
        String WRITE_RETENTION_QUEUE_CAPACITY = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".writeRetentionQueue.capacity").intern();

        /**
         * The #of entries on the write retention queue that will be scanned for
         * a match before a new reference is appended to the queue. This trades
         * off the cost of scanning entries on the queue, which is handled by
         * the queue itself, against the cost of queue churn. Note that queue
         * eviction drives IOs required to write the leaves on the store, but
         * incremental writes occur iff the {@link AbstractNode#referenceCount}
         * is zero and the node or leaf is dirty.
         */
        String WRITE_RETENTION_QUEUE_SCAN = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".writeRetentionQueue.scan").intern();

        String DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY = "500";// was 500

        String DEFAULT_WRITE_RETENTION_QUEUE_SCAN = "20";

        /**
         * Override the {@link IKeyBuilderFactory} used by the
         * {@link DefaultTupleSerializer} (the default is a
         * {@link DefaultKeyBuilderFactory} initialized with an empty
         * {@link Properties} object).
         * 
         * FIXME {@link KeyBuilder} configuration support is not finished.
         */
        String KEY_BUILDER_FACTORY = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + "keyBuilderFactory").intern();

        /**
         * Override the {@link IRabaCoder} used for the keys in the nodes of a
         * B+Tree (the default is a {@link FrontCodedRabaCoder} instance).
         */
        String NODE_KEYS_CODER = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + "nodeKeysCoder").intern();

        /**
         * Override the {@link IRabaCoder} used for the keys of leaves in
         * B+Trees (the default is a {@link FrontCodedRabaCoder} instance).
         * 
         * @see DefaultTupleSerializer#setLeafKeysCoder(IRabaCoder)
         */
        String LEAF_KEYS_CODER = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".leafKeysCoder").intern();

        /**
         * Override the {@link IRabaCoder} used for the values of leaves in
         * B+Trees (default is a {@link CanonicalHuffmanRabaCoder}).
         * 
         * @see DefaultTupleSerializer#setLeafValuesCoder(IRabaCoder)
         */
        String LEAF_VALUES_CODER = (com.bigdata.btree.AbstractBTree.class
                .getPackage().getName()
                + ".leafValuesCoder").intern();

//        /**
//         * Option determines whether or not per-child locks are used by
//         * {@link Node} for a <em>read-only</em> {@link AbstractBTree} (default
//         * {@value #DEFAULT_CHILD_LOCKS}). This option effects synchronization
//         * in {@link Node#getChild(int)}. Synchronization is not required for
//         * mutable {@link BTree}s as they already impose the constraint that the
//         * caller is single threaded. Synchronization is required in this method
//         * to ensure that the data structure remains coherent when concurrent
//         * threads demand access to the same child of a given {@link Node}.
//         * Per-child locks have higher potential concurrency since locking is
//         * done on a distinct {@link Object} for each child rather than on a
//         * shared {@link Object} for all children of a given {@link Node}.
//         * However, per-child locks require more {@link Object} allocation (for
//         * the locks) and thus contribute to heap demand.
//         * <p>
//         * Note: While this can improve read concurrency, this option imposes
//         * additional RAM demands since there is on {@link Object} allocated for
//         * each {@link Node} in the {@link BTree}.  This is why it is turned off
//         * by default.
//         */
//        String CHILD_LOCKS = com.bigdata.btree.AbstractBTree.class.getPackage()
//                .getName()
//                + ".childLocks";
//
//        String DEFAULT_CHILD_LOCKS = "false";
        
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
        String BTREE_CLASS_NAME = (BTree.class.getName()+".className").intern();
        
        /**
         * The name of an optional property whose value specifies the branching
         * factor for a mutable {@link BTree}.
         * 
         * @see #DEFAULT_BTREE_BRANCHING_FACTOR
         * @see #INDEX_SEGMENT_BRANCHING_FACTOR
         */
        String BTREE_BRANCHING_FACTOR = (BTree.class.getName()+".branchingFactor").intern();

        /**
         * The default branching factor for a mutable {@link BTree}.
         * <p>
         * Note: on 9/11/2009 I changed the default B+Tree branching factor and
         * write retention queue capacity to 64 (was 32) and 8000 (was 500)
         * respectively. This change in the B+Tree branching factor reduces the
         * height of B+Trees on the Journal, increases the size of the
         * individual records on the disk, and aids performance substantially.
         * The larger write retention queue capacity helps to prevent B+Tree
         * nodes and leaves from being coded and flushed to disk too soon, which
         * decreases disk IO and keeps things in their mutable form in memory
         * longer, which improves search performance and keeps down the costs of
         * mutation operations. [Dropped back to 32/500 on 9/15/09 since
         * this does not do so well at scale on machines with less RAM.]
         */
        String DEFAULT_BTREE_BRANCHING_FACTOR = "32"; //"256"

//        /**
//         * The capacity of the hard reference queue used to retain recently used
//         * nodes (or leaves) (default
//         * {@value #DEFAULT_BTREE_READ_RETENTION_QUEUE_CAPACITY}). When zero
//         * (0), this queue is disabled.
//         * <p>
//         * The read retention queue complements the write retention queue. The
//         * latter has a strong buffering effect, but we can not increase the
//         * size of the write retention queue without bound as that will increase
//         * the commit latency. However, the read retention queue can be quite
//         * large and will "simply" buffer "recently" used nodes and leaves in
//         * memory. This can have a huge effect, especially when a complex
//         * high-level query would otherwise thrash the disk as nodes that are
//         * required for query processing fall off of the write retention queue
//         * and get garbage collected. The pragmatic upper bound for this
//         * probably depends on the index workload. At some point, you will stop
//         * seeing an increase in performance as a function of the read retention
//         * queue for a given workload. The larger the read retention queue, the
//         * more burden the index can impose on the heap. However, copy-on-write
//         * explicitly clears all references in a node so the JVM can collect the
//         * data for nodes that are no longer part of the index before they fall
//         * off of the queue even if it can not collect the node reference
//         * itself.
//         * <p>
//         * A large values works well for scale-up but you <i>might</i> need to
//         * reduce the read retention queue capacity since if you expect to have
//         * a large #of smaller indices open, e.g., for scale-out scenarios. Zero
//         * will disable the read-retention queue. This queue ONLY applies to
//         * {@link BTree}s (vs {@link IndexSegment}s).
//         * 
//         * @todo The size of the read retention queue should be set dynamically
//         *       as a function of the depth of the BTree (or the #of nodes and
//         *       leaves), the branching factor, and the RAM available to the
//         *       HOST (w/o swapping) and to the JVM. For a mutable {@link BTree}
//         *       the depth changes only slowly, but the other factors are always
//         *       changing. Regardless, changing the read-retention queue size is
//         *       never a problem as cleared references will never cause a
//         *       strongly reachable node to be released.
//         *       <p>
//         *       To avoid needless effort, there should be a minimum queue
//         *       capacity that is used up to depth=2/3. If the queue capacity is
//         *       set to n=~5-10% of the maximum possible #of nodes in a btree of
//         *       a given depth, then we can compute the capacity dynamically
//         *       based on that parameter. And of course it can be easily
//         *       provisioned when the BTree is {@link #reopen()}ed.
//         */
//        String BTREE_READ_RETENTION_QUEUE_CAPACITY = com.bigdata.btree.BTree.class
//                .getPackage().getName()
//                + ".readRetentionQueue.capacity";
//
//        String DEFAULT_BTREE_READ_RETENTION_QUEUE_CAPACITY = "10000";
//
//        /**
//         * The #of entries on the hard reference queue that will be scanned for
//         * a match before a new reference is appended to the queue. This trades
//         * off the cost of scanning entries on the queue, which is handled by
//         * the queue itself, against the cost of queue churn.
//         */
//        String BTREE_READ_RETENTION_QUEUE_SCAN = com.bigdata.btree.BTree.class
//                .getPackage().getName()
//                + ".readRetentionQueue.scan";
//
//        String DEFAULT_BTREE_READ_RETENTION_QUEUE_SCAN = "20";

        /**
         * An optional factory providing record-level compression for the nodes
         * and leaves of an {@link IndexSegment} (default
         * {@value #DEFAULT_BTREE_RECORD_COMPRESSOR_FACTORY}).
         * 
         * @see #INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY
         * 
         * FIXME Record level compression support is not finished.
         */
        String BTREE_RECORD_COMPRESSOR_FACTORY = (BTree.class.getName()
                + ".recordCompressorFactory").intern();

        /**
         * 
         * @see #BTREE_RECORD_COMPRESSOR_FACTORY
         */
        String DEFAULT_BTREE_RECORD_COMPRESSOR_FACTORY = null;
        
        /**
         * The maximum number of threads that will be used to evict dirty nodes
         * or leaves in a given level of a persistence capable index (BTree or
         * HTree). Parallel level set eviction can reduce the latency required
         * to checkpoint a large index when a large bulk update has been
         * applied.
         * <p>
         * Note: This is currently a System property (set with -D).
         *
         * @see https://jira.blazegraph.com/browse/BLZG-1665 (Reduce commit
         *      latency by parallel checkpoint by level of dirty pages in an
         *      index)
         */
        String MAX_PARALLEL_EVICT_THREADS = BTree.class.getName()+".maxParallelEvictThreads";
        String DEFAULT_MAX_PARALLEL_EVICT_THREADS = "10";
        
        /**
         * The minimum number of dirty nodes or leaves in a given level of the
         * index (BTree or HTree) before parallel eviction will be used. You may
         * set this value to a {@link Integer#MAX_VALUE} to disable parallel
         * level set eviction.
         * <p>
         * Note: This is currently a System property (set with -D).
         *
         * @see #MAX_PARALLEL_EVICT_THREADS
         * @see https://jira.blazegraph.com/browse/BLZG-1665 (Reduce commit
         *      latency by parallel checkpoint by level of dirty pages in an
         *      index)
         */
        String MIN_DIRTY_LIST_SIZE_FOR_PARALLEL_EVICT = BTree.class.getName()+".minDirtyListSizeForParallelEvict";
        String DEFAULT_MIN_DIRTY_LIST_SIZE_FOR_PARALLEL_EVICT = "5";
 
        
        /*
         * Options that are specific to IndexSegment.
         * 
         * Note: These options are in the IndexSegment namespace.
         */
        
        /**
         * The name of the property whose value specifies the branching factory
         * for an immutable {@link IndexSegment}.
         */
        String INDEX_SEGMENT_BRANCHING_FACTOR = (IndexSegment.class
                .getName()
                + ".branchingFactor").intern();

        /**
         * The default branching factor for an {@link IndexSegment}.
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
         * 
         * @todo should be on by default? (but verify that the unit tests do
         * not run out of memory when it is enabled by default).
         */
        String INDEX_SEGMENT_BUFFER_NODES = (IndexSegment.class.getName()
                + ".bufferNodes").intern();
        
        /**
         * @see #INDEX_SEGMENT_BUFFER_NODES
         */
        String DEFAULT_INDEX_SEGMENT_BUFFER_NODES = "false";
     
//        /**
//         * The size of the LRU cache backing the weak reference cache for leaves
//         * (default {@value #DEFAULT_INDEX_SEGMENT_LEAF_CACHE_CAPACITY}).
//         * <p>
//         * While the {@link AbstractBTree} already provides caching for nodes
//         * and leaves based on navigation down the hierarchy from the root node,
//         * the {@link IndexSegment} uses an additional leaf cache to optimize
//         * access to leaves based on the double-linked list connecting the
//         * leaves.
//         * <p>
//         * A larger value will tend to retain leaves longer at the expense of
//         * consuming more RAM when many parts of the {@link IndexSegment} are
//         * hot.
//         */
//        String INDEX_SEGMENT_LEAF_CACHE_CAPACITY = IndexSegment.class.getName()
//                + ".leafCacheCapacity";
//
//        /**
//         * 
//         * @see #INDEX_SEGMENT_LEAF_CACHE_CAPACITY
//         */
//        String DEFAULT_INDEX_SEGMENT_LEAF_CACHE_CAPACITY = "100";
//
//        /**
//         * The timeout in nanoseconds for the LRU cache backing the weak
//         * reference cache for {@link IndexSegment} leaves (default
//         * {@value #DEFAULT_INDEX_SEGMENT_LEAF_CACHE_TIMEOUT}).
//         * <p>
//         * While the {@link AbstractBTree} already provides caching for nodes
//         * and leaves based on navigation down the hierarchy from the root node,
//         * the {@link IndexSegment} uses an additional leaf cache to optimize
//         * access to leaves based on the double-linked list connecting the
//         * leaves.
//         * <p>
//         * A larger value will tend to retain leaves longer at the expense of
//         * consuming more RAM when many parts of the {@link IndexSegment} are
//         * hot.
//         */
//        String INDEX_SEGMENT_LEAF_CACHE_TIMEOUT = IndexSegment.class.getName()
//                + ".leafCacheTimeout";
//
//        /**
//         * 
//         * @see #INDEX_SEGMENT_LEAF_CACHE_TIMEOUT
//         */
//        String DEFAULT_INDEX_SEGMENT_LEAF_CACHE_TIMEOUT = ""
//                + TimeUnit.SECONDS.toNanos(30);

        /**
         * An optional factory providing record-level compression for the nodes
         * and leaves of an {@link IndexSegment} (default
         * {@value #DEFAULT_INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY}).
         * 
         * @see #BTREE_RECORD_COMPRESSOR_FACTORY
         * 
         * FIXME Record level compression support is not finished.
         */
        String INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY = (IndexSegment.class.getName()
                + ".recordCompressorFactory").intern();

        /**
         * 
         * @see #INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY
         */
        String DEFAULT_INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY = null;
        
        /*
         * Split handler properties.
         */

//        * @see DefaultSplitHandler
//        * 
//        * Note: Use these settings to trigger splits sooner and thus enter the
//        * more interesting regions of the phase space more quickly BUT DO NOT
//        * use these settings for deployment!
//        * 
//        * final int minimumEntryCount = 1 * Bytes.kilobyte32; (or 10k)
//        * 
//        * final int entryCountPerSplit = 5 * Bytes.megabyte32; (or 50k)
//        /**
//         * An index partition which has no more than this many tuples should be
//         * joined with its rightSibling (if any).
//         */
//        String SPLIT_HANDLER_MIN_ENTRY_COUNT = DefaultSplitHandler.class
//                .getName()
//                + ".minimumEntryCount";
//
//        /**
//         * The target #of tuples for an index partition.
//         */
//        String SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT = DefaultSplitHandler.class
//                .getName()
//                + ".entryCountPerSplit";
//
//        /**
//         * The index partition will be split when its actual entry count is GTE
//         * to <code>overCapacityMultiplier * entryCountPerSplit</code>
//         */
//        String SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER = DefaultSplitHandler.class
//                .getName()
//                + ".overCapacityMultiplier";
//
//        /**
//         * When an index partition will be split, the #of new index partitions
//         * will be chosen such that each index partition is approximately
//         * <i>underCapacityMultiplier</i> full.
//         */
//        String SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER = DefaultSplitHandler.class
//                .getName()
//                + ".underCapacityMultiplier";
//
//        /**
//         * The #of samples to take per estimated split (non-negative, and
//         * generally on the order of 10s of samples). The purpose of the samples
//         * is to accommodate the actual distribution of the keys in the index.
//         */
//        String SPLIT_HANDLER_SAMPLE_RATE = DefaultSplitHandler.class.getName()
//                + ".sampleRate";
//
//        String DEFAULT_SPLIT_HANDLER_MIN_ENTRY_COUNT = ""
//                + (500 * Bytes.kilobyte32);
//
//        String DEFAULT_SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT = ""
//                + (1 * Bytes.megabyte32);
//
//        String DEFAULT_SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER = "1.5";
//
//        String DEFAULT_SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER = ".75";
//
//        String DEFAULT_SPLIT_HANDLER_SAMPLE_RATE = "20"; 
        
        /*
         * Asynchronous index write API.
         */

        /**
         * The capacity of the queue on which the application writes. Chunks are
         * drained from this queue by the {@link AbstractTaskMaster}, broken
         * into splits, and each split is written onto the
         * {@link AbstractSubtask} sink handling writes for the associated index
         * partition.
         */
        String MASTER_QUEUE_CAPACITY = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".masterQueueCapacity").intern();

        String DEFAULT_MASTER_QUEUE_CAPACITY = "5000";

        /**
         * The desired size of the chunks that the master will draw from its
         * queue.
         */
        String MASTER_CHUNK_SIZE = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".masterChunkSize").intern();

        String DEFAULT_MASTER_CHUNK_SIZE = "10000";

        /**
         * The time in nanoseconds that the master will combine smaller chunks
         * so that it can satisfy the desired <i>masterChunkSize</i>.
         */
        String MASTER_CHUNK_TIMEOUT_NANOS = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".masterChunkTimeoutNanos").intern();

        String DEFAULT_MASTER_CHUNK_TIMEOUT_NANOS = ""
                + TimeUnit.MILLISECONDS.toNanos(50);

        /**
         * The time in nanoseconds that the {@link AbstractSubtask sink} will
         * wait inside of the {@link IAsynchronousIterator} when it polls the
         * iterator for a chunk. This value should be relatively small so that
         * the sink remains responsible rather than blocking inside of the
         * {@link IAsynchronousIterator} for long periods of time.
         */
        String SINK_POLL_TIMEOUT_NANOS = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".sinkPollTimeoutNanos").intern();

        String DEFAULT_SINK_POLL_TIMEOUT_NANOS = ""
                + TimeUnit.MILLISECONDS.toNanos(50);

        /**
         * The capacity of the internal queue for the per-sink output buffer.
         */
        String SINK_QUEUE_CAPACITY = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".sinkQueueCapacity").intern();

        String DEFAULT_SINK_QUEUE_CAPACITY = "5000";

        /**
         * The desired size of the chunks written that will be written by the
         * {@link AbstractSubtask sink}.
         */
        String SINK_CHUNK_SIZE = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".sinkChunkSize").intern();

        String DEFAULT_SINK_CHUNK_SIZE = "10000";

        /**
         * The maximum amount of time in nanoseconds that a sink will combine
         * smaller chunks so that it can satisfy the desired <i>sinkChunkSize</i>
         * (default {@value #DEFAULT_SINK_CHUNK_TIMEOUT_NANOS}). The default is
         * an infinite timeout. This means that the sink will simply wait until
         * {@link #SINK_CHUNK_SIZE} elements have accumulated before writing on
         * the index partition. This makes it much easier to adjust the
         * performance since you simply adjust the {@link #SINK_CHUNK_SIZE}.
         */
        String SINK_CHUNK_TIMEOUT_NANOS = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".sinkChunkTimeoutNanos").intern();

        String DEFAULT_SINK_CHUNK_TIMEOUT_NANOS = "" + Long.MAX_VALUE;

        /**
         * The time in nanoseconds after which an idle sink will be closed
         * (default {@value #DEFAULT_SINK_IDLE_TIMEOUT_NANOS}). Any buffered
         * writes are flushed when the sink is closed. The idle timeout is reset
         * (a) if a chunk is available to be drained by the sink; or (b) if a
         * chunk is drained from the sink. If no chunks become available the the
         * sink will eventually decide that it is idle, will flush any buffered
         * writes, and will close itself.
         * <p>
         * If the idle timeout is LT the {@link #SINK_CHUNK_TIMEOUT_NANOS} then
         * a sink will remain open as long as new chunks appear and are combined
         * within idle timeout, otherwise the sink will decide that it is idle
         * and will flush its last chunk and close itself. If this is
         * {@link Long#MAX_VALUE} then the sink will identify itself as idle and
         * will only be closed if the master is closed or the sink has received
         * a {@link StaleLocatorException} for the index partition on which the
         * sink is writing.
         */
        // GTE chunkTimeout
        String SINK_IDLE_TIMEOUT_NANOS = (AsynchronousIndexWriteConfiguration.class
                .getName()
                + ".sinkIdleTimeoutNanos").intern();

        String DEFAULT_SINK_IDLE_TIMEOUT_NANOS = "" + Long.MAX_VALUE;

        /*
         * Scatter split configuration.
         */

        /**
         * Boolean option indicates whether or not scatter splits are performed
         * (default {@value #SCATTER_SPLIT_ENABLED}). Scatter splits only apply
         * for scale-out indices where they "scatter" the initial index
         * partition across the {@link IDataService}s in the federation. This
         * is normally very useful.
         * <P>
         * Sometimes a scatter split is not the "right" thing for an index. An
         * example would be an index where you have to do a LOT of synchronous
         * RPC rather than using asynchronous index writes. In this case, the
         * synchronous RPC can be a bottleneck unless the "chunk" size of the
         * writes is large. This is especially true when writes on other indices
         * must wait for the outcome of the synchronous RPC. E.g., foreign keys.
         * 
         * @see OverflowManager.Options#SCATTER_SPLIT_ENABLED
         */
        String SCATTER_SPLIT_ENABLED = (ScatterSplitConfiguration.class
                .getName()
                + ".enabled").intern();

        String DEFAULT_SCATTER_SPLIT_ENABLED = "true";

        /**
         * The percentage of the nominal index partition size at which a scatter
         * split is triggered when there is only a single index partition for a
         * given scale-out index (default
         * {@link #DEFAULT_SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD}). The
         * scatter split will break the index into multiple partitions and
         * distribute those index partitions across the federation in order to
         * allow more resources to be brought to bear on the scale-out index.
         * The value must LT the nominal index partition split point or normal
         * index splits will take precedence and a scatter split will never be
         * performed. The allowable range is therefore constrained to
         * <code>(0.1 : 1.0)</code>.
         */
        String SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD = (ScatterSplitConfiguration.class
                .getName()
                + ".percentOfSplitThreshold").intern();

        String DEFAULT_SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD = ".25";

        /**
         * The #of data services on which the index will be scattered or ZERO(0)
         * to use all discovered data services (default
         * {@value #DEFAULT_SCATTER_SPLIT_DATA_SERVICE_COUNT}).
         */
        String SCATTER_SPLIT_DATA_SERVICE_COUNT = (ScatterSplitConfiguration.class
                .getName()
                + ".dataServiceCount").intern();

        String DEFAULT_SCATTER_SPLIT_DATA_SERVICE_COUNT = "0";

        /**
         * The #of index partitions to generate when an index is scatter split.
         * The index partitions will be evenly distributed across up to
         * {@link #SCATTER_SPLIT_DATA_SERVICE_COUNT} discovered data services.
         * When ZERO(0), the scatter split will generate
         * <code>(NDATA_SERVICES x 2)</code> index partitions, where
         * NDATA_SERVICES is either {@link #SCATTER_SPLIT_DATA_SERVICE_COUNT} or
         * the #of discovered data services when that option is ZERO (0).
         * <p>
         * The "ideal" number of index partitions is generally between (NCORES x
         * NDATA_SERVICES / NINDICES) and (NCORES x NDATA_SERVICES). When there
         * are NCORES x NDATA_SERVICES index partitions, each core is capable of
         * servicing a distinct index partition assuming that the application
         * and the "schema" are capable of driving the data service writes with
         * that concurrency. However, if you have NINDICES, and the application
         * drives writes on all index partitions of all indices at the same
         * rate, then a 1:1 allocation of index partitions to cores would be
         * "ideal".
         * <p>
         * The "right" answer also depends on the data scale. If you have far
         * less data than can fill that many index partitions to 200M each, then
         * you should adjust the scatter split to use fewer index partitions or
         * fewer data services.
         * <p>
         * Finally, the higher the scatter the more you will need to use
         * asynchronous index writes in order to obtain high throughput with
         * sustained index writes.
         */
        String SCATTER_SPLIT_INDEX_PARTITION_COUNT = (ScatterSplitConfiguration.class
                .getName()
                + ".indexPartitionCount").intern();

        String DEFAULT_SCATTER_SPLIT_INDEX_PARTITION_COUNT = "0";

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
     * @see AbstractFederation#registerIndex(IndexMetadata, UUID)
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
    /**
	 * The type of the index.
	 * 
	 * @see #VERSION4
	 */
    private IndexTypeEnum indexType;
    private int branchingFactor;
    private int writeRetentionQueueCapacity;
    private int writeRetentionQueueScan;
//    private int btreeReadRetentionQueueCapacity;
//    private int btreeReadRetentionQueueScan;
    private LocalPartitionMetadata pmd;
    private String btreeClassName;
    private String checkpointClassName;
    private IRabaCoder nodeKeysCoder;
    private ITupleSerializer<?, ?> tupleSer;
    private IRecordCompressorFactory<?> btreeRecordCompressorFactory;
    private IRecordCompressorFactory<?> indexSegmentRecordCompressorFactory;
    private IConflictResolver conflictResolver;
    private boolean deleteMarkers;
    private boolean versionTimestamps;
    private boolean versionTimestampFilters;
    private boolean rawRecords;
    private short maxRecLen;
    private BloomFilterFactory bloomFilterFactory;
    private IOverflowHandler overflowHandler;
    private ISimpleSplitHandler splitHandler2;
    private AsynchronousIndexWriteConfiguration asynchronousIndexWriteConfiguration;
    private ScatterSplitConfiguration scatterSplitConfiguration;

    /* 
     * IndexSegment fields.
     */
    
    /**
     * @see Options#INDEX_SEGMENT_BRANCHING_FACTOR
     */
    private int indexSegmentBranchingFactor;

    /**
     * @see Options#INDEX_SEGMENT_BUFFER_NODES
     */
    private boolean indexSegmentBufferNodes;

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
	 * The type of the associated persistence capable data structure.
	 */
	public final IndexTypeEnum getIndexType() {
		return indexType;
	}

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
	 * There are two reasons why it makes sense to use a larger branching factor
	 * for an index segment. First, the WORM Journal is used to buffer writes in
	 * scale-out and IO on an index on the WORM Journal is driven by node and
	 * leaf revisions so the index often uses a smaller branching factor on the
	 * WORM. Second, the index segment is laid out in total key order in the
	 * file and each node and leaf is a contiguous sequences of bytes on the
	 * disk (like the WORM, but unlike the RWStore). Since most of the latency
	 * of the disk is the seek, reading larger leaves from an index segment is
	 * efficient.
	 * <p>
	 * Note: the value of this property will determine the branching factor of
	 * the {@link IndexSegment}. When the {@link IndexSegment} is built, it will
	 * be given a {@link #clone()} of this {@link IndexMetadata} and the actual
	 * branching factor for the {@link IndexSegment} be set on the
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
     * Return the record-level compression provider for a {@link BTree} (may be
     * null, which implies no compression).
     */
    public IRecordCompressorFactory getBtreeRecordCompressorFactory() {
        
        return btreeRecordCompressorFactory;
        
    }

    public void setBtreeRecordCompressorFactory(
            final IRecordCompressorFactory btreeRecordCompressorFactory) {
        
        this.btreeRecordCompressorFactory = btreeRecordCompressorFactory;
        
    }

    /**
     * Return the record-level compression provider for an {@link IndexSegment}
     * (may be null, which implies no compression).
     */
    public IRecordCompressorFactory getIndexSegmentRecordCompressorFactory() {
       
        return indexSegmentRecordCompressorFactory;
        
    }

    public void setIndexSegmentRecordCompressorFactory(
            final IRecordCompressorFactory segmentRecordCompressorFactory) {
        
        this.indexSegmentRecordCompressorFactory = segmentRecordCompressorFactory;
        
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

//    /**
//     * @see Options#BTREE_READ_RETENTION_QUEUE_CAPACITY
//     */
//    public final int getBTreeReadRetentionQueueCapacity() {
//        
//        return btreeReadRetentionQueueCapacity;
//        
//    }
//    
//    public final void setBTreeReadRetentionQueueCapacity(int v) {
//        
//        this.btreeReadRetentionQueueCapacity = v;
//        
//    }
//
//    /**
//     * @see Options#BTREE_READ_RETENTION_QUEUE_SCAN
//     */
//    public final int getBTreeReadRetentionQueueScan() {
//        
//        return btreeReadRetentionQueueScan;
//        
//    }
//    
//    public final void setBTreeReadRetentionQueueScan(int v) {
//        
//        this.btreeReadRetentionQueueScan = v;
//        
//    }

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
     * overridden to store additional state with each {@link Checkpoint} record.
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
     * Object used to code (compress) the keys in a node.
     * <p>
     * Note: The keys for nodes are separator keys for the leaves. Since they
     * are chosen to be the minimum length separator keys dynamically when a
     * leaf is split or joined the keys in the node typically DO NOT conform to
     * application expectations and MAY be assigned a different
     * {@link IRabaCoder} for that reason.
     * 
     * @see #getTupleSerializer()
     */
    public final IRabaCoder getNodeKeySerializer() {return nodeKeysCoder;}
    
    /**
     * The object used to form unsigned byte[] keys from Java objects, to
     * (de-)serialize Java object stored in the index, and to (de-)compress the
     * keys and values when stored in a leaf or {@link ResultSet}.
     * <p>
     * Note: If you change this value in a manner that is not backward
     * compatible once entries have been written on the index then you may be
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
    
    public final void setDeleteMarkers(final boolean deleteMarkers) {

        this.deleteMarkers = deleteMarkers;
        
    }
    
    /**
     * When <code>true</code> the index will maintain a per-index entry revision
     * timestamp. The primary use of this is in support of transactional
     * isolation. Delete markers MUST be enabled when using revision timestamps.
     * 
     * @see #getVersionTimestampFilters()
     */
    public final boolean getVersionTimestamps() {

        return versionTimestamps;
        
    }

    /**
     * When <code>true</code> the index will maintain the min/max of the per
     * tuple-revision timestamp on each {@link Node} of the B+Tree. This
     * information can be used to perform efficient filtering of iterators such
     * that they visit only nodes and leaves having data for a specified tuple
     * revision timestamp range. This filtering is efficient because it skips
     * any node (and all spanned nodes or leaves) which does not have data for
     * the desired revision timestamp range. In order to find all updates after
     * a given timestamp revision, you specify (fromRevision,Long.MAX_VALUE). In
     * order to visit the delta between two revisions, you specify
     * (fromRevision, toRevision+1).
     * <p>
     * Tuple revision filtering can be very efficient for some purposes. For
     * example, it can be used to synchronize disconnected clients or compute
     * the write set of a committed transaction. However, it requires more space
     * in the {@link INodeData} records since we must store the minimum and
     * maximum timestamp revision for each child of a given node.
     * <p>
     * Per-tuple timestamp revisions MAY be used without support for per-tuple
     * revision filtering.
     * 
     * @see #getVersionTimestamps()
     */
    public final boolean getVersionTimestampFilters() {

        return versionTimestampFilters;
        
    }

    /**
     * Sets {@link #versionTimestampFilters}. You MUST also use
     * {@link #setVersionTimestamps(boolean)} to <code>true</code> for version
     * timestamp filtering to be supported.
     * 
     * @param versionTimestampFilters
     *            <code>true</code> iff version timestamp filtering should be
     *            supported.
     */
    public final void setVersionTimestampFilters(
            final boolean versionTimestampFilters) {

        this.versionTimestampFilters = versionTimestampFilters;

    }

    public final void setVersionTimestamps(final boolean versionTimestamps) {

        this.versionTimestamps = versionTimestamps;

    }

    /**
     * True iff the index supports transactional isolation (both delete markers
     * and version timestamps are required).
     */
    public final boolean isIsolatable() {
        
        return deleteMarkers && versionTimestamps;
        
    }

    /**
     * Convenience method sets both {@link #setDeleteMarkers(boolean)} and
     * {@link #setVersionTimestamps(boolean)} at the same time.
     * 
     * @param isolatable
     *            <code>true</code> if delete markers and version timestamps
     *            will be enabled -or- <code>false</code> if they will be
     *            disabled.
     */
    public void setIsolatable(final boolean isolatable) {

        setDeleteMarkers(isolatable);

        setVersionTimestamps(isolatable);
        
    }

	/**
	 * When <code>true</code> the index transparently promote large
	 * <code>byte[]</code> values associated with a key to raw records on the
	 * backing store. This feature is disabled by default. Indices which do use
	 * large records should enable this option in order to reduce their IO churn
	 * and disk footprint.
	 * 
	 * @see #getMaxRecLen()
	 */
    public final boolean getRawRecords() {return rawRecords;}

	/**
	 * (Dis|En)able automatic promotion of index <code>byte[]</code> values
	 * larger than a configured byte length out of the index leaf and into raw
	 * records on the backing persistence store. This option can significicantly
	 * reduce the IO churn for indices which do make use of large values.
	 * However, the leaves will occupy slightly more space (~ 1 bit per tuple)
	 * if this option is enabled and none of the values stored in the index
	 * exceed the configured maximum value length. {@link IRabaCoder}s which
	 * rely on a uniform value length generally already use small values and
	 * should typically turn this feature off in order to make the leaf as
	 * compact as possible.
	 * 
	 * @param rawRecords
	 *            <code>true</code> if the feature is to be enabled.
	 * 
	 * @see #setMaxRecLen(int)
	 */
    public final void setRawRecords(final boolean rawRecords) {

        this.rawRecords = rawRecords;
        
    }

    /**
     * When {@link #getRawRecords()} returns <code>true</code>, this method
     * returns the maximum byte length of a <code>byte[]</code> value will be be
     * stored in a B+Tree leaf (default {@link Options#MAX_REC_LEN}) while
     * values larger than this will be automatically converted into raw record
     * references. Note that this method returns the configured value regardless
     * of the value of {@link #getRawRecords()} - the caller must check
     * {@link #getRawRecords()} in order to correctly interpret the value
     * returned by this method.
     * 
     * @see Options#MAX_REC_LEN
     * @see Options#DEFAULT_MAX_REC_LEN
     */
	public final int getMaxRecLen() {return maxRecLen;}

	/**
	 * Set the maximum length of a <code>byte[]</code> value in a leaf of the
	 * index.
	 * 
	 * @param maxRecLen
	 *            The maximum length of a <code>byte[]</code> value in a leaf of
	 *            the index. A value of ZERO (0) may be used to force all values
	 *            into raw records.
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is negative or greater than
	 *             {@link Short#MAX_VALUE}
	 * 
	 * @see #setRawRecords(boolean)
	 */
	public final void setMaxRecLen(final int maxRecLen) {

		if (maxRecLen < 0 || maxRecLen > Short.MAX_VALUE)
			throw new IllegalArgumentException();
		
		this.maxRecLen = (short) maxRecLen;
		
	}
    
	public void setPartitionMetadata(final LocalPartitionMetadata pmd) {
        
        this.pmd = pmd;
        
    }

    public void setNodeKeySerializer(final IRabaCoder nodeKeysCoder) {
        
        if (nodeKeysCoder == null)
            throw new IllegalArgumentException();
        
        this.nodeKeysCoder = nodeKeysCoder;
        
    }

    public void setTupleSerializer(final ITupleSerializer tupleSer) {

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
    public void setBranchingFactor(final int branchingFactor) {
        
        if(branchingFactor < Options.MIN_BRANCHING_FACTOR) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.branchingFactor = branchingFactor;
        
    }

    public void setIndexSegmentBranchingFactor(final int branchingFactor) {

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

    public void setConflictResolver(final IConflictResolver conflictResolver) {

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
    public void setBloomFilterFactory(final BloomFilterFactory bloomFilterFactory) {
        
        this.bloomFilterFactory = bloomFilterFactory;
        
    }
    
    /**
     * An optional object that may be used to inspect, and possibly operate on,
     * each index entry as it is copied into an {@link IndexSegment}.
     */
    public IOverflowHandler getOverflowHandler() {
        
        return overflowHandler;
        
    }

    public void setOverflowHandler(final IOverflowHandler overflowHandler) {
        
        this.overflowHandler = overflowHandler;
        
    }

    /**
     * Object which decides whether and where to split an index partition into 2
     * or more index partitions. The default is a <code>null</code> reference.
     * The default behavior when no split handler is specified will work for
     * nearly all use cases and will result in index partitions whose size on
     * the disk is bounded by the parameter specified using
     * {@link OverflowManager.Options#NOMINAL_SHARD_SIZE}. Indices which require
     * certain guarantees for atomicity, such as the {@link SparseRowStore},
     * must override this default.
     * 
     * @return The {@link ISimpleSplitHandler} -or- <code>null</code> if the
     *         application has not imposed any additional constraints on the
     *         separator keys when splitting index partitions.
     */
    public ISimpleSplitHandler getSplitHandler() {

        return splitHandler2;
        
    }
    
    public void setSplitHandler(final ISimpleSplitHandler splitHandler) {
        
        this.splitHandler2 = splitHandler;
        
    }
    
    /**
     * The asynchronous index write API configuration for this index.
     */
    public AsynchronousIndexWriteConfiguration getAsynchronousIndexWriteConfiguration() {
        
        return asynchronousIndexWriteConfiguration;
        
    }

    /**
     * Set the asynchronous index write API configuration for this index.
     */
    public void getAsynchronousIndexWriteConfiguration(
            final AsynchronousIndexWriteConfiguration newVal) {

        if (newVal == null)
            throw new IllegalArgumentException();
        
        this.asynchronousIndexWriteConfiguration = newVal;
        
    }

    /**
     * The scatter split configuration for a scale-out index.
     */
    public ScatterSplitConfiguration getScatterSplitConfiguration() {
        
        return scatterSplitConfiguration;
        
    }

    public void setScatterSplitConfiguration(
            final ScatterSplitConfiguration newVal) {

        if (newVal == null)
            throw new IllegalArgumentException();

        this.scatterSplitConfiguration = newVal;

    }

    /**
     * Create an instance of a class known to implement the specified interface
     * from a class name.
     * 
     * @param className
     *            The class name.
     * 
     * @return An instance of that class -or- <code>null</code> iff the class
     *         name is <code>null</code>.
     * 
     * @throws RuntimeException
     *             if the class does not implement that interface or for any
     *             other reason.
     */
    @SuppressWarnings("unchecked")
    static private <T> T newInstance(final String className,
            final Class<T> iface) {

        if (iface == null)
            throw new IllegalArgumentException();

        if (className == null) {

            return null;
            
        }
        
        try {

            final Class cls = Class.forName(className);

            if (!iface.isAssignableFrom(cls)) {

                throw new IllegalArgumentException("Does not implement " + cls
                        + " : " + className);

            }

            return (T) cls.getConstructor(new Class[] {}).newInstance(
                    new Object[] {});

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * <strong>De-serialization constructor only</strong> - DO NOT use this ctor
     * for creating a new instance! It will result in a thrown exception,
     * typically from {@link #firstCheckpoint()}.
     */
    public IndexMetadata() {
        
    }
    
    /**
     * Constructor used to configure a new <em>unnamed</em> B+Tree. The index
     * UUID is set to the given value and all other fields are defaulted as
     * explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
     * defaults may be overridden using the various setter methods, but some
     * values can not be safely overridden after the index is in use.
     * 
     * @param indexUUID
     *            The indexUUID.
     * 
     * @throws IllegalArgumentException
     *             if the indexUUID is <code>null</code>.
     */
    public IndexMetadata(final UUID indexUUID) {

		this(null/* name */, indexUUID);

    }
        
	/**
	 * Constructor used to configure a new <em>named</em> {@link BTree}. The
	 * index UUID is set to the given value and all other fields are defaulted
	 * as explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
	 * defaults may be overridden using the various setter methods, but some
	 * values can not be safely overridden after the index is in use.
	 * 
	 * @param name
	 *            The index name. When this is a scale-out index, the same
	 *            <i>name</i> is specified for each index resource. However they
	 *            will be registered on the journal under different names
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

		this(null/* name */, System.getProperties(), name, indexUUID,
				IndexTypeEnum.BTree);

	}

    /**
	 * Constructor used to configure a new <em>named</em> B+Tree. The index UUID
	 * is set to the given value and all other fields are defaulted as explained
	 * at {@link #getProperty(Properties, String, String, String)}. Those
	 * defaults may be overridden using the various setter methods.
	 * 
	 * @param indexManager
	 *            Optional. When given and when the {@link IIndexManager} is a
	 *            scale-out {@link IBigdataFederation}, this object will be used
	 *            to interpret the {@link Options#INITIAL_DATA_SERVICE}
	 *            property.
	 * @param properties
	 *            Properties object used to overridden the default values for
	 *            this {@link IndexMetadata} instance.
	 * @param namespace
	 *            The index name. When this is a scale-out index, the same
	 *            <i>name</i> is specified for each index resource. However they
	 *            will be registered on the journal under different names
	 *            depending on the index partition to which they belong.
	 * @param indexUUID
	 *            The indexUUID. The same index UUID MUST be used for all
	 *            component indices in a scale-out index.
	 * @param indexType
	 *            Type-safe enumeration specifying the type of the persistence
	 *            class data structure (historically, this was always a B+Tree).
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>properties</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if <i>indexUUID</i> is <code>null</code>.
	 */
	public IndexMetadata(final IIndexManager indexManager,
			final Properties properties, final String namespace,
			final UUID indexUUID, final IndexTypeEnum indexType) {

        if (indexUUID == null)
            throw new IllegalArgumentException();

        if (indexType == null)
            throw new IllegalArgumentException();

        this.name = namespace;

        this.indexType = indexType;
        
        this.indexUUID = indexUUID;

        {

            final String val = getProperty(indexManager, properties, namespace,
                    Options.INITIAL_DATA_SERVICE, null/* default */);

            if (val != null) {

                /*
                 * Attempt to interpret the value as either a UUID or the name of 
                 * a data service joined with the federation.
                 */
                
                UUID uuid = null;
                try {

                    uuid = UUID.fromString(val);
                    
                } catch (Throwable t) {
                
                    // Not a UUID.
                    
                    if (log.isInfoEnabled())
                        log.info("Not a UUID: " + val);
                    
                    // Ignore & fall through.
                    
                }
                
                if (uuid == null && indexManager != null
                        && indexManager instanceof IBigdataFederation<?>) {

                    final IBigdataFederation<?> fed = (IBigdataFederation<?>) indexManager;

                    final IDataService dataService = fed
                            .getDataServiceByName(val);

                    if (dataService != null) {

                        try {

                            uuid = dataService.getServiceUUID();

                        } catch (IOException ex) {

                            log.warn("Could not get serviceUUID", ex);

                            // ignore and fall through.

                        }
                        
                    }
                    
                }
             
                this.initialDataServiceUUID = uuid;
                
            }
            
        }

        this.branchingFactor = getProperty(indexManager, properties, namespace,
                Options.BTREE_BRANCHING_FACTOR,
                Options.DEFAULT_BTREE_BRANCHING_FACTOR,
                new IntegerRangeValidator(Options.MIN_BRANCHING_FACTOR,
                        Options.MAX_BTREE_BRANCHING_FACTOR));

        {
            /*
             * The default capacity is set dynamically based on the maximum java
             * heap as specified by -Xmx on the command line. This provides
             * better ergonomics, but the larger write retention queue capacity
             * will only benefit applications which perform sustained writes on
             * the index.
             * 
             * Note: For now I am turning off the write retention queue capacity
             * "ergonomics". I am exploring whether or not this is too
             * aggressive. The advantage of the ergonomics is that it
             * automatically tunes the indices for a store used for a single
             * purpose, such as a KB. However, if you have a lot of open
             * indices, then this is not a good idea as each open index will
             * allocate a ring buffer of that capacity.
             */
            final String defaultCapacity;
//            final long maxMemory = Runtime.getRuntime().maxMemory();
//            if (maxMemory >= Bytes.gigabyte * 10) {
//                defaultCapacity = "20000";
//            } else if (maxMemory >= Bytes.gigabyte * 1) {
//                defaultCapacity = "8000";
//            } else {
                defaultCapacity = Options.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY;
//            }
            this.writeRetentionQueueCapacity = getProperty(indexManager,
                    properties, namespace,
                    Options.WRITE_RETENTION_QUEUE_CAPACITY,
                    defaultCapacity,
//                    Options.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY,
                    new IntegerRangeValidator(
                            Options.MIN_WRITE_RETENTION_QUEUE_CAPACITY,
                            Options.MAX_WRITE_RETENTION_QUEUE_CAPACITY));
        }

        this.writeRetentionQueueScan = getProperty(indexManager,
                properties, namespace, Options.WRITE_RETENTION_QUEUE_SCAN,
                Options.DEFAULT_WRITE_RETENTION_QUEUE_SCAN,
                IntegerValidator.GTE_ZERO);

//        this.btreeReadRetentionQueueCapacity = getProperty(indexManager,
//                properties, namespace, Options.BTREE_READ_RETENTION_QUEUE_CAPACITY,
//                Options.DEFAULT_BTREE_READ_RETENTION_QUEUE_CAPACITY,
//                IntegerValidator.GTE_ZERO);
//
//        this.btreeReadRetentionQueueScan = getProperty(indexManager,
//                properties, namespace, Options.BTREE_READ_RETENTION_QUEUE_SCAN,
//                Options.DEFAULT_BTREE_READ_RETENTION_QUEUE_SCAN,
//                IntegerValidator.GTE_ZERO);

        this.btreeRecordCompressorFactory = newInstance(getProperty(
                indexManager, properties, namespace,
                Options.BTREE_RECORD_COMPRESSOR_FACTORY,
                Options.DEFAULT_BTREE_RECORD_COMPRESSOR_FACTORY/* default */),
                IRecordCompressorFactory.class);

        this.indexSegmentBranchingFactor = getProperty(indexManager,
                properties, namespace, Options.INDEX_SEGMENT_BRANCHING_FACTOR,
                Options.DEFAULT_INDEX_SEGMENT_BRANCHING_FACTOR,
                new IntegerRangeValidator(Options.MIN_BRANCHING_FACTOR,
                        Options.MAX_INDEX_SEGMENT_BRANCHING_FACTOR));

        this.indexSegmentBufferNodes = Boolean.parseBoolean(getProperty(
                indexManager, properties, namespace,
                Options.INDEX_SEGMENT_BUFFER_NODES,
                Options.DEFAULT_INDEX_SEGMENT_BUFFER_NODES));

//        this.indexSegmentLeafCacheCapacity = getProperty(indexManager,
//                properties, namespace,
//                Options.INDEX_SEGMENT_LEAF_CACHE_CAPACITY,
//                Options.DEFAULT_INDEX_SEGMENT_LEAF_CACHE_CAPACITY,
//                IntegerValidator.GT_ZERO);
//
//        this.indexSegmentLeafCacheTimeout = getProperty(indexManager,
//                properties, namespace,
//                Options.INDEX_SEGMENT_LEAF_CACHE_TIMEOUT,
//                Options.DEFAULT_INDEX_SEGMENT_LEAF_CACHE_TIMEOUT,
//                LongValidator.GT_ZERO);

        this.indexSegmentRecordCompressorFactory = newInstance(
                getProperty(indexManager, properties, namespace,
                        Options.INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY,
                        Options.DEFAULT_INDEX_SEGMENT_RECORD_COMPRESSOR_FACTORY/* default */),
                IRecordCompressorFactory.class);
        
        // Note: default assumes NOT an index partition.
        this.pmd = null;
        
        /* Intern'd to reduce duplication on the heap. Will be com.bigdata.btree.BTree or
         * com.bigdata.btree.IndexSegment and occasionally a class derived from BTree.
         */
        this.btreeClassName = getProperty(indexManager, properties, namespace,
                Options.BTREE_CLASS_NAME, BTree.class.getName()).intern();

        // Intern'd to reduce duplication on the heap.
        this.checkpointClassName = Checkpoint.class.getName().intern();
        
//        this.addrSer = AddressSerializer.INSTANCE;
        
//        this.nodeKeySer = PrefixSerializer.INSTANCE;
        final Class keyRabaCoder;
        if (this instanceof HTreeIndexMetadata) {
        	keyRabaCoder = FrontCodedRabaCoderDupKeys.class;
        } else {
        	keyRabaCoder = DefaultFrontCodedRabaCoder.class;
        }
        
        this.nodeKeysCoder = newInstance(getProperty(indexManager, properties,
                namespace, Options.NODE_KEYS_CODER,
                keyRabaCoder.getName()), IRabaCoder.class);

        // this.tupleSer = DefaultTupleSerializer.newInstance();
        {

            /*
             * FIXME allow override of the keyBuilderFactory.
             * 
             * FIXME there are a bunch of subclasses of DefaultTupleSerializer.
             * In order to be able to override the specific key/value
             * serialization providers we need to make the tupleSer instance
             * itself a configuration parameter.
             */
            final IKeyBuilderFactory keyBuilderFactory = DefaultTupleSerializer
                    .getDefaultKeyBuilderFactory();

            final IRabaCoder leafKeysCoder = newInstance(getProperty(
                    indexManager, properties, namespace,
                    Options.LEAF_KEYS_CODER, keyRabaCoder
                            .getName()), IRabaCoder.class);

            final IRabaCoder valuesCoder = newInstance(getProperty(
                    indexManager, properties, namespace,
                    Options.LEAF_VALUES_CODER, CanonicalHuffmanRabaCoder.class
                            .getName()), IRabaCoder.class);

            this.tupleSer = new DefaultTupleSerializer(keyBuilderFactory,
                    leafKeysCoder, valuesCoder);
            
        }

        this.conflictResolver = null;

//        this.childLocks = Boolean.parseBoolean(getProperty(
//                indexManager, properties, namespace, Options.CHILD_LOCKS,
//                Options.DEFAULT_CHILD_LOCKS));
        
        this.deleteMarkers = false;
        
        this.versionTimestamps = false;

        this.versionTimestampFilters = false;

		/*
		 * Default to false for new indices. This follows the same principle of
		 * requiring people to opt in for special features. Many indices tend to
		 * always use small records and this option represents overhead for such
		 * indices. Indices which do use large records should enable this option
		 * in order to reduce their IO churn and disk footprint.
		 */
        this.rawRecords = false;
        
		this.maxRecLen = Short.parseShort(getProperty(indexManager,
				properties, namespace, Options.MAX_REC_LEN,
				Options.DEFAULT_MAX_REC_LEN));

		// Note: May be used to force testing with raw records.
//		this.rawRecords = true;
//		this.maxRecLen = 1;

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
            
//            final int minimumEntryCount = Integer.parseInt(getProperty(
//                    indexManager, properties, namespace,
//                    Options.SPLIT_HANDLER_MIN_ENTRY_COUNT,
//                    Options.DEFAULT_SPLIT_HANDLER_MIN_ENTRY_COUNT));
//
//            final int entryCountPerSplit = Integer.parseInt(getProperty(
//                    indexManager, properties, namespace,
//                    Options.SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT,
//                    Options.DEFAULT_SPLIT_HANDLER_ENTRY_COUNT_PER_SPLIT));
//
//            final double overCapacityMultiplier = Double.parseDouble(getProperty(
//                    indexManager, properties, namespace,
//                    Options.SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER,
//                    Options.DEFAULT_SPLIT_HANDLER_OVER_CAPACITY_MULTIPLIER));
//
//            final double underCapacityMultiplier = Double.parseDouble(getProperty(
//                    indexManager, properties, namespace,
//                    Options.SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER,
//                    Options.DEFAULT_SPLIT_HANDLER_UNDER_CAPACITY_MULTIPLIER));
//
//            final int sampleRate = Integer.parseInt(getProperty(
//                    indexManager, properties, namespace,
//                    Options.SPLIT_HANDLER_SAMPLE_RATE,
//                    Options.DEFAULT_SPLIT_HANDLER_SAMPLE_RATE));
//
//            this.splitHandler = new DefaultSplitHandler(//
//                    minimumEntryCount, //
//                    entryCountPerSplit, //
//                    overCapacityMultiplier, //
//                    underCapacityMultiplier, //
//                    sampleRate //
//            );

            /*
             * Note: The default behavior when no split handler is specified
             * will work for nearly all use cases and will result in index
             * partitions whose size on the disk is bounded by the parameter
             * specified to the OverflowManager class.  Indices which require
             * certain guarantees for atomicity, such as the SparseRowStore or
             * the SPO index, must override this default.
             */
            this.splitHandler2 = null;
            
        }

        /*
         * Asynchronous index write API configuration.
         */
        {

            final int masterQueueCapacity = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.MASTER_QUEUE_CAPACITY,
                    Options.DEFAULT_MASTER_QUEUE_CAPACITY));

            final int masterChunkSize = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.MASTER_CHUNK_SIZE,
                    Options.DEFAULT_MASTER_CHUNK_SIZE));

            final long masterChunkTimeoutNanos = Long.parseLong(getProperty(
                    indexManager, properties, namespace,
                    Options.MASTER_CHUNK_TIMEOUT_NANOS,
                    Options.DEFAULT_MASTER_CHUNK_TIMEOUT_NANOS));
            
            final long sinkIdleTimeoutNanos = Long.parseLong(getProperty(
                    indexManager, properties, namespace,
                    Options.SINK_IDLE_TIMEOUT_NANOS,
                    Options.DEFAULT_SINK_IDLE_TIMEOUT_NANOS));
            
            final long sinkPollTimeoutNanos = Long.parseLong(getProperty(
                    indexManager, properties, namespace,
                    Options.SINK_POLL_TIMEOUT_NANOS,
                    Options.DEFAULT_SINK_POLL_TIMEOUT_NANOS));

            final int sinkQueueCapacity = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.SINK_QUEUE_CAPACITY,
                    Options.DEFAULT_SINK_QUEUE_CAPACITY));

            final int sinkChunkSize = Integer.parseInt(getProperty(
                    indexManager, properties, namespace,
                    Options.SINK_CHUNK_SIZE,
                    Options.DEFAULT_SINK_CHUNK_SIZE));

            final long sinkChunkTimeoutNanos = Long.parseLong(getProperty(
                    indexManager, properties, namespace,
                    Options.SINK_CHUNK_TIMEOUT_NANOS,
                    Options.DEFAULT_SINK_CHUNK_TIMEOUT_NANOS));

            this.asynchronousIndexWriteConfiguration = new AsynchronousIndexWriteConfiguration(
                    masterQueueCapacity,//
                    masterChunkSize,//
                    masterChunkTimeoutNanos,//
                    sinkIdleTimeoutNanos,//
                    sinkPollTimeoutNanos,//
                    sinkQueueCapacity,//
                    sinkChunkSize,//
                    sinkChunkTimeoutNanos//
            );

        }

        // Scatter-split configuration
        {

            final boolean scatterSplitEnabled = Boolean
                    .parseBoolean(getProperty(indexManager, properties,
                            namespace, Options.SCATTER_SPLIT_ENABLED,
                            Options.DEFAULT_SCATTER_SPLIT_ENABLED));

            final double scatterSplitPercentOfSplitThreshold = Double
                    .parseDouble(getProperty(
                            indexManager,
                            properties,
                            namespace,
                            Options.SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD,
                            Options.DEFAULT_SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD));

            final int scatterSplitDataServicesCount = Integer
                    .parseInt(getProperty(indexManager, properties, namespace,
                            Options.SCATTER_SPLIT_DATA_SERVICE_COUNT,
                            Options.DEFAULT_SCATTER_SPLIT_DATA_SERVICE_COUNT));

            final int scatterSplitIndexPartitionsCount = Integer
                    .parseInt(getProperty(indexManager, properties, namespace,
                            Options.SCATTER_SPLIT_INDEX_PARTITION_COUNT,
                            Options.DEFAULT_SCATTER_SPLIT_INDEX_PARTITION_COUNT));

            this.scatterSplitConfiguration = new ScatterSplitConfiguration(
                    scatterSplitEnabled, scatterSplitPercentOfSplitThreshold,
                    scatterSplitDataServicesCount,
                    scatterSplitIndexPartitionsCount);
            
        }
        
        if (log.isInfoEnabled())
            log.info(toString());
        
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
        
        final StringBuilder sb = new StringBuilder();

        // transient
        sb.append("addrMetadata=" + addrMetadata);

        // persistent
        sb.append(", name=" + (name == null ? "N/A" : name));
		sb.append(", indexType=" + indexType);
        sb.append(", indexUUID=" + indexUUID);
        if (initialDataServiceUUID != null) {
            sb.append(", initialDataServiceUUID=" + initialDataServiceUUID);
        }
        sb.append(", branchingFactor=" + branchingFactor);
        sb.append(", pmd=" + pmd);
        sb.append(", btreeClassName=" + btreeClassName);
        sb.append(", checkpointClass=" + checkpointClassName);
//        sb.append(", childAddrSerializer=" + addrSer.getClass().getName());
        sb.append(", nodeKeysCoder=" + nodeKeysCoder);//.getClass().getName());
        sb.append(", btreeRecordCompressorFactory="
                + (btreeRecordCompressorFactory == null ? "N/A"
                        : btreeRecordCompressorFactory));
        sb.append(", tupleSerializer=" + tupleSer);//.getClass().getName());
        sb.append(", conflictResolver="
                + (conflictResolver == null ? "N/A" : conflictResolver
                        .getClass().getName()));
        sb.append(", deleteMarkers=" + deleteMarkers);
        sb.append(", versionTimestamps=" + versionTimestamps);
        sb.append(", versionTimestampFilters=" + versionTimestampFilters);
        sb.append(", isolatable=" + isIsolatable());
        sb.append(", rawRecords=" + rawRecords);
        sb.append(", maxRecLen=" + maxRecLen);
        sb.append(", bloomFilterFactory=" + (bloomFilterFactory == null ? "N/A"
                : bloomFilterFactory.toString())); 
        sb.append(", overflowHandler="
                + (overflowHandler == null ? "N/A" : overflowHandler.getClass()
                        .getName()));
        sb.append(", splitHandler="
                + (splitHandler2 == null ? "N/A" : splitHandler2.toString()));
        sb.append(", indexSegmentBranchingFactor=" + indexSegmentBranchingFactor);
        sb.append(", indexSegmentBufferNodes=" + indexSegmentBufferNodes);
//        sb.append(", indexSegmentLeafCacheCapacity=" + indexSegmentLeafCacheCapacity);
//        sb.append(", indexSegmentLeafCacheTimeout=" + indexSegmentLeafCacheTimeout);
        sb.append(", indexSegmentRecordCompressorFactory="
                + (indexSegmentRecordCompressorFactory == null ? "N/A"
                        : indexSegmentRecordCompressorFactory));
        sb.append(", asynchronousIndexWriteConfiguration=" + asynchronousIndexWriteConfiguration);
        sb.append(", scatterSplitConfiguration=" + scatterSplitConfiguration);
        toString(sb); // extension hook

        return sb.toString();
        
    }
    
	/**
	 * Extension hook for {@link #toString()}.
	 * 
	 * @param sb
	 *            Where to write additional metadata.
	 */
    protected void toString(final StringBuilder sb) {
    	
    		// NOP
    	
    }
    
    /**
     * The initial version.
     */
    private static transient final int VERSION0 = 0x0;

	/**
	 * This version adds support for {@link ILeafData#getRawRecord(int)} and
	 * {@link IndexMetadata#getRawRecords()} will report <code>false</code> for
	 * earlier versions and {@link IndexMetadata#getMaxRecLen()} will report
	 * {@link Options#DEFAULT_MAX_REC_LEN}.
	 */
    private static transient final int VERSION1 = 0x1;

    /**
     * This version adds support for {@link HTree}. This includes
     * {@link #addressBits} and {@link #htreeClassName}.
     */
    private static transient final int VERSION2 = 0x2;

    /**
     * This version adds support for a fixed length key option for the
     * {@link HTree} using {@link #keyLen}.
     */
    private static transient final int VERSION3 = 0x3;

	/**
	 * This version moves the {@link HTree} specific metadata into a derived
	 * class. Prior to this version, the {@link HTree} was not used in a durable
	 * context. Thus, there is no need to recover HTree specific index metadata
	 * records before {@link #VERSION4}. This version also introduces the
	 * {@link #indexType} field. This field defaults to
	 * {@link IndexTypeEnum#BTree} for all prior versions.
	 */
    private static transient final int VERSION4 = 0x4;

    /**
     * The version that will be serialized by this class.
     */
    private static transient final int CURRENT_VERSION = VERSION4;

    /**
	 * The actual version as set by {@link #readExternal(ObjectInput)} and
	 * {@link #writeExternal(ObjectOutput)}.
	 */
    private transient int version;
    
    /**
     * @todo review generated record for compactness.
     */
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

		final int version = this.version = (int) LongPacker.unpackLong(in);

        switch (version) {
        case VERSION0:
        case VERSION1:
        case VERSION2:
        case VERSION3:
        case VERSION4:
//        case VERSION5:
//        case VERSION6:
//        case VERSION7:
//        case VERSION8:
//        case VERSION9:
//        case VERSION10:
            break;
        default:
            throw new IOException("Unknown version: version=" + version);
        }

        final boolean hasName = in.readBoolean();

        if (hasName) {

            name = in.readUTF();

        }

		if (version >= VERSION4) {

			indexType = IndexTypeEnum.valueOf(in.readShort());
			
		} else {
			
			indexType = IndexTypeEnum.BTree;
			
        }
        
        indexUUID = new UUID(in.readLong()/* MSB */, in.readLong()/* LSB */);

        branchingFactor = (int) LongPacker.unpackLong(in);

        writeRetentionQueueCapacity = (int) LongPacker.unpackLong(in);

        writeRetentionQueueScan = (int) LongPacker.unpackLong(in);

//        if (version < VERSION7) {
//        
//            /* btreeReadRetentionQueueCapacity = (int) */LongPacker
//                    .unpackLong(in);
//
//            /* btreeReadRetentionQueueScan = (int) */LongPacker.unpackLong(in);
//
//        }

        pmd = (LocalPartitionMetadata) in.readObject();

        btreeClassName = in.readUTF();

        checkpointClassName = in.readUTF();

        nodeKeysCoder = (IRabaCoder) in.readObject();

        tupleSer = (ITupleSerializer<?,?>) in.readObject();

        btreeRecordCompressorFactory = (IRecordCompressorFactory<?>) in
                .readObject();

        conflictResolver = (IConflictResolver) in.readObject();

        deleteMarkers = in.readBoolean();

		if (version >= VERSION1) {
			rawRecords = in.readBoolean();
			maxRecLen = in.readShort();
		} else {
			rawRecords = false;
			maxRecLen = Short.parseShort(Options.DEFAULT_MAX_REC_LEN);
		}
        
        versionTimestamps = in.readBoolean();

        versionTimestampFilters = in.readBoolean();

        bloomFilterFactory = (BloomFilterFactory) in.readObject();

        overflowHandler = (IOverflowHandler) in.readObject();

        splitHandler2 = (ISimpleSplitHandler) in.readObject();

        /*
         * IndexSegment.
         */

        indexSegmentBranchingFactor = (int) LongPacker.unpackLong(in);

        indexSegmentBufferNodes = in.readBoolean();

        indexSegmentRecordCompressorFactory = (IRecordCompressorFactory<?>) in
                .readObject();

        asynchronousIndexWriteConfiguration = (AsynchronousIndexWriteConfiguration) in
                .readObject();

        scatterSplitConfiguration = (ScatterSplitConfiguration) in.readObject();

		if (version >= VERSION2 && version < VERSION4) {

			/*
			 * These data were moved into the HTreeIndexMetadata subclass
			 * in VERSION4. The HTree was only used against the memory
			 * manager before VERSION4. Therefore, we never have durable
			 * data for an HTree before VERSION4.
			 */
			
			if (version >= VERSION3) {

				// keyLen
				LongPacker.unpackInt(in);

			}

			// addressBits
			LongPacker.unpackInt(in);

			// htreeClassName
			in.readUTF();

		}

    }

    public void writeExternal(final ObjectOutput out) throws IOException {
    	
    		final int version = CURRENT_VERSION;
        
        LongPacker.packLong(out, version);

        // hasName?
        out.writeBoolean(name != null ? true : false);
        
        // the name
        if (name != null) {

            out.writeUTF(name);
            
        }
        
		if (version >= VERSION4) {

			out.writeShort(indexType.getCode());

		}
        
        out.writeLong(indexUUID.getMostSignificantBits());
        
        out.writeLong(indexUUID.getLeastSignificantBits());
        
        LongPacker.packLong(out, branchingFactor);

        LongPacker.packLong(out, writeRetentionQueueCapacity);

        LongPacker.packLong(out, writeRetentionQueueScan);

        // Note: gone with version7.
//        LongPacker.packLong(out, btreeReadRetentionQueueCapacity);
//        LongPacker.packLong(out, btreeReadRetentionQueueScan);

        out.writeObject(pmd);

        out.writeUTF(btreeClassName);

        out.writeUTF(checkpointClassName);

        out.writeObject(nodeKeysCoder);

        out.writeObject(tupleSer);

        out.writeObject(btreeRecordCompressorFactory);

        out.writeObject(conflictResolver);

        out.writeBoolean(deleteMarkers);

        if (version >= VERSION1) {
            out.writeBoolean(rawRecords);
            out.writeShort(maxRecLen);
        }

        out.writeBoolean(versionTimestamps);

        out.writeBoolean(versionTimestampFilters);

        out.writeObject(bloomFilterFactory);

        out.writeObject(overflowHandler);

        out.writeObject(splitHandler2);

        /*
         * IndexSegment.
         */

        LongPacker.packLong(out, indexSegmentBranchingFactor);

        out.writeBoolean(indexSegmentBufferNodes);

        out.writeObject(btreeRecordCompressorFactory);

        // introduced in VERSION1
        out.writeObject(asynchronousIndexWriteConfiguration);

        // introduced in VERSION2
        out.writeObject(scatterSplitConfiguration);

//        if (version >= VERSION2) {
//
//            if (version >= VERSION3) {
//
//                LongPacker.packLong(out, keyLen);
//
//            }
//
//			LongPacker.packLong(out, addressBits);
//
//			out.writeUTF(htreeClassName);
//
//        }
            
    }

	/**
	 * Makes a copy of the persistent data, clearing the address of the
	 * {@link IndexMetadata} record on the cloned copy.
	 * 
	 * @return The cloned copy.
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
     * Create an initial {@link Checkpoint} for a new persistence capable data
     * structure described by this metadata record.
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
    @SuppressWarnings("unchecked")
    final public Checkpoint firstCheckpoint() {

        final String checkpointClassName = getCheckpointClassName();

        if (checkpointClassName == null) {
            /*
             * This exception can be thrown if you originally created the
             * IndexMetadata object using the zero argument constructor. That
             * form of the constructor is only for deserialization and as such
             * it does not set any properties.
             */
            throw new RuntimeException(
                    "checkpointClassName not set: did you use the deserialization constructor by mistake?");
          
        }
        
        try {
            
            final Class cl = Class.forName(checkpointClassName);
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor.
             */
            
            final Constructor ctor = cl.getConstructor(new Class[] {//
                    IndexMetadata.class//
                    });

            final Checkpoint checkpoint = (Checkpoint) ctor
                    .newInstance(new Object[] { this });

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
    @SuppressWarnings("unchecked")
    final public Checkpoint overflowCheckpoint(final Checkpoint oldCheckpoint) {
       
        if (oldCheckpoint == null) {
         
            throw new IllegalArgumentException();
            
        }
        
        try {
            
            final Class cl = Class.forName(getCheckpointClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor.
             */
            
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IndexMetadata.class, //
                    Checkpoint.class//
                    });

            final Checkpoint checkpoint = (Checkpoint) ctor
                    .newInstance(new Object[] { //
                            this, //
                            oldCheckpoint //
                    });
            
            // sanity check makes sure the counter is propagated to the new store.
            assert checkpoint.getCounter() == oldCheckpoint.getCounter();
            
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
    @Override
    public IKeyBuilder getKeyBuilder() {

        return getTupleSerializer().getKeyBuilder();
        
    }

    @Override
    public IKeyBuilder getPrimaryKeyBuilder() {

        return getTupleSerializer().getPrimaryKeyBuilder();
        
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
