package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.LRUNexus;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.DefaultEvictionListener;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.HardReferenceQueueWithBatchingUpdates;
import com.bigdata.cache.IHardReferenceQueue;
import com.bigdata.cache.RingBuffer;
import com.bigdata.htree.HTree.AbstractPage;
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.DataService;

/**
 * Abstract base class for a persistence capable extensible hash tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractHTree {

    /**
     * The index is already closed.
     */
    protected static final String ERROR_CLOSED = "Closed";

    /**
     * A parameter was less than zero.
     */
    protected static final String ERROR_LESS_THAN_ZERO = "Less than zero";

    /**
     * A parameter was too large.
     */
    protected static final String ERROR_TOO_LARGE = "Too large";

    /**
     * The index is read-only but a mutation operation was requested.
     */
    final protected static String ERROR_READ_ONLY = "Read-only";
    
    /**
     * The index is transient (not backed by persistent storage) but an
     * operation that requires persistence was requested.
     */
    final protected static String ERROR_TRANSIENT = "Transient";
    
    private static final transient Logger log = Logger.getLogger(AbstractHTree.class);
    
    /**
     * Counters tracking various aspects of the btree.
     * <p>
     * Note: This is <code>volatile</code> to avoid the need for
     * synchronization in order for changes in the reference to be visible to
     * threads.
     */
    private volatile BTreeCounters btreeCounters = new BTreeCounters();

    /**
     * Counters tracking various aspects of the btree.
     */
    final public BTreeCounters getBtreeCounters() {
    
        return btreeCounters;
        
    }

    /**
     * Replace the {@link BTreeCounters}.
     * <p>
     * Note: This is used by the {@link IndexManager} to ensure that an index
     * loaded from its backing store uses the {@link BTreeCounters} associated
     * with that index since the {@link DataService} was last (re-)started.
     * 
     * @param btreeCounters
     *            The counters to be used.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    final public void setBTreeCounters(final BTreeCounters btreeCounters) {

        if (btreeCounters == null)
            throw new IllegalArgumentException();

        /*
         * Note: synchronized NOT required since reference is volatile.
         */

        this.btreeCounters = btreeCounters;

    }
    
    /**
     * The backing store.
     */
    protected final IRawStore store;

	/**
	 * The #of bits in the address space for a directory page (from the
	 * constructor). This constant is specified when the hash tree is created. A
	 * directory page has <code>2^addressBits</code> entries. Those entries are
	 * divided up among one or more buddy hash tables on that page.
	 */
    protected final int addressBits;

//	/**
//	 * The #of entries in a directory bucket, which is 2^{@link #addressBits}
//	 * (aka <code>1<<addressBits</code>).
//	 */
//    protected final int branchingFactor;
    
    /**
     * The root directory.
     */
    protected volatile DirectoryPage root;

    /**
     * Nodes (that is nodes or leaves) are added to a hard reference queue when
     * they are created or read from the store. On eviction from the queue a
     * dirty node is serialized by a listener against the {@link IRawStore}.
     * The nodes and leaves refer to their parent with a {@link WeakReference}s.
     * Likewise, nodes refer to their children with a {@link WeakReference}.
     * The hard reference queue in combination with {@link #touch(AbstractNode)}
     * and with hard references held on the stack ensures that the parent and/or
     * children remain reachable during operations. Once the node is no longer
     * strongly reachable weak references to that node may be cleared by the VM -
     * in this manner the node will become unreachable by navigation from its
     * ancestors in the btree.  The special role of the hard reference queue is
     * to further ensure that dirty nodes remain dirty by defering persistence
     * until the reference count for the node is zero during an eviction from
     * the queue.
     * <p>
     * Note that nodes are evicted as new nodes are added to the hard reference
     * queue. This occurs in two situations: (1) when a new node is created
     * during a split of an existing node; and (2) when a node is read in from
     * the store. Inserts on this hard reference queue always drive evictions.
     * Incremental writes basically make it impossible for the commit set to get
     * "too large" - the maximum #of nodes to be written is bounded by the size
     * of the hard reference queue. This helps to ensure fast commit operations
     * on the store.
     * <p>
     * The minimum capacity for the hard reference queue is two (2) so that a
     * split may occur without forcing eviction of either node participating in
     * the split.
     * <p>
     * Note: The code in {@link Node#postOrderNodeIterator(boolean, boolean)} and
     * {@link DirtyChildIterator} MUST NOT touch the hard reference queue since
     * those iterators are used when persisting a node using a post-order
     * traversal. If a hard reference queue eviction drives the serialization of
     * a node and we touch the hard reference queue during the post-order
     * traversal then we break down the semantics of
     * {@link HardReferenceQueue#add(Object)} as the eviction does not
     * necessarily cause the queue to reduce in length. Another way to handle
     * this is to have {@link HardReferenceQueue#add(Object)} begin to evict
     * objects before is is actually at capacity, but that is also a bit
     * fragile.
     * <p>
     * Note: The {@link #writeRetentionQueue} uses a {@link HardReferenceQueue}.
     * This is based on a {@link RingBuffer} and is very fast. It does not use a
     * {@link HashMap} because we can resolve the {@link WeakReference} to the
     * child {@link Node} or {@link Leaf} using top-down navigation as long as
     * the {@link Node} or {@link Leaf} remains strongly reachable (that is, as
     * long as it is on the {@link #writeRetentionQueue} or otherwise strongly
     * held). This means that lookup in a map is not required for top-down
     * navigation.
     * <p>
     * The {@link LRUNexus} provides an {@link INodeData} / {@link ILeafData}
     * data record cache based on a hash map with lookup by the address of the
     * node or leaf. This is tested when the child {@link WeakReference} was
     * never set or has been cleared. This cache is also used by the
     * {@link IndexSegment} for the linked-leaf traversal pattern, which does
     * not use top-down navigation.
     * 
     * @todo consider a policy that dynamically adjusts the queue capacities
     *       based on the height of the btree in order to maintain a cache that
     *       can contain a fixed percentage, e.g., 5% or 10%, of the nodes in
     *       the btree. The minimum and maximum size of the cache should be
     *       bounded. Bounding the minimum size gives better performance for
     *       small trees. Bounding the maximum size is necessary when the trees
     *       grow very large. (Partitioned indices may be used for very large
     *       indices and they can be distributed across a cluster of machines.)
     *       <p>
     *       There is a discussion of some issues regarding such a policy in the
     *       code inside of
     *       {@link Node#Node(BTree btree, AbstractNode oldRoot, int nentries)}.
     */
    final protected IHardReferenceQueue<PO> writeRetentionQueue;

    /**
     * The #of distinct nodes and leaves on the {@link #writeRetentionQueue}.
     */
    protected int ndistinctOnWriteRetentionQueue;

    /**
     * Return <code>true</code> iff this B+Tree is read-only.
     */
    abstract public boolean isReadOnly();
    
    /**
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree is read-only.
     * 
     * @see #isReadOnly()
     */
    final protected void assertNotReadOnly() {
        
        if(isReadOnly()) {
            
            throw new UnsupportedOperationException(ERROR_READ_ONLY);
            
        }
        
    }
    
//    /**
//     * The metadata record for the index. This data rarely changes during the
//     * life of the {@link HTree} object, but it CAN be changed.
//     */
//    protected IndexMetadata metadata;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final protected NodeSerializer nodeSer = null;

    /**
     * The backing store.
     */
    public IRawStore getStore() {
    	return store;
    }

	/**
	 * The #of bits in the address space for a hash directory page. This
	 * constant is specified to the constructor. The #of child pages is
	 * <code>2^addressBits</code>. When <i>addressBits</i> is <code>10</code> we
	 * have <code>2^10 := 1024</code>. If the size of a child address is 4
	 * bytes, then a 10 bit address space implies a 4k page size.
	 */
    public final int getAddressBits() {
    	return addressBits;
    }
    
	/**
	 * The #of {@link DirectoryPage}s in the {@link HTree} (not buddy hash
	 * tables, but the pages on which they appear).
	 */
	abstract public int getNodeCount();

	/**
	 * The #of {@link BucketPage}s in the {@link HTree} (not buddy hash buckets,
	 * but the pages on which they appear).
	 */
    abstract public int getLeafCount();
    
    /**
     * The #of tuples in the {@link HTree}.
     */
    abstract public int getEntryCount();
    
    /**
	 * @param store
	 *            The persistence store.
	 * @param nodeFactory
	 *            Object that provides a factory for node and leaf objects.
	 * @param readOnly
	 *            <code>true</code> IFF it is <em>known</em> that the
	 *            {@link AbstractBTree} is read-only.
	 * @param metadata
	 *            The {@link IndexMetadata} object for this B+Tree.
	 * @param recordCompressorFactory
	 *            Object that knows how to (de-)compress the serialized data
	 *            records.
	 */
    protected AbstractHTree(//
            final IRawStore store,//
//            final INodeFactory nodeFactory,//
            final boolean readOnly,
            final int addressBits // TODO Move into IndexMetadata?
//            final IndexMetadata metadata,//
//            final IRecordCompressorFactory<?> recordCompressorFactory
            ) {

        // show the copyright banner during startup.
        Banner.banner();

        if (store == null)
            throw new IllegalArgumentException();

        if (addressBits <= 0)
            throw new IllegalArgumentException();

        if (addressBits > 32)
            throw new IllegalArgumentException();

//        if (nodeFactory == null)
//            throw new IllegalArgumentException();

        // Note: MAY be null (implies a transient BTree).
//        assert store != null;

//        if (metadata == null)
//            throw new IllegalArgumentException();
//
//        // save a reference to the immutable metadata record.
//        this.metadata = metadata;

//        this.writeTuple = new Tuple(this, KEYS | VALS);
        
        this.store = store;
        this.addressBits = addressBits;
//        this.branchingFactor = 1 << addressBits;

//        /*
//         * Compute the minimum #of children/values. This is the same whether
//         * this is a Node or a Leaf.
//         */
//        minChildren = (branchingFactor + 1) >> 1;

////        /*
////         * The Memoizer is not used by the mutable B+Tree since it is not safe
////         * for concurrent operations.
////         */
////        memo = !readOnly ? null : new ChildMemoizer(loadChild);
//        /*
//         * Note: The Memoizer pattern is now used for both mutable and read-only
//         * B+Trees. This is because the real constraint on the mutable B+Tree is
//         * that mutation may not be concurrent with any other operation but
//         * concurrent readers ARE permitted. The UnisolatedReadWriteIndex
//         * explicitly permits concurrent read operations by virtue of using a
//         * ReadWriteLock rather than a single lock.
//         */
//        memo = new ChildMemoizer(loadChild);
        
        /*
         * Setup buffer for Node and Leaf objects accessed via top-down
         * navigation. While a Node or a Leaf remains on this buffer the
         * parent's WeakReference to the Node or Leaf will not be cleared and it
         * will remain reachable.
         */
        this.writeRetentionQueue = newWriteRetentionQueue(readOnly);

//        this.nodeSer = new NodeSerializer(//
//                store, // addressManager
//                nodeFactory,//
//                branchingFactor,//
//                0, //initialBufferCapacity
//                metadata,//
//                readOnly,//
//                recordCompressorFactory
//                );
        
//        if (store == null) {
//
//            /*
//             * Transient BTree.
//             * 
//             * Note: The write retention queue controls how long nodes remain
//             * mutable. On eviction, they are coded but not written onto the
//             * backing store (since there is none for a transient BTree).
//             * 
//             * The readRetentionQueue is not used for a transient BTree since
//             * the child nodes and the parents are connected using hard links
//             * rather than weak references.
//             */
//
//            this.storeCache = null;
//            
////            this.globalLRU = null;
//            
////            this.readRetentionQueue = null;
//            
//        } else {
//
//            /*
//             * Persistent BTree.
//             * 
//             * The global LRU is used to retain recently used node/leaf data
//             * records in memory and the per-store cache provides random access
//             * to those data records. Only the INodeData or ILeafData is stored
//             * in the cache. This allows reuse of the data records across B+Tree
//             * instances since the data are read-only and the data records
//             * support concurrent read operations. The INodeData or ILeafData
//             * will be wrapped as a Node or Leaf by the owning B+Tree instance.
//             */
//
//            /*
//             * FIXME if the LRUNexus is disabled, then use a
//             * ConcurrentWeakValueCacheWithTimeout to buffer the leaves of an
//             * IndexSegment. Essentially, a custom cache. Otherwise we lose some
//             * of the performance of the leaf iterator for the index segment
//             * since leaves are not recoverable by random access without a
//             * cache.
//             */
//            this.storeCache = LRUNexus.getCache(store);
//            
////            this.readRetentionQueue = newReadRetentionQueue();
//        
//        }

    }

    /**
     * Note: Method is package private since it must be overridden for some unit
     * tests.
     */
    IHardReferenceQueue<PO> newWriteRetentionQueue(final boolean readOnly) {

    	// FIXME Restore use of the [metadata] object.
    	final int writeRetentionQueueCapacity = 1000;//metadata.getWriteRetentionQueueCapacity();
    	final int writeRetentionQueueScan = 5;//metadata.getWriteRetentionQueueScan();
    	
        if(readOnly) {

            /*
             * This provisions an alternative hard reference queue using thread
             * local queues to collect hard references which are then batched
             * through to the backing hard reference queue in order to reduce
             * contention for the lock required to write on the backing hard
             * reference queue (no lock is required for the thread-local
             * queues).
             * 
             * The danger with a true thread-local design is that a thread can
             * come in and do some work, get some updates buffered in its
             * thread-local array, and then never visit again. In this case
             * those updates would remain buffered on the thread and would not
             * in fact cause the access order to be updated in a timely manner.
             * Worse, if you are relying on WeakReference semantics, the
             * buffered updates would remain strongly reachable and the
             * corresponding objects would be wired into the cache.
             * 
             * I've worked around this issue by scoping the buffers to the
             * AbstractBTree instance. When the B+Tree container is closed, all
             * buffered updates were discarded. This nicely eliminated the
             * problems with "escaping" threads. This approach also has the
             * maximum concurrency since there is no blocking when adding a
             * touch to the thread-local buffer.
             * 
             * Another approach is to use striped locks. The infinispan BCHM
             * does this. In this approach, the Segment is guarded by a lock and
             * the array buffering the touches is inside of the Segment. Since
             * the Segment is selected by the hash of the key, all Segments will
             * be visited in a timely fashion for any reasonable workload. This
             * ensures that updates can not "escape" and will be propagated to
             * the shared backing buffer in a timely manner.
             */
            
            return new HardReferenceQueueWithBatchingUpdates<PO>(//
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            writeRetentionQueueCapacity, 0/* nscan */),
//                    new DefaultEvictionListener(),//
//                    metadata.getWriteRetentionQueueCapacity(),// shared capacity
                    writeRetentionQueueScan,// thread local
                    128,//64, // thread-local queue capacity @todo config
                    64, //32 // thread-local tryLock size @todo config
                    null // batched updates listener.
            );

        }
        
        return new HardReferenceQueue<PO>(//
                new DefaultEvictionListener(),//
                writeRetentionQueueCapacity,//
                writeRetentionQueueScan//
        );

    }

	/**
	 * Recursive dump of the tree.
	 * 
	 * @param out
	 *            The dump is written on this stream.
	 * 
	 * @return true unless an inconsistency is detected.
	 */
	public boolean dump(final PrintStream out) {

		return dump(BTree.dumpLog.getEffectiveLevel(), out, false/* materialize */);

    }

	public boolean dump(final Level level, final PrintStream out,
			final boolean materialize) {

        // True iff we will write out the node structure.
        final boolean info = level.toInt() <= Level.INFO.toInt();

//        final IBTreeUtilizationReport utils = getUtilization();

        if (info) {

			out.print("addressBits=" + addressBits);
			out.print(", (2^addressBits)=" + (1 << addressBits));
			out.print(", #nodes=" + getNodeCount());
			out.print(", #leaves=" + getLeafCount());
			out.print(", #entries=" + getEntryCount());
			out.println();
//                    + ", nodeUtil="
//                    + utils.getNodeUtilization() + "%, leafUtil="
//                    + utils.getLeafUtilization() + "%, utilization="
//                    + utils.getTotalUtilization() + "%"
        }

        if (root != null) {

            return root.dump(level, out, 0, true, materialize);

        } else
            return true;

    }

    /**
     * <p>
     * This method is responsible for putting the node or leaf onto the ring
     * buffer which controls (a) how long we retain a hard reference to the node
     * or leaf; and (b) for writes, when the node or leaf is evicted with a zero
     * reference count and made persistent (along with all dirty children). The
     * concurrency requirements and the implementation behavior and guarentees
     * differ depending on whether the B+Tree is read-only or mutable for two
     * reasons: For writers, the B+Tree is single-threaded so there is no
     * contention. For readers, every touch on the B+Tree goes through this
     * point, so it is vital to make this method non-blocking.
     * </p>
     * <h3>Writers</h3>
     * <p>
     * This method guarantees that the specified node will NOT be synchronously
     * persisted as a side effect and thereby made immutable. (Of course, the
     * node may be already immutable.)
     * </p>
     * <p>
     * In conjunction with {@link DefaultEvictionListener}, this method
     * guarantees that the reference counter for the node will reflect the #of
     * times that the node is actually present on the
     * {@link #writeRetentionQueue}.
     * </p>
     * <p>
     * If the node is not found on a scan of the head of the queue, then it is
     * appended to the queue and its {@link AbstractNode#referenceCount} is
     * incremented. If a node is being appended to the queue and the queue is at
     * capacity, then this will cause a reference to be evicted from the queue.
     * If the reference counter for the evicted node or leaf is zero and the
     * evicted node or leaf is dirty, then a data record will be coded for the
     * evicted node or leaf and written onto the backing store. A subsequent
     * attempt to modify the node or leaf will force copy-on-write for that node
     * or leaf. Regardless of whether or not the node or leaf is dirty, it is
     * touched on the {@link LRUNexus#getGlobalLRU()} when it is evicted from
     * the write retention queue.
     * </p>
     * <p>
     * For the mutable B+Tree we also track the #of references to the node/leaf
     * on the ring buffer. When that reference count reaches zero we do an
     * eviction and the node/leaf is written onto the backing store if it is
     * dirty. Those reference counting games DO NOT matter for read-only views
     * so we can take a code path which does not update the per-node/leaf
     * reference count and we do not need to use either synchronization or
     * atomic counters to track the reference counts.
     * </p>
     * <h3>Readers</h3>
     * <p>
     * In order to reduce contention for the lock required to update the backing
     * queue, the {@link #writeRetentionQueue} is configured to collect
     * references for touched nodes or leaves in a thread-local queue and then
     * batch those references through to the backing hard reference queue while
     * holding the lock.
     * </p>
     * 
     * @param node
     *            The node or leaf.
     * 
     * @todo The per-node/leaf reference counts and
     *       {@link #ndistinctOnWriteRetentionQueue} fields are not guaranteed
     *       to be consistent for of concurrent readers since no locks are held
     *       when those fields are updated. Since the reference counts only
     *       effect when a node is made persistent (assuming its dirty) and
     *       since we already require single threaded access for writes on the
     *       btree, this does not cause a problem but can lead to unexpected
     *       values for the reference counters and
     *       {@link #ndistinctOnWriteRetentionQueue}.
     * 
     * @todo This might be abstract here and final in BTree and in IndexSegment.
     *       For {@link IndexSegment}, we know it is read-only so we do not need
     *       to test anything. For {@link BTree}, we can break encapsulation and
     *       check whether or not the {@link BTree} is read-only more readily
     *       than we can in this class.
     */
//    synchronized
//    final 
    protected void touch(final AbstractPage node) {

        assert node != null;

        /*
         * Note: DO NOT update the last used timestamp for the B+Tree here! This
         * is a huge performance penalty!
         */
//        touch();

        if (isReadOnly()) {

            doTouch(node);

            return;

        }

        /*
         * Note: Synchronization appears to be necessary for the mutable BTree.
         * Presumably this provides safe publication when the application is
         * invoking operations on the same mutable BTree instance from different
         * threads but it coordinating those threads in order to avoid
         * concurrent operations, e.g., by using the UnisolatedReadWriteIndex
         * wrapper class. The error which is prevented by synchronization is
         * RingBuffer#add(ref) reporting that size == capacity, which indicates
         * that the size was not updated consistently and hence is basically a
         * concurrency problem.
         * 
         * @todo Actually, I think that this is just a fence post in ringbuffer
         * beforeOffer() method and the code might work without the synchronized
         * block if the fence post was fixed.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/201
         */

        synchronized (this) {

            doTouch(node);

        }

    }
    
    private final void doTouch(final AbstractPage node) {
        
        /*
         * We need to guarantee that touching this node does not cause it to be
         * made persistent. The condition of interest would arise if the queue
         * is full and the referenceCount on the node is zero before this method
         * was called. Under those circumstances, simply appending the node to
         * the queue would cause it to be evicted and made persistent.
         * 
         * We avoid this by incrementing the reference counter before we touch
         * the queue. Since the reference counter will therefore be positive if
         * the node is selected for eviction, eviction will not cause the node
         * to be made persistent.
         * 
         * Note: Only mutable BTrees may have dirty nodes and the mutable BTree
         * is NOT thread-safe so we do not need to use synchronization or an
         * AtomicInteger for the referenceCount field.
         * 
         * Note: The reference counts and the #of distinct nodes or leaves on
         * the writeRetentionQueue are not exact for a read-only B+Tree because
         * neither synchronization nor atomic counters are used to track that
         * information.
         */

//        assert isReadOnly() || ndistinctOnWriteRetentionQueue > 0;

        node.referenceCount++;

        if (!writeRetentionQueue.add(node)) {

            /*
             * A false return indicates that the node was found on a scan of the
             * tail of the queue. In this case we do NOT want the reference
             * counter to be incremented since we have not actually added
             * another reference to this node onto the queue. Therefore we
             * decrement the counter (since we incremented it above) for a net
             * change of zero(0) across this method.
             */

            node.referenceCount--;

        } else {

            /*
             * Since we just added a node or leaf to the hard reference queue we
             * now update the #of distinct nodes and leaves on the hard
             * reference queue.
             * 
             * Also see {@link DefaultEvictionListener}.
             */

            if (node.referenceCount == 1) {

                ndistinctOnWriteRetentionQueue++;

            }

        }

//      if (useFinger && node instanceof ILeafData) {
        //
//                    if (finger == null || finger.get() != node) {
        //
//                        finger = new WeakReference<Leaf>((Leaf) node);
        //
//                    }
        //
//                }

    }

	/**
	 * Read an {@link AbstractPage} from the store.
	 * <p>
	 * Note: Callers SHOULD be synchronized in order to ensure that only one
	 * thread will read the desired node or leaf in from the store and attach
	 * the reference to the newly read node or leaf as appropriate into the
	 * existing data structures (e.g., as the root reference or as a child of a
	 * node or leaf already live in memory).
	 * 
	 * @param addr
	 *            The address in the store.
	 * @param globalDepth
	 *            The global depth of the child directory or bucket page being
	 *            read from the store. This will be set on the materialized
	 *            {@link AbstractPage}.
	 * 
	 * @return The {@link AbstractPage}.
	 * 
	 * @throws IllegalArgumentException
	 *             if the address is {@link IRawStore#NULL}.
	 */
	protected AbstractPage readNodeOrLeaf(final long addr,
			final int globalDepth) {

        if (addr == IRawStore.NULL)
            throw new IllegalArgumentException();
        
//        final Long addr2 = Long.valueOf(addr); 
//
//        if (storeCache != null) {
//
//            // test cache : will touch global LRU iff found.
//            final IAbstractNodeData data = (IAbstractNodeData) storeCache
//                    .get(addr);
//
//            if (data != null) {
//
//                // Node and Leaf MUST NOT make it into the global LRU or store
//                // cache!
//                assert !(data instanceof AbstractNode<?>);
//                
//                final AbstractNode<?> node;
//                
//                if (data.isLeaf()) {
//
//                    node = nodeSer.nodeFactory.allocLeaf(this, addr,
//                            (ILeafData) data);
//
//                } else {
//
//                    node = nodeSer.nodeFactory.allocNode(this, addr,
//                            (INodeData) data);
//
//                }
//
//                // cache hit.
//                return node;
//                
//            }
//            
//        }
        
        final ByteBuffer tmp;
        {

            final long begin = System.nanoTime();
            
            tmp = store.read(addr);
            
            assert tmp.position() == 0;
            
            // Note: This assertion is invalidated when checksums are inlined in the store records.
//            assert tmp.limit() == store.getByteCount(addr) : "limit="
//                    + tmp.limit() + ", byteCount(addr)="
//                    + store.getByteCount(addr)+", addr="+store.toString(addr);

            btreeCounters.readNanos.add( System.nanoTime() - begin );
            
            final int bytesRead = tmp.limit();

            btreeCounters.bytesRead.add(bytesRead);
            
        }
// Note: This is not necessary.  The most likely place to be interrupted is in the IO on the raw store.  It is not worth testing for an interrupt here since we are more liklely to notice one in the raw store and this method is low latency except for the potential IO read.
//        if (Thread.interrupted()) {
//
//            throw new RuntimeException(new InterruptedException());
//
//        }

        /* 
         * Extract the node from the buffer.
         */
        try {

            IAbstractNodeData data;
            {

                final long begin = System.nanoTime();

                // decode the record.
                data = nodeSer.decode(tmp);

                btreeCounters.deserializeNanos.add(System.nanoTime() - begin);

                if (data.isLeaf()) {

                    btreeCounters.leavesRead.increment();

                } else {

                    btreeCounters.nodesRead.increment();

                }

            }

//            if (storeCache != null) {
//             
//                // update cache : will touch global LRU iff cache is modified.
//                final IAbstractNodeData data2 = (IAbstractNodeData) storeCache
//                        .putIfAbsent(addr2, data);
//
//                if (data2 != null) {
//
//                    // concurrent insert, use winner's value.
//                    data = data2;
//
//                } 
//                
//            }

            // wrap as Node or Leaf.
            final AbstractPage node = null;// FIXME nodeSer.wrap(this, addr, data);

            // Note: The de-serialization ctor already does this.
//            node.setDirty(false);

            // Note: The de-serialization ctor already does this.
//            touch(node);

            // return Node or Leaf.
            return node;

        } catch (Throwable t) {

            throw new RuntimeException("De-serialization problem: addr="
                    + store.toString(addr) + " from store=" + store.getFile()
                    + " : cause=" + t, t);

        }

    }

	/**
	 * Create the reference that will be used by an {@link AbstractPage} to
	 * refer to its children (nodes or leaves).
	 * 
	 * @param child
	 *            A node.
	 * 
	 * @return A reference to that node.
	 * 
	 * @see AbstractNode#self
	 * @see SoftReference
	 * @see WeakReference
	 */
    final <T extends AbstractPage> Reference<AbstractPage> newRef(
            final AbstractPage child) {
        
        /*
         * Note: If the parent refers to its children using soft references the
         * the presence of the parent will tend to keep the children wired into
         * memory until the garbage collector is forced to sweep soft references
         * in order to make room on the heap. Such major garbage collections
         * tend to make the application "hesitate".
         * 
         * @todo it may be that frequently used access paths in the btree should
         * be converted dynamically from a weak reference to soft reference in
         * order to bias the garbage collector to leave those paths alone. if we
         * play this game then we should limit the #of soft references and make
         * the node choose among its children for those it will hold with a soft
         * reference so that the notion of frequent access is dynamic and can
         * change as the access patterns on the index change.
         */

        if (store == null) {

            /*
             * Note: Used for transient BTrees.
             */
            
            return new HardReference<AbstractPage>(child);
            
        } else {
        
            return new WeakReference<AbstractPage>( child );
//        return new SoftReference<AbstractNode>( child ); // causes significant GC "hesitations".
        }
        
        
    }

	/**
	 * A class that provides hard reference semantics for use with transient
	 * {@link HTree}s. While the class extends {@link WeakReference}, it
	 * internally holds a hard reference and thereby prevents the reference from
	 * being cleared. This approach is necessitated on the one hand by the use
	 * of {@link Reference} objects for {@link AbstractPage#self},
	 * {@link AbstractPage#parent}, {@link DirectoryPage#childRefs}, etc. and on
	 * the other hand by the impossibility of defining your own direct subclass
	 * of {@link Reference} (a runtime security manager exception will result).
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * 
	 * @param <T>
	 */
    static class HardReference<T> extends WeakReference<T> {
        
        final private T ref;
        
        HardReference(final T ref) {

            super(null);
            
            this.ref = ref;
            
        }
        
        /**
         * Returns the hard reference.
         */
        public T get() {
            
            return ref;
            
        }
        
        /**
         * Overridden as a NOP.
         */
        public void clear() {

            // NOP
            
        }
        
    }
    
}
