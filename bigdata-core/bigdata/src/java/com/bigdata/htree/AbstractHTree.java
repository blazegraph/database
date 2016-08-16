package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree.IBTreeCounters;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.EntryScanIterator;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IReadWriteLockManager;
import com.bigdata.btree.ISimpleTreeIndexAccess;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexInconsistentError;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.PO;
import com.bigdata.btree.ReadWriteLockManager;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.HardReferenceQueueWithBatchingUpdates;
import com.bigdata.cache.IHardReferenceQueue;
import com.bigdata.cache.RingBuffer;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.compression.IRecordCompressorFactory;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.DataService;
import com.bigdata.util.concurrent.Computable;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class for a persistence capable extensible hash tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractHTree implements ICounterSetAccess,
        ICheckpointProtocol, ISimpleTreeIndexAccess {

    /**
     * The index is already closed.
     */
    protected static final String ERROR_CLOSED = "Closed";

    /**
     * A parameter was less than zero.
     */
    protected static final String ERROR_LESS_THAN_ZERO = "Less than zero";

    /**
     * A parameter was too small
     */
    protected static final String ERROR_TOO_SMALL = "Too small: ";

    /**
     * A parameter was too large.
     */
    protected static final String ERROR_TOO_LARGE = "Too large: ";

    /**
     * The index is read-only but a mutation operation was requested.
     */
    final protected static String ERROR_READ_ONLY = "Read-only";

	/**
	 * The index is transient (not backed by persistent storage) but an
	 * operation that requires persistence was requested.
	 */
	final protected static String ERROR_TRANSIENT = "Transient";

    /**
     * An unisolated index view is in an error state. It must be discarded and
     * reloaded from the current checkpoint record.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate BTree
     *      objects if error occurs during eviction </a>
     */
    final protected static String ERROR_ERROR_STATE = "Index is in error state";

	final public int MIN_ADDRESS_BITS = 1;
    /**
     * The maximum value for the <code>addressBits</code> parameter.
     * <p>
     * Note: <code>1^32</code> overflows an int32. However, <code>2^16</code> is
     * 65536 which is a pretty large page. <code>2^20</code> is 1,048,576. It is
     * pretty difficult to imagine use cases where the fan out for the
     * {@link HTree} should be that large.
     */
	final public int MAX_ADDRESS_BITS = 16;
	
	protected static final transient Logger log = Logger
			.getLogger(AbstractHTree.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();
    
	/**
	 * Log for {@link AbstractHTree#dump(PrintStream)} and friends.
	 */
	public static final Logger dumpLog = Logger.getLogger(AbstractHTree.class
			.getName() + "#dump");

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
	 * 
	 * TODO Refactor / reuse performance counters for HTree and collect counters
	 * in the code.
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
    
//	/**
//	 * {@inheritDoc}
//	 * <p>
//	 * Return some "statistics" about the btree including both the static
//	 * {@link CounterSet} and the {@link BTreeCounters}s.
//	 * <p>
//	 * Note: counters reporting directly on the {@link AbstractBTree} use a
//	 * snapshot mechanism which prevents a hard reference to the
//	 * {@link AbstractBTree} from being attached to the return
//	 * {@link CounterSet} object. One consequence is that these counters will
//	 * not update until the next time you invoke {@link #getCounters()}.
//	 * <p>
//	 * Note: In order to snapshot the counters use {@link OneShotInstrument} to
//	 * prevent the inclusion of an inner class with a reference to the outer
//	 * {@link AbstractBTree} instance.
//	 * 
//	 * @see BTreeCounters#getCounters()
//	 * 
//	 * @todo use same instance of BTreeCounters for all BTree instances in
//	 *       standalone!
//	 * 
//	 * @todo estimate heap requirements for nodes and leaves based on their
//	 *       state (keys, values, and other arrays). report estimated heap
//	 *       consumption here.
//	 */
    public CounterSet getCounters() {

		final CounterSet counterSet = new CounterSet();
		{
			
			counterSet.addCounter("index UUID", new OneShotInstrument<String>(
					getIndexMetadata().getIndexUUID().toString()));

			counterSet.addCounter("class", new OneShotInstrument<String>(
					getClass().getName()));

		}

		/*
		 * Note: These statistics are reported using a snapshot mechanism which
		 * prevents a hard reference to the AbstractBTree from being attached to
		 * the CounterSet object!
		 */
		{

			final CounterSet tmp = counterSet
					.makePath(IBTreeCounters.WriteRetentionQueue);

			tmp.addCounter("Capacity", new OneShotInstrument<Integer>(
					writeRetentionQueue.capacity()));

			tmp.addCounter("Size", new OneShotInstrument<Integer>(
					writeRetentionQueue.size()));

			tmp.addCounter("Distinct", new OneShotInstrument<Integer>(
					ndistinctOnWriteRetentionQueue));

        }
        
		/*
		 * Note: These statistics are reported using a snapshot mechanism which
		 * prevents a hard reference to the AbstractBTree from being attached to
		 * the CounterSet object!
		 */
		{

			final CounterSet tmp = counterSet
					.makePath(IBTreeCounters.Statistics);

			tmp.addCounter("addressBits", new OneShotInstrument<Integer>(
					addressBits));

//			tmp.addCounter("height",
//					new OneShotInstrument<Integer>(getHeight()));

			tmp.addCounter("nodeCount", new OneShotInstrument<Long>(
					getNodeCount()));

			tmp.addCounter("leafCount", new OneShotInstrument<Long>(
					getLeafCount()));

			tmp.addCounter("tupleCount", new OneShotInstrument<Long>(
					getEntryCount()));

//			/*
//			 * Note: The utilization numbers reported here are a bit misleading.
//			 * They only consider the #of index positions in the node or leaf
//			 * which is full, but do not take into account the manner in which
//			 * the persistence store allocates space to the node or leaf. For
//			 * example, for the WORM we do perfect allocations but retain many
//			 * versions. For the RWStore, we do best-fit allocations but recycle
//			 * old versions. The space efficiency of the persistence store is
//			 * typically the main driver, not the utilization rate as reported
//			 * here.
//			 */
//			final IBTreeUtilizationReport r = getUtilization();
//			
//			// % utilization in [0:100] for nodes
//			tmp.addCounter("%nodeUtilization", new OneShotInstrument<Integer>(r
//					.getNodeUtilization()));
//			
//			// % utilization in [0:100] for leaves
//			tmp.addCounter("%leafUtilization", new OneShotInstrument<Integer>(r
//					.getLeafUtilization()));
//
//			// % utilization in [0:100] for the whole tree (nodes + leaves).
//			tmp.addCounter("%totalUtilization", new OneShotInstrument<Integer>(r
//					.getTotalUtilization())); // / 100d

			/*
			 * Compute the average bytes per tuple. This requires access to the
			 * current entry count, so we have to do this as a OneShot counter
			 * to avoid dragging in the B+Tree reference.
			 */

			final long entryCount = getEntryCount();

			final long bytes = btreeCounters.bytesOnStore_nodesAndLeaves.get()
					+ btreeCounters.bytesOnStore_rawRecords.get();

			final long bytesPerTuple = (long) (entryCount == 0 ? 0d
					: (bytes / entryCount));

			tmp.addCounter("bytesPerTuple", new OneShotInstrument<Long>(
					bytesPerTuple));

		}

		/*
		 * Attach detailed performance counters.
		 * 
		 * Note: The BTreeCounters object does not have a reference to the
		 * AbstractBTree. Its counters will update "live" since we do not
		 * need to snapshot them.
		 */
		counterSet.attach(btreeCounters.getCounters());

        return counterSet;

    }
   
    /**
     * The backing store.
     */
    protected final IRawStore store;

    /**
     * When <code>true</code> the {@link AbstractHTree} does not permit
     * mutation.
     */
    final protected boolean readOnly;

    /**
	 * The #of bits in the address space for a directory page (from the
	 * constructor). This constant is specified when the hash tree is created. A
	 * directory page has <code>2^addressBits</code> entries. Those entries are
	 * divided up among one or more buddy hash tables on that page.
	 */
    protected final int addressBits;
    
    /**
     * The #of bucket slots in a bucket page. There is a case for allowing the number of
     * bucket slots to be different than the number of directory page slots (as determined
     * by the number of addressBits) since the storage requirements for bucket 
     * values/keys is not set.
     */
    protected final int bucketSlots;

    /**
     * Hard reference iff the index is mutable (aka unisolated) allows us to
     * avoid patterns that create short life time versions of the object to
     * protect {@link #writeCheckpoint2()} and similar operations.
     */
    private final IReadWriteLockManager lockManager;

//	/**
//	 * The #of entries in a directory bucket, which is 2^{@link #addressBits}
//	 * (aka <code>1<<addressBits</code>).
//	 */
//    protected final int branchingFactor;
    
    /**
     * Helper class models a request to load a child node.
     * <p>
     * Note: This class must implement equals() and hashCode() since it is used
     * within the {@link Memoizer} pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static class LoadChildRequest {

        /** The parent node. */

    	final DirectoryPage parent;

        /** The child index. */
        final int index;

        /**
         * 
         * @param parent
         *            The parent node.
         * @param index
         *            The child index.
         */
        public LoadChildRequest(final DirectoryPage parent, final int index) {
    
            this.parent = parent;
            
            this.index = index;
            
        }

        /**
         * Equals returns true iff parent == o.parent and index == o.index.
         */
        public boolean equals(final Object o) {

            if (!(o instanceof LoadChildRequest))
                return false;

            final LoadChildRequest r = (LoadChildRequest) o;

            return parent == r.parent && index == r.index;
            
        }

        /**
         * The hashCode() implementation assumes that the parent's hashCode() is
         * well distributed and just adds in the index to that value to improve
         * the chance of a distinct hash value.
         */
        public int hashCode() {
            
            return parent.hashCode() + index;
            
        }
        
    }

    /**
     * Helper loads a child node from the specified address by delegating to
     * {@link Node#_getChild(int)}.
     */
    final private static Computable<LoadChildRequest, AbstractPage> loadChild = new Computable<LoadChildRequest, AbstractPage>() {

        /**
         * Loads a child node from the specified address.
         * 
         * @return A hard reference to that child node.
         * 
         * @throws IllegalArgumentException
         *             if addr is <code>null</code>.
         * @throws IllegalArgumentException
         *             if addr is {@link IRawStore#NULL}.
         */
        public AbstractPage compute(final LoadChildRequest req)
                throws InterruptedException {

//            try {

                return req.parent._getChild(req.index, req);

//            } finally {
//
//                /*
//                 * Clear the future task from the memoizer cache.
//                 * 
//                 * Note: This is necessary in order to prevent the cache from
//                 * retaining a hard reference to each child materialized for the
//                 * B+Tree.
//                 * 
//                 * Note: This does not depend on any additional synchronization.
//                 * The Memoizer pattern guarantees that only one thread actually
//                 * call ft.run() and hence runs this code.
//                 */
//
//                req.parent.htree.memo.removeFromCache(req);
//
//            }

        }
        
    };

    /**
     * A {@link Memoizer} subclass which exposes an additional method to remove
     * a {@link FutureTask} from the internal cache. This is used as part of an
     * explicit protocol in {@link DirectoryPage#_getChild(int)} to clear out
     * cache entries once the child reference has been set on
     * {@link DirectoryPage#childRefs}. This is package private since it must be
     * visible to {@link DirectoryPage#_getChild(int)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static class ChildMemoizer extends
            Memoizer<LoadChildRequest/* request */, AbstractPage/* child */> {

        /**
         * @param c
         */
        public ChildMemoizer(
                final Computable<LoadChildRequest, AbstractPage> c) {

            super(c);

        }

//        /**
//         * The approximate size of the cache (used solely for debugging to
//         * detect cache leaks).
//         */
//        int size() {
//            
//            return cache.size();
//            
//        }

        /**
         * Called by the thread which atomically sets the
         * {@link DirectoryPage#childRefs} element to the computed
         * {@link AbstractPage}. At that point a reference exists to the child
         * on the parent.
         * 
         * @param req
         *            The request.
         */
        void removeFromCache(final LoadChildRequest req) {

            if (cache.remove(req) == null) {

                throw new AssertionError();
                
            }

        }

//        /**
//         * Called from {@link AbstractBTree#close()}.
//         * 
//         * @todo should we do this?  There should not be any reads against the
//         * the B+Tree when it is close()d.  Therefore I do not believe there 
//         * is any reason to clear the FutureTask cache.
//         */
//        void clear() {
//            
//            cache.clear();
//            
//        }
        
    };

    /**
     * Used to materialize children without causing concurrent threads passing
     * through the same parent node to wait on the IO for the child. This is
     * <code>null</code> for a mutable B+Tree since concurrent requests are not
     * permitted for the mutable B+Tree.
     * 
     * @see DirectoryPage#getChild(int)
     */
    final ChildMemoizer memo;

    /**
     * {@link Memoizer} pattern for non-blocking concurrent reads of child
     * nodes. This is package private. Use {@link Node#getChild(int)} instead.
     * 
     * @param parent
     *            The node whose child will be materialized.
     * @param index
     *            The index of that child.
     * 
     * @return The child and never <code>null</code>.
     * 
     * @see DirectoryPage#getChild(int)
     */
    AbstractPage loadChild(final DirectoryPage parent, final int index) {

            try {

                return memo.compute(new LoadChildRequest(parent, index));

            } catch (InterruptedException e) {

                /*
                 * Note: This exception will be thrown iff interrupted while
                 * awaiting the FutureTask inside of the Memoizer.
                 */

                throw new RuntimeException(e);

            }

    }

    /**
     * The root directory.
     */
    protected volatile DirectoryPage root;

    /**
     * This field is set if an error is encountered that renders an unisolated
     * index object unusable. For example, this can occur if an error was
     * detected during incremental eviction of dirty nodes for a mutable index
     * view since that means that there are partly serialized (and possibly
     * inconsistenly serialized) evicted pages. Once this becomes non-
     * <code>null</code> the index MUST be reloaded from the most recent
     * checkpoint before it can be used (that is, you need to obtain a new view
     * of the unisolated index since this field is sticky once set).
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate BTree
     *      objects if error occurs during eviction </a>
     */
    protected volatile Throwable error;

    /**
	 * Nodes (that is nodes or leaves) are added to a hard reference queue when
	 * they are created or read from the store. On eviction from the queue a
	 * dirty node is serialized by a listener against the {@link IRawStore}. The
	 * nodes and leaves refer to their parent with a {@link WeakReference}s.
	 * Likewise, nodes refer to their children with a {@link WeakReference}. The
	 * hard reference queue in combination with {@link #touch(AbstractPage)} and
	 * with hard references held on the stack ensures that the parent and/or
	 * children remain reachable during operations. Once the node is no longer
	 * strongly reachable weak references to that node may be cleared by the VM
	 * - in this manner the node will become unreachable by navigation from its
	 * ancestors in the btree. The special role of the hard reference queue is
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
	 * Note: The code in
	 * {@link AbstractPage#postOrderNodeIterator(boolean, boolean)} and
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
	 * child {@link DirectoryPage} or {@link BucketPage} using top-down
	 * navigation as long as the {@link DirectoryPage} or {@link BucketPage}
	 * remains strongly reachable (that is, as long as it is on the
	 * {@link #writeRetentionQueue} or otherwise strongly held). This means that
	 * lookup in a map is not required for top-down navigation.
	 */
    final protected IHardReferenceQueue<PO> writeRetentionQueue;

    /**
     * The #of distinct nodes and leaves on the {@link #writeRetentionQueue}.
     */
    protected int ndistinctOnWriteRetentionQueue;

    /**
     * The maximum number of threads to apply when evicting a level set of 
     * nodes or leaves in parallel. When ONE (1), parallel eviction will be
     * disabled for the index.
     * 
     * @see #writeNodeRecursiveConcurrent(AbstractNode)
     * 
     * @see BLZG-1665 (Reduce commit latency by parallel checkpoint by level of
     *      dirty pages in an index)
     */
    final private int maxParallelEvictThreads;
    
    /**
     * The minimum number of threads to apply when evicting a level set of nodes
     * or leaves in parallel (GTE ONE(2)). When TWO (2), parallel eviction will
     * be used even if there are only two nodes / leaves in a given level set.
     * A higher value may be used to ensure that parallelism is only applied
     * when there is a significantly opportunity for concurrent eviction, such
     * as during a checkpoint against an index with a large dirty write set.
     * 
     * @see #writeNodeRecursiveConcurrent(AbstractNode)
     * 
     * @see BLZG-1665 (Reduce commit latency by parallel checkpoint by level of
     *      dirty pages in an index)
     */
    final private int minDirtyListSizeForParallelEvict;
    
    /**
     * Returns the metadata record for this index.
     * <p>
     * Note: If the index is read-only then the metadata object will be cloned
     * to avoid potential modification. However, only a single cloned copy of
     * the metadata record will be shared between all callers for a given
     * instance of this class.
     * 
     * @todo the clone once policy is a compromise between driving the read only
     *       semantics into the {@link IndexMetadata} object, cloning each time,
     *       and leaving it to the caller to clone as required. Either the
     *       former or the latter might be a better choice.
     * 
     * FIXME if the metadata record is updated (and it can be) then we really
     * need to invalidate the cloned metadata record also.
     * 
     * @return The metadata record for this btree and never <code>null</code>.
     */
    public HTreeIndexMetadata getIndexMetadata() {

        if (isReadOnly()) {

            if (metadata2 == null) {

                synchronized (this) {

                    if (metadata2 == null) {

                        metadata2 = metadata.clone();

                    }

                }

            }

            return metadata2;
            
        }
        
        return metadata;
        
    }
    private volatile HTreeIndexMetadata metadata2;

    /**
     * The metadata record for the index. This data rarely changes during the
     * life of the {@link HTree} object, but it CAN be changed.
     */
    protected HTreeIndexMetadata metadata;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final protected NodeSerializer nodeSer;

    /**
     * {@inheritDoc}
     * <p>
     * Note: CLOSING A TRANSIENT INDEX WILL DISCARD ALL DATA!
     * <p>
     * This implementation clears the hard reference queue (releasing all node
     * references), releases the hard reference to the root node, and releases
     * the buffers on the {@link NodeSerializer} (they will be naturally
     * reallocated on reuse).
     * <p>
     * Note: {@link AbstractHTree} is NOT thread-safe and {@link #close()} MUST
     * be invoked in a context in which there will not be concurrent threads --
     * the natural choice being the single-threaded commit service on the
     * journal.
     * 
     * @exception IllegalStateException
     *                if the root is <code>null</code>, indicating that the
     *                index is already closed.
     * 
     * @exception IllegalStateException
     *                if the root is dirty (this implies that this is a mutable
     *                HTree and there are mutations that have not been written
     *                through to the store)
     */
    @Override
    synchronized public void close() {

        if (root == null) {

            throw new IllegalStateException(ERROR_CLOSED);

        }

        if (INFO || BigdataStatics.debug) {

            final String msg = "HTree close: name="
                    + metadata.getName()
                    + ", dirty="
                    + root.isDirty()
                    + ", nnodes="
                    + getNodeCount()
                    + ", nleaves="
                    + getLeafCount()
                    + ", nentries="
                    + getEntryCount()
                    + ", impl="
                    + (this instanceof HTree ? ((HTree) this).getCheckpoint()
                            .toString() : getClass().getSimpleName());

            if (INFO)
                log.info(msg);

        }

        /*
         * Release buffers.
         */
        if (nodeSer != null) {
         
            nodeSer.close();
            
        }

        /*
         * Clear the hard reference queue.
         * 
         * Note: This is safe since we know as a pre-condition that the root
         * node is clean and therefore that there are no dirty nodes or leaves
         * in the hard reference queue.
         * 
         * @todo Clearing the write retention queue here is important. However,
         * it may fail to transfer clean nodes and leaves to the global LRU.
         */
        writeRetentionQueue.clear(true/* clearRefs */);
        ndistinctOnWriteRetentionQueue = 0;
        
//        if (readRetentionQueue != null) {
//            
//            readRetentionQueue.clear(true/* clearRefs */);
//            
//        }

        /*
         * Clear the reference to the root node (permits GC).
         */
        root = null;

// TODO Support bloom filter?
//         release the optional bloom filter.
//        bloomFilter = null;
        
    }

    /**
     * {@inheritDoc}.
     * <p>
     * This method delegates to {@link #_reopen()} if double-checked locking
     * demonstrates that the {@link #root} is <code>null</code> (indicating that
     * the index has been closed). This method is automatically invoked by a
     * variety of methods that need to ensure that the index is available for
     * use.
     */
    final public void reopen() {

        if (root == null) {

            /*
             * reload the root node.
             * 
             * Note: This is synchronized to avoid race conditions when
             * re-opening the index from the backing store.
             * 
             * Note: [root] MUST be marked as [volatile] to guarentee correct
             * semantics.
             * 
             * See http://en.wikipedia.org/wiki/Double-checked_locking
             */

            synchronized(this) {
            
                if (root == null) {

                    // invoke with lock on [this].
                    _reopen();
                    
                }
                
            }

        }

    }

    /**
     * This method is responsible for setting up the root leaf (either new or
     * read from the store), the bloom filter, etc. It is invoked by
     * {@link #reopen()} once {@link #root} has been show to be
     * <code>null</code> with double-checked locking. When invoked in this
     * context, the caller is guaranteed to hold a lock on <i>this</i>. This is
     * done to ensure that at most one thread gets to re-open the index from the
     * backing store.
     */
    abstract protected void _reopen();

    final public boolean isOpen() {

        return root != null;

    }

    /**
     * Return <code>true</code> iff this is a transient data structure (no
     * backing store).
     */
    final public boolean isTransient() {
        
        return store == null;
        
    }
    
    final protected void assertNotTransient() {
        
        if(isTransient()) {
            
            throw new UnsupportedOperationException(ERROR_TRANSIENT);
            
        }
        
    }
    
    /**
     * Return <code>true</code> iff this B+Tree is read-only.
     */
    final public boolean isReadOnly() {
        
        return readOnly;
        
    }
    
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
        
        if( error != null )
            throw new IndexInconsistentError(ERROR_ERROR_STATE, error);
        
    }
    
    /**
     * The timestamp associated with the last {@link IAtomicStore#commit()} in
     * which writes buffered by this index were made restart-safe on the backing
     * store. The lastCommitTime is set when the index is loaded from the
     * backing store and updated after each commit. It is ZERO (0L) when
     * {@link HTree} is first created and will remain ZERO (0L) until the
     * {@link HTree} is committed.  If the backing store does not support atomic
     * commits, then this value will always be ZERO (0L).
     */
    @Override
    abstract public long getLastCommitTime();

//    /**
//     * The timestamp associated with unisolated writes on this index. This
//     * timestamp is designed to allow the interleaving of full transactions
//     * (whose revision timestamp is assigned by the transaction service) with
//     * unisolated operations on the same indices.
//     * <p>
//     * The revision timestamp assigned by this method is
//     * <code>lastCommitTime+1</code>. The reasoning is as follows. Revision
//     * timestamps are assigned by the transaction manager when the transaction
//     * is validated as part of its commit protocol. Therefore, revision
//     * timestamps are assigned after the transaction write set is complete.
//     * Further, the assigned revisionTimestamp will be strictly LT the
//     * commitTime for that transaction. By using <code>lastCommitTime+1</code>
//     * we are guaranteed that the revisionTimestamp for new writes (which will
//     * be part of some future commit point) will always be strictly GT the
//     * revisionTimestamp of historical writes (which were part of some prior
//     * commit point).
//     * <p>
//     * Note: Unisolated operations using this timestamp ARE NOT validated. The
//     * timestamp is simply applied to the tuple when it is inserted or updated
//     * and will become part of the restart safe state of the B+Tree once the
//     * unisolated operation participates in a commit.
//     * <p>
//     * Note: If an unisolated operation were to execute concurrent with a
//     * transaction commit for the same index then that could produce
//     * inconsistent results in the index and could trigger concurrent
//     * modification errors. In order to avoid such concurrent modification
//     * errors, unisolated operations which are to be mixed with full
//     * transactions MUST ensure that they have exclusive access to the
//     * unisolated index before proceeding. There are two ways to do this: (1)
//     * take the application off line for transactions; (2) submit your unisolated
//     * operations to the {@link IConcurrencyManager} which will automatically
//     * impose the necessary constraints on concurrent access to the unisolated
//     * indices.
//     * 
//     * @return The revision timestamp to be assigned to an unisolated write.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the index is read-only.
//     */
//    abstract public long getRevisionTimestamp();

    @Override
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
	abstract public long getNodeCount();

	/**
	 * The #of {@link BucketPage}s in the {@link HTree} (not buddy hash buckets,
	 * but the pages on which they appear).
	 */
    abstract public long getLeafCount();
    
    /**
     * The #of tuples in the {@link HTree}.
     */
    abstract public long getEntryCount();
    
    /**
     * {@inheritDoc}
     * 
     * @return <code>false</code> since an {@link HTree} is NOT a balanced tree.
     */
    @Override
    public final boolean isBalanced() {
    
        return false;
        
    }
    
    /**
     * Throws an exception since the {@link HTree} is not a balanced tree.
     */
    @Override
    public int getHeight() {
    
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * The object responsible for (de-)serializing the nodes and leaves of the
     * {@link IIndex}.
     */
    final public NodeSerializer getNodeSerializer() {

        return nodeSer;

    }

    /**
	 * The root of the {@link HTree}. This is always a {@link DirectoryPage}.
	 * <p>
	 * The hard reference to the root node is cleared if the index is
	 * {@link #close() closed}. This method automatically {@link #reopen()}s the
	 * index if it is closed, making it available for use.
	 */
    final protected DirectoryPage getRoot() {

        // make sure that the root is defined.
        if (root == null)
            reopen();

        touch(root);
        
        return root;

    }

    /**
     * Fast summary information about the B+Tree.
     */
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ");
        
        if (metadata.getName() != null) {

            sb.append("name=" + metadata.getName());

        } else {

            sb.append("uuid=" + metadata.getIndexUUID());
            
        }
        
        sb.append(", addressBits=" + getAddressBits());

//        sb.append(", height=" + getHeight());

        sb.append(", entryCount=" + getEntryCount());

        sb.append(", nodeCount=" + getNodeCount());

        sb.append(", leafCount=" + getLeafCount());

        sb.append(", lastCommitTime=" + getLastCommitTime());
        
        sb.append("}");
        
        return sb.toString();
        
    }

	/**
	 * @param store
	 *            The persistence store.
	 * @param nodeFactory
	 *            Object that provides a factory for node and leaf objects.
	 * @param readOnly
	 *            <code>true</code> IFF it is <em>known</em> that the
	 *            {@link AbstractHTree} is read-only.
	 * @param metadata
	 *            The {@link IndexMetadata} object for this
	 *            {@link AbstractHTree}.
	 * @param recordCompressorFactory
	 *            Object that knows how to (de-)compress the serialized data
	 *            records.
	 * 
	 * @throws IllegalArgumentException
	 *             if addressBits is LT ONE (1).
	 * @throws IllegalArgumentException
	 *             if addressBits is GT (16).
	 */
    protected AbstractHTree(//
            final IRawStore store,//
            final INodeFactory nodeFactory,//
            final boolean readOnly,
            final HTreeIndexMetadata metadata,//
            final IRecordCompressorFactory<?> recordCompressorFactory
            ) {

        // show the copyright banner during startup.
        Banner.banner();

        if (nodeFactory == null)
            throw new IllegalArgumentException();

        // Note: MAY be null (implies a transient HTree).
//        assert store != null;

        if (metadata == null)
            throw new IllegalArgumentException();

        // save a reference to the immutable metadata record.
        this.metadata = metadata;

//        this.writeTuple = new Tuple(this, KEYS | VALS);
        
        this.store = store;
        this.readOnly = readOnly;

        this.addressBits = metadata.getAddressBits();

        if (addressBits < MIN_ADDRESS_BITS)
            throw new IllegalArgumentException(ERROR_TOO_SMALL + "addressBits="
                    + addressBits);

        if (addressBits > MAX_ADDRESS_BITS)
            throw new IllegalArgumentException(ERROR_TOO_LARGE + "addressBits="
                    + addressBits);

        /**
         * FIXME: add bucketSlots to IndexMataData
         */
        this.bucketSlots = 1 << addressBits;

//        /*
//         * The Memoizer is not used by the mutable B+Tree since it is not safe
//         * for concurrent operations.
//         */
//        memo = !readOnly ? null : new ChildMemoizer(loadChild);
        /*
         * Note: The Memoizer pattern is now used for both mutable and read-only
         * B+Trees. This is because the real constraint on the mutable B+Tree is
         * that mutation may not be concurrent with any other operation but
         * concurrent readers ARE permitted. The UnisolatedReadWriteIndex
         * explicitly permits concurrent read operations by virtue of using a
         * ReadWriteLock rather than a single lock.
         */
        memo = new ChildMemoizer(loadChild);
        
        /*
         * Setup buffer for Node and Leaf objects accessed via top-down
         * navigation. While a Node or a Leaf remains on this buffer the
         * parent's WeakReference to the Node or Leaf will not be cleared and it
         * will remain reachable.
         */
        this.writeRetentionQueue = newWriteRetentionQueue(readOnly);

        this.nodeSer = new NodeSerializer(//
                store, // addressManager
                nodeFactory,//
                addressBits,//
                0, //initialBufferCapacity
                metadata,//
                readOnly,//
                recordCompressorFactory
                );
        
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

        lockManager = ReadWriteLockManager.getLockManager(this);

        // Use at most this many threads for concurrent eviction.
        maxParallelEvictThreads = Integer.parseInt(
                System.getProperty(IndexMetadata.Options.MAX_PARALLEL_EVICT_THREADS,
                                   IndexMetadata.Options.DEFAULT_MAX_PARALLEL_EVICT_THREADS));
        
        // Do not use concurrent eviction unless we have at least this many
        // dirty nodes / leaves in a given level set.
        minDirtyListSizeForParallelEvict = Integer.parseInt(
                System.getProperty(IndexMetadata.Options.MIN_DIRTY_LIST_SIZE_FOR_PARALLEL_EVICT,
                        IndexMetadata.Options.DEFAULT_MIN_DIRTY_LIST_SIZE_FOR_PARALLEL_EVICT));

    }

    /**
     * Note: Method is package private since it must be overridden for some unit
     * tests.
     * <p>
     * Note: If the retention queue is less than the maximum depth of the HTree
     * then we can encounter a copy-on-write problem where the parent directory
     * becomes immutable during a mutation on the child.
     */
    IHardReferenceQueue<PO> newWriteRetentionQueue(final boolean readOnly) {

		final int writeRetentionQueueCapacity = metadata
				.getWriteRetentionQueueCapacity();

		final int writeRetentionQueueScan = metadata
				.getWriteRetentionQueueScan();

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
             * AbstractBTree instance. When the HTree container is closed, all
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
                    BigdataStatics.threadLocalBuffers, // threadLocalBuffers
                    16,// concurrencyLevel
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

		return dump(HTree.dumpLog.getEffectiveLevel(), out, false/* materialize */);

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
     * appended to the queue and its {@link AbstractPage#referenceCount} is
     * incremented. If a node is being appended to the queue and the queue is at
     * capacity, then this will cause a reference to be evicted from the queue.
     * If the reference counter for the evicted node or leaf is zero and the
     * evicted node or leaf is dirty, then a data record will be coded for the
     * evicted node or leaf and written onto the backing store. A subsequent
     * attempt to modify the node or leaf will force copy-on-write for that node
     * or leaf. 
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
     */
//  synchronized
//  final 
	protected void touch(final AbstractPage node) {

		assert node != null;

		/*
		 * Note: DO NOT update the last used timestamp for the B+Tree here! This
		 * is a huge performance penalty!
		 */
		// touch();

		if (readOnly) {

			doTouch(node);

			return;

		}

        /**
         * At this point we know that the B+Tree object is a mutable data
         * structure (!readOnly). If we can prove that the current thread is
         * conducting a read-only operation on the B+Tree, then we DO NOT touch
         * the node in order to prevent having read-only operations drive
         * evictions. This test relies on the UnisolatedReadWriteIndex class to
         * provide concurrency control for such interleaved read-only and
         * mutation operations on an unisolated (aka mutable) index.
         * 
         * There are three broad ways in which concurrency controls for the
         * index classes are realized:
         * 
         * (1) Explicit synchronization. For example, the AbstractJournal uses
         * explicit synchronization to protect operations on the unisolated
         * Name2Addr.
         * 
         * (2) Explicit pre-declaration of ordered locks. The ConcurrencyManager
         * and AbstractTask support this protection mechanism. The task runs
         * once it has acquired the locks for the declared unisolated indices.
         * 
         * (3) UnisolatedReadWriteIndex. This is used to provide transparent
         * concurrency control for unisolated indices for the triple and quad
         * store classes.
         * 
         * The index is mutable (unisolated view). If the thread owns a
         * read-only lock then the operation is read-only and we MUST NOT drive
         * evictions from this thread.
         * 
         * Note: The order in which we obtain the real read lock and increment
         * (and decrement) the per-thread read lock counter on the AbstractBTree
         * is not critical because AbstractBTree.touch() relies on the thread
         * both owning the read lock and having the per-thread read lock counter
         * incremented for that thread.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError:
         *      Child does not have persistent identity </a>
         */
        final int rcount = lockManager.getReadLockCount();
    
        if (rcount > 0) {
            
            /*
             * The current thread is executing a read-only operation against the
             * mutable index view. DO NOT TOUCH THE EVICTION QUEUE.
             */

            // NOP

        } else {
        
            /*
             * The current thread has not promised that it is using a read-only
             * operation. Either the operation is a mutation or the index is
             * being managed by one of the other two concurrency control
             * patterns. In any of these cases, we touch the write retention
             * queue for this node reference.
             */

            doSyncTouch(node);
            
        }

	}

	/**
	 * Note: Synchronization is necessary for the mutable {@link BTree}. The
	 * underlying reason is the {@link UnisolatedReadWriteIndex} permits
	 * concurrent readers. Reads drive evictions so concurrent calls of
	 * {@link #touch()} are possible. When the B+Tree is mutable, those calls
	 * must be coordinated via a lock to prevent concurrent modification when
	 * touches drive the eviction of a dirty node or leaf.
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/71 (Concurrency
	 *      problem with unisolated btree and memoizer)
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/201 (Hot spot in
	 *      AbstractBTree#touch())
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/284
	 *      (IndexOfOfBounds? in Node#getChild())
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/288 (Node already
	 *      coded)
	 * 
	 *      and possibly
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/149 (NULL passed to
	 *      readNodeOrLeaf)
	 */
	private final void doSyncTouch(final AbstractPage node) {

//        final long beginNanos = System.nanoTime();
        
        synchronized (this) {

            doTouch(node);

        }

//        final long elapsedNanos = System.nanoTime() - beginNanos;
//
//        // See BLZG-1664
//        btreeCounters.syncTouchNanos.add(elapsedNanos);

	}

	private final void doTouch(final AbstractPage node) {

//        final long beginNanos = System.nanoTime();
//        
//        // See BLZG-1664
//        btreeCounters.touchCount.increment();
        
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

		// assert isReadOnly() || ndistinctOnWriteRetentionQueue > 0;

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

		// if (useFinger && node instanceof ILeafData) {
		//
		// if (finger == null || finger.get() != node) {
		//
		// finger = new WeakReference<Leaf>((Leaf) node);
		//
		// }
		//
		// }

//        final long elapsedNanos = System.nanoTime() - beginNanos;
//        
//        // See BLZG-1664
//        btreeCounters.touchNanos.add(elapsedNanos);

	}

    /**
     * Write a dirty node and its children using a post-order traversal that
     * first writes any dirty leaves and then (recursively) their parent nodes.
     * The parent nodes are guaranteed to be dirty if there is a dirty child so
     * this never triggers copy-on-write. This is used as part of the commit
     * protocol where it is invoked with the root of the tree, but it may also
     * be used to incrementally flush dirty non-root {@link Node}s.
     * 
     * Note: This will throw an exception if the backing store is read-only.
     * 
     * @param node
     *            The root of the hierarchy of nodes to be written. The node
     *            MUST be dirty. The node this does NOT have to be the root of
     *            the tree and it does NOT have to be a {@link Node}.
     * 
     * @see BLZG-1665 (Reduce commit latency by parallel checkpoint by level of
     *      dirty pages in an index)
     */
    final protected void writeNodeRecursive(final AbstractPage node) {
        
        if (node instanceof DirectoryPage && getStore() instanceof IIndexManager) {

            /*
             * Requires IIndexManager for ExecutorService, but can write the
             * nodes and leaves in level sets (one level at a time) with up to
             * one thread per dirty node/leave in a given level.
             * 
             * See BLZG-1665 Enable concurrent dirty node eviction by level.
             */

            writeNodeRecursiveConcurrent(node);

        } else {

            writeNodeRecursiveCallersThread(node);

        }

    }
    
    final protected void writeNodeRecursiveCallersThread(final AbstractPage node) {

//        final long begin = System.currentTimeMillis();
        
        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.isDirty();
        assert !node.isDeleted();
        assert !node.isPersistent();

        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway. If we were to evict everything from the hard
         * reference queue before a commit then the counters would be zero but
         * the queue would no longer be holding our nodes and leaves and they
         * would be GC'd soon since they would no longer be strongly reachable.
         */
        assert node.referenceCount >= 0;

        // #of dirty nodes written (nodes or leaves)
        int ndirty = 0;

        // #of dirty leaves written.
        int nleaves = 0;

        /*
         * Post-order traversal of children and this node itself. Dirty nodes
         * get written onto the store.
         * 
         * Note: This iterator only visits dirty nodes.
         */
        final Iterator<AbstractPage> itr = node.postOrderNodeIterator(
                true/* dirtyNodesOnly */, false/* nodesOnly */);

        while (itr.hasNext()) {

            final AbstractPage t = itr.next();

            assert t.isDirty();

            if (t != root) {

                /*
                 * The parent MUST be defined unless this is the root node.
                 */

                assert t.parent != null;
                assert t.parent.get() != null;

            }
            
            // write the dirty node on the store.
            writeNodeOrLeaf(t);
            
//            assert t.isClean();

            ndirty++;
            
//            if (BigdataStatics.debug && ndirty > 0 && ndirty % 1000 == 0) {
//				System.out.println("nwritten=" + ndirty + " in "
//						+ (System.currentTimeMillis() - begin) + "ms");
//            }

            if (t instanceof BucketPage)
                nleaves++;

        }

//        final long elapsed = System.currentTimeMillis() - begin;
//        
//        if (log.isInfoEnabled() || elapsed > 5000) {
//
//            /*
//             * Note: latency here is nearly always a side effect of GC. Unless
//             * you are running the cms-i or similar GC policy, you can see
//             * multi-second GC pauses. Those pauses will cause messages to be
//             * emitted here. This is especially true with multi-GB heaps.
//             */
//
//            final int nnodes = ndirty - nleaves;
//            
//			final String s = "wrote: "
//					+ (metadata.getName() != null ? "name="
//							+ metadata.getName() + ", " : "") + ndirty
//					+ " records (#nodes=" + nnodes + ", #leaves=" + nleaves
//					+ ") in " + elapsed + "ms : addrRoot=" + node.getIdentity();
//
//            if (elapsed > 5000) {
//
////            	System.err.println(s);
////
////            } else if (elapsed > 500/*ms*/) {
//
//                // log at warning level when significant latency results.
//                log.warn(s);
//
//            } else {
//            
//                log.info(s);
//                
//            }
//            
//        }
        
    }

    /**
     * Writes the dirty nodes and leaves in level sets (one level at a time)
     * with up to one thread per dirty node/leave in a given level. This can
     * reduce the latency of {@link #writeCheckpoint()} or for a {@link Node}
     * evicted from the {@link #writeRetentionQueue}. Whether this is driven by
     * {@link #writeCheckpoint()} or {@link #touch(AbstractNode)}, we have the
     * same contract with respect to eviction as the single-threaded eviction
     * logic - we are just doing it in parallel level sets.
     * 
     * @param node
     * 
     * @see BLZG-1665 (Reduce commit latency by parallel checkpoint by level of
     *      dirty pages in an index)
     */
//    @SuppressWarnings("rawtypes")
    final protected void writeNodeRecursiveConcurrent(final AbstractPage node) {

        final long beginNanos = System.nanoTime();
        
        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.isDirty();
        assert !node.isDeleted();
        assert !node.isPersistent();

            /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway. If we were to evict everything from the hard
         * reference queue before a commit then the counters would be zero but
         * the queue would no longer be holding our nodes and leaves and they
         * would be GC'd soon since they would no longer be strongly reachable.
         */
        assert node.referenceCount >= 0;

        // #of dirty nodes written (nodes or leaves)
        int ndirty = 0;

        // #of dirty leaves written.
        int nleaves = 0;

        /*
         * Post-order traversal of children and this node itself. The dirty
         * nodes are written into the dirtyList. We will then partition the
         * dirtyList by B+Tree level of the nodes and leaves. Finally, we will
         * write out all nodes and leaves at a given level with limited
         * parallelism.
         * 
         * Note: This iterator only visits dirty nodes.
         * 
         * Note: Each dirty node or leaf is visited at most once and each parent
         * of a dirty node/leaf is marked as dirty by copy-on-write, therefore
         * (a) each dirty node/leaf below (and including) [node] should appear
         * exactly once in the dirtyMap; and (b) each parent of a dirty node or
         * leaf should appear exactly once in the dirtyMap.
         * 
         * Note: The levels assigned are levels *below* the top-level node that
         * we are evicting. Thus, the level of [node] is always ZERO. The levels
         * only correspond to the actual depth in the B+Tree when the top-most
         * [node] is the B+Tree root.
             */
        
//        final long beginDirtyListNanos = System.nanoTime();
        
        final int nodeLevel = node.getLevel();
        
        // Visit dirty nodes and leaves.
        final Iterator<AbstractPage> itr = node.postOrderNodeIterator(
                true/* dirtyNodesOnly */, false/* nodesOnly */);

        // Map with one entry per level. Each entry is a dirtyList for that level.
        final Map<Integer/* level */, List<AbstractPage>> dirtyMap = new HashMap<Integer, List<AbstractPage>>();
            
        while (itr.hasNext()) {

            final AbstractPage t = itr.next();

            assert t.isDirty();

            if (t != root) {

                /*
                 * The parent MUST be defined unless this is
                 * the root node.
                 */

                assert t.parent != null;
                assert t.parent.get() != null;

            }

            /*
             * Find the level of a node or leaf by chasing its parent ref.
             * 
             * Note: parent references of dirty nodes/leaves are guaranteed to
             * be non-null except for the root.
             */
            final Integer level = t.getLevel() - nodeLevel; // FIXME Write getLevel(A,B) for HTree. This will check for cases where [t] is not a child of [node].

            // Lookup dirty list for that level.
            List<AbstractPage> dirtyList = dirtyMap.get(level);

            if (dirtyList == null) {

                // First node or level at this level.
                dirtyMap.put(level, dirtyList = new LinkedList<AbstractPage>());
            
            }
            
//          log.error("Adding to dirty list: t="+t.toShortString()+", level="+level+", decendentOrSelfOf="+node.toShortString());
            
            // Add node/leaf to dirtyList for that level in the index.
            dirtyList.add(t);

            ndirty++;
            
            if (t instanceof BucketPage)
                nleaves++;
                
            }

//        // Time to generate the dirtyList.
//        final long beginEvictNodes = System.nanoTime();
//        final long elapsedDirtyListNanos = beginEvictNodes - beginDirtyListNanos;

        /*
         * The #of dirty levels from the Node we are evicting and down to the
         * leaves. If we have only a dirty root leaf, then there will be one
         * dirty level.  If we are evicting the root node, then there will 
         * always be one level per level in the B+Tree.  If we are evicting a
         * non-root node, then there will be one level for each level that the
         * evicted node is above the leaves.
         */
        final int dirtyLevelCount = dirtyMap.size();

//        // max would be the B+Tree height+1 (if evicting the root).
//        assert dirtyLevelCount <= getHeight() + 1 : "dirtyLevelCount=" + dirtyLevelCount + ", height=" + getHeight()
//                + ", dirtyMap.keys=" + dirtyMap.keySet();

        /*
         * Now evict each dirtyList in parallel starting at the deepest, and
         * then proceeding one by one until we reach the list at level ZERO (the
         * node that we were asked to evict). Since the parent of a dirty child
         * is always dirty, all levels from N-1..0 *must* exist in our dirty
         * list by level map.
         */
        for (int i = dirtyLevelCount - 1; i >= 0; i--) {

            // The dirty list for level [i].
            final List<AbstractPage> dirtyList = dirtyMap.get(i);

            if (dirtyList == null)
                throw new AssertionError("Dirty list not found: level=" + i + ", #levels=" + dirtyLevelCount+", dirtyMap.keys="+dirtyMap.keySet());

            // #of dirty nodes / leaves in this level set.
            final int dirtyListSize = dirtyList.size();
            
            // No more parallelism than we have in the dirty list for this level.
            final int nparallel = Math.min(maxParallelEvictThreads, dirtyListSize);

            if (dirtyListSize < minDirtyListSizeForParallelEvict) {

                /*
                 * Avoid parallelism when only a few nodes or leaves will be
                 * evicted.
                 */
                for (AbstractPage t : dirtyList) {
                    
                    writeNodeOrLeaf(t, nodeSer);
                    
                }

//                if (log.isInfoEnabled())
                    log.error("Evicting " + dirtyListSize + " dirty nodes/leaves using " + nparallel + " threads.");

            } else {

                final ArrayList<Future<Void>> futureList = new ArrayList<Future<Void>>(dirtyListSize);

                // Note: Must have the same level of concurrency in
                // NodeSerializer instances.
                final LatchedExecutor executor = new LatchedExecutor(((IIndexManager) getStore()).getExecutorService(),
                        nparallel);

                try {

                    for (AbstractPage t : dirtyList) {

                        // Need [final] to be visible inside Runnable().
                        final AbstractPage u = t;

                        final FutureTask<Void> ft = new FutureTask<Void>(new Runnable() {

                            @Override
                            public void run() {

                                if (u != root) {

                                    /*
                                     * The parent MUST be defined unless this is
                                     * the root node.
                                     */

                                    assert u.parent != null;
                                    assert u.parent.get() != null;

                                }

                                // An instance just for this thread.
                                final NodeSerializer myNodeSer = new NodeSerializer(//
                                        store, // addressManager
                                        nodeSer.nodeFactory,//
                                        addressBits,//
                                        nodeSer.getWriteBufferCapacity(),//
                                        metadata,//
                                        readOnly,//
                                        nodeSer.recordCompressorFactory
                                        );

                                // write dirty node on store (non-recursive)
                                writeNodeOrLeaf(u, myNodeSer);

        }

                        }, null/* Void */);

                        // Add to list of Futures we will check.
                        futureList.add(ft);
        
                        // Schedule eviction on
                        executor.execute(ft);

                    }

                    // Check futures.
                    for (Future<Void> f : futureList) {

                        try {
                            f.get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    }

                } finally {

                    // Ensure all futures are done.
                    for (Future<Void> ft : futureList) {

                        ft.cancel(true/* mayInterruptIfRunning */);

                    }

                }

            }

        }

        final long elapsedNanos = System.nanoTime() - beginNanos;

        if (log.isInfoEnabled())
            log.info("Evicting " + ndirty + " dirty nodes/leaves in " + TimeUnit.NANOSECONDS.toMillis(elapsedNanos)
                    + "ms.");

    }

    /**
     * Codes the node and writes the coded record on the store (non-recursive).
     * The node MUST be dirty. If the node has a parent, then the parent is
     * notified of the persistent identity assigned to the node by the store.
     * This method is NOT recursive and dirty children of a node will NOT be
     * visited. By coding the nodes and leaves as they are evicted from the
     * {@link #writeRetentionQueue}, the B+Tree continuously converts nodes and
     * leaves to their more compact coded record forms which results in a
     * smaller in memory footprint.
     * <p>
     * Note: For a transient B+Tree, this merely codes the node but does not
     * write the node on the store (there is none).
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree (or the backing store) is read-only.
     * 
     * @return The persistent identity assigned by the store.
     */
    protected long writeNodeOrLeaf(final AbstractPage node) {

        return writeNodeOrLeaf(node, nodeSer);

    }

    private long writeNodeOrLeaf(final AbstractPage node, final NodeSerializer nodeSer) {

        if (error != null)
            throw new IllegalStateException(ERROR_ERROR_STATE, error);

        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.htree == this;
        assert node.isDirty();
        assert !node.isDeleted();
        assert !node.isPersistent();
        assert !node.isReadOnly();
        assertNotReadOnly();
        
        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway. If we were to evict everything from the hard
         * reference queue before a commit then the counters would be zero but
         * the queue would no longer be holding our nodes and leaves and they
         * would be GC'd soon as since they would no longer be strongly
         * reachable.
         */
        assert node.referenceCount >= 0;

        /*
         * Note: The parent should be defined unless this is the root node.
         * 
         * Note: A parent CAN NOT be serialized before all of its children have
         * persistent identity since it needs to write the identity of each
         * child in its serialization record.
         */
        final DirectoryPage parent = node.getParentDirectory();

        if (parent == null) {

        		assert node == root;

        } else {

            // parent must be dirty if child is dirty.
            if(!parent.isDirty()) { // TODO remove (debug point).
                throw new AssertionError();
            }
            assert parent.isDirty();

            // parent must not be persistent if it is dirty.
            assert !parent.isPersistent();

        }

//        if (debug)
//            node.assertInvariants();
        
        // the coded data record.
        final AbstractFixedByteArrayBuffer slice;
        {

            final long beginNanos = System.nanoTime();

            /*
             * Code the node or leaf, replacing the data record reference on the
             * node/leaf with a reference to the coded data record.
             * 
             * Note: This is optimized for the very common use case where we
             * want to have immediate access to the coded data record. In that
             * case, many of the IRabaCoder implementations can be optimized by
             * passing the underlying coding object (FrontCodedByteArray,
             * HuffmanCodec's decoder) directly into an alternative ctor for the
             * decoder. This gives us "free" decoding for the case when we are
             * coding the record. The coded data record is available from the
             * IRabaDecoder.
             * 
             * About the only time when we do not need to do this is the
             * IndexSegmentBuilder, since the coded record will not be used
             * other than to write it on the disk.
             */
			if (node.isLeaf()) {
				
				assert (1 << (addressBits - node.globalDepth) == parent.countChildRefs((BucketPage) node));

				// code data record and _replace_ the data ref.
				((BucketPage) node).data = nodeSer
						.encodeLive(((BucketPage) node).data);

				// slice onto the coded data record.
				slice = ((BucketPage) node).data();

				btreeCounters.leavesWritten.increment();;

			} else {

				// code data record and _replace_ the data ref.
				((DirectoryPage) node).data = nodeSer
						.encodeLive(((DirectoryPage) node).data);

				// slice onto the coded data record.
				slice = ((DirectoryPage) node).data();

                btreeCounters.nodesWritten.increment();;

            }
            
            btreeCounters.serializeNanos.add(System.nanoTime() - beginNanos);
            
        }

        if (store == null) {

            /*
             * This is a transient B+Tree so we do not actually write anything
             * on the backing store.
             */

            // No longer dirty (prevents re-coding on re-eviction).
            node.setDirty(false);

            return 0L;
            
        }
        
        // write the serialized node or leaf onto the store.
        final long addr;
        final long oldAddr;
        {

            final long beginNanos = System.nanoTime();
            
            // wrap as ByteBuffer and write on the store.
            addr = store.write(slice.asByteBuffer());
            
            // now we have a new address, delete previous identity if any
            if (node.isPersistent()) {
            	oldAddr = node.getIdentity();
            } else {
            	oldAddr = 0;
            }

            final int nbytes = store.getByteCount(addr);
            
            btreeCounters.writeNanos.add(System.nanoTime() - beginNanos);
    
            btreeCounters.bytesWritten.add(nbytes);

    		btreeCounters.bytesOnStore_nodesAndLeaves.addAndGet(nbytes);

        }

        /*
         * The node or leaf now has a persistent identity and is marked as
         * clean. At this point it's data record is read-only. Any changes
         * directed to this node or leaf MUST trigger copy-on-write and convert
         * the data record to a mutable instance before proceeding.
         */

        node.setIdentity(addr);
        if (oldAddr != 0L) {
//            if (storeCache!=null) {
//                // remove from cache.
//            	storeCache.remove(oldAddr);
//            }
			deleteNodeOrLeaf(oldAddr);//, node instanceof Node);
        }

        node.setDirty(false);

        if (parent != null) {

            // Set the persistent identity of the child on the parent.
            parent.setChildAddr(node);

            // // Remove from the dirty list on the parent.
            // parent.dirtyChildren.remove(node);

        }

//        if (storeCache != null) {
//
//            /*
//             * Put the data record (the delegate) into the cache, touching it on
//             * the backing LRU.
//             * 
//             * Note: This provides an unfair retention for recently written
//             * nodes or leaves equal to that of recently read nodes or leaves. I
//             * do not know what to do about that. However, the total size across
//             * all per-store caches is (SHOULD BE) MUCH larger than the write
//             * retention queue so that bias may not matter that much.
//             */
//            if (null != storeCache.putIfAbsent(addr, node.getDelegate())) {
//
//                /*
//                 * Note: For a WORM store, the address is always new so there
//                 * will not be an entry in the cache for that address.
//                 * 
//                 * Note: For a RW store, the addresses can be reused and the
//                 * delete of the old address MUST have cleared the entry for
//                 * that address from the store's cache.
//                 */
//                
//                throw new AssertionError("addr already in cache: " + addr
//                        + " for " + store.getFile());
//                
//            }
//            
//        }
        
        return addr;

    }

	/**
	 * Read an {@link AbstractPage} from the store.
	 * <p>
	 * Note: Callers SHOULD be synchronized in order to ensure that only one
	 * thread will read the desired node or leaf in from the store and attach
	 * the reference to the newly read node or leaf as appropriate into the
	 * existing data structures (e.g., as the root reference or as a child of a
	 * node or leaf already live in memory).
	 * <p>
	 * Note: The caller MUST set the {@link AbstractPage#globalDepth} on the
	 * returned value.
	 * 
	 * @param addr
	 *            The address in the store.
	 * 
	 * @return The {@link AbstractPage}.
	 * 
	 * @throws IllegalArgumentException
	 *             if the address is {@link IRawStore#NULL}.
	 */
	protected AbstractPage readNodeOrLeaf(final long addr) {

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
            final AbstractPage node = nodeSer.wrap(this, addr, data);

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
	 * @see AbstractPage#self
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
             * Note: Used for transient HTrees.
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
    
	/**
	 * Encode a raw record address into a byte[] suitable for storing in the
	 * value associated with a tuple and decoding using
	 * {@link #decodeRecordAddr(byte[])}
	 * 
	 * @param recordAddrBuf
	 *            The buffer that will be used to format the record address.
	 * @param addr
	 *            The raw record address.
	 * 
	 * @return A newly allocated byte[] which encodes that address.
	 */
	static public byte[] encodeRecordAddr(final ByteArrayBuffer recordAddrBuf,
			final long addr) {

		recordAddrBuf.reset().putLong(addr);

		return recordAddrBuf.toByteArray();

	}

    /**
     * Decodes a signed long value as encoded by {@link #appendSigned(long)}.
     * 
     * @param buf
     *            The buffer containing the encoded record address.
     *            
     * @return The signed long value.
     */
    static public long decodeRecordAddr(final byte[] buf) {

        long v = 0L;
        
        // big-endian.
        v += (0xffL & buf[0]) << 56;
        v += (0xffL & buf[1]) << 48;
        v += (0xffL & buf[2]) << 40;
        v += (0xffL & buf[3]) << 32;
        v += (0xffL & buf[4]) << 24;
        v += (0xffL & buf[5]) << 16;
        v += (0xffL & buf[6]) <<  8;
        v += (0xffL & buf[7]) <<  0;

        return v;
        
    }

	/**
	 * The maximum length of a <code>byte[]</code> value stored within a leaf
	 * for this {@link HTree}. This value only applies when raw record support
	 * has been enabled for the {@link HTree}. Values greater than this in
	 * length will be written as raw records on the backing persistence store.
	 * 
	 * @return The maximum size of an inline <code>byte[]</code> value before it
	 *         is promoted to a raw record.
	 */
    int getMaxRecLen() {
    	
    	return metadata.getMaxRecLen();
    	
    }

	/**
	 * Read the raw record from the backing store.
	 * <p>
	 * Note: This does not cache the record. In general, the file system cache
	 * should do a good job here.
	 * 
	 * @param addr
	 *            The record address.
	 * 
	 * @return The data.
	 * 
	 * @todo performance counters for raw records read.
	 * 
	 * FIXME Add raw record compression.
	 */
	ByteBuffer readRawRecord(final long addr) {

		// read from the backing store.
		final ByteBuffer b = getStore().read(addr);

		final int nbytes = getStore().getByteCount(addr);
		
		btreeCounters.rawRecordsRead.increment();
        btreeCounters.rawRecordsBytesRead.add(nbytes);

		return b;

	}

	/**
	 * Write a raw record on the backing store.
	 * 
	 * @param b
	 *            The data.
	 *            
	 * @return The address at which the data was written.
	 * 
	 * FIXME Add raw record compression.
	 */
	long writeRawRecord(final byte[] b) {

		if(isReadOnly())
			throw new IllegalStateException(ERROR_READ_ONLY);
		
		// write the value on the backing store.
		final long addr = getStore().write(ByteBuffer.wrap(b));
		
		final int nbytes = b.length;
		
		btreeCounters.rawRecordsWritten.increment();
		btreeCounters.rawRecordsBytesWritten.add(nbytes);
		btreeCounters.bytesOnStore_rawRecords.addAndGet(nbytes);

		return addr;
		
    }

	/**
	 * Delete a raw record from the backing store.
	 * 
	 * @param addr
	 *            The address of the record.
	 */
	void deleteRawRecord(final long addr) {
		
		if(isReadOnly())
			throw new IllegalStateException(ERROR_READ_ONLY);

		getStore().delete(addr);
		
		final int nbytes = getStore().getByteCount(addr);
		
		btreeCounters.bytesOnStore_rawRecords.addAndGet(-nbytes);
		
	}

	/**
	 * Delete a node or leaf from the backing store, updating various
	 * performance counters.
	 * 
	 * @param addr
	 *            The address of the node or leaf.
	 */
	void deleteNodeOrLeaf(final long addr) {

		if(addr == IRawStore.NULL)
			throw new IllegalArgumentException();
		
		if (isReadOnly())
			throw new IllegalStateException(ERROR_READ_ONLY);

		getStore().delete(addr);

		final int nbytes = getStore().getByteCount(addr);

		btreeCounters.bytesOnStore_nodesAndLeaves.addAndGet(-nbytes);

	}

	/** The #of index entries. */
	abstract public long rangeCount();
	
	@Override
    final public ICloseableIterator<?> scan() {
        
        return new EntryScanIterator(rangeIterator());
        
    }
    
	/**
	 * Simple iterator visits all tuples in the {@link HTree} in order by the
	 * effective prefix of their keys. Since the key is typically a hash of some
	 * fields in the associated application data record, this will normally
	 * visit application data records in what appears to be an arbitrary order.
	 * Tuples within a buddy hash bucket will be delivered in a random order.
	 * <p>
	 * Note: The {@link HTree} does not currently maintain metadata about the
	 * #of spanned tuples in a {@link DirectoryPage}. Without that we can not
	 * provide fast range counts, linear list indexing, etc.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ITupleIterator rangeIterator() {
		
		return new BucketPageTupleIterator(this,
				IRangeQuery.DEFAULT/* flags */, new Striterator(
						getRoot().postOrderIterator()).addFilter(new Filter() {
					private static final long serialVersionUID = 1L;
					public boolean isValid(final Object obj) {
						return ((AbstractPage) obj).isLeaf();
					}
				}));
	
	}

	/**
	 * Visits the values stored in the {@link HTree} in order by the effective
	 * prefix of their keys. Since the key is typically a hash of some fields in
	 * the associated application data record, this will normally visit
	 * application data records in what appears to be an arbitrary order. Tuples
	 * within a buddy hash bucket will be delivered in a random order.
	 * <p>
	 * Note: This allocates a new byte[] for each visited value. It is more
	 * efficient to reuse a buffer for each visited {@link Tuple}. This can be
	 * done using {@link #rangeIterator()}.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<byte[]> values() {
		
		return new Striterator(rangeIterator()).addFilter(new Resolver(){
			private static final long serialVersionUID = 1L;
			protected Object resolve(final Object obj) {
				return ((ITuple<?>)obj).getValue();
			}});
		
	}
	
	/**
	 * Exception that can be thrown for asserts and testing, retaining the
	 * source page of the error
	 */
	static public class HTreePageStateException extends RuntimeException {
		final AbstractPage m_pge;
		public HTreePageStateException(final AbstractPage pge) {
			super("Problem with Page: " + pge);
			
			m_pge = pge;
		}
		public AbstractPage getPage() {
			return m_pge;
		}
	}
	
	/**
	 * Checks globalDepth of each node and also whether any BucketPages
	 * are empty.
	 * @param full indicates whether to check consistency on disk
	 */
	public void checkConsistency(final boolean full) {
		checkConsistency(root, full);
	}
	
	public void checkConsistency(final AbstractPage pge, final boolean full) {
		final DirectoryPage parent = pge.getParentDirectory();

		if (parent != null) { // check depth
			if (1 << (addressBits-pge.globalDepth) != parent.countChildRefs(pge)) {
				throw new HTreePageStateException(pge);
			}
		}
		if (pge instanceof BucketPage) {
			if (parent != root && ((BucketPage)pge).data.getKeyCount() == 0) {
				throw new HTreePageStateException(pge);
			}
		} else {
			final DirectoryPage dpge = (DirectoryPage) pge;
			final int nslots = dpge.childRefs.length;
			for (int i = 0; i < nslots; i++) {
				AbstractPage ch = dpge.deref(i);
				if (ch == null && full) {
					ch = dpge.getChild(i);
				}
				if (ch != null) {
					checkConsistency(ch, full);
				}
			}
		}
	}
	
    @Override
    final public Lock readLock() {

        return lockManager.readLock();
        
    }

    @Override
    final public Lock writeLock() {

        return lockManager.writeLock();
        
    }

    @Override
    final public int getReadLockCount() {
        
        return lockManager.getReadLockCount();
    }
    
}
