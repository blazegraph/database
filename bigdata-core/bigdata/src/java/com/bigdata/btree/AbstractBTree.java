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
/*
 * Created on Dec 19, 2006
 * 
 * RESYNC
 */

package com.bigdata.btree;

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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTreeTupleCursor.MutableBTreeTupleCursor;
import com.bigdata.btree.AbstractBTreeTupleCursor.ReadOnlyBTreeTupleCursor;
import com.bigdata.btree.IndexMetadata.Options;
import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.filter.Reverserator;
import com.bigdata.btree.filter.TupleRemover;
import com.bigdata.btree.filter.WrappedTupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.view.FusedView;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.HardReferenceQueueWithBatchingUpdates;
import com.bigdata.cache.IHardReferenceQueue;
import com.bigdata.cache.RingBuffer;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.compression.IRecordCompressorFactory;
import com.bigdata.journal.CompactTask;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.TransientResourceMetadata;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.OverflowManager;
import com.bigdata.service.DataService;
import com.bigdata.service.Split;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.Computable;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IFilter;

/**
 * <p>
 * Base class for mutable and immutable B+-Tree implementations.
 * </p>
 * <p>
 * The B+-Tree implementation supports variable length unsigned byte[] keys and
 * provides a {@link IKeyBuilder} utilities designed to make it possible to
 * generate keys from any combination of primitive data types and Unicode
 * strings. The total ordering imposed by the index is that of a bit-wise
 * comparison of the variable length unsigned byte[] keys. Encoding Unicode keys
 * is support by an integration with ICU4J and applications may choose the
 * locale, strength, and other properties that govern the sort order of sort
 * keys generated from Unicode strings. Sort keys produces by different collator
 * objects are NOT compatible and applications that use Unicode data in their keys
 * MUST make sure that they use a collator that imposes the same sort order each
 * time they provision a {@link KeyBuilder}.
 * </p>
 * <p>
 * The use of variable length unsigned byte[] keys makes it possible for the
 * B+-Tree to perform very fast comparison of a search key with keys in the
 * nodes and leaves of the tree. To support fast search, the leading prefix is
 * factored out each time a node or leaf is made immutable, e.g., directly
 * proceeding serialization. Further, the separator keys are chosen to be the
 * shortest separator key in order to further shorten the keys in the nodes of
 * the tree.
 * </p>
 * <p>
 * The B+Tree can optionally maintain version metadata (a version timestamp and
 * deletion marker). Version metadata MUST be enabled (a) if the index will be
 * used with transactional isolation; or (b) if the index is part of a scale-out
 * index. In both cases the version timestamps and deletion markers play a
 * critical role when reading from a fused view of an ordered set of indices
 * describing an index or an index partition. Note that the existence of
 * deletion markers means that {@link #rangeCount(byte[], byte[])} and
 * {@link #getEntryCount()} are upper bounds as deleted entries will be reported
 * until they are purged from the index by a compacting merge of the view.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see KeyBuilder
 */
abstract public class AbstractBTree implements IIndex, IAutoboxBTree,
        ILinearList, IBTreeStatistics, ILocalBTreeView, ISimpleTreeIndexAccess,
        ICheckpointProtocol {

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
    
    /**
     * An unisolated index view is in an error state. It must be discarded and
     * reloaded from the current checkpoint record.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate BTree
     *      objects if error occurs during eviction </a>
     */
    final protected static String ERROR_ERROR_STATE = "Index is in error state";

    /**
     * Log for btree opeations.
     */
    protected static final Logger log = Logger.getLogger(AbstractBTree.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

    /**
     * Log for {@link BTree#dump(PrintStream)} and friends.
     */
    public static final Logger dumpLog = Logger.getLogger(BTree.class
            .getName()
            + "#dump");

    /**
     * Flag turns on the use of {@link AbstractNode#assertInvariants()} and is
     * automatically enabled when the {@link #log logger} is set to
     * {@link Level#DEBUG}.
     */
    final protected boolean debug = DEBUG;

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
     * The persistence store -or- <code>null</code> iff the B+Tree is transient.
     */
    final protected IRawStore store;
    
    /**
     * When <code>true</code> the {@link AbstractBTree} does not permit
     * mutation.
     */
    final protected boolean readOnly;
    
    /**
     * Optional cache for {@link INodeData} and {@link ILeafData} instances and
     * always <code>null</code> if the B+Tree is transient.
     */
	/*
	 * The storeCache field is marked as "Deprecated" but it should stick around
	 * for a while since we might wind up reusing this feature on an index local
	 * basis at some point.
	 * 
	 * @see BLZG-1501 (remove LRUNexus)
	 */
    @Deprecated
    protected final ConcurrentMap<Long, Object> storeCache;

    /**
     * Hard reference iff the index is mutable (aka unisolated) allows us to
     * avoid patterns that create short life time versions of the object to
     * protect {@link ICheckpointProtocol#writeCheckpoint2()} and similar
     * operations.
     */
    private final IReadWriteLockManager lockManager;
    
    /**
     * The branching factor for the btree.
     */
    final protected int branchingFactor;

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
        final Node parent;

        /** The child index. */
        final int index;

        /**
         * 
         * @param parent
         *            The parent node.
         * @param index
         *            The child index.
         */
        public LoadChildRequest(final Node parent, final int index) {
    
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
    final private static Computable<LoadChildRequest, AbstractNode<?>> loadChild = new Computable<LoadChildRequest, AbstractNode<?>>() {

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
        public AbstractNode<?> compute(final LoadChildRequest req)
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
////                if (req != null) {
//
//                req.parent.btree.memo.removeFromCache(req);
//
////                }
//
//            }
        
        }
        
    };

    /**
     * A {@link Memoizer} subclass which exposes an additional method to remove
     * a {@link FutureTask} from the internal cache. This is used as part of an
     * explicit protocol in {@link Node#_getChild(int)} to clear out cache
     * entries once the child reference has been set on {@link Node#childRefs}.
     * This is package private since it must be visible to
     * {@link Node#_getChild(int)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static class ChildMemoizer extends
            Memoizer<LoadChildRequest/* request */, AbstractNode<?>/* child */> {

        /**
         * @param c
         */
        public ChildMemoizer(
                final Computable<LoadChildRequest, AbstractNode<?>> c) {

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
         * Called by the thread which atomically sets the {@link Node#childRefs}
         * element to the computed {@link AbstractNode}. At that point a
         * reference exists to the child on the parent.
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
     * @see Node#getChild(int)
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
     * @see Node#getChild(int)
     */
    AbstractNode<?> loadChild(final Node parent, final int index) {

        if (false && store instanceof Journal
                && ((Journal) store).getReadExecutor() != null) {

         /*
          * This code path materializes the child node using the read service.
          * This has the effect of bounding the #of concurrent IO requests
          * against the local disk based on the allowed parallelism for that
          * read service.
          * 
          * Note: getReadExecutor() is not defined for IJournal. If we want to
          * support the read executor pre-fetch pattern then the code needs to
          * be updated to use IJournal and IJournal needs to expose
          * getReadExecutor. 
          */

            final Executor s = ((Journal) store).getReadExecutor();

            final FutureTask<AbstractNode<?>> ft = new FutureTask<AbstractNode<?>>(
                    new Callable<AbstractNode<?>>() {

                        public AbstractNode<?> call() throws Exception {

                            return memo.compute(new LoadChildRequest(parent,
                                    index));

                        }

                    });
            
            s.execute(ft);

            try {

                return ft.get();

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            } catch (ExecutionException e) {

                throw new RuntimeException(e);

            }

        } else {

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

    }

    /**
     * The root of the btree. This is initially a leaf until the leaf is split,
     * at which point it is replaced by a node. The root is also replaced each
     * time copy-on-write triggers a cascade of updates.
     * <p>
     * This hard reference is cleared to <code>null</code> if an index is
     * {@link #close() closed}. {@link #getRoot()} automatically uses
     * {@link #reopen()} to reload the root so that closed indices may be
     * transparently made ready for further use (indices are closed to reduce
     * their resource burden, not to make their references invalid). The
     * {@link AbstractNode} and derived classes <em>assume</em> that the root
     * is non-null. This assumption is valid if {@link #close()} is invoked by
     * the application in a manner consistent with the single-threaded contract
     * for the {@link AbstractBTree}.
     * <p>
     * Note: This field MUST be marked as [volatile] in order to guarantee
     * correct semantics for double-checked locking in {@link BTree#reopen()}
     * and {@link IndexSegment#reopen()}.
     * 
     * @see http://en.wikipedia.org/wiki/Double-checked_locking
     */
    protected volatile AbstractNode<?> root;

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
     * An optional bloom filter that will be used to filter point tests against
     * <i>this</i> {@link AbstractBTree}. A bloom filter provides a strong
     * guarantee when it reports that a key was not found, but only a weak
     * guarantee when it reports that a key was found. Therefore a positive
     * report by the bloom filter MUST be confirmed by testing against the index
     * to verify that the key was in fact present in the index. Since bloom
     * filters do not support removal of keys, any key that is deleted from the
     * index will remain "present" in the bloom filter but a read against the
     * index will serve to disprove the existence of the key. The bloom filter
     * is enabled by specifying a non-zero value for the
     * {@link IndexMetadata#getErrorRate()}.
     * <p>
     * Note: The bloom filter is read with the root node when the index is
     * {@link #reopen()}ed and is discarded when the index is {@link #close()}ed.
     * While we do not use double-checked locking on the {@link #bloomFilter},
     * it is assigned from within the same code that assigns the {@link #root}.
     * For historical reasons this is handled somewhat differently by the
     * {@link BTree} and the {@link IndexSegment}.
     * <p>
     * Note: Not <code>private</code> since {@link Checkpoint} reads this
     * field.
     */
    /*private*/ volatile BloomFilter bloomFilter;
    
    /**
     * Return the optional {@link IBloomFilter}, transparently
     * {@link #reopen()}ing the index if necessary.
     * 
     * @return The bloom filter -or- <code>null</code> if there is no bloom
     *         filter (including the case where there is a bloom filter but it
     *         has been disabled since the {@link BTree} has grown too large and
     *         the expected error rate of the bloom filter would be too high).
     */
    abstract public BloomFilter getBloomFilter();
    
//    /**
//     * The finger is a trial feature. The purpose is to remember the last
//     * leaf(s) in the tree that was visited by a search operation and to
//     * pre-test that those leaf(s) on the next search operation.
//     * <p>
//     * Fingers can do wonders by reducing search in common cases where the same
//     * leaf is visited by a sequence of operations, e.g., sequential insert or
//     * conditional insert realized by lookup + insert. Multiple fingers may be
//     * required if the tree is used by concurrent readers (and the finger might
//     * be part of the reader state) while a single finger would do for a tree
//     * being used by a writer (since concurrent modification is not supported).
//     * <p>
//     * The finger is set each time a leaf is found as the result of a search. It
//     * is tested by each search operation before walking the tree. The finger is
//     * a {@link WeakReference} so that we do not need to worry about having a
//     * hard reference to an arbitrary part of the tree.
//     * 
//     * @todo Does the finger need to encapsulate the hard references for the
//     *       parents in order to ensure that the parents remain strongly
//     *       reachable? (Parents are only weakly reachable from their children.)
//     */
//    private WeakReference<Leaf> finger;
//
//    final private boolean useFinger = false;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final protected NodeSerializer nodeSer;

//    /**
//     * Count of the #of times that a reference to this {@link AbstractBTree}
//     * occurs on a {@link HardReferenceQueue}. This field will remain zero(0)
//     * unless the {@link AbstractBTree} is placed onto a
//     * {@link HardReferenceQueue} maintained by the application.
//     * <p>
//     * Note: <em>DO NOT MODIFY THIS FIELD DIRECTLY</em> -- The
//     * {@link IResourceManager} is responsible for setting up the
//     * {@link HardReferenceQueue}, updating this field, and
//     * {@link #close() closing} {@link AbstractBTree}s that are not in use.
//     */
//    public int referenceCount = 0;

    /**
	 * Nodes (that is nodes or leaves) are added to a hard reference queue when
	 * they are created or read from the store. On eviction from the queue a
	 * dirty node is serialized by a listener against the {@link IRawStore}. The
	 * nodes and leaves refer to their parent with a {@link WeakReference}s.
	 * Likewise, nodes refer to their children with a {@link WeakReference}. The
	 * hard reference queue in combination with {@link #touch(AbstractNode)} and
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
	 * Note: The code in {@link Node#postOrderNodeIterator(boolean, boolean)}
	 * and {@link DirtyChildIterator} MUST NOT touch the hard reference queue
	 * since those iterators are used when persisting a node using a post-order
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
    
//    /**
//     * The {@link #readRetentionQueue} reduces reads through to the backing
//     * store in order to prevent disk reads and reduces de-serialization costs
//     * for nodes by maintaining them as materialized objects.
//     * <p>
//     * Non-deleted nodes (that is nodes or leaves) are placed onto this hard
//     * reference queue when they are evicted from the
//     * {@link #writeRetentionQueue}. Nodes are always converted into their
//     * immutable variants when they are evicted from the
//     * {@link #writeRetentionQueue} so the {@link #readRetentionQueue} only
//     * contains hard references to immutable nodes. If there is a need to write
//     * on a node then {@link AbstractNode#copyOnWrite()} will be used to create
//     * a mutable copy of the node and the old node will be
//     * {@link AbstractNode#delete()}ed. Delete is responsible for releasing any
//     * state associated with the old node, so deleted nodes on the
//     * {@link #readRetentionQueue} will occupy very little space in the heap.
//     * <p>
//     * Note: evictions from this hard reference cache are driven by inserts.
//     * Inserts are driven by evictions of non-deleted nodes from the
//     * {@link #writeRetentionQueue}.
//     * <p>
//     * Note: the {@link #readRetentionQueue} will typically contain multiple
//     * references to a given node.
//     * <p>
//     * Note: the {@link IndexSegment} has an additional leaf cache since its
//     * iterators do not use the {@link Node}s to scan to the prior or next
//     * {@link Leaf}.
//     */
//    final protected RingBuffer<PO> readRetentionQueue;

	/**
	 * Interface declaring namespaces for performance counters collected for a
	 * B+Tree.
	 * 
	 * @see AbstractBTree#getCounters()
	 */
    public static interface IBTreeCounters {

		/** Counters for the {@link IBTreeStatistics} interface. */
    	String Statistics = "Statistics";
		/** Counters for the {@link AbstractBTree#writeRetentionQueue}. */
		String WriteRetentionQueue = "WriteRetentionQueue";
		/** Counters for key search performance. */
		String KeySearch = "KeySearch";
		/** Counters for {@link IRangeQuery}. */
		String RangeQuery = "RangeQuery";
		/** Counters for the {@link ILinearList} API. */
		String LinearList = "LinearList";
		/** Counters for structural modifications. */
		String Structure = "Structure";
		/** Counters for tuple-level operations. */
		String Tuples = "Tuples";
		/** Counters for IO. */
		String IO = "IO";
//        /** Counters for touch(). */
//        String TOUCH = "Touch";
    
    }

    /**
     * {@inheritDoc}
     * <p>
     * Return some "statistics" about the btree including both the static
     * {@link CounterSet} and the {@link BTreeCounters}s.
     * <p>
     * Note: counters reporting directly on the {@link AbstractBTree} use a
     * snapshot mechanism which prevents a hard reference to the
     * {@link AbstractBTree} from being attached to the return
     * {@link CounterSet} object. One consequence is that these counters will
     * not update until the next time you invoke {@link #getCounters()}.
     * <p>
     * Note: In order to snapshot the counters use {@link OneShotInstrument} to
     * prevent the inclusion of an inner class with a reference to the outer
     * {@link AbstractBTree} instance.
     * <p>
     * Note: This method reports information that is NOT available from
     * {@link BTreeCounters}s. Some of this information is either static or
     * relatively static (e.g., the {@link UUID} for this index or its
     * implementation class). Other information is dynamic (such as the #of
     * nodes and leaves or the current size of the writeRetentionQueue). The
     * dynamic information is valid for the specific index view as of the moment
     * that it is sampled.
     * 
     * @see BTreeCounters#getCounters()
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/626">
     *      Expose performance counters for read-only indices </a>
     * 
     * @todo estimate heap requirements for nodes and leaves based on their
     *       state (keys, values, and other arrays). report estimated heap
     *       consumption here.
     */
    final public CounterSet getCounters() {

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

			tmp.addCounter("branchingFactor", new OneShotInstrument<Integer>(
					branchingFactor));

			tmp.addCounter("height",
					new OneShotInstrument<Integer>(getHeight()));

			tmp.addCounter("nodeCount", new OneShotInstrument<Long>(
					getNodeCount()));

			tmp.addCounter("leafCount", new OneShotInstrument<Long>(
					getLeafCount()));

			tmp.addCounter("tupleCount", new OneShotInstrument<Long>(
					getEntryCount()));

			/*
			 * Note: The utilization numbers reported here are a bit misleading.
			 * They only consider the #of index positions in the node or leaf
			 * which is full, but do not take into account the manner in which
			 * the persistence store allocates space to the node or leaf. For
			 * example, for the WORM we do perfect allocations but retain many
			 * versions. For the RWStore, we do best-fit allocations but recycle
			 * old versions. The space efficiency of the persistence store is
			 * typically the main driver, not the utilization rate as reported
			 * here.
			 */
			final IBTreeUtilizationReport r = getUtilization();
			
			// % utilization in [0:100] for nodes
			tmp.addCounter("%nodeUtilization", new OneShotInstrument<Integer>(r
					.getNodeUtilization()));
			
			// % utilization in [0:100] for leaves
			tmp.addCounter("%leafUtilization", new OneShotInstrument<Integer>(r
					.getLeafUtilization()));

			// % utilization in [0:100] for the whole tree (nodes + leaves).
			tmp.addCounter("%totalUtilization", new OneShotInstrument<Integer>(r
					.getTotalUtilization())); // / 100d

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
    protected AbstractBTree(//
            final IRawStore store,//
            final INodeFactory nodeFactory,//
            final boolean readOnly,
            final IndexMetadata metadata,//
            final IRecordCompressorFactory<?> recordCompressorFactory
            ) {

        // show the copyright banner during startup.
        Banner.banner();

        // Note: MAY be null (implies a transient BTree).
//        assert store != null;

        if (metadata == null)
            throw new IllegalArgumentException();

        // save a reference to the immutable metadata record.
        this.metadata = metadata;

        this.writeTuple = new Tuple(this, KEYS | VALS);
        
        this.branchingFactor = metadata.getBranchingFactor();

        if(branchingFactor < Options.MIN_BRANCHING_FACTOR)
            throw new IllegalArgumentException();

        /*
         * Compute the minimum #of children/values. This is the same whether
         * this is a Node or a Leaf.
         */
        minChildren = (branchingFactor + 1) >> 1;

        if (nodeFactory == null)
            throw new IllegalArgumentException();

        this.store = store;
        
        this.readOnly = readOnly;
        
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
                branchingFactor,//
                0, //initialBufferCapacity
                metadata,//
                readOnly,//
                recordCompressorFactory
                );
        
        if (store == null) {

            /*
             * Transient BTree.
             * 
             * Note: The write retention queue controls how long nodes remain
             * mutable. On eviction, they are coded but not written onto the
             * backing store (since there is none for a transient BTree).
             * 
             * The readRetentionQueue is not used for a transient BTree since
             * the child nodes and the parents are connected using hard links
             * rather than weak references.
             */

            this.storeCache = null;
            
//            this.globalLRU = null;
            
//            this.readRetentionQueue = null;
            
        } else {

            /*
             * Persistent BTree.
             * 
             * The global LRU is used to retain recently used node/leaf data
             * records in memory and the per-store cache provides random access
             * to those data records. Only the INodeData or ILeafData is stored
             * in the cache. This allows reuse of the data records across B+Tree
             * instances since the data are read-only and the data records
             * support concurrent read operations. The INodeData or ILeafData
             * will be wrapped as a Node or Leaf by the owning B+Tree instance.
             */

            /*
             * FIXME if the LRUNexus is disabled, then use a
             * ConcurrentWeakValueCacheWithTimeout to buffer the leaves of an
             * IndexSegment. Essentially, a custom cache. Otherwise we lose some
             * of the performance of the leaf iterator for the index segment
             * since leaves are not recoverable by random access without a
             * cache.
             * 
             * @see BLZG-1501 (remove LRUNexus)
             */
//            this.storeCache = LRUNexus.getCache(store);
            this.storeCache = null;
            
//            this.readRetentionQueue = newReadRetentionQueue();
        
        }

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
     */
    IHardReferenceQueue<PO> newWriteRetentionQueue(final boolean readOnly) {

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
                    BigdataStatics.threadLocalBuffers, // threadLocalBuffers
                    16,// concurrencyLevel
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            metadata.getWriteRetentionQueueCapacity(), 0/* nscan */),
//                    new DefaultEvictionListener(),//
//                    metadata.getWriteRetentionQueueCapacity(),// shared capacity
                    metadata.getWriteRetentionQueueScan(),// thread local
                    128,//64, // thread-local queue capacity @todo config
                    64, //32 // thread-local tryLock size @todo config
                    null // batched updates listener.
            );

        }
        
        return new HardReferenceQueue<PO>(//
                new DefaultEvictionListener(),//
                metadata.getWriteRetentionQueueCapacity(),//
                metadata.getWriteRetentionQueueScan()//
        );

    }
    
//    /**
//     * Note: Method is package private so that it may be overridden for unit
//     * tests.
//     */
//    final protected RingBuffer<PO> newReadRetentionQueue() {
//
//        final int capacity = getReadRetentionQueueCapacity(); 
//        
//        return (capacity != 0
//
//        ? new HardReferenceQueue<PO>(//
//                NOPEvictionListener.INSTANCE,//
//                capacity, //
//                getReadRetentionQueueScan())
//
//        : null
//
//        );
//          
//    }

//    /**
//     * The capacity for the {@link #readRetentionQueue} (may differ for
//     * {@link BTree} and {@link IndexSegment}).
//     */
//    abstract protected int getReadRetentionQueueCapacity();
//
//    /**
//     * The capacity for the {@link #readRetentionQueue} (may differ for
//     * {@link BTree} and {@link IndexSegment}).
//     */
//    abstract protected int getReadRetentionQueueScan();
    
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
     * Note: {@link AbstractBTree} is NOT thread-safe and {@link #close()} MUST
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
     *                btree and there are mutations that have not been written
     *                through to the store)
     */
    synchronized public void close() {

        if (root == null) {

            throw new IllegalStateException(ERROR_CLOSED);

        }

        if (INFO || BigdataStatics.debug) {

            final String msg = "BTree close: name="
                    + metadata.getName()
                    + ", dirty="
                    + root.dirty
                    + ", height="
                    + getHeight()
                    + ", nentries="
                    + getEntryCount()
                    + ", impl="
                    + (this instanceof BTree ? ((BTree) this).getCheckpoint()
                            .toString() : getClass().getSimpleName());

            if (INFO)
                log.info(msg);

            if (BigdataStatics.debug)
                System.err.println(msg);

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

        // release the optional bloom filter.
        bloomFilter = null;
        
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
            throw new IndexInconsistentError(ERROR_ERROR_STATE + ": " + this.getIndexMetadata().getName() + ", store: " + store, error);
    }
    
    /**
     * The timestamp associated with the last {@link IAtomicStore#commit()} in
     * which writes buffered by this index were made restart-safe on the backing
     * store. The lastCommitTime is set when the index is loaded from the
     * backing store and updated after each commit. It is ZERO (0L) when
     * {@link BTree} is first created and will remain ZERO (0L) until the
     * {@link BTree} is committed.  If the backing store does not support atomic
     * commits, then this value will always be ZERO (0L).
     * <p>
     * Note: This is fixed for an {@link IndexSegment}.
     * 
     * @see ICheckpointProtocol#getLastCommitTime()
     */
    abstract public long getLastCommitTime();

    /**
     * The timestamp associated with unisolated writes on this index. This
     * timestamp is designed to allow the interleaving of full transactions
     * (whose revision timestamp is assigned by the transaction service) with
     * unisolated operations on the same indices.
     * <p>
     * The revision timestamp assigned by this method is
     * <code>lastCommitTime+1</code>. The reasoning is as follows. Revision
     * timestamps are assigned by the transaction manager when the transaction
     * is validated as part of its commit protocol. Therefore, revision
     * timestamps are assigned after the transaction write set is complete.
     * Further, the assigned revisionTimestamp will be strictly LT the
     * commitTime for that transaction. By using <code>lastCommitTime+1</code>
     * we are guaranteed that the revisionTimestamp for new writes (which will
     * be part of some future commit point) will always be strictly GT the
     * revisionTimestamp of historical writes (which were part of some prior
     * commit point).
     * <p>
     * Note: Unisolated operations using this timestamp ARE NOT validated. The
     * timestamp is simply applied to the tuple when it is inserted or updated
     * and will become part of the restart safe state of the B+Tree once the
     * unisolated operation participates in a commit.
     * <p>
     * Note: If an unisolated operation were to execute concurrent with a
     * transaction commit for the same index then that could produce
     * inconsistent results in the index and could trigger concurrent
     * modification errors. In order to avoid such concurrent modification
     * errors, unisolated operations which are to be mixed with full
     * transactions MUST ensure that they have exclusive access to the
     * unisolated index before proceeding. There are two ways to do this: (1)
     * take the application off line for transactions; (2) submit your unisolated
     * operations to the {@link IConcurrencyManager} which will automatically
     * impose the necessary constraints on concurrent access to the unisolated
     * indices.
     * 
     * @return The revision timestamp to be assigned to an unisolated write.
     * 
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    abstract public long getRevisionTimestamp();

    final public IResourceMetadata[] getResourceMetadata() {
        
        if (store == null) {

            /*
             * This covers the case of a transient BTree (no backing store).
             * 
             * Note that the indexUUID is reported as the store's UUID in this
             * case.
             */
            return new IResourceMetadata[] {
                    
                new TransientResourceMetadata(metadata.getIndexUUID())
                    
            };

        }

        return new IResourceMetadata[] {
          
                store.getResourceMetadata()
                
        };
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: If the B+Tree is read-only then the metadata object will be cloned
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
    public IndexMetadata getIndexMetadata() {

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
    private volatile IndexMetadata metadata2;
   
    /**
     * The metadata record for the index. This data rarely changes during the
     * life of the {@link BTree} object, but it CAN be changed.
     */
    protected IndexMetadata metadata;
    
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
        
        sb.append(", m=" + getBranchingFactor());

        sb.append(", height=" + getHeight());

        sb.append(", entryCount=" + getEntryCount());

        sb.append(", nodeCount=" + getNodeCount());

        sb.append(", leafCount=" + getLeafCount());

        sb.append(", lastCommitTime=" + getLastCommitTime());

        sb.append("}");
        
        return sb.toString();
        
    }
    
    @Override
   public BaseIndexStats dumpPages(final boolean recursive,
         final boolean visitLeaves) {

        if(!recursive) {
            
            return new BaseIndexStats(this);
            
        }
        
        final BTreePageStats stats = new BTreePageStats();
        
        dumpPages(this, getRoot(), visitLeaves, stats);

        return stats;
        
    }

   static private void dumpPages(//
         final AbstractBTree ndx,//
         final AbstractNode<?> node, //
         final boolean visitLeaves,//
         final BTreePageStats stats//
         ) {

        //node.dump(System.out);

        stats.visit(ndx, node);

        if (!node.isLeaf()) {

            final int nkeys = node.getKeyCount();

            for (int i = 0; i <= nkeys; i++) {

                try {
                    
                    // normal read following the node hierarchy, using cache, etc.
                    final AbstractNode<?> child = ((Node) node).getChild(i);

                    // recursive dump
                    dumpPages(ndx, child, visitLeaves, stats);
                    
                } catch (Throwable t) {
                    
                    if (InnerCause.isInnerCause(t, InterruptedException.class)
                            || InnerCause.isInnerCause(t,
                                    InterruptedException.class)) {
                        throw new RuntimeException(t);
                    }
                    /*
                     * Log the error and track the #of errors, but keep scanning
                     * the index.
                     */
                    stats.nerrors++;
                    log.error("Error reading child[i=" + i + "]: " + t, t);
                    continue;
                }

            }

        }

    }

    /**
     * Iff the B+Tree is an index partition then verify that the key lies within
     * the key range of an index partition.
     * <p>
     * Note: An index partition is identified by
     * {@link IndexMetadata#getPartitionMetadata()} returning non-
     * <code>null</code>.
     * <p>
     * Note: This method is used liberally in <code>assert</code>s to detect
     * errors that can arise is client code when an index partition is split,
     * joined, or moved.
     * 
     * @param key
     *            The key.
     * 
     * @param allowUpperBound
     *            <code>true</code> iff the <i>key</i> represents an inclusive
     *            upper bound and thus must be allowed to be LTE to the right
     *            separator key for the index partition. For example, this would
     *            be <code>true</code> for the <i>toKey</i> parameter on
     *            rangeCount or rangeIterator methods.
     * 
     * @return <code>true</code> always.
     * 
     * @throws IllegalArgumentException
     *             if the key is <code>null</code>
     * @throws KeyOutOfRangeException
     *             if the key does not lie within the index partition.
     * 
     * @deprecated This method has been disabled. It always returns true.
     *             <p>
     *             The method is disabled because it forces the caller to ensure
     *             that the query lies within the specified range. While that is
     *             all well and good, this places an undue burden on
     *             {@link FusedView} which must substitute the actual key range
     *             of a shard view for the caller's key range. This makes the
     *             test effectively into a self-check of whether
     *             {@link FusedView} has correctly done this substitution rather
     *             than a check of whether a request was mapped onto the correct
     *             shard, which was the original purpose of this test. The logic
     *             for performing such range checks was moved into
     *             {@link RangeCheckUtil}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/461">
     *      KeyAfterPartitionException </a>
     */
    final protected boolean rangeCheck(final byte[] key,
            final boolean allowUpperBound) {

//        if (key == null)
//            throw new IllegalArgumentException();
//
//        final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();
//
//        if (pmd == null) {
//
//            // nothing to check.
//
//            return true;
//
//        }
//
//        return RangeCheckUtil.rangeCheck(pmd, key, allowUpperBound);

        return true;
        
    }
    
    /*
     * IBTreeStatistics
     */
    
    final public int getBranchingFactor() {

        return branchingFactor;

    }

    /**
     * The minimum #of children (for a node) or the minimum #of values (for
     * a leaf).  This is computed in the same manner for nodes and leaves.
     * <pre>
     * (branchingFactor + 1) &lt;&lt; 1
     * </pre>
     */
    final int minChildren;

    /**
     * {@inheritDoc}
     * 
     * @return <code>true</code> since a B+Tree is a balanced tree.
     */
    @Override
    public final boolean isBalanced() {
    
        return true;
        
    }
    
//    abstract public int getHeight();
//
//    abstract public long getNodeCount();
//
//    abstract public long getLeafCount();

    abstract public long getEntryCount();

    /**
     * Return a statistics snapshot of the B+Tree.
     */
    public IBTreeStatistics getStatistics() {
    
        return new BTreeStatistics(this);
        
    }
    
    public IBTreeUtilizationReport getUtilization() {

        return new BTreeUtilizationReport(this);

    }

    /**
     * The object responsible for (de-)serializing the nodes and leaves of the
     * {@link IIndex}.
     */
    final public NodeSerializer getNodeSerializer() {

        return nodeSer;

    }

    /**
     * The root of the btree. This is initially a leaf until the leaf is split,
     * at which point it is replaced by a node. The root is also replaced each
     * time copy-on-write triggers a cascade of updates.
     * <p>
     * The hard reference to the root node is cleared if the index is
     * {@link #close() closed}. This method automatically {@link #reopen()}s
     * the index if it is closed, making it available for use.
     */
    final protected AbstractNode<?> getRoot() {

        // make sure that the root is defined.
        if (root == null)
            reopen();

        return root;

    }

    /**
     * Returns the node or leaf to be used for search. This implementation is
     * aware of the {@link #finger} and will return it if the key lies within
     * the finger.
     * 
     * @param key
     *            The key.
     * 
     * @return Either the root node of the tree or a recently touched leaf that
     *         is known to span the key.
     * 
     * @todo This is a place holder for finger(s) for hot spots in the B+Tree,
     *       but the finger(s) are currently disabled.
     *       <p>
     *       One way to implement fingers is to track the N most frequent leaf
     *       accesses using a streaming algorithm.
     */
    protected AbstractNode<?> getRootOrFinger(final byte[] key) {

        return getRoot();
//        
//        if (finger == null)
//            return getRoot();
//
//        Leaf leaf = finger.get();
//
//        if (leaf == null || leaf.deleted) {
//
//            // @todo clear finger on delete/copyOnWrite
//
//            return getRoot();
//
//        }
//
//        int entryIndex = leaf.keys.search(key);
//
//        if (entryIndex >= 0) {
//
//            /*
//             * There is an exact match on the finger so we know that this key
//             * belongs in this leaf.
//             */
//
//            return leaf;
//
//        }
//
//        if (entryIndex < 0) {
//
//            // Convert the position to obtain the insertion point.
//            entryIndex = -entryIndex - 1;
//
//            if (entryIndex > 0 && entryIndex < leaf.nkeys - 1) {
//
//                /*
//                 * The key belongs somewhere in between the first and the last
//                 * key in the leaf. For all those positions, we are again
//                 * guaranteed that this key belongs within this leaf.
//                 */
//
//                return leaf;
//
//            }
//
//        }
//
//        /*
//         * The key might belong in the leaf, but we are not sure since it lies
//         * on either the low or the high edge of the leaf.
//         * 
//         * @todo We could disambiguate this using the actual value of the
//         * separator keys on the parent. If the key is greater than or equal to
//         * the separatorKey for this leaf and strictly less than the
//         * separatorKey for the rightSibling, then it belongs in this leaf.
//         */
//
//        return getRoot();

    }

//    /*
//     * bloom filter support.
//     */
//
//    /**
//     * Returns true if the optional bloom filter reports that the key exists.
//     * 
//     * @param key
//     *            The key.
//     * 
//     * @return <code>true</code> if the bloom filter believes that the key is
//     *         present in the index. When <code>true</code>, you MUST still
//     *         test the key to verify that it is, in fact, present in the index.
//     *         When <code>false</code>, you SHOULD NOT test the index since a
//     *         <code>false</code> response is conclusive.
//     */
//    final protected boolean containsKey(byte[] key) {
//
//        return getBloomFilter().contains(key);
//
//    }

    /**
     * Private instance used for mutation operations (insert, remove) which are
     * single threaded. Both {@link IRangeQuery#KEYS} and
     * {@link IRangeQuery#VALS} are requested so that indices which encode part
     * of the application object within the key can recover the application
     * object in {@link #insert(Object, Object)} and {@link #remove(Object)}.
     * <p>
     * While the {@link Tuple} is not safe for use by concurrent threads, the
     * mutation API is not safe for concurrent threads either.
     */
    public final Tuple getWriteTuple() {
        
        return writeTuple;
        
    }

    /**
     * Note: This field is NOT static. This limits the scope of the
     * {@link Tuple} to the containing {@link AbstractBTree} instance.
     */
    private final Tuple writeTuple;

    /**
     * Return a {@link Tuple} that may be used to copy the value associated with
     * a key out of the {@link AbstractBTree}.
     * <p>
     * While the returned {@link Tuple} is not safe for use by concurrent
     * threads, each instance returned by this method is safe for use by the
     * thread in which it was obtained.
     * 
     * @see #lookup(byte[], Tuple)
     */
    public final Tuple getLookupTuple() {

//        WeakReference<Tuple> ref = lookupTupleRef.get();
//
//        Tuple tuple = ref == null ? null : ref.get();
//
//        if (tuple == null) {
//
//            tuple = new Tuple(AbstractBTree.this, VALS);
//
//            ref = new WeakReference<Tuple>(tuple);
//
//            lookupTupleRef.set(ref);
//
//        }
//        
//        return tuple;
        
        return lookupTuple.get();
        
    };

    /**
     * Return a {@link Tuple} that may be used to test for the existence of
     * tuples having a given key within the {@link AbstractBTree}.
     * <p>
     * The tuple does not copy either the keys or the values. Contains is
     * implemented as a lookup operation that either return this tuple or
     * <code>null</code>. When isolation is supported, the version metadata is
     * examined to determine if the matching entry is flagged as deleted in
     * which case contains() will report "false".
     * <p>
     * While the returned {@link Tuple} is not safe for use by concurrent
     * threads, each instance returned by this method is safe for use by the
     * thread in which it was obtained.
     * 
     * @see #contains(byte[])
     */
    public final Tuple getContainsTuple() {

//        WeakReference<Tuple> ref = containsTupleRef.get();
//
//        Tuple tuple = ref == null ? null : ref.get();
//
//        if (tuple == null) {
//
//            tuple = new Tuple(AbstractBTree.this, 0/* neither keys nor values */);
//
//            ref = new WeakReference<Tuple>(tuple);
//
//            containsTupleRef.set(ref);
//
//        }
//        
//        return tuple;
        
        return containsTuple.get();
        
    };

//    private final ThreadLocal<WeakReference<Tuple>> lookupTupleRef = new ThreadLocal<WeakReference<Tuple>>() {
//
//        @Override
//        protected WeakReference<Tuple> initialValue() {
//
//            return null;
//            
//        }
//        
//    };
//
//    private final ThreadLocal<WeakReference<Tuple>> containsTupleRef = new ThreadLocal<WeakReference<Tuple>>() {
//
//        @Override
//        protected WeakReference<Tuple> initialValue() {
//
//            return null;
//            
//        }
//        
//    };

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used to copy the value
     * associated with a key out of the btree during lookup operations.
     * <p>
     * Note: This field is NOT static. This limits the scope of the
     * {@link ThreadLocal} {@link Tuple} to the containing {@link AbstractBTree}
     * instance.
     */
    private final ThreadLocal<Tuple> lookupTuple = new ThreadLocal<Tuple>() {

        @Override
        protected Tuple initialValue() {

            return new Tuple(AbstractBTree.this, VALS);
            
        }
        
    };

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used for contains() tests.
     * The tuple does not copy either the keys or the values. Contains is
     * implemented as a lookup operation that either return this tuple or
     * <code>null</code>. When isolation is supported, the version metadata
     * is examined to determine if the matching entry is flagged as deleted in
     * which case contains() will report "false".
     * <p>
     * Note: This field is NOT static. This limits the scope of the
     * {@link ThreadLocal} {@link Tuple} to the containing {@link AbstractBTree}
     * instance.
     */
    private final ThreadLocal<Tuple> containsTuple = new ThreadLocal<Tuple>() {

        @Override
        protected Tuple initialValue() {

            return new Tuple(AbstractBTree.this, 0);

        }
        
    };

	@Override
    final public Object insert(Object key, Object value) {

        key = metadata.getTupleSerializer().serializeKey(key);

        value = metadata.getTupleSerializer().serializeVal(value);
        
        final ITuple tuple = insert((byte[]) key, (byte[]) value,
                false/* delete */, false/*putIfAbsent*/, getRevisionTimestamp(), getWriteTuple());
        
        if (tuple == null || tuple.isDeletedVersion()) {
            
            // There was no old tuple for that key.
            return null;
            
        }
        
        // de-serialize the old tuple.
        return tuple.getObject();
        
    }

    @Override
    final public byte[] insert(final byte[] key, final byte[] value) {

        if (key == null)
            throw new IllegalArgumentException();

        // non-conditional insert.
        final Tuple tuple = insert(key, value, false/* deleted */, false/*putIfAbsent*/,
                getRevisionTimestamp(), getWriteTuple());

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getValue();

    }

    @Override
    final public byte[] putIfAbsent(final byte[] key, final byte[] value) {

        if (key == null)
            throw new IllegalArgumentException();

        // Conditional insert. See BLZG-1539.
        final Tuple tuple = insert(key, value, false/* deleted */, true/*putIfAbsent*/,
                getRevisionTimestamp(), getWriteTuple());

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getValue();

    }

    /**
     * Core method for inserting or updating a value under a key.
     * 
     * @param key
     *            The variable length unsigned byte[] key.
     * @param value
     *            The variable length byte[] value (MAY be <code>null</code>).
     * @param delete
     *            <code>true</code> iff the index entry should be marked as
     *            deleted (this behavior is supported iff the btree supports
     *            delete markers).
     * @param putIfAbsent
     * 			  When <code>true</code>, a pre-existing entry for the key will
     *            NOT be replaced (unless it is a deleted tuple, which is the
     *            same as if there was no entry under the key). This should ONLY
     *            be true when the top-level method is <code>putIfAbsent</code>.
     *            Historical code paths should specify false for an unconditional
     *            mutation. See BLZG-1539.
     * @param timestamp
     *            The timestamp to be associated with the new or updated index
     *            entry (required iff the btree supports transactional isolation
     *            and otherwise 0L).
     * @param tuple
     *            Data and metadata for the old value under the key will be
     *            copied onto this object (optional).
     * 
     * @return <i>tuple</i> -or- <code>null</code> if there was no entry under
     *         that key. See {@link ITuple#isDeletedVersion()} to determine
     *         whether or not the entry is marked as deleted.
     * 
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    final public Tuple insert(final byte[] key, final byte[] value,
            final boolean delete, final boolean putIfAbsent, final long timestamp, final Tuple tuple) {

        assert delete == false || getIndexMetadata().getDeleteMarkers();

        assert delete == false || value == null;
        
//        assert timestamp == 0L
//                || (getIndexMetadata().getVersionTimestamps() && timestamp != 0L);

        if (key == null)
            throw new IllegalArgumentException();

        assertNotReadOnly();

        // conditional range check on the key.
        assert rangeCheck(key, false);

        btreeCounters.ninserts.incrementAndGet();
        
        final Tuple oldTuple = getRootOrFinger(key).insert(key, value, delete, putIfAbsent,
                timestamp, tuple);

        if (oldTuple == null) {

            final BloomFilter filter = getBloomFilter();

            if (filter != null) {

                if (getEntryCount() > filter.getMaxN()) {

                    /*
                     * Disable the filter since the index has exceeded the
                     * maximum #of index entries for which the bloom filter will
                     * have an acceptable error rate.
                     */
                    
//                    /*
//                     * TODO The code to recycle the old checkpoint addr, the old
//                     * root addr, and the old bloom filter has been disabled in
//                     * writeCheckpoint2 and AbstractBTree#insert pending the
//                     * resolution of ticket #440. This is being done to minimize
//                     * the likelyhood that the underlying bug for that ticket
//                     * can be tripped by the code.
//                     * 
//                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
//                     */
//                    filter.disable();
                    recycle(filter.disable());
                    
                    if(INFO) log.info("Bloom filter disabled - maximum error rate would be exceeded"
                                    + ": entryCount="
                                    + getEntryCount()
                                    + ", factory="
                                    + getIndexMetadata()
                                            .getBloomFilterFactory());

                } else {

                    /*
                     * Add the key to the bloom filter.
                     * 
                     * Note: While this will be invoked when the sequence is
                     * insert(key), remove(key), followed by insert(key) again,
                     * it does prevent update of the bloom filter when there was
                     * already an index entry for that key.
                     */

                    filter.add(key);

                }

            }

        }

        return oldTuple;

    }

    @Override
    final public Object remove(Object key) {

        key = metadata.getTupleSerializer().serializeKey(key);
        
        final ITuple tuple;
        if (getIndexMetadata().getDeleteMarkers()) {
        
            // set the delete marker.
            tuple = insert((byte[]) key, null/* val */, true/* delete */, false/*putIfAbsent*/,
                    getRevisionTimestamp(), getWriteTuple());
            
        } else {
        
            // remove the tuple.
            tuple = remove((byte[]) key, getWriteTuple());

        }

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getObject();
        
    }

    /**
     * Remove the tuple under that key (will write a delete marker if delete
     * markers are enabled).
     */
    @Override
    final public byte[] remove(final byte[] key) {

        final Tuple tuple;
        
        if (getIndexMetadata().getDeleteMarkers()) {

            // set the delete marker.
            tuple = insert(key, null/* val */, true/* delete */,false/*putIfAbsent*/,
                    getRevisionTimestamp(), getWriteTuple());
            
        } else {
        
            // remove the tuple.
            tuple = remove(key, getWriteTuple());
            
        }

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getValue();

    }

    /**
     * Core method for deleting a value under a key. If there is an entry under
     * the key then it is removed from the index. It is an error to use this
     * method if delete markers are being maintained. {@link #remove(byte[])}
     * uses {@link #insert(byte[], byte[], boolean, long, Tuple)} instead of
     * this method to mark the index entry as deleted when delete markers are
     * being maintained.
     * <p>
     * Note: removing a key has no effect on the optional bloom filter. If a key
     * is removed from the index by this method then the bloom filter will
     * report a <em>false positive</em> for that key, which will be detected
     * when we test the index itself. This works out fine in the scale-out
     * design since the bloom filter is per {@link AbstractBTree} instance and
     * split/join/move operations all result in new mutable {@link BTree}s with
     * new bloom filters to absorb new writes. For a non-scale-out deployment,
     * this can cause the performance of the bloom filter to degrade if you are
     * removing a lot of keys. However, in the special case of a {@link BTree}
     * that does NOT use delete markers, {@link BTree#removeAll()} will create a
     * new root leaf and a new (empty) bloom filter as well.
     * 
     * @param key
     *            The search key.
     * @param tuple
     *            An object that will be used to report data and metadata for
     *            the pre-existing version (optional).
     * 
     * @return <i>tuple</i> or <code>null</code> if there was no version
     *         under that key.
     * 
     * @throws UnsupportedOperationException
     *             if delete markers are being maintained.
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    final public Tuple remove(final byte[] key, final Tuple tuple) {

        if (key == null)
            throw new IllegalArgumentException();

        if(getIndexMetadata().getDeleteMarkers()) {
            
            throw new UnsupportedOperationException();
            
        }
        
        assertNotReadOnly();

        // conditional range check on the key.
        assert rangeCheck(key, false);

        btreeCounters.nremoves.incrementAndGet();

        return getRootOrFinger(key).remove(key, tuple);
        
    }

    /**
     * Remove all entries in the B+Tree.
     * <p>
     * Note: The {@link IIndexManager} defines methods for registering (adding)
     * and dropping indices vs removing the entries in an individual
     * {@link AbstractBTree}.
     */
    @Override
    abstract public void removeAll();
    
    @Override
    public Object lookup(Object key) {

        key = metadata.getTupleSerializer().serializeKey(key);

        final ITuple tuple = lookup((byte[]) key, getLookupTuple());

        if (tuple == null || tuple.isDeletedVersion()) {

            // no value under that key.
            return null;
            
        }

        // de-serialize the value under that key.
        return tuple.getObject();

    }

    @Override
    public byte[] lookup(final byte[] key) {

        final Tuple tuple = lookup(key, getLookupTuple());

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getValue();

    }
    
    /**
     * Core method for retrieving a value under a key. This method allows you to
     * differentiate an index entry whose value is <code>null</code> from a
     * missing index entry or (when delete markers are enabled) from a deleted
     * index entry. Applies the optional bloom filter if it exists. If the bloom
     * filter exists and reports <code>true</code>, then looks up the value
     * for the key in the index (note that the key might not exist in the index
     * since a bloom filter allows false positives, further the key might exist
     * for a deleted entry).
     * 
     * @param key
     *            The search key.
     * @param tuple
     *            An object that will be used to report data and metadata for
     *            the pre-existing version (required).
     * 
     * @return <i>tuple</i> or <code>null</code> if there is no entry in the
     *         index under the key.
     */
    public Tuple lookup(final byte[] key, Tuple tuple) {

        if (key == null)
            throw new IllegalArgumentException();

        if (tuple == null)
            throw new IllegalArgumentException();

        // Note: Sometimes we pass in the containsTuple so this assert is too strong.
//        assert tuple.getValuesRequested() : "tuple does not request values.";
        
        // conditional range check on the key.
        assert rangeCheck(key, false);

        boolean bloomHit = false;

        final BloomFilter filter = getBloomFilter(); 
        
        if (filter != null) {

            if (!filter.contains(key)) {

                // rejected by the bloom filter.
                
                return null;

            }

            bloomHit = true;
            
            /*
             * Fall through.
             * 
             * Note: Lookup against the index since this may be a false positive
             * or may be paired to a deleted entry and we need the tuple paired
             * to the key in any case.
             */

        }

        /*
         * Note: This is a hot spot with concurrent readers and does not provide
         * terribly useful information so I have taken it out. BBT 1/24/2010.
         */
//        btreeCounters.nfinds.incrementAndGet();

        tuple = getRootOrFinger(key).lookup(key, tuple);
        
        if (bloomHit && (tuple == null || tuple.isDeletedVersion())) {

            if (bloomHit)
                filter.falsePos();

        }

        return tuple;

    }

    @Override
    public boolean contains(Object key) {
        
        key = metadata.getTupleSerializer().serializeKey(key);
 
        return contains((byte[]) key);
        
    }

    /**
     * Core method to decide whether the index has a (non-deleted) entry under a
     * key. Applies the optional bloom filter if it exists. If the bloom filter
     * reports <code>true</code>, then verifies that the key does in fact
     * exist in the index.
     * 
     * @return <code>true</code> iff the key does not exist. Or, if the btree
     *         supports isolation, if the key exists but it is marked as
     *         "deleted".
     * 
     * @todo add unit test to btree suite w/ and w/o delete markers.
     */
    @Override
    public boolean contains(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        // conditional range check on the key.
        assert rangeCheck(key,false);

        boolean bloomHit = false;

        final BloomFilter filter = getBloomFilter();
        
        if (filter != null) {
            
            if (!filter.contains(key)) {
                
                // rejected by the bloom filter.
                return false;

            }

            bloomHit = true;
            
            /*
             * Fall through (we have to test for a false positive by reading on
             * the index).
             */
            
        }

        final ITuple tuple = getRootOrFinger(key).lookup(key,
                getContainsTuple());
        
        if(tuple == null || tuple.isDeletedVersion()) {
            
            if (bloomHit)
                filter.falsePos();
            
            return false;
            
        }
        
        return true;

    }

    @Override
    public long indexOf(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        // conditional range check on the key.
        assert rangeCheck(key, false);

        btreeCounters.nindexOf.increment();

        return getRootOrFinger(key).indexOf(key);

    }

    @Override
    public byte[] keyAt(final long index) {

        if (index < 0)
            throw new IndexOutOfBoundsException(ERROR_LESS_THAN_ZERO);

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException(ERROR_TOO_LARGE);

        btreeCounters.ngetKey.increment();

        return getRoot().keyAt(index);

    }

    @Override
    public byte[] valueAt(final long index) {

        final Tuple tuple = getLookupTuple();
        
        getRoot().valueAt(index, tuple);

        return tuple.isDeletedVersion() ? null : tuple.getValue();

    }

    final public Tuple valueAt(final long index, final Tuple tuple) {

        if (index < 0)
            throw new IndexOutOfBoundsException(ERROR_LESS_THAN_ZERO);

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException(ERROR_TOO_LARGE);

        if (tuple == null || !tuple.getValuesRequested())
            throw new IllegalArgumentException();

        btreeCounters.ngetKey.increment();

        getRoot().valueAt(index, tuple);

        return tuple.isDeletedVersion() ? null : tuple;

    }

    /*
     * IRangeQuery
     */

    @Override
    final public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        if (!metadata.getDeleteMarkers()) {

            /*
             * Delete markers are not in use, therefore rangeCount() will report
             * the exact count.
             */

            return rangeCount(fromKey, toKey);

        }
        
        /*
         * The use of delete markers means that index entries are not removed
         * immediately but rather a delete flag is set. This is always true for
         * the scale-out indices because delete markers are used to support
         * index partition views. It is also true for indices that can support
         * transactions regardless or whether or not the database is using
         * scale-out indices. In any case, if you want an exact range count when
         * delete markers are in use then you need to actually visit every tuple
         * in the index, which is what this code does. Note that the [flags] are
         * 0 since we do not need either the KEYS or VALS. We are just
         * interested in the #of tuples that the iterator is willing to visit.
         */

        long n = 0L;

        final Iterator itr = rangeIterator(fromKey, toKey, 0/* capacity */,
                0/* flags */, null/* filter */);

        while (itr.hasNext()) {

            itr.next();

            n++;

        }

        return n;

    }

    @Override
    final public long rangeCount() {
        
        return rangeCount(null, null);
        
    }

    /**
     * Variant implicitly converts the optional application keys into unsigned
     * byte[]s.
     * 
     * @param fromKey
     * @param toKey
     * @return
     */
    final public long rangeCount(Object fromKey, Object toKey) {

        fromKey = fromKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(fromKey);

        toKey = toKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(toKey);

        return rangeCount((byte[]) fromKey, (byte[]) toKey);
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * This method computes the #of entries in the half-open range using
     * {@link AbstractNode#indexOf(Object)}. Since it does not scan the tuples
     * it can not differentiate between deleted and undeleted tuples for an
     * index that supports delete markers, but the result will be exact if
     * delete markers are not being used. The cost is equal to the cost of
     * lookup of the both keys. If both keys are <code>null</code>, then the
     * cost is zero (no IOs).
     */
    @Override
    final public long rangeCount(final byte[] fromKey, final byte[] toKey) {

        if (fromKey == null && toKey == null) {

            /*
             * Note: getEntryCount() is very efficient. Both the BTree and the
             * IndexSegment record the entryCount in a field and just return the
             * value of that field.
             */

            return getEntryCount();

        }

        // only count the expensive ones.
        btreeCounters.nrangeCount.increment();
        
        final AbstractNode root = getRoot();

        // conditional range check on the key.

        if (fromKey != null)
            assert rangeCheck(fromKey,false);
        
        if (toKey != null)
            assert rangeCheck(toKey,true);

        long fromIndex = (fromKey == null ? 0 : root.indexOf(fromKey));

        long toIndex = (toKey == null ? getEntryCount() : root.indexOf(toKey));

        // Handle case when fromKey is not found.
        if (fromIndex < 0)
            fromIndex = -fromIndex - 1;

        // Handle case when toKey is not found.
        if (toIndex < 0)
            toIndex = -toIndex - 1;

        if (toIndex <= fromIndex) {

            return 0;

        }

        return (toIndex - fromIndex);

    }

    /**
     * Note: {@link #rangeCount(byte[], byte[])} already reports deleted tuples
     * for an {@link AbstractBTree} so this method is just delegated to that
     * one. However,
     * {@link FusedView#rangeCountExactWithDeleted(byte[], byte[])} treats this
     * case differently since it must report the exact range count when
     * considering all sources at once. It uses a range iterator scan visiting
     * both deleted and undeleted tuples for that.
     */
    @Override
    public long rangeCountExactWithDeleted(final byte[] fromKey,
            final byte[] toKey) {

        return rangeCount(fromKey, toKey);
        
//        if (fromKey == null && toKey == null) {
//
//            /*
//             * In this case the entryCount() is exact and will report both
//             * deleted and undeleted tuples (assuming that delete markers are
//             * enabled).
//             */
//
//            return getEntryCount();
//
//        }
//
//        /*
//         * Only a key-range will be used. 
//         */
//
//        long n = 0L;
//
//        final Iterator itr = rangeIterator(fromKey, toKey, 0/* capacity */,
//                IRangeQuery.DELETED/* flags */, null/* filter */);
//
//        while (itr.hasNext()) {
//
//            itr.next();
//
//            n++;
//
//        }
//
//        return n;

    }

    @Override
    final public ICloseableIterator<?> scan() {
        
        return new EntryScanIterator(rangeIterator());
        
    }
    
    @Override
    final public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);

    }

    /**
     * Variant implicitly converts the optional application keys into unsigned
     * byte[]s.
     * 
     * @param fromKey
     * @param toKey
     * @return
     */
    final public ITupleIterator rangeIterator(Object fromKey, Object toKey) {

        fromKey = fromKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(fromKey);

        toKey = toKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(toKey);

        return rangeIterator((byte[]) fromKey, (byte[]) toKey);
        
    }
    
    @Override
    final public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                KEYS | VALS/* flags */, null/* filter */);

    }

//    /**
//     * Return an iterator based on the post-order {@link Striterator}. This
//     * iterator does not support random seeks, reverse scans, or concurrent
//     * modification during traversal but it is <em>faster</em> than the
//     * {@link AbstractBTreeTupleCursor} when all you need is a forward
//     * visitation of the tuples in the key range.
//     * <p>
//     * This iterator is automatically used for a {@link BTree} unless one of the
//     * following flags is specified:
//     * <ul>
//     * <li>{@link IRangeQuery#CURSOR}</li>
//     * <li>{@link IRangeQuery#REVERSE}</li>
//     * <li>{@link IRangeQuery#REMOVEALL}</li>
//     * </ul>
//     * This iterator is NOT be used for an {@link IndexSegment} since it does
//     * not exploit the double-linked leaves and is therefore slower than the
//     * {@link IndexSegmentTupleCursor}.
//     */
//    protected ITupleIterator fastForwardIterator(byte[] fromKey, byte[] toKey,
//            int capacity, int flags) {
//
//        // conditional range check on the key.
//        
//        if (fromKey != null)
//            assert rangeCheck(fromKey,false);
//
//        if (toKey != null)
//            assert rangeCheck(toKey,true);
//
//        final ITupleIterator src = getRoot().rangeIterator(fromKey, toKey,
//                flags);
//
//        return src;
//        
//    }

    /**
     * Variant implicitly converts the optional application keys into unsigned
     * byte[]s.
     * 
     * @param fromKey
     * @param toKey
     * @return
     */
    final public ITupleIterator rangeIterator(Object fromKey, Object toKey,
            final int capacity,//
            final int flags,//
            final IFilter filter//
    ) {

        fromKey = fromKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(fromKey);

        toKey = toKey == null ? null : metadata.getTupleSerializer()
                .serializeKey(toKey);

        return rangeIterator((byte[]) fromKey, (byte[]) toKey, capacity, flags,
                filter);

    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Core implementation.
	 * <p>
	 * Note: If {@link IRangeQuery#CURSOR} is specified the returned iterator
	 * supports traversal with concurrent modification by a single-threaded
	 * process (the {@link BTree} is NOT thread-safe for writers). Write are
	 * permitted iff {@link AbstractBTree} allows writes.
	 * <p>
	 * Note: {@link IRangeQuery#REVERSE} is handled here by wrapping the
	 * underlying {@link ITupleCursor}.
	 * <p>
	 * Note: {@link IRangeQuery#REMOVEALL} is handled here by wrapping the
	 * iterator.
	 * <p>
	 * Note: {@link FusedView#rangeIterator(byte[], byte[], int, int, IFilter)}
	 * is also responsible for constructing an {@link ITupleIterator} in a
	 * manner similar to this method. If you are updating the logic here, then
	 * check the logic in that method as well!
	 * 
	 * @todo add support to the iterator construct for filtering by a tuple
	 *       revision timestamp range.
	 */
    @Override
    public ITupleIterator rangeIterator(//
            final byte[] fromKey,//
            final byte[] toKey,//
            final int capacityIsIgnored,//
            final int flags,//
            final IFilter filter//
            ) {

        btreeCounters.nrangeIterator.increment();

        /*
         * Does the iterator declare that it will not write back on the index?
         */
        final boolean readOnly = ((flags & IRangeQuery.READONLY) != 0);

        if (readOnly && ((flags & IRangeQuery.REMOVEALL) != 0)) {

            throw new IllegalArgumentException();

        }

        /*
         * Note: this does not work out since it is not so easy to determine when
         * the iterator is a point test as toKey is the exclusive upper bound.
         */
//        * Note: this method will automatically apply the optional bloom filter to
//        * reject range iterator requests that correspond to a point test. However
//        * this can only be done when the fromKey and toKey are both non-null and
//        * equals and further when the iterator was not requested with any options
//        * that would permit concurrent modification of the index.
//        if (isBloomFilter()
//                && fromKey != null
//                && toKey != null
//                && (readOnly || (((flags & REMOVEALL) == 0) && ((flags & CURSOR) == 0)))
//                && BytesUtil.bytesEqual(fromKey, toKey)) {
//
//            /*
//             * Do a fast rejection test using the bloom filter.
//             */
//            if(!getBloomFilter().contains(fromKey)) {
//                
//                /*
//                 * The key is known to not be in the index so return an empty
//                 * iterator.
//                 */
//                return EmptyTupleIterator.INSTANCE;
//                
//            }
//            
//            /*
//             * Since the bloom filter accepts the key we fall through into the
//             * normal iterator logic. Using this code path is still possible
//             * that the filter gave us a false positive and that the key is not
//             * (in fact) in the index. Either way, the logic below will sort
//             * things out.
//             */
//            
//        }
        
        /*
         * Figure out what base iterator implementation to use.  We will layer
         * on the optional filter(s) below. 
         */
        ITupleIterator src;

        if ((this instanceof BTree) && ((flags & REVERSE) == 0)
                && ((flags & REMOVEALL) == 0) && ((flags & CURSOR) == 0)) {

            /*
             * Use the recursion-based striterator since it is faster for a
             * BTree (but not for an IndexSegment).
             * 
             * Note: The recursion-based striterator does not support remove()!
             * 
             * @todo we could pass in the Tuple here to make the APIs a bit more
             * consistent across the recursion-based and the cursor based
             * iterators.
             * 
             * @todo when the capacity is one and REVERSE is specified then we
             * can optimize this using a reverse traversal striterator - this
             * will have lower overhead than the cursor for the BTree (but not
             * for an IndexSegment).
             */

//            src = fastForwardIterator(fromKey, toKey, capacity, flags);

            src = getRoot().rangeIterator(fromKey, toKey, flags);

        } else {

            final Tuple tuple = new Tuple(this, flags);

            if (this instanceof IndexSegment) {
                
                final IndexSegment seg = (IndexSegment) this;

                /*
                 * @todo we could scan the list of pools and chose the best fit
                 * pool and then allocate a buffer from that pool. Best fit
                 * would mean either the byte range fits without "too much" slop
                 * or the #of reads will have to perform is not too large. We
                 * might also want to limit the maximum size of the reads.
                 */

//                final DirectBufferPool pool = DirectBufferPool.INSTANCE_10M;
                final DirectBufferPool pool = DirectBufferPool.INSTANCE;
                
                if (true
                        && ((flags & REVERSE) == 0)
                        && ((flags & CURSOR) == 0)
                        && (seg.getStore().getCheckpoint().maxNodeOrLeafLength <= pool
                                .getBufferCapacity())
                        && ((rangeCount(fromKey, toKey) / branchingFactor) > 2)) {

                    src = new IndexSegmentMultiBlockIterator(seg, pool,
                            fromKey, toKey, flags);

                } else {

                    src = new IndexSegmentTupleCursor(seg, tuple, fromKey,
                            toKey);

                }

            } else if (this instanceof BTree) {

                if (isReadOnly()) {

                    // Note: this iterator does not allow removal.
                    src = new ReadOnlyBTreeTupleCursor(((BTree) this), tuple,
                            fromKey, toKey);

                } else {

                    // Note: this iterator supports traversal with concurrent
                    // modification.
                    src = new MutableBTreeTupleCursor(((BTree) this),
                            tuple, fromKey, toKey);

                }

            } else {

                throw new UnsupportedOperationException(
                        "Unknown B+Tree implementation: "
                                + this.getClass().getName());

            }

            if ((flags & REVERSE) != 0) {

                /*
                 * Reverse scan iterator.
                 * 
                 * Note: The reverse scan MUST be layered directly over the
                 * ITupleCursor. Most critically, REMOVEALL combined with a
                 * REVERSE scan needs to process the tuples in reverse index
                 * order and then delete them as it goes.
                 */

                src = new Reverserator((ITupleCursor) src);

            }

        }

        if (filter != null) {

            /*
             * Apply the optional filter.
             * 
             * Note: This needs to be after the reverse scan and before
             * REMOVEALL (those are the assumptions for the flags).
             */
            
            src = new WrappedTupleIterator(filter
                    .filter(src, null/* context */));

        }
        
        if ((flags & REMOVEALL) != 0) {
            
            assertNotReadOnly();
            
            /*
             * Note: This iterator removes each tuple that it visits from the
             * source iterator.
             */
            
            src = new TupleRemover() {
                @Override
                protected boolean remove(ITuple e) {
                    // remove all visited tuples.
                    return true;
                }
            }.filterOnce(src, null/* context */);

        }
        
        return src;

    }

    /**
     * Copy all data, including deleted index entry markers and timestamps iff
     * supported by the source and target. The goal is an exact copy of the data
     * in the source btree.
     * 
     * @param src
     *            The source index.
     * @param fromKey
     *            The first key that will be copied (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be copied (exclusive). When
     *            <code>null</code> there is no upper bound.
     * @param overflow
     *            When <code>true</code> the {@link IOverflowHandler} defined
     *            for the source index (if any) will be applied to copy raw
     *            records onto the backing store for this index. This value
     *            SHOULD be <code>false</code> if the two indices have the
     *            same backing store and <code>true</code> otherwise.
     * 
     * @throws UnsupportedOperationException
     *             if the <i>src</i> and <i>this</i> do not have the same
     *             setting for {@link IndexMetadata#getDeleteMarkers()}
     * 
     * @throws UnsupportedOperationException
     *             if the <i>src</i> and <i>this</i> do not have the same
     *             setting for {@link IndexMetadata#getVersionTimestamps()}
     * 
     * @return The #of index entries that were copied.
     * 
     * @see CompactTask
     * @see OverflowManager
     * 
     * @todo this would be more efficient if we could reuse the same buffer for
     *       keys and values in and out. As it stands it does a lot of byte[]
     *       allocation.
     * 
     * @todo write tests for all variations (delete markers, timestamps,
     *       overflow handler, etc).
     */
    public long rangeCopy(final IIndex src, final byte[] fromKey,
            final byte[] toKey, final boolean overflow) {

        assertNotReadOnly();

        final ITupleIterator itr = src.rangeIterator(fromKey, toKey,
                0/* capacity */, IRangeQuery.ALL, null/* filter */);

        final IndexMetadata thisMetadata = getIndexMetadata();
        
        final IndexMetadata sourceMetadata = src.getIndexMetadata();
        
        final boolean deleteMarkers = thisMetadata.getDeleteMarkers();

        final boolean versionTimestamps = thisMetadata.getVersionTimestamps();

        if (sourceMetadata.getDeleteMarkers() != deleteMarkers) {

            throw new UnsupportedOperationException(
                    "Support for delete markers not consistent");

        }

        if (sourceMetadata.getVersionTimestamps() != versionTimestamps) {

            throw new UnsupportedOperationException(
                    "Support for version timestamps not consistent");

        }

        final IOverflowHandler overflowHandler = (overflow ? sourceMetadata
                .getOverflowHandler() : null);
        
        ITuple tuple = null;
        
        while (itr.hasNext()) {

            tuple = itr.next();

            final byte[] key = tuple.getKey();

            final boolean deletedVersion = deleteMarkers && tuple.isDeletedVersion();
            
            final byte[] val;
            
            if (deletedVersion) {
                
                val = null;
                
            } else {
                
                if (overflowHandler != null) {

                    /*
                     * Provide the handler with the opportunity to copy the
                     * blob's data onto the backing store for this index and
                     * re-write the value, which is presumably the blob
                     * reference.
                     */

                    val = overflowHandler.handle(tuple, getStore());

                } else {

                    val = tuple.getValue();

                }

            }

            if (versionTimestamps) {

                final long timestamp = tuple.getVersionTimestamp();

                if (deletedVersion) {

                    insert(key, null/* value */, true/* delete */, false/*putIfAbsent*/, timestamp,
                            null/* tuple */);

                } else {

                    insert(key, val, false/* delete */, false/*putIfAbsent*/, timestamp, null/* tuple */);

                }

            } else {

                if (deletedVersion) {

                    remove(key);

                } else {

                    insert(key, val);

                }

            }

        }
        
        return tuple == null ? 0L : tuple.getVisitCount();

    }
    
    @Override
    public <T> T submit(final byte[] key, final ISimpleIndexProcedure<T> proc) {

        // conditional range check on the key.
        if (key != null)
            assert rangeCheck(key, false);

        return proc.apply(this);
        
    }

    @SuppressWarnings("unchecked")
    @Override
    public void submit(final byte[] fromKey, final byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        // conditional range check on the key.
        if (fromKey != null)
            assert rangeCheck(fromKey,false);

        if (toKey != null)
            assert rangeCheck(toKey,true);

        final Object result = proc.apply(this);

        if (handler != null) {

            handler.aggregate(result, new Split(null, 0, 0));

        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void submit(final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        final Object result = ctor.newInstance(this, fromIndex, toIndex, keys,
                vals).apply(this);

        if (aggregator != null) {

            aggregator.aggregate(result, new Split(null, fromIndex, toIndex));

        }

    }
    
    /**
     * Utility method returns the right-most node in the B+Tree.
     * 
     * @param nodesOnly
     *            when <code>true</code> the search will halt at the
     *            right-most non-leaf. Otherwise it will return the right-most
     *            leaf.
     * 
     * @return The right-most child in the B+Tree -or- <code>null</code> if
     *         the root of the B+Tree is a leaf and
     *         <code>nodesOnly == true</code>.
     */
    public AbstractNode getRightMostNode(final boolean nodesOnly) {
        
        final AbstractNode root = getRoot();

        if (root.isLeaf()) {

            return null;

        }

        return ((Node)root).getRightMostChild(nodesOnly);
        
    }

    /**
     * Return a cursor that may be used to efficiently locate and scan the
     * leaves in the B+Tree. The cursor will be initially positioned on the leaf
     * identified by the symbolic constant.
     */
    abstract public ILeafCursor newLeafCursor(SeekEnum where);
    
    /**
     * Return a cursor that may be used to efficiently locate and scan the
     * leaves in the B+Tree. The cursor will be initially positioned on the leaf
     * that spans the given <i>key</i>.
     * 
     * @param key
     *            The key (required).
     * 
     * @throws IllegalArgumentException
     *             if <i>key</i> is <code>null</code>.
     */
    abstract public ILeafCursor newLeafCursor(byte[] key);
    
//    /**
//     * Clone the caller's cursor.
//     *
//     * @param leafCursor
//     *            Another leaf cursor.
//     * 
//     * @return A clone of the caller's cursor.
//     * 
//     * @throws IllegalArgumentException
//     *             if the argument is <code>null</code>
//     * @throws IllegalArgumentException
//     *             if the argument is a cursor for a different
//     *             {@link AbstractBTree}.
//     */
//    abstract public ILeafCursor newLeafCursor(ILeafCursor leafCursor);
    
    /**
     * Recursive dump of the tree.
     * 
     * @param out
     *            The dump is written on this stream.
     * 
     * @return true unless an inconsistency is detected.
     */
    public boolean dump(final PrintStream out) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out);

    }

    public boolean dump(final Level level, final PrintStream out) {

        // True iff we will write out the node structure.
        final boolean info = level.toInt() <= Level.INFO.toInt();

        final IBTreeUtilizationReport utils = getUtilization();

        if (info) {

        	final int branchingFactor = getBranchingFactor();

            final int height = getHeight();

            final long nnodes = getNodeCount();

            final long nleaves = getLeafCount();

            final long nentries = getEntryCount();

			out.println("branchingFactor=" + branchingFactor + ", height="
					+ height + ", #nodes=" + nnodes + ", #leaves=" + nleaves
					+ ", #entries=" + nentries + ", nodeUtil="
					+ utils.getNodeUtilization() + "%, leafUtil="
					+ utils.getLeafUtilization() + "%, utilization="
					+ utils.getTotalUtilization() + "%");
        }

        if (root != null) {

            return root.dump(level, out, 0, true);

        } else
            return true;

    }

    /**
     * Return the level of <i>t</i> below the root node or leaf.
     * 
     * @param t
     *            A node or leaf that belongs to this B+Tree.
     * 
     * @return ZERO (0) iff <i>t == root</i> and otherwise the number of levels
     *         that <i>t</i> is below the root.
     * 
     * @throws IllegalArgumentException
     *             if <i>t</i> is null.
     * @throws IllegalArgumentException
     *             if <i>t</i> does not belong to this B+Tree.
     */
    @SuppressWarnings("rawtypes")
    public int getLevel( final AbstractNode t) {

        return getLevel(t, getRoot());

    }

    /**
     * Return the level of <i>t</i> below the given node or leaf.
     * 
     * @param t
     *            A node or leaf that belongs to this B+Tree.
     * 
     * @param node
     *            A node or leaf that is either <i>t</i> or (recursively) some
     *            parent of <i>t</i>.
     * 
     * @return ZERO (0) iff <i>t == root</i> and otherwise the number of levels
     *         that <i>t</i> is below the root.
     * 
     * @throws IllegalArgumentException
     *             if <i>t</i> is null.
     * @throws IllegalArgumentException
     *             if <i>t</i> does not belong to this B+Tree.
     * @throws IllegalArgumentException
     *             if <i>node</i> is null.
     * @throws IllegalArgumentException
     *             if <i>node</i> does not belong to this B+Tree.
     * @throws NotChildException
     *             if <i>t</i> is neither <i>node</node> nor some descendant of
     *             <i>node</i>.
     */
    @SuppressWarnings("rawtypes")
    public int getLevel(final AbstractNode t, final AbstractNode node) {

        if (t == null)
            throw new IllegalArgumentException();

        if (t.btree != this)
            throw new IllegalArgumentException();

        final int level = _getLevel(t, node, 0);

        if (level == -1) {

            throw new NotChildException("Not a child of the given node: t=" + t.toShortString() + ", node=" + node);
            
        }
        
        return level;
        
    }

    /**
     * Return the level of [t] beneath [node], ZERO (0) iff t==node, and -1 if
     * [t] is not dominated by [node].
     * 
     * @param t
     *            Either node or a descendant of node.
     * @param node
     *            Some node.
     * @param level
     *            Zero on the first invocation.
     *            
     * @return The discovered level of [t] beneath [node], ZERO (0) iff t==node,
     *         and -1 if [t] is not dominated by [node].
     */
    @SuppressWarnings("rawtypes")
    private static int _getLevel(final AbstractNode t, final AbstractNode node, final int level) {

        if (t == node) {
            
            /*
             * Note: if node is root, then t.getParent() would be null but we
             * will not hit this below because t == node and hence we return the
             * current level.
             */
            
            return level;
            
        }

        final Node p = t.getParent(); // parent (must be non-null t != node so t is not root).

        if (p == null) {

            /*
             * We have chased parents up to the root. Since we did not find
             * [node] along the path to the root, [t] must not be dominated by
             * [node]. This is an illegal request.  We return -1 so the caller
             * can report the actual (t,node) arguments from the top-level 
             * invocation.
             */

            return -1;

        }
        
        return _getLevel(p, node, level + 1);
        
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
     * 
     * @todo This might be abstract here and final in BTree and in IndexSegment.
     *       For {@link IndexSegment}, we know it is read-only so we do not need
     *       to test anything. For {@link BTree}, we can break encapsulation and
     *       check whether or not the {@link BTree} is read-only more readily
     *       than we can in this class.
     */
//    synchronized
//    final 
    protected void touch(final AbstractNode<?> node) {

        assert node != null;

        /*
         * Note: DO NOT update the last used timestamp for the B+Tree here! This
         * is a huge performance penalty!
         */
//        touch();

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
    private final void doSyncTouch(final AbstractNode<?> node) {

//        final long beginNanos = System.nanoTime();
        
        synchronized (this) {

            doTouch(node);

        }

//        final long elapsedNanos = System.nanoTime() - beginNanos;
//        
//        // See BLZG-1664
        //btreeCounters.syncTouchNanos.add(elapsedNanos);
        
    }
    
    private final void doTouch(final AbstractNode<?> node) {
        
//        final long beginNanos = System.nanoTime();
//        
//        // See BLZG-1664
        //btreeCounters.touchCount.increment();
        
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

//        final long elapsedNanos = System.nanoTime() - beginNanos;
//        
//        // See BLZG-1664
        //btreeCounters.touchNanos.add(elapsedNanos);

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
    final protected void writeNodeRecursive(final AbstractNode<?> node) {

        if (node instanceof Node && getStore() instanceof IIndexManager) {

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

    /**
     * This is the historical implementation and runs entirely in the caller's
     * thread.
     * 
     * @param node
     */
    final protected void writeNodeRecursiveCallersThread(final AbstractNode<?> node) {
        
//        final long beginNanos = System.nanoTime();
        
        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.dirty;
        assert !node.deleted;
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
        final Iterator<AbstractNode> itr = node.postOrderNodeIterator(
                true/* dirtyNodesOnly */, false/* nodesOnly */);

        while (itr.hasNext()) {

            final AbstractNode<?> t = itr.next();

            assert t.dirty;

            if (t != root) {

                /*
                 * The parent MUST be defined unless this is the root node.
                 */

                assert t.parent != null;
                assert t.parent.get() != null;

            }

            // write the dirty node on the store.
            writeNodeOrLeaf(t);

            ndirty++;
            
//            if (BigdataStatics.debug && ndirty > 0 && ndirty % 1000 == 0) {
//				System.out.println("nwritten=" + ndirty + " in "
//						+ (System.currentTimeMillis() - begin) + "ms");
//            }

            if (t instanceof Leaf)
                nleaves++;

        }

//        final long elapsedNanos = System.nanoTime() - beginNanos;
//        
//        if (log.isInfoEnabled() || elapsedNanos > TimeUnit.MILLISECONDS.toNanos(5000)) {
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
//            final String s = "wrote: "+(metadata.getName()!=null?"name="+metadata.getName()+", ":"") + ndirty + " records (#nodes=" + nnodes
//                    + ", #leaves=" + nleaves + ") in " + elapsedNanos
//                    + "ms : addrRoot=" + node.getIdentity();
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
    @SuppressWarnings("rawtypes")
    final protected void writeNodeRecursiveConcurrent(final AbstractNode<?> node) {

        final long beginNanos = System.nanoTime();
        
        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.dirty;
        assert !node.deleted;
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
            
        // Visit dirty nodes and leaves.
        final Iterator<AbstractNode> itr = node.postOrderNodeIterator(
                true/* dirtyNodesOnly */, false/* nodesOnly */);

        // Map with one entry per level. Each entry is a dirtyList for that level.
        final Map<Integer/* level */, List<AbstractNode>> dirtyMap = new HashMap<Integer, List<AbstractNode>>();
        
        while (itr.hasNext()) {

            final AbstractNode<?> t = itr.next();

            assert t.dirty;

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
            final Integer level = getLevel(t, node);

            // Lookup dirty list for that level.
            List<AbstractNode> dirtyList = dirtyMap.get(level);

            if (dirtyList == null) {

                // First node or level at this level.
                dirtyMap.put(level, dirtyList = new LinkedList<AbstractNode>());
                
            }
            
//          log.error("Adding to dirty list: t="+t.toShortString()+", level="+level+", decendentOrSelfOf="+node.toShortString());
            
            // Add node/leaf to dirtyList for that level in the index.
            dirtyList.add(t);

            ndirty++;
            
            if (t instanceof Leaf)
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

        // max would be the B+Tree height+1 (if evicting the root).
        assert dirtyLevelCount <= getHeight() + 1 : "dirtyLevelCount=" + dirtyLevelCount + ", height=" + getHeight()
                + ", dirtyMap.keys=" + dirtyMap.keySet();

        /*
         * Now evict each dirtyList in parallel starting at the deepest, and
         * then proceeding one by one until we reach the list at level ZERO (the
         * node that we were asked to evict). Since the parent of a dirty child
         * is always dirty, all levels from N-1..0 *must* exist in our dirty
         * list by level map.
         */
        for (int i = dirtyLevelCount - 1; i >= 0; i--) {

            // The dirty list for level [i].
            final List<AbstractNode> dirtyList = dirtyMap.get(i);

            if (dirtyList == null)
                throw new AssertionError("Dirty list not found: level=" + i + ", #levels=" + dirtyLevelCount);

            // #of dirty nodes / leaves in this level set.
            final int dirtyListSize = dirtyList.size();
            
            // No more parallelism than we have in the dirty list for this level.
            final int nparallel = Math.min(maxParallelEvictThreads, dirtyListSize);

            if (dirtyListSize < minDirtyListSizeForParallelEvict) {

                /*
                 * Avoid parallelism when only a few nodes or leaves will be
                 * evicted.
                 */
                for (AbstractNode t : dirtyList) {
                    
                    writeNodeOrLeaf(t, nodeSer);
            
        }

                if (log.isInfoEnabled())
                    log.info("Evicting " + dirtyListSize + " dirty nodes/leaves using " + nparallel + " threads.");

            } else {

                final ArrayList<Future<Void>> futureList = new ArrayList<Future<Void>>(dirtyListSize);

                // Note: Must have the same level of concurrency in
                // NodeSerializer instances.
                final LatchedExecutor executor = new LatchedExecutor(((IIndexManager) getStore()).getExecutorService(),
                        nparallel);

                try {

                    for (AbstractNode t : dirtyList) {

                        // Need [final] to be visible inside Runnable().
                        final AbstractNode u = t;

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
                                        nodeSer.nodeFactory, //
                                        branchingFactor, //
                                        nodeSer.getWriteBufferCapacity(),//
                                        metadata, //
                                        readOnly, //
                                        nodeSer.recordCompressorFactory);

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

//    private void badNode(final AbstractNode<?> node) {
////    	try {
////			Thread.sleep(50);
////		} catch (InterruptedException e) {
////			// ignore;
////		}
//    	throw new AssertionError("ReadOnly and identity: " + node.identity);
//    }
    
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
    protected long writeNodeOrLeaf(final AbstractNode<?> node) {

        return writeNodeOrLeaf(node, nodeSer);

    }

    private long writeNodeOrLeaf(final AbstractNode<?> node,final NodeSerializer nodeSer) {
    	
        if (error != null)
            throw new IllegalStateException(ERROR_ERROR_STATE, error);
    	
        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.btree == this;
        assert node.dirty;
        assert !node.deleted;
        assert !node.isPersistent();
        /**
         * Occasional CI errors on this assert for have been observed for
         * StressTestUnisolatedReadWriteIndex. This has been traced to a test
         * error. The test was interrupting the tasks, but the tasks were not
         * being cancelled simultaneously. This meant that one task could be
         * interrupted during an eviction from the write retention queue and
         * that another task could obtain the UnisolatedReadWriteIndex lock and
         * then hit the error since the BTree, the write retention queue, and
         * the nodes that were being evicted would an inconsistent state state.
         * The test does not fail if it is run to completion (no timeout).
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/343" >Stochastic assert
         *      in AbstractBTree#writeNodeOrLeaf() in CI </a>
         * 
         *      TestMROWTransactions might also demonstrate an issue
         *      occasionally. If so, then check for the same root cause.
         */
//        if (node.isReadOnly()) {
//        	badNode(node); // supports debugging
//        }
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
        final Node parent = node.getParent();

        if (parent == null) {

            assert node == root;

        } else {

            // parent must be dirty if child is dirty.
            assert parent.isDirty();

            // parent must not be persistent if it is dirty.
            assert !parent.isPersistent();

        }

        if (debug)
            node.assertInvariants();
        
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

                // code data record and _replace_ the data ref.
                ((Leaf) node).data = nodeSer.encodeLive(((Leaf) node).data);

                // slice onto the coded data record.
                slice = ((Leaf) node).data();

                btreeCounters.leavesWritten.increment();;

            } else {

                // code data record and _replace_ the data ref.
                ((Node) node).data = nodeSer.encodeLive(((Node) node).data);

                // slice onto the coded data record.
                slice = ((Node) node).data();

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

//            if (node.writing == null) {
//            	log.warn("Concurrent modification of thread guard", new RuntimeException("WTF2: " + node.hashCode()));
//            	
//            	throw new AssertionError("Concurrent modification of thread guard");
//            }

//            node.writing = null;
        	
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
            if (storeCache!=null) {
                // remove from cache.
            	storeCache.remove(oldAddr);
            }
			deleteNodeOrLeaf(oldAddr);//, node instanceof Node);
        }

        node.setDirty(false);

        if (parent != null) {

            // Set the persistent identity of the child on the parent.
            parent.setChildAddr(node);

            // // Remove from the dirty list on the parent.
            // parent.dirtyChildren.remove(node);

        }

        if (storeCache != null) {

            /*
             * Put the data record (the delegate) into the cache, touching it on
             * the backing LRU.
             * 
             * Note: This provides an unfair retention for recently written
             * nodes or leaves equal to that of recently read nodes or leaves. I
             * do not know what to do about that. However, the total size across
             * all per-store caches is (SHOULD BE) MUCH larger than the write
             * retention queue so that bias may not matter that much.
             */
            if (null != storeCache.putIfAbsent(addr, node.getDelegate())) {

                /*
                 * Note: For a WORM store, the address is always new so there
                 * will not be an entry in the cache for that address.
                 * 
                 * Note: For a RW store, the addresses can be reused and the
                 * delete of the old address MUST have cleared the entry for
                 * that address from the store's cache.
                 */
                
                throw new AssertionError("addr already in cache: " + addr
                        + " for " + store.getFile());
                
            }
            
        }
        
//        if (node.writing == null) {
//        	log.warn("Concurrent modification of thread guard", new RuntimeException("WTF2: " + node.hashCode()));
//        	
//        	throw new AssertionError("Concurrent modification of thread guard");
//        }
//
//        node.writing = null;

        return addr;

    }

    /**
     * Read a node or leaf from the store.
     * <p>
     * Note: Callers SHOULD be synchronized in order to ensure that only one
     * thread will read the desired node or leaf in from the store and attach
     * the reference to the newly read node or leaf as appropriate into the
     * existing data structures (e.g., as the root reference or as a child of a
     * node or leaf already live in memory).
     * 
     * @param addr
     *            The address in the store.
     * 
     * @return The node or leaf.
     * 
     * @throws IllegalArgumentException
     *             if the address is {@link IRawStore#NULL}.
     */
    protected AbstractNode<?> readNodeOrLeaf(final long addr) {

        if (addr == IRawStore.NULL)
            throw new IllegalArgumentException();
        
        
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

            // wrap as Node or Leaf.
            final AbstractNode<?> node = nodeSer.wrap(this, addr, data);

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
     * Create the reference that will be used by a {@link Node} to refer to its
     * children (nodes or leaves).
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
    final <T extends AbstractNode<T>> Reference<AbstractNode<T>> newRef(
            final AbstractNode<T> child) {
        
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
            
            return new HardReference<AbstractNode<T>>(child);
            
        } else {
        
            return new WeakReference<AbstractNode<T>>( child );
//        return new SoftReference<AbstractNode>( child ); // causes significant GC "hesitations".
        }
        
        
    }
    
    /**
     * A class that provides hard reference semantics for use with transient
     * {@link BTree}s. While the class extends {@link WeakReference}, it
     * internally holds a hard reference and thereby prevents the reference from
     * being cleared. This approach is necessitated on the one hand by the use
     * of {@link Reference} objects for {@link AbstractNode#self},
     * {@link AbstractNode#parent}, {@link Node#childRefs}, etc. and on the
     * other hand by the impossibility of defining your own direct subclass of
     * {@link Reference} (a runtime security manager exception will result).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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
        @Override
        public T get() {
            
            return ref;
            
        }
        
        /**
         * Overridden as a NOP.
         */
        @Override
        public void clear() {

            // NOP
            
        }
        
    }
    
//    /*
//     * API used to report how long it has been since the BTree was last used.
//     * This is used to clear BTrees that are not in active use from a variety of
//     * caches. This helps us to better manage RAM.
//     */
//    
//    /**
//     * Note: DO NOT invoke this method from hot code such as
//     * {@link #touch(AbstractNode)} as that will impose a huge performance
//     * penalty! It is sufficient to let the
//     * {@link SynchronizedHardReferenceQueueWithTimeout} invoke this method
//     * itself when it adds an {@link AbstractBTree} reference.
//     */
//    final public void touch() {
//    
//        timestamp = System.nanoTime();
//        
//    }
//    
//    final public long timestamp() {
//        
//        return timestamp;
//        
//    }
//    
//    private long timestamp = System.nanoTime();

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
	 * for this {@link BTree}. This value only applies when raw record support
	 * has been enabled for the {@link BTree}. Values greater than this in
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
		
		btreeCounters.rawRecordsWritten.increment();;
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

		btreeCounters.bytesOnStore_rawRecords.addAndGet(-recycle(addr));		
	}

	/**
	 * Delete a node or leaf from the backing store, updating various
	 * performance counters.
	 * 
	 * @param addr
	 *            The address of the node or leaf.
	 * 
	 * @see <a
	 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/434#comment:2">
	 *      Simplify tracking of address release for B+Tree and HTree </a>
	 */
	void deleteNodeOrLeaf(final long addr) {

		if(addr == IRawStore.NULL)
			throw new IllegalArgumentException();
		
		if (isReadOnly())
			throw new IllegalStateException(ERROR_READ_ONLY);

		if (this instanceof BTree) {

			if (((BTree) this).getCheckpoint().getRootAddr() == addr) {
			
				/*
				 * TODO This is a bit of a hack.  It is designed to prevent
				 * the double-delete of the last committed root node or leaf.
				 * This should be cleaned up as part of addressing [1].
				 * 
				 * [1] https://sourceforge.net/apps/trac/bigdata/ticket/434
				 */

				return;
			
			}
			
		}

		btreeCounters.bytesOnStore_nodesAndLeaves.addAndGet(-recycle(addr));

    }

    /**
     * Recycle (aka delete) the allocation. This method also adjusts the #of
     * bytes released in the {@link BTreeCounters}.
     * 
     * @param addr
     *            The address to be recycled.
     * 
     * @return The #of bytes which were recycled and ZERO (0) if the address is
     *         {@link IRawStore#NULL}.
     */
    protected int recycle(final long addr) {
        
        if (addr == IRawStore.NULL) 
            return 0;
        
        final int nbytes = store.getByteCount(addr);
        
        getBtreeCounters().bytesReleased.add(nbytes);
        
        store.delete(addr);
        
        return nbytes;

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
