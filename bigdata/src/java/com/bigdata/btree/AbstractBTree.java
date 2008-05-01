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
 * Created on Dec 19, 2006
 */

package com.bigdata.btree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.Split;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

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
 * objects are NOT compable and applications that use Unicode data in their keys
 * MUST make sure that they use a collator that imposes the same sort order each
 * time they provision a {@link KeyBuilder}.
 * </p>
 * <p>
 * The use of variable length unsigned byte[] keys makes it possible for the
 * B+-Tree to perform very fast comparison of a search key with keys in the
 * nodes and leaves of the tree. To support fast search, the leading prefix is
 * factored out each time a node or leaf is made immutable, e.g., directly
 * proceeding serialization. Further, the separator keys are choosen to be the
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
 * @version $Id$
 * 
 * @see KeyBuilder
 */
abstract public class AbstractBTree implements IIndex, ILocalBTree {

    /**
     * Log for btree opeations.
     */
    protected static final Logger log = Logger.getLogger(AbstractBTree.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Log for {@link BTree#dump(PrintStream)} and friends.
     */
    protected static final Logger dumpLog = Logger.getLogger(BTree.class
            .getName()
            + "#dump");

    /**
     * Flag turns on the use of {@link AbstractNode#assertInvariants()} and is
     * automatically enabled when the {@link #log logger} is set to
     * {@link Level#DEBUG}.
     */
    final protected boolean debug = DEBUG;

    /**
     * Text of a message used in exceptions for mutation operations on the index
     * segment.
     */
    final protected transient static String MSG_READ_ONLY = "Read-only index";

    /**
     * Counters tracking various aspects of the btree.
     */
    /* protected */public final Counters counters = new Counters();

    /**
     * The persistence store.
     */
    final protected IRawStore store;

    /**
     * The branching factor for the btree.
     */
    final protected int branchingFactor;

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
     * Note: This field MUST be marked as [volatile] in order to guarentee
     * correct semantics for double-checked locking in {@link #reopen()}
     * 
     * @see http://en.wikipedia.org/wiki/Double-checked_locking
     */
    protected volatile AbstractNode root;

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
     * Note: The code in {@link Node#postOrderNodeIterator(boolean)} and
     * {@link DirtyChildIterator} MUST NOT touch the hard reference queue since
     * those iterators are used when persisting a node using a post-order
     * traversal. If a hard reference queue eviction drives the serialization of
     * a node and we touch the hard reference queue during the post-order
     * traversal then we break down the semantics of
     * {@link HardReferenceQueue#append(Object)} as the eviction does not
     * necessarily cause the queue to reduce in length. Another way to handle
     * this is to have {@link HardReferenceQueue#append(Object)} begin to evict
     * objects before is is actually at capacity, but that is also a bit
     * fragile.
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
    final protected HardReferenceQueue<PO> writeRetentionQueue;

    /**
     * The #of distinct nodes and leaves on the {@link #writeRetentionQueue}.
     */
    protected int ndistinctOnWriteRetentionQueue;

    /**
     * The #of distinct nodes and leaves on the {@link #writeRetentionQueue}.
     */
    final public int getWriteRetentionQueueDistinctCount() {

        return ndistinctOnWriteRetentionQueue;

    }

    /**
     * The capacity of the {@link #writeRetentionQueue}.
     */
    final public int getWriteRetentionQueueCapacity() {

        return writeRetentionQueue.capacity();

    }
    
    /**
     * The {@link #readRetentionQueue} reduces reads through to the backing
     * store in order to prevent disk reads and reduces de-serialization costs
     * for nodes by maintaining them as materialized objects.
     * <p>
     * Non-deleted nodes (that is nodes or leaves) are placed onto this hard
     * reference queue when they are evicted from the
     * {@link #writeRetentionQueue}. Nodes are always converted into their
     * immutable variants when they are evicted from the
     * {@link #writeRetentionQueue} so the {@link #readRetentionQueue} only
     * contains hard references to immutable nodes. If there is a need to write
     * on a node then {@link AbstractNode#copyOnWrite()} will be used to create
     * a mutable copy of the node and the old node will be
     * {@link AbstractNode#delete()}ed. Delete is responsible for releasing any
     * state associated with the old node, so deleted nodes on the
     * {@link #readRetentionQueue} will occupy very little space in the heap.
     * <p>
     * Note: evictions from this hard reference cache are driven by inserts.
     * Inserts are driven by evictions of non-deleted nodes from the
     * {@link #writeRetentionQueue}.
     * <p>
     * Note: the {@link #readRetentionQueue} will typically contain multiple
     * references to a given node.
     * 
     * FIXME This is an experimental feature.  It is turned on or off in the
     * ctor for now.
     */
    final protected HardReferenceQueue<PO> readRetentionQueue;

    /**
     * The minimum allowed branching factor (3). The branching factor may be odd
     * or even.
     */
    static public final int MIN_BRANCHING_FACTOR = 3;

    /**
     * Return some "statistics" about the btree.
     * 
     * @todo fold in {@link #getUtilization()} here.
     */
    synchronized public ICounterSet getCounters() {

        if(counterSet==null) {

            counterSet = new CounterSet();
            
            counterSet.addCounter("index UUID", new OneShotInstrument<String>(
                    getIndexMetadata().getIndexUUID().toString()));
            
            counterSet.addCounter("branchingFactor",
                    new OneShotInstrument<Integer>(branchingFactor));

            counterSet.addCounter("class",
                    new OneShotInstrument<String>(getClass().getName()));

            counterSet.addCounter("queueCapacity",
                    new OneShotInstrument<Integer>(
                            getWriteRetentionQueueCapacity()));

            counterSet.addCounter("#queueSize", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getWriteRetentionQueueDistinctCount());
                }
            });

            // the % utilization in [0:1] for the whole tree (nodes + leaves).
            counterSet.addCounter("% utilization", new Instrument<Double>(){
                protected void sample() {
                    final int[] tmp = getUtilization();
                    setValue(tmp[2] / 100d);
                }
            });
            
            counterSet.addCounter("height", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getHeight());
                }
            });

            counterSet.addCounter("#nnodes", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getNodeCount());
                }
            });

            counterSet.addCounter("#nleaves", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getLeafCount());
                }
            });

            counterSet.addCounter("#entries", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getEntryCount());
                }
            });

            counterSet.attach(counters.getCounters());
    
            /*
             * @todo report on soft vs weak refs.
             * 
             * @todo report on readRetentionQueue iff used. track #distinct.
             * 
             * @todo track life time of nodes and leaves and #of touches during life
             * time.
             * 
             * @todo estimate heap requirements for nodes and leaves based on their
             * state (keys, values, and other arrays). report estimated heap
             * consumption here.
             */
        
        }
        
        return counterSet;
              
    }
    private CounterSet counterSet;
    
    /**
     * @param store
     *            The persistence store.
     * @param nodeFactory
     *            Object that provides a factory for node and leaf objects.
     * @param addrSer
     *            Object that knows how to (de-)serialize the child addresses in
     *            an {@link INodeData}.
     */
    protected AbstractBTree(//
            IRawStore store,//
            INodeFactory nodeFactory,//
            IAddressSerializer addrSer,//
            IndexMetadata metadata//
            ) {

        // show the copyright banner during statup.
        Banner.banner();

        assert store != null;

        assert addrSer != null;

        assert metadata != null;

        // save a reference to the immutable metadata record.
        this.metadata = metadata;
        
        this.branchingFactor = metadata.getBranchingFactor();

        assert branchingFactor >= MIN_BRANCHING_FACTOR;
        
        assert nodeFactory != null;

        this.store = store;

        this.writeRetentionQueue = newWriteRetentionQueue();

        this.readRetentionQueue = newReadRetentionQueue();

        this.nodeSer = new NodeSerializer(//
                nodeFactory,//
                branchingFactor,//
                0, //initialBufferCapacity
                addrSer, //
                metadata,//
                store.isFullyBuffered()
                );

    }

    protected HardReferenceQueue<PO> newWriteRetentionQueue() {

        return new HardReferenceQueue<PO>(//
                new DefaultEvictionListener(),//
                BTree.DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY,//
                BTree.DEFAULT_WRITE_RETENTION_QUEUE_SCAN//
        );

    }
    
    protected HardReferenceQueue<PO> newReadRetentionQueue() {

        if (BTree.DEFAULT_READ_RETENTION_QUEUE_CAPACITY != 0) {

            return new HardReferenceQueue<PO>(
                    NOPEvictionListener.INSTANCE,
                    BTree.DEFAULT_READ_RETENTION_QUEUE_CAPACITY,
                    BTree.DEFAULT_READ_RETENTION_QUEUE_SCAN);
            
        }
        
        return null;
        
    }
    
    /**
     * The contract for {@link #close()} is to reduce the resource burden of the
     * index (by discarding buffers) while not rendering the index inoperative.
     * An index that has been {@link #close() closed} MAY be
     * {@link #reopen() reopened} at any time (conditional on the continued
     * availability of the backing store). The index reference remains valid
     * after a {@link #close()}. A closed index is transparently restored by
     * either {@link #getRoot()} or {@link #reopen()}. A {@link #close()} on a
     * dirty index MUST discard writes rather than flushing them to the store
     * and MUST NOT update its {@link Checkpoint} record ({@link #close()} is
     * used to discard indices with partial writes when an {@link AbstractTask}
     * fails).
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

            throw new IllegalStateException("Already closed");

        }

        if (root.dirty) {

//            throw new IllegalStateException("Root node is dirty");
            log.warn("Root is dirty - discarding writes");

        }

        /*
         * Release buffers.
         */
        nodeSer.close();

        /*
         * Clear the hard reference queue.
         * 
         * Note: This is safe since we know as a pre-condition that the root
         * node is clean and therefore that there are no dirty nodes or leaves
         * in the hard reference queue.
         */
        writeRetentionQueue.clear(true/*clearRefs*/);
        if(readRetentionQueue!=null) {
            readRetentionQueue.clear(true/*clearRefs*/);
        }

        /*
         * Clear the reference to the root node (permits GC).
         */
        root = null;

    }

    /**
     * This is part of a {@link #close()}/{@link #reopen()} protocol that may
     * be used to reduce the resource burden of an {@link AbstractBTree}. The
     * implementation must reload the root node of the tree iff {@link #root} is
     * <code>null</code> (indicating that the index has been closed). This
     * method is automatically invoked by a variety of methods that need to
     * ensure that the index is available for use.
     * 
     * @see #close()
     * @see #isOpen()
     * @see #getRoot()
     */
    abstract protected void reopen();

    /**
     * An "open" index has its buffers and root node in place rather than having
     * to reallocate buffers or reload the root node from the store.
     * 
     * @return If the index is "open".
     * 
     * @see #close()
     * @see #reopen()
     * @see #getRoot()
     */
    final public boolean isOpen() {

        return root != null;

    }

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
            
            throw new UnsupportedOperationException(MSG_READ_ONLY);
            
        }
        
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
     */
    abstract public long getLastCommitTime();
    
    /**
     * The backing store.
     */
    final public IRawStore getStore() {

        return store;

    }

    final public IResourceMetadata[] getResourceMetadata() {
        
        return new IResourceMetadata[] {
          
                store.getResourceMetadata()
                
        };
        
    }
    
    /**
     * Returns the metadata record for this btree.
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
    final public IndexMetadata getIndexMetadata() {

        if(isReadOnly()) {
            
            synchronized (this) {

                if (metadata2 == null) {

                    metadata2 = metadata.clone();

                }

            }

            return metadata2;
            
        }
        
        return metadata;
        
    }
    private IndexMetadata metadata2;
   
    /**
     * The metadata record for the index. This data rarely changes during the
     * life of the {@link BTree} object, but it CAN be changed.
     */
    protected IndexMetadata metadata;
    
    /**
     * Iff the B+Tree is an index partition then verify that the key lies within
     * the key range of an index partition.
     * <p>
     * Note: An index partition is identified by
     * {@link IndexMetadata#getPartitionMetadata()} returning non-<code>null</code>.
     * <p>
     * Note: This method is used liberally in <code>assert</code>s to detect
     * errors that can arise is client code when an index partition is split,
     * joined, or moved.
     * 
     * @param key
     *            The key.
     * 
     * @param allowUpperBound
     *            <code>true</code> iff the <i>key</i> represents an
     *            inclusive upper bound and thus must be allowed to be LTE to
     *            the right separator key for the index partition. For example,
     *            this would be <code>true</code> for the <i>toKey</i>
     *            parameter on rangeCount or rangeIterator methods.
     * 
     * @return <code>true</code> always.
     * 
     * @throws IllegalArgumentException
     *             if the key is <code>null</code>
     * @throws RuntimeException
     *             if the key does not lie within the index partition.
     */
    protected boolean rangeCheck(byte[] key, boolean allowUpperBound) {

        if (key == null)
            throw new IllegalArgumentException();

        final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();
        
        if (pmd == null) {

            // nothing to check.
            
            return true;
            
        }
        
        final byte[] leftSeparatorKey = pmd.getLeftSeparatorKey();

        final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();

        if (BytesUtil.compareBytes(key, leftSeparatorKey) < 0) {

            throw new RuntimeException("KeyBeforePartition: key="
                    + BytesUtil.toString(key) + ", pmd=" + pmd
                    + ", storeFile=" + getStore().getFile());

        }

        if (rightSeparatorKey != null ) {
            
            final int ret = BytesUtil.compareBytes(key, rightSeparatorKey);
            
            if (allowUpperBound) {

                if (ret <= 0) {

                    // key less than or equal to the exclusive upper bound.

                } else {
                    
                    throw new RuntimeException("KeyAfterPartition: key="
                            + BytesUtil.toString(key) + ", allowUpperBound="
                            + allowUpperBound + ", pmd=" + pmd + ", storeFile="
                            + getStore().getFile());
                }

            } else {

                if (ret < 0) {

                    // key strictly less than the exclusive upper bound.
                    
                } else {
                    
                    throw new RuntimeException("KeyAfterPartition: key="
                            + BytesUtil.toString(key) + ", allowUpperBound="
                            + allowUpperBound + ", pmd=" + pmd + ", storeFile="
                            + getStore().getFile());
                }
                
            }

        }
        
        // key lies within the index partition.
        return true;
            
    }
     
    /**
     * The branching factor for the btree.
     */
    final public int getBranchingFactor() {

        return branchingFactor;

    }

    /**
     * The height of the btree. The height is the #of levels minus one. A btree
     * with only a root leaf has <code>height := 0</code>. A btree with a
     * root node and one level of leaves under it has <code>height := 1</code>.
     * Note that all leaves of a btree are at the same height (this is what is
     * means for the btree to be "balanced"). Also note that the height only
     * changes when we split or join the root node (a btree maintains balance by
     * growing and shrinking in levels from the top rather than the leaves).
     */
    abstract public int getHeight();

    /**
     * The #of non-leaf nodes in the {@link AbstractBTree}. This is zero (0)
     * for a new btree.
     */
    abstract public int getNodeCount();

    /**
     * The #of leaf nodes in the {@link AbstractBTree}. This is one (1) for a
     * new btree.
     */
    abstract public int getLeafCount();

    /**
     * The #of entries (aka values) in the {@link AbstractBTree}. This is zero
     * (0) for a new btree.
     * 
     * @todo this could be re-defined as the exact entry count if we tracked the
     *       #of deleted index entries and subtracted that from the total #of
     *       index entries before returning the result. The #of deleted index
     *       entries would be stored in the index {@link Checkpoint} record.
     *       <p>
     *       Since {@link #getEntryCount()} is also used to give the total #of
     *       index enties (and we need that feature) so we need to either add
     *       another method with the appropriate semantics, add a boolean flag,
     *       or add a method returning the #of deleted entries, etc.
     */
    abstract public int getEntryCount();

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
    final public AbstractNode getRoot() {

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
     */
    protected AbstractNode getRootOrFinger(byte[] key) {

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
//                 * guarenteed that this key belongs within this leaf.
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

    /**
     * Private instance used for mutation operations (insert, remove) which are
     * single threaded.
     */
    protected final Tuple writeTuple = new Tuple(/*KEYS|*/VALS);

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used to copy the value
     * associated with a key out of the btree during lookup operations.
     */
    protected final ThreadLocal<Tuple> lookupTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(VALS);
            
        }
        
    };

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used for contains() tests.
     * The tuple does not copy either the keys or the values. Contains is
     * implemented as a lookup operation that either return this tuple or
     * <code>null</code>. When isolation is supported, the version metadata
     * is examined to determine if the matching entry is flagged as deleted in
     * which case contains() will report "false".
     */
    protected final ThreadLocal<Tuple> containsTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(0);

        }
        
    };
    
    final public byte[] insert(Object key, Object value) {

        if (!(key instanceof byte[])) {

            key = KeyBuilder.asSortKey(key);

        }

        if( !(value instanceof byte[])) {
            
            value = SerializerUtil.serialize(value);
            
        }
        
        return insert((byte[])key,(byte[])value);
        
    }

    final public byte[] insert(byte[] key, byte[] value) {

        if (key == null)
            throw new IllegalArgumentException();

        final Tuple tuple = insert(key, value, false/* deleted */,
                0L/* timestamp */, writeTuple);

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
     * @param timestamp
     *            The timestamp to be associated with the new or updated index
     *            entry (required iff the btree supports transactional isolation
     *            and otherwise 0L).
     * @param tuple
     *            Data and metadata for the old value under the key will be
     *            copied onto this object (optional).
     * 
     * @return <i>tuple</i> -or- <code>null</code> if there was no entry
     *         under that key. See {@link ITuple#isDeletedVersion()} to
     *         determine whether or not the entry is marked as deleted.
     *         
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    final public Tuple insert(byte[] key, byte[] value, boolean delete,
            long timestamp, Tuple tuple) {

        assert delete == false || getIndexMetadata().getDeleteMarkers();

        assert delete == false || value == null;
        
        assert timestamp == 0L
                || (getIndexMetadata().getVersionTimestamps() && timestamp != 0L);

        if (key == null)
            throw new IllegalArgumentException();

        assertNotReadOnly();

        // conditional range check on the key.
        assert rangeCheck(key,false);

        counters.ninserts++;
        
        return getRootOrFinger(key)
                .insert(key, value, delete, timestamp, tuple);

    }

    final public byte[] remove(Object key) {

        if (!(key instanceof byte[])) {

            key = KeyBuilder.asSortKey(key);

        }
        
        return remove((byte[]) key);
        
    }

    final public byte[] remove(byte[] key) {

        final Tuple tuple;
        
        if (getIndexMetadata().getDeleteMarkers()) {
            
            tuple = insert(key, null/* val */, true/* delete */,
                    0L/* timestamp */, writeTuple);
            
        } else {
        
            tuple = remove(key, writeTuple);
            
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
    final public Tuple remove(byte[] key, Tuple tuple) {

        if (key == null)
            throw new IllegalArgumentException();

        if(getIndexMetadata().getDeleteMarkers()) {
            
            throw new UnsupportedOperationException();
            
        }
        
        assertNotReadOnly();

        // conditional range check on the key.
        assert rangeCheck(key,false);
        
        counters.nremoves++;

        return getRootOrFinger(key).remove(key,tuple);

    }

    public byte[] lookup(Object key) {

        if (!(key instanceof byte[])) {

            key = KeyBuilder.asSortKey(key);

        }

        return lookup((byte[])key);

    }

    public byte[] lookup(byte[] key) {

        final Tuple tuple = lookup(key, lookupTuple.get());

        return tuple == null || tuple.isDeletedVersion() ? null : tuple
                .getValue();

    }
    
    /**
     * Core method for retrieving a value under a key. This method allows you to
     * differentiate an index entry whose value is <code>null</code> from a
     * missing index entry or (when delete markers are enabled) from a deleted
     * index entry.
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
    public Tuple lookup(byte[] key, Tuple tuple) {

        if (key == null)
            throw new IllegalArgumentException();

        if (tuple == null)
            throw new IllegalArgumentException();

        // Note: Sometimes we pass in the containsTuple so this assert is too strong.
//        assert tuple.getValuesRequested() : "tuple does not request values.";
        
        // conditional range check on the key.
        assert rangeCheck(key,false);

        counters.nfinds++;

        return getRootOrFinger(key).lookup(key,tuple);

    }

    public boolean contains(Object key) {
        
        if (!(key instanceof byte[])) {

            key = KeyBuilder.asSortKey(key);

        }
 
        return contains((byte[]) key);
        
    }

    /**
     * Core method to decide whether the index has a (non-deleted) entry under a
     * key.
     * <p>
     * True iff the key does not exist. Or, if the btree supports isolation, if
     * the key exists but it is marked as "deleted".
     * 
     * @todo add unit test to btree suite w/ and w/o delete markers.
     */
    public boolean contains(byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        // conditional range check on the key.
        assert rangeCheck(key,false);

        final ITuple tuple = getRootOrFinger(key).lookup(key, containsTuple.get());
        
        if(tuple == null || tuple.isDeletedVersion()) {
            
            return false;
            
        }
        
        return true;

    }

    public int indexOf(byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        // conditional range check on the key.
        assert rangeCheck(key,false);

        counters.nindexOf++;

        int index = getRootOrFinger( key).indexOf(key);

        return index;

    }

    public byte[] keyAt(int index) {

        if (index < 0)
            throw new IndexOutOfBoundsException("less than zero");

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException("too large");

        counters.ngetKey++;

        return getRoot().keyAt(index);

    }

    public byte[] valueAt(int index) {

        final Tuple tuple = lookupTuple.get();
        
        getRoot().valueAt(index, tuple);

        return tuple.isDeletedVersion() ? null : tuple.getValue();

    }

    final public byte[] valueAt(int index,Tuple tuple) {

        if (index < 0)
            throw new IndexOutOfBoundsException("less than zero");

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException("too large");

        if (tuple == null || !tuple.getValuesRequested())
            throw new IllegalArgumentException();
        
        counters.ngetKey++;

        getRoot().valueAt(index, tuple);

        return tuple.isDeletedVersion() ? null : tuple.getValue();

    }

    /*
     * IRangeQuery
     */
    
    final public long rangeCount(byte[] fromKey, byte[] toKey) {

        if (fromKey == null && toKey == null) {

            /*
             * Note: this assumes that getEntryCount() is more efficient. Both
             * the BTree and the IndexSegment record the entryCount in a field
             * and just return the value of that field.
             */

            return getEntryCount();

        }

        // only count the expensive ones.
        counters.nrangeCount++;
        
        final AbstractNode root = getRoot();

        // conditional range check on the key.

        if (fromKey != null)
            assert rangeCheck(fromKey,false);
        
        if (toKey != null)
            assert rangeCheck(toKey,true);

        int fromIndex = (fromKey == null ? 0 : root.indexOf(fromKey));

        int toIndex = (toKey == null ? getEntryCount() : root.indexOf(toKey));

        // Handle case when fromKey is not found.
        if (fromIndex < 0)
            fromIndex = -fromIndex - 1;

        // Handle case when toKey is not found.
        if (toIndex < 0)
            toIndex = -toIndex - 1;

        if (toIndex <= fromIndex) {

            return 0;

        }

        int ret = toIndex - fromIndex;

        return ret;

    }

    final public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                KEYS | VALS/* flags */, null/* filter */);

    }

    /**
     * Returns a {@link ChunkedLocalRangeIterator} that supports
     * {@link Iterator#remove()}.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, ITupleFilter filter) {

        counters.nrangeIterator++;

        if ((flags & REMOVEALL) != 0) {

            assertNotReadOnly();

        }

        // conditional range check on the key.
        
        if (fromKey != null)
            assert rangeCheck(fromKey,false);

        if (toKey != null)
            assert rangeCheck(toKey,true);

        final ITupleIterator src;

        if ((flags & REMOVEALL) == 0) {

            /*
             * Simple case since we will not be writing on the btree.
             */

            src = getRoot().rangeIterator(fromKey, toKey, flags, filter);

        } else {

            /*
             * The iterator will populate its buffers up to the capacity and
             * then delete behind once the buffer is full or as soon as the
             * iterator is exhausted.
             * 
             * Note: This would cause a stack overflow if the caller is already
             * using a chunked range iterator. The problem is that the ResultSet
             * is populated using IIndex#rangeIterator(...). This situation is
             * handled by explicitly turning off the REMOVEALL flag when forming
             * the iterator for the ResultSet. See ChunkedRangeIterator.
             */

            src = new ChunkedLocalRangeIterator(this, fromKey, toKey, capacity,
                    KEYS | flags, filter);

        }

        if (isReadOnly()) {

            /*
             * Must explicitly disable Iterator#remove().
             */

            return new ReadOnlyEntryIterator(src);

        } else {

            /*
             * Iterator#remove() MAY be supported.
             */

            return src;

        }

    }
    
    /**
     * Copy all data, including deleted index entry markers and timestamps iff
     * supported by the source and target. The goal is an exact copy of the data
     * in the source btree.
     * 
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
     * @todo write tests for all variations (delete markers, timestamps,
     *       overflow handler, etc).
     */
    public long rangeCopy(IIndex src, byte[] fromKey, byte[] toKey, boolean overflow) {

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

                    insert(key, null/* value */, true/* delete */, timestamp,
                            null/* tuple */);

                } else {

                    insert(key, val, false/* delete */, timestamp, null/* tuple */);

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
    
    public Object submit(byte[] key, ISimpleIndexProcedure proc) {
        
        // conditional range check on the key.
        if(key!=null) assert rangeCheck(key,false);

        return proc.apply(this);
        
    }

    @SuppressWarnings("unchecked")
    public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        // conditional range check on the key.
        if (fromKey != null)
            assert rangeCheck(fromKey,false);

        if (toKey != null)
            assert rangeCheck(toKey,true);

        Object result = proc.apply(this);

        if (handler != null) {

            handler.aggregate(result, new Split(null, 0, 0));

        }
        
    }
    
    @SuppressWarnings("unchecked")
    public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractIndexProcedureConstructor ctor, IResultHandler aggregator) {

        Object result = ctor.newInstance(this, fromIndex, toIndex, keys, vals)
                .apply(this);
        
        if (aggregator != null) {

            aggregator.aggregate(result, new Split(null, fromIndex, toIndex));

        }
        
    }
    
    /**
     * Visits all entries in key order. This is identical to
     * 
     * <pre>
     * rangeIterator(null, null)
     * </pre>
     * 
     * @return An iterator that will visit all entries in key order.
     * 
     * @todo rename as rangeIterator and promote to IRangeQuery interface.
     */
    final public ITupleIterator entryIterator() {

        return rangeIterator(null, null);

    }

    /**
     * Iterator visits the leaves of the tree.
     * 
     * @return Iterator visiting the {@link Leaf leaves} of the tree.
     * 
     * @todo optimize this when prior-next leaf references are present, e.g.,
     *       for an {@link IndexSegment}.
     */
    protected Iterator leafIterator() {

        return new Striterator(getRoot().postOrderNodeIterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    protected boolean isValid(Object arg0) {

                        return arg0 instanceof Leaf;

                    }
                });

    }

    /**
     * Computes and returns the utilization of the tree. The utilization figures
     * do not factor in the space requirements of nodes and leaves.
     * 
     * @return An array whose elements are:
     *         <ul>
     *         <li>0 - the leaf utilization percentage [0:100]. The leaf
     *         utilization is computed as the #of values stored in the tree
     *         divided by the #of values that could be stored in the #of
     *         allocated leaves.</li>
     *         <li>1 - the node utilization percentage [0:100]. The node
     *         utilization is computed as the #of non-root nodes divided by the
     *         #of non-root nodes that could be addressed by the tree.</li>
     *         <li>2 - the total utilization percentage [0:100]. This is the
     *         average of the leaf utilization and the node utilization.</li>
     *         </ul>
     */
    public int[] getUtilization() {

        final int nnodes = getNodeCount();

        final int nleaves = getLeafCount();

        final int nentries = getEntryCount();

        final int numNonRootNodes = nnodes + nleaves - 1;

        final int branchingFactor = getBranchingFactor();

        final int nodeUtilization = nnodes == 0 ? 100 : (100 * numNonRootNodes)
                / (nnodes * branchingFactor);

        final int leafUtilization = (100 * nentries)
                / (nleaves * branchingFactor);

        final int utilization = (nodeUtilization + leafUtilization) / 2;

        return new int[] { nodeUtilization, leafUtilization, utilization };

    }

    /**
     * Recursive dump of the tree.
     * 
     * @param out
     *            The dump is written on this stream.
     * 
     * @return true unless an inconsistency is detected.
     * 
     * @todo modify to write on log vs PrintStream.
     */
    public boolean dump(PrintStream out) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out);

    }

    public boolean dump(Level level, PrintStream out) {

        // True iff we will write out the node structure.
        final boolean info = level.toInt() <= Level.INFO.toInt();

        int[] utils = getUtilization();

        if (info) {

            final int height = getHeight();

            final int nnodes = getNodeCount();

            final int nleaves = getLeafCount();

            final int nentries = getEntryCount();

            final int branchingFactor = getBranchingFactor();

            log.info("height=" + height + ", branchingFactor="
                    + branchingFactor + ", #nodes=" + nnodes + ", #leaves="
                    + nleaves + ", #entries=" + nentries + ", nodeUtil="
                    + utils[0] + "%, leafUtil=" + utils[1] + "%, utilization="
                    + utils[2] + "%");
        }

        if (root != null) {

            return root.dump(level, out, 0, true);

        } else
            return true;

    }

    /**
     * <p>
     * Touch the node or leaf on the {@link #writeRetentionQueue}. If the node
     * is not found on a scan of the head of the queue, then it is appended to
     * the queue and its {@link AbstractNode#referenceCount} is incremented. If
     * the a node is being appended to the queue and the queue is at capacity,
     * then this will cause a reference to be evicted from the queue. If the
     * reference counter for the evicted node or leaf is zero, then the node or
     * leaf will be written onto the store and made immutable. A subsequent
     * attempt to modify the node or leaf will force copy-on-write for that node
     * or leaf.
     * </p>
     * <p>
     * This method guarantees that the specified node will NOT be synchronously
     * persisted as a side effect and thereby made immutable. (Of course, the
     * node may be already immutable.)
     * </p>
     * <p>
     * In conjunction with {@link DefaultEvictionListener}, this method
     * guarentees that the reference counter for the node will reflect the #of
     * times that the node is actually present on the
     * {@link #writeRetentionQueue}.
     * </p>
     * 
     * @param node
     *            The node or leaf.
     * 
     * @todo review the guarentees offered by this method under the assumption
     *       of concurrent readers. See
     *       http://www.ibm.com/developerworks/java/library/j-jtp06197.html for
     *       some related issues. Among them ++ and -- are not atomic unless you
     *       are single-threaded, so this code probably needs to use
     *       synchronized(){} or the reference counts could be wrong. Since the
     *       reference counts only effect when a node is made persistent
     *       (assuming its dirty) and since we already require single threaded
     *       access for writes on the btree, there may not actually be a problem
     *       here. The reference counts could only be wrong for concurrent
     *       readers, and nodes are never dirty and hence will never be made
     *       persistent on eviction so it probably does not matter if the
     *       reference counts are over or under for this case.
     *       <p>
     *       Note: I have since seen an exception where the evicted node
     *       reference was [null]. The problem is that the
     *       {@link HardReferenceQueue} is NOT thread-safe. Concurrent readers
     *       driving {@link HardReferenceQueue#append(Object)} can cause the
     *       data structure to become inconsistent. This is not much of a
     *       problem for readers since the queue is being used to retain hard
     *       references - effectively a cache and the null reference could be
     *       ignored. For writers, we are always single threaded.
     *       <p>
     *       Still, it seems best to either make this method synchronized or to
     *       replace the {@link HardReferenceQueue} with an
     *       {@link ArrayBlockingQueue} since there could well be side-effects
     *       of concurrent appends on the queue with concurrent writers. The
     *       latency for synchronization will be short for readers since
     *       eviction is a NOP while the latency of append can be high for a
     *       single threaded writer since IO may result.
     *       <p>
     *       The use of an {@link ArrayBlockingQueue} opens up some interesting
     *       possibilities since a concurrent thread could now handle
     *       serialization and eviction of nodes while the caller was blocked
     *       and the caller would not need to wait for serialization and IO
     *       unless the queue was full. This really sounds like the same old
     *       concept of a write retention queue (we do not want to evict nodes
     *       too quickly or we will make them persistent while they are still
     *       likely to be touched) and a read retention queue (keeping nodes
     *       around for a while after they have been made persistent) with the
     *       new twist being that serialization and IO could be asynchronous in
     *       the zone of action available between those queues in order to keep
     *       the write reference queue at a minimium capacity).
     */
    synchronized
    final protected void touch(AbstractNode node) {

        assert node != null;

        /*
         * We need to guarentee that touching this node does not cause it to be
         * made persistent. The condition of interest would arise if the queue
         * is full and the referenceCount on the node is zero before this method
         * was called. Under those circumstances, simply appending the node to
         * the queue would cause it to be evicted and made persistent.
         * 
         * We avoid this by incrementing the reference counter before we touch
         * the queue. Since the reference counter will therefore be positive if
         * the node is selected for eviction, eviction will not cause the node
         * to be made persistent.
         */

        assert ndistinctOnWriteRetentionQueue >= 0;

        node.referenceCount++;

        if (!writeRetentionQueue.append(node)) {

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

//        if (useFinger && node instanceof ILeafData) {
//
//            if (finger == null || finger.get() != node) {
//
//                finger = new WeakReference<Leaf>((Leaf) node);
//
//            }
//
//        }

    }

    /**
     * Write a dirty node and its children using a post-order traversal that
     * first writes any dirty leaves and then (recursively) their parent nodes.
     * The parent nodes are guarenteed to be dirty if there is a dirty child so
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
     */
    protected void writeNodeRecursive(AbstractNode node) {

        final long begin = System.currentTimeMillis();
        
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
        final Iterator itr = node.postOrderNodeIterator(true);

        while (itr.hasNext()) {

            final AbstractNode t = (AbstractNode) itr.next();

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

            if (t instanceof Leaf)
                nleaves++;

        }

        {

            final long elapsed = System.currentTimeMillis() - begin;
            
            final int nnodes = ndirty - nleaves;
            
            final String s = "wrote: " + ndirty + " records (#nodes=" + nnodes
                    + ", #leaves=" + nleaves + ") in " + elapsed
                    + "ms : addrRoot=" + node.getIdentity();
            
            if (elapsed > 500/*ms*/) {

                // log at warning level when significant latency results.
                log.warn(s);

            } else {
            
                log.info(s);
                
            }
            
        }
        
    }

    /**
     * Writes the node on the store (non-recursive). The node MUST be dirty. If
     * the node has a parent, then the parent is notified of the persistent
     * identity assigned to the node by the store. This method is NOT recursive
     * and dirty children of a node will NOT be visited.
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree (or the backing store) is read-only.
     * 
     * @return The persistent identity assigned by the store.
     */
    protected long writeNodeOrLeaf(AbstractNode node) {

        assert root != null; // i.e., isOpen().
        assert node != null;
        assert node.btree == this;
        assert node.dirty;
        assert !node.deleted;
        assert !node.isPersistent();
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

        /*
         * Serialize the node or leaf onto a shared buffer.
         */

        if (debug)
            node.assertInvariants();

        final ByteBuffer buf;
        {
            
            final long begin = System.nanoTime();
            
            if (node.isLeaf()) {

                buf = nodeSer.putLeaf((Leaf) node);

                counters.leavesWritten++;

            } else {

                buf = nodeSer.putNode((Node) node);

                counters.nodesWritten++;

            }
            
            counters.serializeTimeNanos += System.nanoTime() - begin;
            
        }
        
        // write the serialized node or leaf onto the store.
        final long addr;
        {

            final long begin = System.nanoTime();
            
            addr = store.write(buf);

            counters.writeTimeNanos += System.nanoTime() - begin;
    
            counters.bytesWritten += store.getByteCount(addr);

        }

        /*
         * The node or leaf now has a persistent identity and is marked as
         * clean. At this point is MUST be treated as being immutable. Any
         * changes directed to this node or leaf MUST trigger copy-on-write.
         */

        node.setIdentity(addr);

        node.setDirty(false);

        if (parent != null) {

            // Set the persistent identity of the child on the parent.
            parent.setChildKey(node);

            // // Remove from the dirty list on the parent.
            // parent.dirtyChildren.remove(node);

        }

        return addr;

    }

    /**
     * Read a node or leaf from the store.
     * <p>
     * Note: Callers SHOULD be synchronized in order to ensures that only one
     * thread will read the desired node or leaf in from the store and attach
     * the reference to the newly read node or leaf as appropriate into the
     * existing data structures (e.g., as the root reference or as a child of a
     * node or leaf already live in memory).
     * 
     * @param addr
     *            The address in the store.
     * 
     * @return The node or leaf.
     */
    protected AbstractNode readNodeOrLeaf(long addr) {

        final ByteBuffer tmp;
        {
            final long begin = System.nanoTime();
            
        // /*
        // * offer the node serializer's buffer to the IRawStore. it will be
        // used
        // * iff it is large enough and the store does not prefer to return a
        // * read-only slice.
        // */
        // ByteBuffer tmp = store.read(addr, nodeSer._buf);

            tmp = store.read(addr);
            
            assert tmp.position() == 0;
            
            assert tmp.limit() == store.getByteCount(addr) : "limit="
                    + tmp.limit() + ", byteCount(addr)="
                    + store.getByteCount(addr)+", addr="+store.toString(addr);

            counters.readTimeNanos += System.nanoTime() - begin;
            
            final int bytesRead = tmp.limit();

            counters.bytesRead += bytesRead;
            
        }

        /* 
         * Extract the node from the buffer.
         */
        final AbstractNode node;
        {

            final long begin = System.nanoTime();
            
            node = (AbstractNode) nodeSer.getNodeOrLeaf(this, addr, tmp);

            counters.deserializeTimeNanos += System.nanoTime() - begin;
            
        }

        // Note: The de-serialization ctor already does this.
//        node.setDirty(false);

        if (node instanceof Leaf) {

            counters.leavesRead++;

        } else {

            counters.nodesRead++;

        }

        // Note: The de-serialization ctor already does this.
//        touch(node);

        return node;

    }

//    /**
//     * Configuration options.
//     * <p>
//     * Note: This class is {@link Serializable} so that an instance of the class
//     * may be passed to a remote data service in the scale-out architecture in
//     * order to provision a btree with the desired options.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class Config implements Serializable {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = 1728456753818367361L;
//
//        public int branchingFactor;
//        public int initialBufferCapacity;
//        public HardReferenceQueue<PO> hardReferenceQueue;
//        public IAddressSerializer addrSer;
//        public IValueSerializer valueSer;
//        public INodeFactory nodeFactory;
//        public RecordCompressor recordCompressor;
//        public boolean useChecksum;
//        public UUID indexUUID;
//        
//        /**
//         * @param branchingFactor
//         *            The branching factor is the #of children in a node or
//         *            values in a leaf and must be an integer greater than or
//         *            equal to three (3). Larger branching factors result in
//         *            trees with fewer levels. However there is a point of
//         *            diminishing returns at which the amount of copying
//         *            performed to move the data around in the nodes and leaves
//         *            exceeds the performance gain from having fewer levels.
//         * @param initialBufferCapacity
//         *            When non-zero, this is the initial buffer capacity used by
//         *            the {@link NodeSerializer}. When zero the initial buffer
//         *            capacity will be estimated based on the branching factor,
//         *            the key serializer, and the value serializer. The initial
//         *            estimate is not critical and the buffer will be resized by
//         *            the {@link NodeSerializer} if necessary.
//         * @param headReferenceQueue
//         *            The hard reference queue.
//         * @param addrSer
//         *            Object that knows how to (de-)serialize the child
//         *            addresses in an {@link INodeData}.
//         * @param valueSer
//         *            Object that knows how to (de-)serialize the values in an
//         *            {@link ILeafData}.
//         * @param nodeFactory
//         *            Object that provides a factory for node and leaf objects.
//         * @param recordCompressor
//         *            Object that knows how to (de-)compress serialized nodes
//         *            and leaves (optional).
//         * @param useChecksum
//         *            When true, computes and verifies checksum of serialized
//         *            nodes and leaves. This option is not recommended for use
//         *            with a fully buffered store, such as a {@link Journal},
//         *            since all reads are against memory which is presumably
//         *            already parity checked.
//         * @param indexUUID
//         *            The unique identifier for the index whose data is stored
//         *            in this B+Tree data structure. When using a scale-out
//         *            index the same <i>indexUUID</i> MUST be assigned to each
//         *            mutable and immutable B+Tree having data for any partition
//         *            of that scale-out index. This makes it possible to work
//         *            backwards from the B+Tree data structures and identify the
//         *            index to which they belong.
//         */
//        public Config(int branchingFactor,
//                int initialBufferCapacity,
//                HardReferenceQueue<PO> hardReferenceQueue,
//                IAddressSerializer addrSer, IValueSerializer valueSer,
//                INodeFactory nodeFactory, RecordCompressor recordCompressor,
//                boolean useChecksum, UUID indexUUID) {
//        }
//
//    }

//    /**
//     * When <code>true</code> {@link Node}s will hold onto their parents and
//     * children using {@link SoftReference}s. When <code>false</code> they
//     * will use {@link WeakReference}s.
//     */
//    private final static boolean softReferences = false;
    
    /**
     * Create the reference that will be used by a {@link Node} to refer to its
     * children (nodes or leaves).
     * 
     * @param child
     *            A node.
     *            
     * @return A reference to that node.
     * 
     * @see SoftReference
     * @see WeakReference
     */
    final Reference<AbstractNode> newRef(AbstractNode child) {
        
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
         * 
         * @todo examine heap usage further. it appears that we are reading the
         * disk too much primarily because we are not holding onto "interesting"
         * nodes and leaves long enough. the readRetentionQueue should help
         * here. Maybe all references should be weak and the read retention
         * queue should have a capacity of 10000 or more node references? 
         */

            return new WeakReference<AbstractNode>( child );
//        return new SoftReference<AbstractNode>( child ); // causes significant GC "hesitations".

    }
    
    /**
     * Create the reference that will be used by a node to refer to its parent.
     * 
     * @param parent A node.
     * 
     * @return A reference to that node.
     */
    final Reference<Node> newRef(Node parent) {
        
        /*
         * Note: A weak reference by a child to its parent means that the
         * existence of the child object does not incline the garbage collector
         * to keep the parent.
         */

        return new WeakReference<Node>( parent );
//      return new SoftReference<Node>( parent ); // retains too much.

    }
    
}
