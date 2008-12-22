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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.AbstractBTreeTupleCursor.MutableBTreeTupleCursor;
import com.bigdata.btree.Leaf.ILeafListener;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * This class implements a variant of a B+Tree in which all values are stored in
 * leaves, but the leaves are not connected with prior-next links. This
 * constraint arises from the requirement to support a copy-on-write policy.
 * </p>
 * <p>
 * Note: No mechanism is exposed for fetching a node or leaf of the tree other
 * than the root by its key. This is because the parent reference on the node
 * (or leaf) can only be set when it is read from the store in context by its
 * parent node.
 * </p>
 * <p>
 * Note: the leaves can not be stitched together with prior and next references
 * without forming cycles that make it impossible to write out the leaves of the
 * btree. This restriction arises because each time we write out a node or leaf
 * it is assigned a persistent identifier as an unavoidable artifact of
 * providing isolation for the object index.
 * </p>
 * <p>
 * Note: This implementation is thread-safe for concurent readers BUT NOT for
 * concurrent writers. If a writer has access to a {@link BTree} then there MUST
 * NOT be any other reader -or- writer operating on the {@link BTree} at the
 * same time.
 * </p>
 * 
 * @todo consider also tracking the #of deleted entries in a key range (parallel
 *       to how we track the #of entries in a key range) so that we can report
 *       the exact #of non-deleted entries when the btree supports isolation
 *       (right now you have to do a key range scan and count the #of
 *       non-deleted entries).
 * 
 * @todo Choose the split point in the branch nodes within a parameterized
 *       region such that we choose the shortest separator keys to promote to
 *       the parent. This is a bit more tricky for the
 *       {@link IndexSegmentBuilder}.
 * 
 * @todo reuse a buffer when copying keys and values out of the index, e.g., for
 *       the {@link ITupleIterator}, in order to minimize heap churn.
 * 
 * @todo The B+Tree implementation does not support limits on the serialized
 *       size of a node or leaf. The design strategy is to allow flexible sizes
 *       for serialized node and leaf records since larger branching factors and
 *       continuous IO are cheaper and nothing in particular requires the use of
 *       fixed size records when accessing disk. However, it would still be
 *       useful to be able to place an upper bound on the serialized size of a
 *       node or leaf. Nodes and leaves are only serialized on eviction, so this
 *       would require the ability to split nodes and/or leaves immediately
 *       before eviction if they would be likely to overflow the upper bound on
 *       the record size. Probably the node or leaf needs to be made immutable,
 *       the serialized size checked, and then a split operation invoked if the
 *       record would exceed the upper bound. If this happens a lot, then the
 *       branching factor is too high for the data and would have to be lowered
 *       to regain performance.
 *       <p>
 *       Another use case for limits on the size of a node/leaf is maintaining
 *       the data in a serialized form. This can have several benefits,
 *       including reducing heap churn by copying keys and values (not their
 *       references) into the record structure, and reducing (eliminating)
 *       deserialization costs (which are substantially more expensive than
 *       serialization). This can be approached using new implementations of the
 *       key buffer and a value buffer implementation (one that can be used for
 *       nodes or for leaves). If an object implements both then it is a small
 *       additional step towards using it for the entire serialized record
 *       (minus some header/tailer).
 *       <p>
 *       Adaptive packed memory arrays can be used in this context to minimize
 *       the amount of copying that needs to be performed on mutation of a node
 *       or leaf.
 *       <p>
 *       Leading key compression is intrinsically part of how the key buffer
 *       maintains its representation so, for example, a micro-index would have
 *       to be maintained as part of the key buffer.
 * 
 * @todo create ring buffers to track the serialized size of the last 50 nodes
 *       and leaves so that we can estimate the serialized size of the total
 *       btree based on recent activity. we could use a moving average and
 *       persist it as part of the btree metadata. this could be used when
 *       making a decision to evict a btree vs migrate it onto a new journal and
 *       whether to split or join index segments during a journal overflow
 *       event.
 * 
 * @todo we could defer splits by redistributing keys to left/right siblings
 *       that are under capacity - this makes the tree a b*-tree. however, this
 *       is not critical since the journal is designed to be fully buffered and
 *       the index segments are read-only but it would reduce memory by reducing
 *       the #of nodes -- and we can expect that the siblings will be either
 *       resident or in the direct buffer for the journal
 * 
 * @todo evict subranges by touching the node on the way up so that a node that
 *       is evicted from the hard reference cache will span a subrange that can
 *       be evicted together. this will help to preserve locality on disk. with
 *       this approach leaves might be evicted independently, but also as part
 *       of a node subrange eviction.
 * 
 * @todo since forward scans are much more common, change the post-order
 *       iterator for commit processing to use a reverse traversal so that we
 *       can write the next leaf field whenever we evict a sequence of leaves as
 *       part of a sub-range commit (probably not much of an issue since the
 *       journal is normally fully buffered and the perfect index segments will
 *       not have this problem).
 * 
 * @todo pre-fetch leaves for range scans? if we define a parallel iterator
 *       flag?
 * 
 * @todo Note that efficient support for large branching factors requires a more
 *       sophisticated approach to maintaining the key order within a node or
 *       leaf. E.g., using a red-black tree or adaptive packed memory array.
 *       However, we can use smaller branching factors for btrees in the journal
 *       and use a separate implementation for bulk generating and reading
 *       "perfect" read-only key range segments.
 * 
 * @todo derive a string index that uses patricia trees in the leaves per
 *       several published papers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTree extends AbstractBTree implements IIndex, ICommitter,
        ILocalBTreeView {
    
    final public int getHeight() {
        
        return height;
        
    }

    final public int getNodeCount() {
        
        return nnodes;
        
    }

    final public int getLeafCount() {
        
        return nleaves;
        
    }

    final public int getEntryCount() {
        
        return nentries;
        
    }

    /**
     * The backing store.
     */
    final public IRawStore getStore() {

        return store;

    }
    
    /**
     * Returns a mutable counter. All {@link ICounter}s returned by this method
     * report and increment the same underlying counter.
     * <p>
     * Note: When the {@link BTree} is part of a scale-out index then the
     * counter will assign values within a namespace defined by the partition
     * identifier.
     */
    public ICounter getCounter() {

        ICounter counter = new Counter(this);
        
        final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();

        if (pmd != null) {

            counter = new PartitionedCounter(pmd.getPartitionId(), counter);

        }

        if (isReadOnly()) {

            return new ReadOnlyCounter(counter);

        }

        return counter;

    }
    
    /**
     * The constructor sets this field initially based on a {@link Checkpoint}
     * record containing the only address of the {@link IndexMetadata} for the
     * index. Thereafter this reference is maintained as the {@link Checkpoint}
     * record last written by {@link #writeCheckpoint()} or read by
     * {@link #load(IRawStore, long)}.
     */
    private Checkpoint checkpoint = null;
    
//    /**
//     * The root of the btree. This is initially a leaf until the leaf is split,
//     * at which point it is replaced by a node. The root is also replaced each
//     * time copy-on-write triggers a cascade of updates.
//     */
//    protected AbstractNode root;

    /**
     * The height of the btree. The height is the #of leaves minus one. A btree
     * with only a root leaf is said to have <code>height := 0</code>. Note
     * that the height only changes when we split the root node.
     */
    protected int height;

    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     */
    protected int nnodes;

    /**
     * The #of leaf nodes in the btree. This is one (1) for a new btree.
     */
    protected int nleaves;

    /**
     * The #of entries in the btree. This is zero (0) for a new btree.
     */
    protected int nentries;

    /**
     * The mutable counter exposed by #getCounter()}.
     * <p>
     * Note: This is <code>protected</code> so that it will be visible to
     * {@link Checkpoint} which needs to write the actual value store in this
     * counter into its serialized record (without the partition identifier).
     */
    protected AtomicLong counter;
  
//    /**
//     * The last address from which the {@link IndexMetadata} record was read or
//     * on which it was written.
//     */
//    private long lastMetadataAddr;

    final protected int getReadRetentionQueueCapacity() {
        
        return metadata.getBTreeReadRetentionQueueCapacity();
        
    }
    
    final protected int getReadRetentionQueueScan() {
        
        return metadata.getBTreeReadRetentionQueueScan();
        
    }
    
    /**
     * Required constructor form for {@link BTree} and any derived subclasses.
     * This ctor is used both to create a new {@link BTree}, and to load a
     * {@link BTree} from the store using a {@link Checkpoint} record.
     * 
     * @param store
     *            The store.
     * @param checkpoint
     *            A {@link Checkpoint} record for that {@link BTree}.
     * @param metadata
     *            The metadata record for that {@link BTree}.
     * 
     * @see BTree#create(IRawStore, IndexMetadata)
     * @see BTree#load(IRawStore, long, boolean)
     */
    public BTree(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata) {

        super(  store, 
                NodeFactory.INSTANCE, //
                /*
                 * Note: A BTree is not known to be read-only during its ctor.
                 * It might be marked as read-only afterwards, but we can't be
                 * sure at this point.
                 */
                false, // read-only
                metadata
                );
        
        if (checkpoint == null) {

            throw new IllegalArgumentException();
            
        }

        if (store != null) {

            if (checkpoint.getMetadataAddr() != metadata.getMetadataAddr()) {

                // must agree.
                throw new IllegalArgumentException();

            }

        }

        if (metadata.getConflictResolver() != null && !metadata.isIsolatable()) {

            throw new IllegalArgumentException(
                    "Conflict resolver may only be used with isolatable indices");
            
        }

        setCheckpoint(checkpoint);
        
//        // save the address from which the index metadata record was read.
//        this.lastMetadataAddr = metadata.getMetadataAddr();
        
        /*
         * Note: the mutable BTree has a limit here so that split() will always
         * succeed. That limit does not apply for an immutable btree.
         */
        assert writeRetentionQueue.capacity() >= IndexMetadata.Options.MIN_WRITE_RETENTION_QUEUE_CAPACITY;

        /*
         * Note: Re-open is deferred so that we can mark the BTree as read-only
         * before we read the root node.
         */
//        reopen();
        
    }
    
    /**
     * Sets the {@link #checkpoint} and initializes the mutable fields from the
     * checkpoint record. In order for this operation to be atomic, the caller
     * must be synchronized on the {@link BTree} or otherwise guarenteed to have
     * exclusive access, e.g., during the ctor or when the {@link BTree} is
     * mutable and access is therefore required to be single-threaded.
     */
    private void setCheckpoint(final Checkpoint checkpoint) {

        this.checkpoint = checkpoint; // save reference.
        
        this.height = checkpoint.getHeight();
        
        this.nnodes = checkpoint.getNodeCount();
        
        this.nleaves = checkpoint.getLeafCount();
        
        this.nentries = checkpoint.getEntryCount();
        
        this.counter = new AtomicLong( checkpoint.getCounter() );

    }

    /**
     * Creates and sets new root {@link Leaf} on the B+Tree and (re)sets the
     * various counters to be consistent with that root.  This is used both
     * by the constructor for a new {@link BTree} and by {@link #removeAll()}.
     * <p>
     * Note: The {@link #getCounter()} is NOT changed by this method.
     */
    final private void newRootLeaf() {

        height = 0;

        nnodes = 0;
        
        nentries = 0;

        final boolean wasDirty = root != null && root.dirty;
        
        root = new Leaf(this);
        
        nleaves = 1;
        
        // Note: Counter is unchanged!
//        counter = new AtomicLong( 0L );

        if (metadata.getBloomFilterFactory() != null) {

            /*
             * Note: Allocate a new bloom filter since the btree is now empty
             * and set its reference on the BTree. We need to do this here in
             * order to (a) overwrite the old reference when we are replacing
             * the root, e.g., for removeAll(); and (b) to make sure that the
             * bloomFilter reference is defined since the checkpoint will not
             * have its address until the next time we call writeCheckpoint()
             * and therefore readBloomFilter() would fail if we did not create
             * and assign the bloom filter here.
             */
            
            bloomFilter = metadata.getBloomFilterFactory().newBloomFilter();
            
        }
        
        /*
         * Note: a new root leaf is created when an empty btree is (re-)opened.
         * However, if the BTree is marked as read-only then do not fire off a
         * dirty event.
         */
        if(!wasDirty && !readOnly) {
            
            fireDirtyEvent();
            
        }
        
    }

    @Override
    protected void _reopen() {
        
        if (checkpoint.getRootAddr() == 0L) {

            /*
             * Create the root leaf.
             * 
             * Note: if there is an optional bloom filter, then it is created
             * now.
             */
            
            newRootLeaf();
            
        } else {
            
            /*
             * Read the root node of the btree.
             */
            root = readNodeOrLeaf(checkpoint.getRootAddr());

            // Note: The optional bloom filter will be read lazily. 
            
        }

    }

    /**
     * Lazily reads the bloom filter from the backing store if it exists and is
     * not already in memory.
     */
    @Override
    final public BloomFilter getBloomFilter() {

        // make sure the index is open.
        reopen();
        
        if (bloomFilter == null) {

            if (checkpoint.getBloomFilterAddr() == 0L) {

                // No bloom filter.
                
                return null;
                
            }

            synchronized(this) {
                
                if (bloomFilter == null) {

                    bloomFilter = readBloomFilter();
                    
                }
                
            }
            
        }

        if (bloomFilter != null && !bloomFilter.isEnabled()) {
            
            /*
             * Do NOT return the reference if the bloom filter has been
             * disabled. This simplifies things for the caller. The only code
             * that needs the reference regardless of whether it is enabled is
             * writeCheckpoint() and Checkpoint and those bits use the reference
             * directly.
             */
            
            return null;
            
        }
        
        // Note: will be null if there is no bloom filter!
        return bloomFilter;
        
    }

    /**
     * Read the bloom filter from the backing store using the address stored in
     * the last {@link #checkpoint} record. This method will be invoked by
     * {@link #getBloomFilter()} when the bloom filter reference is
     * <code>null</code> but the bloom filter is known to exist and the bloom
     * filter object is requested.
     * <p>
     * Note: A bloom filter can be relatively large. The bit length of a bloom
     * filter is approximately one byte per index entry, so a filter for an
     * index with 10M index entries will be on the order of 10mb. Therefore this
     * method will typically have high latency.
     * <p>
     * Note: the {@link Checkpoint} record initially stores <code>0L</code>
     * for the bloom filter address. {@link #newRootLeaf()} is responsible for
     * allocating the bloom filter (if one is to be used) when the root leaf is
     * (re-)created. The address then gets stored in the {@link Checkpoint}
     * record by {@link #writeCheckpoint()} (if invoked and once the bloom
     * filter is dirty).
     * 
     * @throws IllegalStateException
     *             if the bloom filter does not exist (the caller should check
     *             this first to avoid obtaining a lock).
     */
    final protected BloomFilter readBloomFilter() {
        
        final long bloomFilterAddr = checkpoint.getBloomFilterAddr();
        
        if (bloomFilterAddr == 0L) {
         
            // No bloom filter.
            throw new IllegalStateException();
            
        }
            
        return BloomFilter.read(store, bloomFilterAddr);
            
    }
    
    final public boolean isReadOnly() {
     
        return readOnly;
        
    }
    
    /**
     * Mark the B+Tree as read-only. Once the B+Tree is marked as read-only,
     * that instance will remain read-only.
     * 
     * @param readOnly
     *            <code>true</code> if you want to mark the B+Tree as
     *            read-only.
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree is already read-only and you pass
     *             <code>false</code>.
     */
    final public void setReadOnly(final boolean readOnly) {

        if (this.readOnly && !readOnly) {

            throw new UnsupportedOperationException(ERROR_READ_ONLY);
            
        }
        
        this.readOnly = readOnly;
        
    }
    transient private boolean readOnly = false;
    
    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }
    
    /**
     * Sets the lastCommitTime.
     * <p>
     * Note: The lastCommitTime is set by a combination of the
     * {@link AbstractJournal} and {@link Name2Addr} based on the actual
     * commitTime of the commit during which an {@link Entry} for that index was
     * last committed. It is set for both historical index reads and unisolated
     * index reads using {@link Entry#commitTime}. The lastCommitTime for an
     * unisolated index will advance as commits are performed with that index.
     * 
     * @param lastCommitTime
     *            The timestamp of the last committed state of this index.
     * 
     * @throws IllegalArgumentException
     *             if lastCommitTime is ZERO (0).
     * @throws IllegalStateException
     *             if the timestamp is less than the previous value (it is
     *             permitted to advance but not to go backwards).
     */
    final public void setLastCommitTime(long lastCommitTime) {
        
        if (lastCommitTime == 0L)
            throw new IllegalArgumentException();
        
        if (this.lastCommitTime == lastCommitTime) {

            // No change.
            
            return;
            
        }
        
        if (INFO)
            log.info("old=" + this.lastCommitTime + ", new=" + lastCommitTime);
        
        if (this.lastCommitTime != 0L && this.lastCommitTime > lastCommitTime) {

            throw new IllegalStateException("Updated lastCommitTime: old="
                    + this.lastCommitTime + ", new=" + lastCommitTime);
            
        }

        this.lastCommitTime = lastCommitTime;
        
    }
    private long lastCommitTime = 0L;// Until the first commit.
    
    /**
     * Return the {@link IDirtyListener}.
     */
    final public IDirtyListener getDirtyListener() {
        
        return listener;
        
    }

    /**
     * Set or clear the listener (there can be only one).
     * 
     * @param listener The listener.
     */
    final public void setDirtyListener(IDirtyListener listener) {

        assertNotReadOnly();
        
        this.listener = listener;
        
    }
    
    private IDirtyListener listener;

    /**
     * Fire an event to the listener (iff set).
     */
    final protected void fireDirtyEvent() {

        assertNotReadOnly();

        final IDirtyListener l = this.listener;

        if (l == null)
            return;

        if (Thread.interrupted()) {

            throw new RuntimeException(new InterruptedException());

        }

        l.dirtyEvent(this);
        
        if (INFO)
            log.info("");
        
    }
    
    /**
     * Flush the nodes of the {@link BTree} to the backing store. After invoking
     * this method the root of the {@link BTree} will be clean.
     * <p>
     * Note: This does NOT flush all persistent state. See
     * {@link #writeCheckpoint()} which also handles the optional bloom filter,
     * the {@link IndexMetadata}, and the {@link Checkpoint} record itself.
     * 
     * @return <code>true</code> if anything was written.
     */
    final public boolean flush() {

        assertNotTransient();
        assertNotReadOnly();
        
        if (root != null && root.dirty) {

            writeNodeRecursive( root );
            
            if(INFO)
                log.info("flushed root: addr=" + root.identity);
            
            return true;
            
        }
        
        return false;

    }
    
    /**
     * Checkpoint operation {@link #flush()}es dirty nodes, the optional
     * {@link IBloomFilter} (if dirty), the {@link IndexMetadata} (if dirty),
     * and then writes a new {@link Checkpoint} record on the backing store,
     * saves a reference to the current {@link Checkpoint} and returns the
     * address of that {@link Checkpoint} record.
     * <p>
     * Note: A checkpoint by itself is NOT an atomic commit. The commit protocol
     * is at the store level and uses {@link Checkpoint}s to ensure that the
     * state of the {@link BTree} is current on the backing store.
     * 
     * @return The address at which the {@link Checkpoint} record for the
     *         {@link BTree} was written onto the store. The {@link BTree} can
     *         be reloaded from this {@link Checkpoint} record.
     * 
     * @see #load(IRawStore, long)
     * 
     * @todo this could be modified to return the {@link Checkpoint} object but
     *       I have not yet seen a situation where that was more interesting
     *       than the address of the written {@link Checkpoint} record.
     */
    final public long writeCheckpoint() {
        
        assertNotTransient();
        assertNotReadOnly();
        
        if (INFO)
            log.info("begin");
        
//        assert root != null : "root is null"; // i.e., isOpen().

        // flush any dirty nodes.
        flush();
        
        // pre-condition: all nodes in the tree are clean.
        assert root == null || !root.dirty;

        {
            /*
             * Note: Use the [AbstractBtree#bloomFilter] reference here!!!
             * 
             * If that reference is [null] then the bloom filter is either
             * clean, disabled, or was not configured. For any of those (3)
             * conditions we will use the address of the bloom filter from the
             * last checkpoint record. If the bloom filter is clean, then we
             * will just carry forward its old address. Otherwise the address in
             * the last checkpoint record will be 0L and that will be carried
             * forward.
             */
            final BloomFilter filter = this.bloomFilter;

            if (filter != null && filter.isDirty() && filter.isEnabled()) {

                /*
                 * The bloom filter is enabled, is loaded and is dirty, so write
                 * it on the store now.
                 */

                filter.write(store);

                if (INFO)
                    log.info("wrote updated bloom filter record.");

            }
            
        }
        
        if (metadata.getMetadataAddr() == 0L) {
            
            /*
             * The index metadata has been modified so we write out a new
             * metadata record on the store.
             */
            
            metadata.write(store);
            
            if(INFO)
                log.info("wrote updated metadata record");
            
        }
        
        // create new checkpoint record.
        checkpoint = metadata.newCheckpoint(this);
        
        // write it on the store.
        checkpoint.write(store);
        
        if (INFO) {

            // Note: this is the scale-out index name for a partitioned index.
            final String name = metadata.getName();

            log.info((name == null ? "" : "name=" + name + ", ")
                        + "wroteCheckpoint=" + checkpoint);

        }
        
        // return address of that checkpoint record.
        return checkpoint.getCheckpointAddr();
        
    }
    
    /**
     * Returns the most recent {@link Checkpoint} record for this {@link BTree}.
     * 
     * @return The most recent {@link Checkpoint} record for this {@link BTree}
     *         and never <code>null</code>.
     */
    final public Checkpoint getCheckpoint() {

        assert checkpoint != null;
        
        return checkpoint;
        
    }
    
//    /**
//     * Return true iff the state of this B+Tree has been modified since the last
//     * {@link Checkpoint} record associated with the given address.
//     * 
//     * @param checkpointAddr
//     *            The historical metadata address for this B+Tree.
//     * 
//     * @return true iff the B+Tree has been modified since the identified
//     *         historical version.
//     * 
//     * @see #getCheckpoint()
//     * @see Checkpoint#getMetadataAddr()
//     * 
//     * @todo this test might not work with overflow() handling since the
//     *       {@link Checkpoint} addresses would be independent in each store
//     *       file.
//     */
//    public boolean modifiedSince(long checkpointAddr) {
//        
//        return needsCheckpoint() || checkpoint.getCheckpointAddr() != checkpointAddr;
//        
//    }

    /**
     * Return true iff changes would be lost unless the B+Tree is flushed to the
     * backing store using {@link #writeCheckpoint()}.
     * <p>
     * Note: In order to avoid needless checkpoints this method will return
     * <code>false</code> if:
     * <ul>
     * <li> the metadata record is persistent -AND-
     * <ul>
     * <li> EITHER the root is <code>null</code>, indicating that the index
     * is closed (in which case there are no buffered writes);</li>
     * <li> OR the root of the btree is NOT dirty, the persistent address of the
     * root of the btree agrees with {@link Checkpoint#getRootAddr()}, and the
     * {@link #counter} value agrees {@link Checkpoint#getCounter()}.</li>
     * </ul>
     * </li>
     * </ul>
     * 
     * @return <code>true</code> true iff changes would be lost unless the
     *         B+Tree was flushed to the backing store using
     *         {@link #writeCheckpoint()}.
     */
    public boolean needsCheckpoint() {

        if(checkpoint.addrCheckpoint == 0L) {
            
            /*
             * The checkpoint record needs to be written.
             */
            
            return true;
            
        }
        
        if(metadata.getMetadataAddr() == 0L) {
            
            /*
             * The index metadata record was replaced and has not yet been
             * written onto the store.
             */
            
            return true;
            
        }
        
        if(metadata.getMetadataAddr() != checkpoint.getMetadataAddr()) {
            
            /*
             * The index metadata record was replaced and has been written on
             * the store but the checkpoint record does not reflect the new
             * address yet.
             */
            
            return true;
            
        }
        
        if(checkpoint.getCounter() != counter.get()) {
            
            // The counter has been modified.
            
            return true;
            
        }
        
        if(root != null ) {
            
            if (root.isDirty()) {

                // The root node is dirty.

                return true;

            }
            
            if(checkpoint.getRootAddr() != root.identity) {
        
                // The root node has a different persistent identity.
                
                return true;
                
            }
            
        }

        /*
         * No apparent change in persistent state so we do NOT need to do a
         * checkpoint.
         */
        
        return false;
        
//        if (metadata.getMetadataAddr() != 0L && //
//                (root == null || //
//                        ( !root.dirty //
//                        && checkpoint.getRootAddr() == root.identity //
//                        && checkpoint.getCounter() == counter.get())
//                )
//        ) {
//            
//            return false;
//            
//        }
//
//        return true;
     
    }
    
    /**
     * Method updates the index metadata associated with this {@link BTree}.
     * The new metadata record will be written out as part of the next index
     * {@link #writeCheckpoint()}.
     * <p>
     * Note: this method should be used with caution.
     * 
     * @param indexMetadata
     *            The new metadata description for the {@link BTree}.
     * 
     * @throws IllegalArgumentException
     *             if the new value is <code>null</code>
     * @throws IllegalArgumentException
     *             if the new {@link IndexMetadata} record has already been
     *             written on the store - see
     *             {@link IndexMetadata#getMetadataAddr()}
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    final public void setIndexMetadata(final IndexMetadata indexMetadata) {
        
        assertNotReadOnly();

        if (indexMetadata == null)
            throw new IllegalArgumentException();

        if (indexMetadata.getMetadataAddr() != 0)
            throw new IllegalArgumentException();

        this.metadata = indexMetadata;
        
        // gets us on the commit list for Name2Addr.
        fireDirtyEvent();
        
    }
    
    /**
     * Handle request for a commit by {@link #writeCheckpoint()}ing dirty nodes
     * and leaves onto the store, writing a new metadata record, and returning
     * the address of that metadata record.
     * <p>
     * Note: In order to avoid needless writes the existing metadata record is
     * always returned if {@link #needsCheckpoint()} is <code>false</code>.
     * <p>
     * Note: The address of the existing {@link Checkpoint} record is always
     * returned if {@link #getAutoCommit() autoCommit} is disabled.
     * 
     * @return The address of a {@link Checkpoint} record from which the btree
     *         may be reloaded.
     */
    public long handleCommit(final long commitTime) {

        assertNotTransient();
        assertNotReadOnly();

        if (/*autoCommit &&*/ needsCheckpoint()) {

            /*
             * Flush the btree, write a checkpoint record, and return the
             * address of that checkpoint record. The [checkpoint] reference is
             * also updated.
             */

            return writeCheckpoint();

        }

        /*
         * There have not been any writes on this btree or auto-commit is
         * disabled.
         * 
         * Note: if the application has explicitly invoked writeCheckpoint()
         * then the returned address will be the address of that checkpoint
         * record and the BTree will have a new checkpoint address made restart
         * safe on the backing store.
         */

        return checkpoint.addrCheckpoint;
        
    }
    
    /**
     * Remove all entries in the B+Tree.
     * <p>
     * When delete markers are not enabled this simply replaces the root with a
     * new root leaf and resets the counters (height, #of nodes, #of leaves,
     * etc). to be consistent with that new root. If the btree is then made
     * restart-safe by the commit protocol of the backing store then the effect
     * is as if all entries had been deleted. Old nodes and leaves will be swept
     * from the store eventually when the journal overflows.
     * <p>
     * When delete markers are enabled all un-deleted entries in the index are
     * overwritten with a delete marker.
     * <p>
     * Note: The {@link IIndexManager} defines methods for registering (adding)
     * and dropping indices vs removing the entries in an individual
     * {@link BTree}.
     */
    final public void removeAll() {

        assertNotReadOnly();

        if (getIndexMetadata().getDeleteMarkers()) {
            
            /*
             * Write deletion markers for each non-deleted entry. When the
             * transaction commits, those delete markers will have to validate
             * against the global state of the tree. If the transaction
             * validates, then the merge down onto the global state will cause
             * the corresponding entries to be removed from the global tree.
             * 
             * Note: This operation can change the tree structure by triggering
             * copy-on-write for immutable node or leaves.
             */

            final ITupleIterator itr = rangeIterator(null, null,
                    0/* capacity */, REMOVEALL/* flags */, null/* filter */);

            while (itr.hasNext()) {

                itr.next();

            }
            
        } else {

            /*
             * Clear the hard reference cache (sets the head, tail and count to
             * zero).
             * 
             * Note: This is important since the cache will typically contain
             * dirty nodes and leaves. If those get evicted then an exception
             * will be thrown since their parents are not grounded on the new
             * root leaf.
             * 
             * Note: By clearing the references to null we also facilitate
             * garbage collection of the nodes and leaves in the cache.
             */

            writeRetentionQueue.clear(true/* clearRefs */);

            ndistinctOnWriteRetentionQueue = 0;

            if (readRetentionQueue != null) {

                readRetentionQueue.clear(true/* clearRefs */);

            }

            /*
             * Replace the root with a new root leaf.
             */

            newRootLeaf();

        }
        
    }

    /**
     * Create a new {@link BTree} or derived class. This method works by writing
     * the {@link IndexMetadata} record on the store and then loading the
     * {@link BTree} from the {@link IndexMetadata} record.
     * 
     * @param store
     *            The store.
     * 
     * @param metadata
     *            The metadata record.
     * 
     * @return The newly created {@link BTree}.
     * 
     * @see #load(IRawStore, long)
     * 
     * @exception IllegalStateException
     *                If you attempt to create two btree objects from the same
     *                metadata record since the metadata address will have
     *                already been noted on the {@link IndexMetadata} object.
     *                You can use {@link IndexMetadata#clone()} to obtain a new
     *                copy of the metadata object with the metadata address set
     *                to <code>0L</code>.
     */
    public static BTree create(final IRawStore store, final IndexMetadata metadata) {
        
        if (metadata.getMetadataAddr() != 0L) {

            throw new IllegalStateException("Metadata record already in use");
            
        }
        
        /*
         * Write metadata record on store. The address of that record is set as
         * a side-effect on the metadata object.
         */
        metadata.write(store);

        /*
         * Create checkpoint for the new B+Tree.
         */
        final Checkpoint firstCheckpoint = metadata.firstCheckpoint();
        
        /*
         * Write the checkpoint record on the store. The address of the
         * checkpoint record is set on the object as a side effect.
         */
        firstCheckpoint.write(store);
        
        /*
         * Load the B+Tree from the store using that checkpoint record. There is
         * no root so a new root leaf will be created when the B+Tree is opened.
         */
        return load(store, firstCheckpoint.getCheckpointAddr());
        
    }

    /**
     * Create a new {@link BTree} or derived class that is fully transient (NO
     * backing {@link IRawStore}).
     * <p>
     * Fully transient {@link BTree}s provide the functionality of a B+Tree
     * without a backing persistence store. Internally, reachable nodes and
     * leaves of the transient {@link BTree} use hard references to ensure that
     * remain strongly reachable. Deleted nodes and leaves simply clear their
     * references and will be swept by the garbage collector shortly thereafter.
     * <p>
     * Operations which attempt to write on the backing store will fail.
     * <p>
     * While nodes and leaves are never persisted, the keys and values of the
     * transient {@link BTree} are unsigned byte[]s. This means that application
     * keys and values are always converted into unsigned byte[]s before being
     * stored in the {@link BTree}. Hence if an object that is inserted into
     * the {@link BTree} and then looked up using the {@link BTree} API, you
     * WILL NOT get back the same object reference.
     * <p>
     * Note: CLOSING A TRANSIENT INDEX WILL DISCARD ALL DATA!
     * 
     * @param metadata
     *            The metadata record.
     * 
     * @return The transient {@link BTree}.
     */
    public static BTree createTransient(final IndexMetadata metadata) {
        
        /*
         * Create checkpoint for the new B+Tree.
         */
        final Checkpoint firstCheckpoint = metadata.firstCheckpoint();

        /*
         * Create B+Tree object instance.
         */
        try {

            final Class cl = Class.forName(metadata.getBTreeClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class //
                    });

            final BTree btree = (BTree) ctor.newInstance(new Object[] { //
                    null , // store
                    firstCheckpoint, //
                    metadata //
                    });

            // create the root node.
            btree.reopen();

            return btree;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
    }
    
    /**
     * Load an instance of a {@link BTree} or derived class from the store. The
     * {@link BTree} or derived class MUST declare a constructor with the
     * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, BTreeMetadata metadata)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * 
     * @param addrCheckpoint
     *            The address of a {@link Checkpoint} record for the index.
     * 
     * @return The {@link BTree} or derived class loaded from that
     *         {@link Checkpoint} record.
     */
    public static BTree load(final IRawStore store, final long addrCheckpoint) {

        return load(store, addrCheckpoint, false/* readOnly */);
        
    }
    
    /**
     * Load an instance of a {@link BTree} or derived class from the store. The
     * {@link BTree} or derived class MUST declare a constructor with the
     * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, BTreeMetadata metadata)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * 
     * @param addrCheckpoint
     *            The address of a {@link Checkpoint} record for the index.
     * @param readOnly
     *            When <code>true</code> the {@link BTree} will be marked as
     *            read-only. Marking the {@link BTree} as read-only here rather
     *            than with {@link BTree#setReadOnly(boolean)} has some
     *            advantages relating to the locking scheme used by
     *            {@link Node#getChild(int)} since the root node is known to be
     *            read-only at the time that it is allocated as per-child
     *            locking is therefore in place for all nodes in the read-only
     *            {@link BTree}.
     * 
     * @return The {@link BTree} or derived class loaded from that
     *         {@link Checkpoint} record.
     */
    public static BTree load(final IRawStore store, final long addrCheckpoint,
            final boolean readOnly) {

        /*
         * Read checkpoint record from store.
         */
        final Checkpoint checkpoint = Checkpoint.load(store, addrCheckpoint);

        /*
         * Read metadata record from store.
         */
        final IndexMetadata metadata = IndexMetadata.read(store, checkpoint
                .getMetadataAddr());

        if (INFO) {

            // Note: this is the scale-out index name for a partitioned index.
            final String name = metadata.getName();

            log.info((name == null ? "" : "name=" + name + ", ")
                    + "readCheckpoint=" + checkpoint);

        }

        /*
         * Create B+Tree object instance.
         */
        try {

            final Class cl = Class.forName(metadata.getBTreeClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class //
                    });

            final BTree btree = (BTree) ctor.newInstance(new Object[] { //
                    store,//
                            checkpoint, //
                            metadata //
                    });

            if (readOnly) {

                btree.setReadOnly(true);

            }

            // read the root node.
            btree.reopen();

            return btree;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Factory for mutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class NodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new NodeFactory();

        private NodeFactory() {
        }

        public ILeafData allocLeaf(IIndex btree, long addr,
                int branchingFactor, IKeyBuffer keys, byte[][] values,
                long[] versionTimestamp, boolean[] deleteMarkers,
                long priorAddr, long nextAddr) {

            Leaf leaf = new Leaf((BTree) btree, addr, branchingFactor, keys, values,
                    versionTimestamp, deleteMarkers);
            
            /*
             * Note: The prior/next leaf addr information is not available for
             * mutable BTree so it is not being preserved here when a leaf is
             * de-serialized.
             */
            
            return leaf;

        }

        public INodeData allocNode(IIndex btree, long addr,
                int branchingFactor, int nentries, IKeyBuffer keys,
                long[] childAddr, int[] childEntryCounts) {

            return new Node((BTree) btree, addr, branchingFactor, nentries,
                    keys, childAddr, childEntryCounts);

        }

    }

    /**
     * Mutable counter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Counter implements ICounter {

        private final BTree btree;
        
        public Counter(BTree btree) {
            
            assert btree != null;
            
            this.btree = btree;
            
        }
        
        public long get() {
            
            return btree.counter.get();
            
        }

        public long incrementAndGet() {
            
            final long counter = btree.counter.incrementAndGet();
            
            if (counter == btree.checkpoint.getCounter() + 1) {

                /*
                 * The first time the counter is incremented beyond the value in
                 * the checkpoint record we fire off a dirty event to put the
                 * BTree on the commit list.
                 */
                
                btree.fireDirtyEvent();
                
            }
                
            if (counter == Long.MAX_VALUE) {

                /*
                 * Actually, the counter would overflow on the next call but
                 * this eager error makes the test for overflow atomic.
                 */
                
                throw new RuntimeException("Counter overflow");

            }
            
            return counter;
            
        }
        
    }

    /**
     * Places the counter values into a namespace formed by the partition
     * identifier. The partition identifier is found in the high int32 word and
     * the counter value from the underlying {@link BTree} is found in the low
     * int32 word.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PartitionedCounter implements ICounter {

        private final int partitionId;
        private final ICounter src;
        
        public PartitionedCounter(int partitionId, ICounter src) {

            if (src == null)
                throw new IllegalArgumentException();

            this.partitionId = partitionId;
            
            this.src = src;
            
        }
        
        /**
         * Range checks the source counter (it must fit in the lower 32-bit
         * word) and then jambs the partition identifier into the high word.
         * 
         * @param tmp
         *            The source counter value.
         * 
         * @return The partition-specific counter value.
         * 
         * @throws RuntimeException
         *             if the source counter has overflowed.
         */
        private long wrap(long tmp) {
            
            if (tmp > Integer.MAX_VALUE) {

                throw new RuntimeException("Counter overflow");

            }

            /*
             * Place the partition identifier into the high int32 word and place
             * the truncated counter value into the low int32 word.
             * 
             * Note: You MUST case [partitionId] to a long or left-shifting
             * 32-bits will always clear it to zero.
             */
            
            return (((long)partitionId) << 32) | (int) tmp;
            
        }
        
        public long get() {
            
            return wrap( src.get() );
            
        }

        public long incrementAndGet() {
            
            return wrap( src.incrementAndGet() );

        }
        
    }
    
    final public int getSourceCount() {
        
        return 1;
        
    }
    
    final public BTree getMutableBTree() {
        
        return this;
        
    }

    public LeafCursor newLeafCursor(SeekEnum where) {
     
        return new LeafCursor( where );
        
    }

    public LeafCursor newLeafCursor(byte[] key) {
     
        return new LeafCursor( key );
        
    }
        
    /**
     * A simple stack based on an array used to maintain hard references for the
     * parent {@link Node}s in the {@link LeafCursor}. This class is optimized
     * for light-weight push/pop and copy operations. In particular, the copy
     * operation is used to support atomic state changes for
     * {@link LeafCursor#prior()} and {@link LeafCursor#next()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class Stack {

        /** backing array used to store the elements in the stack. */
        private Node[] a;

        /** #of elements in the stack. */
        private int n = 0; 

        /**
         * Stack with initial capacity of 10.
         * <p>
         * Note: The initial capacity is a relatively small since a B+Tree of
         * depth 10 will have a very large number of tuples even with a small
         * branching factor.
         */
        public Stack() {
        
            this( 10 );
            
        }
        
        public Stack(int capacity) {
            
            a = new Node[capacity];
            
        }
        
        /**
         * The size of the backing array.
         */
        final public int capacity() {
            
            return a.length;
            
        }

        /**
         * The #of elements on the stack.
         */
        final public int size() {
            
            return n;
            
        }

        /**
         * Push an element onto the stack.
         * 
         * @param item
         *            The element (required).
         */
        public void push(Node item) {
            
            assert item != null;
            
            if (n == a.length) {

                // extend the size of the backing array.
                final Node[] t = new Node[a.length * 2];

                // copy the data into the new array.
                System.arraycopy(a, 0, t, 0, n);

                // replace the backing array.
                a = t;

            }

            a[n++] = item;
            
        }
        
        /**
         * Pop an element off of the stack.
         * 
         * @return The element that was on the top of the stack.
         * 
         * @throws IllegalStateException
         *             if the stack is empty.
         */
        public Node pop() {

            if (n == 0)
                throw new IllegalStateException();

            final Node item = a[--n]; 
            
            /*
             * Note: it is important to clear the reference since this stack
             * is being used to manage weak references to parent nodes.
             */ 

            a[n] = null;
            
            return item;

        }

        /**
         * Return the element on the top of the stack.
         * 
         * @throws IllegalStateException
         *             if the stack is empty.
         */
        public Node peek() {
            
            if (n == 0)
                throw new IllegalStateException();
            
            return a[n-1];
            
        }
        
        /**
         * Replace the state of this {@link Stack} with the state of the source
         * {@link Stack}.
         * 
         * @param src
         *            The source {@link Stack}.
         */
        public void copyFrom(Stack src) {

            assert src != null;
            
            if (src.n > a.length) {
             
                // new backing array of same size as the source's backing array.
                a = new Node[src.a.length];
                
            } else {

                clear();
                
            }

            // copy the data into the backing array.
            System.arraycopy(src.a, 0, a, 0, src.n);

            // update the size of the stack.
            n = src.n;

        }

        /**
         * Clear the stack.
         */
        public void clear() {
            
            while (n > 0) {

                /*
                 * Again, clearing the reference is important since we are
                 * managing weak reference reachability with this stack.
                 */

                a[--n] = null;

            }
            
        }
        
    }
    
    /**
     * A cursor that may be used to traversal {@link Leaf}s.
     * <p>
     * Note: Instances of this class do NOT register an {@link ILeafListener}
     * and therefore do NOT notice if mutation causes the current leaf to become
     * invalid. In general, you need to have a specific <em>key</em> in mind
     * in order to re-locate the appropriate leaf after such mutation events.
     * <p>
     * Note: The {@link MutableBTreeTupleCursor} does register such listeners.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class LeafCursor implements ILeafCursor<Leaf> {

        /**
         * A stack containing the ordered ancestors of the current leaf. The
         * root of the B+Tree will always be on the bottom of the stack. Nodes
         * are pushed onto the stack during top-down navigation by the various
         * methods that (re-)locate the current leaf.
         * <p>
         * The purpose of this stack is to ensure that the ancestors of the
         * current leaf remain strongly reachable while the cursor is positioned
         * on that leaf. This is necessary in order for the methods that change
         * the cursor position {@link #next()} {@link #prior()} to succeeed
         * since they depend on the parent references which could otherwise be
         * cleared (parent references of nodes and leaves are weak references).
         * This means that hard references MUST be held to all {@link Node}s
         * that are ancestors of the current {@link Leaf}. This criteria is
         * satisfied by normal top-down navigation since the hard references are
         * on the stack frame. However, non-recursive constructions violate this
         * requirement since the hard references to the parents are not on the
         * stack frame. This {@link Stack} of {@link Node}s is therefore
         * maintained by all of the methods on this class such that we always
         * have hard references for the ancestors of the leaf that is being
         * visited.
         * <p>
         * Another benefit of this stack is that is ensures that not only the
         * current leaf but also all nodes above the current leaf remain
         * strongly reachable and hence require NO IOs. This should have the
         * most pronounced effect on a read-only {@link BTree} with a large
         * number of concurrent cursors as the hard reference queue for the
         * index might otherwise let the relevant nodes become weakly reachable.
         * It will have less of a performance effect for a mutable {@link BTree}
         * since that class is single-threaded for writers.
         * <p>
         * Note: The {@link IndexSegment} handles this differently since it has
         * the address of the prior and next leaf on hand and does not need the
         * parent references for navigation.
         */
        private Stack stack = new Stack();
        
        /**
         * {@link #prior()} and {@link #next()} use this in order to make their
         * operations on the {@link #stack} atomic. If they are unable to find
         * the prior/next leaf (because the cursor is already on the first/last
         * leaf) then they undo their operations on the stack by restoring it
         * from this copy.
         */
        private Stack backup = null;
        
        /**
         * Save a copy of the {@link #stack}.
         */
        @SuppressWarnings("unchecked")
        private void backup() {
            
            assert stack != null;

            if (backup == null) {

                backup = new Stack(stack.capacity());
                
            }
            
            backup.copyFrom(stack);
            
        }

        /**
         * Restore the {@link #stack} from its backup copy.
         */
        private void restore() {

            assert backup != null;
            
            stack = backup;
            
            backup = null;
            
        }
        
        /**
         * The current leaf (always defined).
         */
        private Leaf leaf;

        public Leaf leaf() {
            
            return leaf;
            
        }

        public BTree getBTree() {
            
            return BTree.this;
            
        }
        
        public LeafCursor clone() {
            
            return new LeafCursor(this);
            
        }
        
        /**
         * Copy constructor used by {@link #clone()}.
         */
        private LeafCursor(LeafCursor src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            // copy the stack state from the source cursor.
            stack = new Stack(src.stack.capacity());
            stack.copyFrom(src.stack);

            leaf = src.leaf();
            
        }
        
        public LeafCursor(SeekEnum where) {

            switch (where) {

            case First:
                
                first();
                
                break;
                
            case Last:
                
                last();
                
                break;
                
            default:
                
                throw new AssertionError("Unknown seek directive: " + where);
            
            }
            
        }
        
        public LeafCursor(byte[] key) {
            
            seek(key);
            
        }
        
        public Leaf first() {

            stack.clear();
            
            AbstractNode node = getRoot();

            while (!node.isLeaf()) {

                final Node n = (Node) node;

                stack.push(n);

                // left-most child.
                node = n.getChild(0);
                
            }
            
            return leaf = (Leaf)node;
            
        }

        public Leaf last() {
            
            stack.clear();
            
            AbstractNode node = getRoot();

            while (!node.isLeaf()) {

                final Node n = (Node) node;

                stack.push(n);

                // right-most child.
                node = n.getChild(n.getKeyCount());
                
            }
            
            return leaf = (Leaf)node;

        }

        /**
         * Descend from the root node to the leaf spanning that key. Note that
         * the leaf may not actually contain the key, in which case it is the
         * leaf that contains the insertion point for the key.
         */
        public Leaf seek(byte[] key) {

            stack.clear();
            
            AbstractNode node = getRoot();
            
            while(!node.isLeaf()) {
                
                final Node n = (Node)node;
                
                final int index = n.findChild(key);
                
                stack.push( n );
                
                node = n.getChild( index );
                
            }

            return leaf = (Leaf)node;
            
        }

        public Leaf seek(ILeafCursor<Leaf> src) {

            if (src == null)
                throw new IllegalArgumentException();
            
            if (src == this) {

                // NOP
                return leaf;
                
            }
            
            if (src.getBTree() != BTree.this) {
                
                throw new IllegalArgumentException();
                
            }

            // copy the stack state from the source cursor.
            stack.copyFrom(((LeafCursor) src).stack);

            return leaf = src.leaf();
            
        }
        
        public Leaf next() {

            // make sure that the current leaf is valid.
            if (leaf.isDeleted())
                throw new IllegalStateException("deleted");

            // save a copy of the stack.
            backup();

            /*
             * Starting with the current leaf, recursive ascent until there is a
             * right-sibling of the current child.
             */
            AbstractNode sibling = null;
            {

                AbstractNode child = leaf;

                Node p = child.getParent();

                while (true) {

                    if(p == null) {
                        
                        /*
                         * No right-sibling (must be the last leaf).
                         */
                        
                        // undo changes to the stack.
                        restore();
                        
                        // do not change the cursor position.
                        return null;
                        
                    }

                    sibling = p.getRightSibling(child, true/*materialize*/);
                    
                    if(sibling != null) break;
                    
                    if (p != stack.pop())
                        throw new AssertionError();

                    // check the parent.

                    child = p;
                    
                    p = p.getParent();
                    
                }
                
            }
            
            /*
             * Recursive descent to the left-most child. 
             */
            {
                
                while(!sibling.isLeaf()) {
                    
                    stack.push((Node)sibling);
                    
                    sibling = ((Node) sibling).getChild(0);
                    
                }
                
            }
            
            return leaf = (Leaf) sibling;
            
        }

        /**
         * Materialize the prior leaf in the natural order of the index (this is
         * more general than the right sibling which is restricted to leaves
         * that are children of the same direct parent). The algorithm is:
         * 
         * <ol>
         * 
         * <li>Recursive ascent via the parent until there is a left-sibling of
         * the child node (the index of the child in the parent must be GT
         * zero). If we reach the root and there is no left-sibling then we are
         * already on the left-most leaf and we are done.</li>
         * 
         * <li>t = leftSibling</li>
         * 
         * <li>while(!t.isLeaf()) right-descend</li>
         * 
         * </ol>
         * 
         * As we go up we pop each parent off of the hard reference stack.
         * <p>
         * As we go down we add each non-leaf node to the hard reference stack.
         * <p>
         * In order to make this operation atomic, the nodes to be popped are
         * added to a temporary stack until we 
         * 
         * @return The prior leaf -or- <code>null</code> if there is no
         *         predecessor of this leaf.
         */
        public Leaf prior() {

            // make sure that the current leaf is valid.
            if (leaf.isDeleted())
                throw new IllegalStateException("deleted");

            // save a copy of the stack.
            backup();

            /*
             * Starting with the current leaf, recursive ascent until there is a
             * left-sibling of the current child.
             */
            AbstractNode sibling = null;
            {

                AbstractNode child = leaf;

                Node p = child.getParent();

                while (true) {

                    if(p == null) {
                        
                        /*
                         * No left-sibling (must be the first leaf).
                         */
                        
                        // undo changes to the stack.
                        restore();
                        
                        // do not change the cursor position.
                        return null;
                        
                    }

                    sibling = p.getLeftSibling(child, true/*materialize*/);
                    
                    if(sibling != null) break;
                    
                    if (p != stack.pop())
                        throw new AssertionError();

                    // check the parent.

                    child = p;
                    
                    p = p.getParent();
                    
                }
                
            }
            
            /*
             * Recursive descent to the right-most child. 
             */
            {
                
                while(!sibling.isLeaf()) {
                    
                    stack.push((Node)sibling);
                    
                    sibling = ((Node) sibling)
                            .getChild(sibling.getKeyCount());
                    
                }
                
            }
            
            return leaf = (Leaf) sibling;
            
        }
        
    }

}
