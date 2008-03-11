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

import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IIndexManager;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * This class implements a variant of a B+Tree in which all values are stored in
 * leaves, but the leaves are not connected with prior-next links. This
 * constraint arises from the requirement to support a copy-on-write policy.
 * </p>
 * <p>
 * Note: No mechanism is exposed for recovering a node or leaf of the tree other
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
 * Note: This implementation is NOT thread-safe. The index is intended for use
 * within a single-threaded context.
 * </p>
 * <p>
 * Note: This iterators exposed by this implementation do NOT support concurrent
 * structural modification. Concurrent inserts or removals of keys MAY produce
 * incoherent traversal whether or not they result in addition or removal of
 * nodes in the tree.
 * </p>
 * 
 * @todo write tests to verify that {@link #setReadOnly(boolean)} in fact
 *       disables all means to write on or modify the state of the {@link BTree}.
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
 * @todo Modify the values in the tree to be variable length byte[]s.
 * 
 * @todo indexOf, keyAt, valueAt need batch api compatibility (they use the old
 *       findChild, search, and autobox logic).
 * 
 * @todo test a GOM integration. this will also require an extser service /
 *       index. extser support could be handled using an extensible metadata
 *       record for the {@link BTree} or {@link IndexSegment}, at least for an
 *       embedded database scenario. http://xstream.codehaus.org/ is also an
 *       interesting serialization package with somewhat different goals (you do
 *       not have to write serializers, but it is doubtless less compact and
 *       does not have extensible versioning).
 * 
 * @todo Implement an "extser" index that does not use extser itself, but which
 *       could provide the basis for a database that does use extser. The index
 *       needs to map class names to entries. Those entries are a classId and
 *       set of {version : Serializer} entries.
 * 
 * @todo we could defer splits by redistributing keys to left/right siblings
 *       that are under capacity - this makes the tree a b*-tree. however, this
 *       is not critical since the journal is designed to be fully buffered and
 *       the index segments are read-only but it would reduce memory by reducing
 *       the #of nodes -- and we can expect that the siblings will be either
 *       resident or in the direct buffer for the journal
 * 
 * @todo consider using extser an option for serialization so that we can
 *       continually evolve the node and leaf formats. we will also need a node
 *       serializer that does NOT use extser in order to store the persistent
 *       extser mappings themselves, and perhaps for other things such as an
 *       index of the index ranges that are multiplexed on a given journal.
 *       finally, we will need to use extser to simplify reopening an index so
 *       that we can recover its key serializer, value serializer, and key
 *       comparator as well as various configuration values from its metadata
 *       record. that encapsulation will have to be layered over the basic btree
 *       class so that we can use a more parameterized btree instance to support
 *       extser itself. there will also need to be metadata maintained about the
 *       perfect index range segments so that we know how to decompress blocks,
 *       deserialize keys and values, and compare keys.
 * 
 * @todo automated version history policies that expire old values based on
 *       either an external timestamp or write time on the server.
 * 
 * @todo support key range iterators that allow concurrent structural
 *       modification. structural mutations in a b+tree are relatively limited.
 *       When prior-next references are available, an iterator should be easily
 *       able to adjust for insertion and removal of keys.
 * 
 * @todo maintain prior-next references among leaves (and nodes?) in memory even
 *       if we are not able to write them onto the disk. when reading in a leaf,
 *       always set the prior/next reference iff the corresponding leaf is in
 *       memory - this is easily handled by checking the weak references on the
 *       parent node.
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
 * @todo Actually, I could save both prior and next references using a
 *       hand-over-hand chaining in which I pre-serialize the leaf and separate
 *       the allocation step from the write on the store. With just a small
 *       change to the leaf serialization format so that I can write in the
 *       prior and next fields at a known location (which could even be the end
 *       of the buffer), I would then be able to persistent the prior/next
 *       references. <br>
 *       The first step is to start maintaining those references. Also, consider
 *       that it may be useful to maintain them at the node as well as the leaf
 *       level.<br>
 *       If this is done, also check {@link Thread#isInterrupted()} and throw an
 *       exception when true to support fast abort of scans. See
 *       {@link Node#getChild(int)}.
 * 
 * @todo pre-fetch leaves for range scans?
 * 
 * @todo Note that efficient support for large branching factors requires a more
 *       sophisticated approach to maintaining the key order within a node or
 *       leaf. E.g., using a red-black tree or adaptive packed memory array.
 *       However, we can use smaller branching factors for btrees in the journal
 *       and use a separate implementation for bulk generating and reading
 *       "perfect" read-only key range segments.
 * 
 * @todo prior/next key on entry iterator? prior/next leaf on leaf iterator?
 * 
 * @todo derive a string index that uses patricia trees in the leaves per
 *       several published papers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTree extends AbstractBTree implements IIndex, ICommitter {
    
    /**
     * The default branching factor.
     */
    static public final int DEFAULT_BRANCHING_FACTOR = 32; //256
    
    /**
     * The minimum hard reference queue capacity is two(2) in order to avoid
     * cache evictions of the leaves participating in a split.
     */
    static public final int MINIMUM_WRITE_RETENTION_QUEUE_CAPACITY = 2;
    
    /**
     * The default capacity of the hard reference queue used to defer the
     * eviction of dirty nodes (nodes or leaves).
     */
    static public final int DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY = 500;

    /**
     * The #of entries on the hard reference queue that will be scanned for a
     * match before a new reference is appended to the queue. This trades off
     * the cost of scanning entries on the queue, which is handled by the queue
     * itself, against the cost of queue churn. Note that queue eviction drives
     * IOs required to write the leaves on the store, but incremental writes
     * occurr iff the {@link AbstractNode#referenceCount} is zero and the leaf
     * is dirty.
     */
    static public final int DEFAULT_WRITE_RETENTION_QUEUE_SCAN = 20;

    /**
     * The default capacity of the hard reference queue used to retain clean
     * nodes (or leaves). The goal is to keep the read to write ratio down.
     * 
     * FIXME This is an experimental feature - it is disabled when set to zero.
     * Note: There is an interaction with the branching factor which should be
     * neutralized by a dynamic policy.
     */
    static public final int DEFAULT_READ_RETENTION_QUEUE_CAPACITY = 0;

    /**
     * The #of entries on the hard reference queue that will be scanned for a
     * match before a new reference is appended to the queue. This trades off
     * the cost of scanning entries on the queue, which is handled by the queue
     * itself, against the cost of queue churn. Note that queue eviction drives
     * IOs required to write the leaves on the store, but incremental writes
     * occurr iff the {@link AbstractNode#referenceCount} is zero and the leaf
     * is dirty.
     */
    static public final int DEFAULT_READ_RETENTION_QUEUE_SCAN = 20;

    public int getHeight() {
        
        return height;
        
    }

    public int getNodeCount() {
        
        return nnodes;
        
    }

    public int getLeafCount() {
        
        return nleaves;
        
    }

    public int getEntryCount() {
        
        return nentries;
        
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
    protected Checkpoint checkpoint = null;
    
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
     * 
     * @todo this field as well as nleaves and nentries could be taken from the
     *       root node rather than being maintained directly. when the root is a
     *       leaf, then nnodes=0, nleaves=1, and nentries=root.nkeys
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
     */
    protected AtomicLong counter;
  
    /**
     * The last address from which the {@link IndexMetadata} record was read or
     * on which it was written.
     */
    private long lastMetadataAddr;
    
    /**
     * Load a {@link BTree} from the store using a {@link Checkpoint} record.
     * 
     * @param store
     *            The store.
     * @param checkpoint
     *            A {@link Checkpoint} record for that {@link BTree}.
     * @param metadata
     *            The metadata record for that {@link BTree}.
     * 
     * @see #create(IRawStore, IndexMetadata)
     * @see #load(IRawStore, long)
     */
    public BTree(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {

        super(store, 
                NodeFactory.INSTANCE, //
                // FIXME new PackedAddressSerializer(store),
                AddressSerializer.INSTANCE,
                metadata
                );
        
        assert checkpoint != null;

        assert checkpoint.getMetadataAddr() == metadata.getMetadataAddr(); // must agree.

        if (metadata.getConflictResolver() != null && !metadata.isIsolatable()) {

            throw new IllegalArgumentException(
                    "Conflict resolver may only be used with isolatable indices");
            
        }

        // initialize mutable fields from the checkpoint record.
        this.checkpoint = checkpoint; // save reference.
        this.height = checkpoint.getHeight();
        this.nnodes = checkpoint.getNodeCount();
        this.nleaves = checkpoint.getLeafCount();
        this.nentries = checkpoint.getEntryCount();
        this.counter = new AtomicLong( checkpoint.getCounter() );
        
        // save the address from which the index metadata record was read.
        this.lastMetadataAddr = metadata.getMetadataAddr();
        
        /*
         * Note: the mutable BTree has a limit here so that split() will always
         * succeed. That limit does not apply for an immutable btree.
         */
        assert writeRetentionQueue.capacity() >= MINIMUM_WRITE_RETENTION_QUEUE_CAPACITY;

        reopen();
        
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

        if(!wasDirty) {
            
            fireDirtyEvent();
            
        }
        
    }

//    /**
//     * Uses {@link #handleCommit()} to flush any dirty nodes to the store and
//     * update the metadata so we can clear the hard reference queue and release
//     * the hard reference to the root node. {@link #reopen()} is responsible for
//     * reloading the root node.
//     */
//    public void close() {
//
//        /*
//         * flush any dirty records, noting the address of the metadata record
//         * so that we can reload the store.
//         */
//        handleCommit();
//        
//        /*
//         * this will clear the hard reference cache, release the node serializer
//         * buffers, and release the hard reference to the root node.
//         */
//        super.close();
//        
//    }
    
    /**
     * Reloads the root node iff it is <code>null</code> (indicating a closed
     * index).
     * 
     * @see #close()
     */
    protected void reopen() {

        if (root == null) {

            /*
             * reload the root node.
             * 
             * Note: This is synchronized to avoid race conditions when
             * re-opening the index from the backing store.
             */

            synchronized(this) {
            
                /*
                 * Setup the root leaf.
                 */
                if (checkpoint.getRootAddr() == 0L) {

                    /*
                     * Create the root leaf.
                     */
                    newRootLeaf();
                    
                } else {
                    
                    /*
                     * Read the root node of the btree.
                     */
                    root = readNodeOrLeaf(checkpoint.getRootAddr());
                    
                }
                
            }

        }

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
    final public void setReadOnly(boolean readOnly) {

        if (this.readOnly && !readOnly) {

            throw new UnsupportedOperationException(MSG_READ_ONLY);
            
        }
        
        this.readOnly = readOnly;
        
    }
    transient private boolean readOnly = false;
    
    /**
     * Return <code>true</code> if the {@link BTree} will participate in
     * commits requested via {@link #handleCommit()}.
     */
    final public boolean getAutoCommit() {
        
        return autoCommit;
        
    }

    /**
     * Sets the auto-commit flag (default <code>true</code>). When
     * <code>false</code> the {@link BTree} will NOT write a checkpoint record
     * in response to a {@link #handleCommit()} request.
     * 
     * @param autoCommit
     *            The new value for the auto-commit flag.
     */
    final public void setAutoCommit(boolean autoCommit) {
        
        this.autoCommit = autoCommit;
        
    }
    transient private boolean autoCommit = true;
    
    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }
    
    /**
     * Sets the lastCommitTime
     * 
     * @param lastCommitTime
     *            The timestamp of the last committed state of this index. This
     *            should always be {@link ICommitRecord#getTimestamp()}.
     * 
     * @throws IllegalStateException
     *             if you attempt to replace a non-zero lastCommitTime with a
     *             different timestamp.
     */
    final public void setLastCommitTime(long lastCommitTime) {
        
//        if (this.lastCommitTime != 0L) {
//            
//            throw new IllegalStateException();
//            
//        }
        
        if (this.lastCommitTime != 0L && this.lastCommitTime != lastCommitTime) {

            throw new IllegalStateException("Updated lastCommitTime: old="
                    + this.lastCommitTime + ", new=" + lastCommitTime);
            
        }

        this.lastCommitTime = lastCommitTime;
        
    }
    private long lastCommitTime = 0L; // ITx.UNISOLATED
    
    /**
     * Return the {@link IDirtyListener}.
     */
    final public IDirtyListener getListener() {
        
        return listener;
        
    }

    /**
     * Set or clear the listener (there can be only one).
     * 
     * @param listener The listener.
     */
    final public void setDirtyListener(IDirtyListener listener) {

        this.listener = listener;
        
    }
    
    private IDirtyListener listener;

    /**
     * Fire an event to the listener (iff set).
     */
    final protected void fireDirtyEvent() {
        
        IDirtyListener l = this.listener;
        
        if(l==null) return;
        
        if(Thread.interrupted()) {
            
            throw new RuntimeException(new InterruptedException());
            
        }
        
        l.dirtyEvent(this);
        
        log.info("");
        
    }
    
    /**
     * Checkpoint operation writes dirty nodes using a post-order traversal that
     * first writes any dirty leaves and then (recursively) their parent nodes.
     * The parent nodes are guarenteed to be dirty if there is a dirty child so
     * the commit never triggers copy-on-write.
     * <p>
     * Note: A checkpoint by itself is NOT an atomic commit. The commit protocol
     * is at the store level and uses checkpoints to ensure that the state of
     * the btree is current on the store.
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
        
        log.info("begin");
        
        assertNotReadOnly();
        
        assert root != null; // i.e., isOpen().

        if (root.dirty) {

            writeNodeRecursive( root );
            
        }

        if (metadata.getMetadataAddr() == 0L) {
            
            /*
             * The index metadata has been modified so we write out a new
             * metadata record on the store.
             */
            
            // write the metadata record.
            metadata.write(store);
            
            // note the address of the new metadata record.
            lastMetadataAddr = metadata.getMetadataAddr();
            
            log.info("wrote updated metadata record");
            
        }
        
        // create new checkpoint record.
        checkpoint = metadata.newCheckpoint(this);
        
        // write it on the store.
        checkpoint.write(store);
        
        log.info("new checkpoint="+checkpoint);
        
        // return address of that checkpoint record.
        return checkpoint.getCheckpointAddr();
        
    }
    
    /**
     * Returns the most recent {@link Checkpoint} record for this btree.
     * 
     * @return The most recent {@link Checkpoint} record for this btree and
     *         never <code>null</code>.
     */
    final public Checkpoint getCheckpoint() {

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
     * <li> the metadata record address has not changed -AND-
     * <ul>
     * <li> EITHER the root is <code>null</code>, indicating that the index
     * is closed (flushing the index to disk and updating the metadata record is
     * part of the close protocol so we know that the metadata address is
     * current in this case);</li>
     * <li> OR the root of the btree is NOT dirty, the persistent address of the
     * root of the btree is the same as the address record of the root in the
     * metadata record, and the {@link #counter} value agrees with the counter
     * on the metadata record.</li>
     * </ul>
     * </li>
     * </ul>
     * 
     * @return <code>true</code> true iff changes would be lost unless the
     *         B+Tree was flushed to the backing store using {@link #writeCheckpoint()}.
     */
    public boolean needsCheckpoint() {

        if (metadata.getMetadataAddr() == lastMetadataAddr && //
                (root == null || //
                        ( !root.dirty //
                        && checkpoint.getRootAddr() == root.identity //
                        && checkpoint.getCounter() == counter.get())
                )
        ) {
            
            return false;
            
        }

        return true;
     
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
     *             if the new metadata record has already been written on the
     *             store - see {@link IndexMetadata#getMetadataAddr()}
     * @throws UnsupportedOperationException
     *             if the index is read-only.
     */
    final public void setIndexMetadata(IndexMetadata indexMetadata) {
        
        if (indexMetadata == null)
            throw new IllegalArgumentException();

        if (indexMetadata.getMetadataAddr() != 0)
            throw new IllegalArgumentException();

        assertNotReadOnly();

        this.metadata = indexMetadata;
        
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
    public long handleCommit() {

        if (autoCommit && needsCheckpoint()) {

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
    public static BTree create(IRawStore store, IndexMetadata metadata) {
        
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
    public static BTree load(IRawStore store, long addrCheckpoint) {

        /*
         * Read checkpoint record from store.
         */
        final Checkpoint checkpoint = Checkpoint.load(store,addrCheckpoint);

        /*
         * Read metadata record from store.
         */
        final IndexMetadata metadata = IndexMetadata.read(store,
                checkpoint.getMetadataAddr());

        /*
         * Create B+Tree object instance.
         */
        try {
            
            Class cl = Class.forName(metadata.getClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class //
                    });

            BTree btree = (BTree) ctor.newInstance(new Object[] { //
                    store,//
                    checkpoint, //
                    metadata //
                    });
            
            return btree;
            
        } catch(Exception ex) {
            
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
                long[] versionTimestamp, boolean[] deleteMarkers) {

            return new Leaf((BTree) btree, addr, branchingFactor, keys, values,
                    versionTimestamp, deleteMarkers);

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
        
        public long get() {
            
            return src.get();
            
        }

        public long incrementAndGet() {
            
            final long tmp = src.incrementAndGet();

            if (tmp > Integer.MAX_VALUE) {

                throw new RuntimeException("Counter overflow");

            }

            /*
             * Place the partition identifier into the high int32 word and place
             * the truncated counter value into the low int32 word.
             */
            return partitionId << 32 | (int) tmp;
            
        }
        
    }
    
}
