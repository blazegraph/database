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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndexLocalCounter;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.ReadOnlyCounter;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * An mutable persistence capable extensible hash tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a
 *      href="A robust scheme for multilevel extendible hashing (2003)">
 *      A Robust Scheme for Multilevel Extendible Hashing </a> by Sven Helmer,
 *      Thomas Neumann, Guido Moerkotte. ISCIS 2003: 220-227.
 * 
 *      TODO It should be possible to define an native int32 hash table in
 *      parallel to the unsigned byte[] hash table simply by having an
 *      alternative descent passing an int32 key all the way down and using the
 *      variant of getBits() method which operates on the int32 values. We could
 *      also optimize the storage and retrieval of the int32 keys, perhaps with
 *      a custom mutable bucket page and mutable bucket data implementation for
 *      int32 keys. This optimization is likely to be quite worth while as the
 *      majority of use cases for the hash tree use int32 keys.
 * 
 *      TODO It is quite possible to define a range query interface for the hash
 *      tree. You have to use an order preserving hash function, which is
 *      external to the HTree implementation. Internally, the HTree must either
 *      double-link the pages or crawl the directory structure.
 * 
 *      TODO The keys should be declared as a computed key based on the data
 *      fields in the record. The {@link HTree} supports arbitrary bit length
 *      keys, but can be optimized for int32 keys easily enough.
 * 
 *      TODO Instrument performance counters for structural modifications,
 *      insert, remove, etc. per {@link BTreeCounters}.
 */
public class HTree extends AbstractHTree 
	implements 
	IIndexLocalCounter
//	ICheckpointProtocol // interface declaration moved to subclass
//	IIndex, 
//  ISimpleBTree//, IAutoboxBTree, ILinearList, IBTreeStatistics, ILocalBTreeView
//  IRangeQuery
{

//    private static final transient Logger log = Logger.getLogger(HTree.class);

    /**
     * The #of bits of distinction to be made each time we split a directory
     * page in the {@link HTree}. See
     * {@link #splitDirectoryPage(DirectoryPage, int, AbstractPage)} for a write
     * up on this.
     */
    /*private*/ final int splitBits = 1; // in [1:addressBits];
    
    /*
     * metadata about the index.
     * 
     * @todo this data should be rolled into the IndexMetadata object.
     */
	final boolean versionTimestamps = false;
	final boolean deleteMarkers = false;
    final boolean rawRecords;

	/**
	 * The #of {@link DirectoryPage} in the {@link HTree}. This is ONE (1) for a
	 * new {@link HTree}.
	 */
	protected long nnodes;

	/**
	 * The #of {@link BucketPage}s in the {@link HTree}. This is one (1) for a
	 * new {@link HTree} (one directory page and one bucket page).
	 */
	protected long nleaves;

	/**
	 * The #of entries in the {@link HTree}. This is ZERO (0) for a new
	 * {@link HTree}.
	 */
	protected long nentries;
    
	/**
	 * The value of the record version number that will be assigned to the next
	 * node or leaf written onto the backing store. This number is incremented
	 * each time a node or leaf is written onto the backing store. The initial
	 * value is ZERO (0). The first value assigned to a node or leaf will be
	 * ZERO (0).
	 */
    protected long recordVersion;
    
    /**
     * The mutable counter exposed by #getCounter()}.
     * <p>
     * Note: This is <code>protected</code> so that it will be visible to
     * {@link Checkpoint} which needs to write the actual value store in this
     * counter into its serialized record (without the partition identifier).
     */
    protected AtomicLong counter;

	/**
	 * A buffer used to encode a raw record address for a mutable {@link BTree}
	 * and otherwise <code>null</code>.
	 */
    private final ByteArrayBuffer recordAddrBuf;
    
    @Override
    final public long getNodeCount() {
        
        return nnodes;
        
    }

    @Override
    final public long getLeafCount() {
        
        return nleaves;
        
    }

    @Override
    final public long getEntryCount() {
        
        return nentries;
        
    }

    /**
     * The constructor sets this field initially based on a {@link Checkpoint}
     * record containing the only address of the {@link IndexMetadata} for the
     * index. Thereafter this reference is maintained as the {@link Checkpoint}
     * record last written by {@link #writeCheckpoint()} or read by
     * {@link #load(IRawStore, long)}.
     */
    private Checkpoint checkpoint = null;
    
    @Override
    final public Checkpoint getCheckpoint() {

        if (checkpoint == null)
            throw new AssertionError();
        
        return checkpoint;
        
    }
    
    @Override
    final public long getRecordVersion() {
    	
    	return recordVersion;

    }
    
    @Override
    final public long getMetadataAddr() {

    	return metadata.getMetadataAddr();

    }
        
    @Override
    final public long getRootAddr() {
    	
		return (root == null ? getCheckpoint().getRootAddr() : root
				.getIdentity());
		
    }

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

        if(!checkpoint.hasCheckpointAddr()) {
            
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
            
            if(checkpoint.getRootAddr() != root.getIdentity()) {
        
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
	final public void setIndexMetadata(final HTreeIndexMetadata indexMetadata) {

        assertNotReadOnly();

        if (indexMetadata == null)
            throw new IllegalArgumentException();

        if (indexMetadata.getMetadataAddr() != 0)
            throw new IllegalArgumentException();

        this.metadata = indexMetadata;
        
        // gets us on the commit list for Name2Addr.
        fireDirtyEvent();
        
    }
    
//    /**
//     * Handle request for a commit by {@link #writeCheckpoint()}ing dirty nodes
//     * and leaves onto the store, writing a new metadata record, and returning
//     * the address of that metadata record.
//     * <p>
//     * Note: In order to avoid needless writes the existing metadata record is
//     * always returned if {@link #needsCheckpoint()} is <code>false</code>.
//     * <p>
//     * Note: The address of the existing {@link Checkpoint} record is always
//     * returned if {@link #getAutoCommit() autoCommit} is disabled.
//     * 
//     * @return The address of a {@link Checkpoint} record from which the btree
//     *         may be reloaded.
//     */
    public long handleCommit(final long commitTime) {

        return writeCheckpoint2().getCheckpointAddr();
    	
    }

    @Override
    public void invalidate(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        if (error == null)
            error = t;

    }
    
    /**
     * Returns an {@link ICounter}. The {@link ICounter} is mutable iff the
     * {@link BTree} is mutable. All {@link ICounter}s returned by this method
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

        	throw new UnsupportedOperationException();
//            counter = new PartitionedCounter(pmd.getPartitionId(), counter);

        }

        if (isReadOnly()) {

            return new ReadOnlyCounter(counter);

        }

        return counter;

    }
    
    /**
	 * Required constructor form for {@link HTree} and any derived subclasses.
	 * This constructor is used both to create a new {@link HTree}, and to load
	 * a {@link HTree} from the store using a {@link Checkpoint} record.
	 * 
	 * @param store
	 *            The store.
	 * @param checkpoint
	 *            A {@link Checkpoint} record for that {@link HTree}.
	 * @param metadata
	 *            The metadata record for that {@link HTree}.
	 * @param readOnly
	 *            When <code>true</code> the {@link HTree} will be immutable.
	 * 
	 * @see HTree#create(IRawStore, IndexMetadata)
	 * @see HTree#load(IRawStore, long, boolean)
	 */
//	 * @throws IllegalArgumentException
//	 *             if addressBits is LT ONE (1).
//	 * @throws IllegalArgumentException
//	 *             if addressBits is GT (16) (fan out of 65536).
    public HTree(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata, final boolean readOnly) {

        super(  store, 
                NodeFactory.INSTANCE, //
                readOnly, // read-only
                (HTreeIndexMetadata)metadata,//
                metadata.getBtreeRecordCompressorFactory()
                );

//        this.readOnly = readOnly;
        
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
        assert readOnly || writeRetentionQueue.capacity() >= IndexMetadata.Options.MIN_WRITE_RETENTION_QUEUE_CAPACITY;

        /*
         * Note: Re-open is deferred so that we can mark the BTree as read-only
         * before we read the root node.
         */
//        reopen();

        /*
         * Buffer used to encode addresses into the tuple value for a mutable
         * B+Tree.
         */
		recordAddrBuf = readOnly ? null
				: new ByteArrayBuffer(Bytes.SIZEOF_LONG);
		
		this.rawRecords = metadata.getRawRecords();

    }

	/**
	 * Encode a raw record address into a byte[] suitable for storing in the
	 * value associated with a tuple and decoding using
	 * {@link AbstractBTree#decodeRecordAddr(byte[])}. This method is only
	 * supported for a mutable {@link BTree} instance. Per the contract of the
	 * mutable {@link BTree}, it is not thread-safe.
	 * 
	 * @param addr
	 *            The raw record address.
	 * 
	 * @return A newly allocated byte[] which encodes that address.
	 */
    byte[] encodeRecordAddr(final long addr) {
    	
    	return AbstractBTree.encodeRecordAddr(recordAddrBuf, addr);
    	
    }

    /**
     * Sets the {@link #checkpoint} and initializes the mutable fields from the
     * checkpoint record. In order for this operation to be atomic, the caller
     * must be synchronized on the {@link HTree} or otherwise guaranteed to have
     * exclusive access, e.g., during the ctor or when the {@link HTree} is
     * mutable and access is therefore required to be single-threaded.
     */
    protected void setCheckpoint(final Checkpoint checkpoint) {

        this.checkpoint = checkpoint; // save reference.
        
        // Note: Height is not uniform for an HTree.
//        this.height = checkpoint.getHeight();
        
        this.nnodes = checkpoint.getNodeCount();
        
        this.nleaves = checkpoint.getLeafCount();
        
        this.nentries = checkpoint.getEntryCount();
        
        this.counter = new AtomicLong( checkpoint.getCounter() );

        this.recordVersion = checkpoint.getRecordVersion();
        
    }

    /**
     * Creates and sets new root {@link Leaf} on the B+Tree and (re)sets the
     * various counters to be consistent with that root.  This is used both
     * by the constructor for a new {@link BTree} and by {@link #removeAll()}.
     * <p>
     * Note: The {@link #getCounter()} is NOT changed by this method.
     */
    final private void newRoot() {

//        height = 0;

        nnodes = 0;
        
        nentries = 0;

        final boolean wasDirty = root != null && root.isDirty();
        
        nleaves = 0;

		/*
		 * The initial setup of the hash tree is a root directory page whose
		 * global depth is the #of address bits (which is the maximum value that
		 * global depth can take on for a given hash tree). There is a single
		 * bucket page and all entries in the root directory page point to that
		 * bucket. This means that the local depth of the initial bucket page
		 * will be zero (you can prove this for yourself by consulting the
		 * tables generated by TestHTree). With a depth of zero, the initial
		 * bucket page will have buddy hash buckets which can hold only a single
		 * distinct hash key (buckets always have to handle duplicates of a hash
		 * key).
		 * 
		 * From this initial configuration, inserts of 2 distinct keys which
		 * fall into the same buddy hash tables will cause that buddy hash
		 * buckets in the initial bucket page to split, increasing the depth of
		 * the resulting bucket page by one. Additional splits driven by inserts
		 * of distinct keys will eventually cause the local depth of some bucket
		 * page to exceed the global depth of the root and a new level will be
		 * introduced below the root. The hash tree can continue to grow in this
		 * manner, gradually adding depth where there are bit prefixes for which
		 * there exists a lot of variety in the observed keys.
		 */

		// Initial root.
		final DirectoryPage r = new DirectoryPage(//
				this,// the owning htree instance
				null, // overflowKey
				addressBits // the global depth of the root.
		);
		nnodes++;
		assert r.getSlotsPerBuddy() == (1 << addressBits) : "slotsPerBuddy="
				+ r.getSlotsPerBuddy();
		assert r.getNumBuddies() == 1 : "numBuddies=" + r.getNumBuddies();

		// Data for the root.
		final MutableDirectoryPageData rdata = (MutableDirectoryPageData) r.data;

		// Initial bucket.
		final BucketPage b = new BucketPage(this, 0/* globalDepth */);
		nleaves++;

		final int nslots = r.getSlotsPerBuddy() * r.getNumBuddies();

		for (int i = 0; i < nslots; i++) {

			b.parent = (Reference) r.self;

			r.childRefs[i] = (Reference) b.self;

			rdata.childAddr[i] = 0L;

		}

		this.root = r;

        // Note: Counter is unchanged!
//        counter = new AtomicLong( 0L );

        // TODO support bloom filter?
//        if (metadata.getBloomFilterFactory() != null) {
//
//            /*
//             * Note: Allocate a new bloom filter since the btree is now empty
//             * and set its reference on the BTree. We need to do this here in
//             * order to (a) overwrite the old reference when we are replacing
//             * the root, e.g., for removeAll(); and (b) to make sure that the
//             * bloomFilter reference is defined since the checkpoint will not
//             * have its address until the next time we call writeCheckpoint()
//             * and therefore readBloomFilter() would fail if we did not create
//             * and assign the bloom filter here.
//             */
//            
//            bloomFilter = metadata.getBloomFilterFactory().newBloomFilter();
//            
//        }
        
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
            
            newRoot();
            
        } else {

			/*
			 * Read the root node of the HTree and set its depth, which is
			 * always [addressBits].
			 */
			root = (DirectoryPage) readNodeOrLeaf(checkpoint.getRootAddr());
			root.globalDepth = addressBits;

            // Note: The optional bloom filter will be read lazily. 
            
        }

    }

    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }

    final public long getRevisionTimestamp() {
        
        if (readOnly)
            throw new UnsupportedOperationException(ERROR_READ_ONLY);

        return lastCommitTime + 1;
        
    }
    
    @Override
    final public void setLastCommitTime(final long lastCommitTime) {
        
        if (lastCommitTime == 0L)
            throw new IllegalArgumentException();
        
        if (this.lastCommitTime == lastCommitTime) {

            // No change.
            
            return;
            
        }
        
        if (INFO)
            log.info("old=" + this.lastCommitTime + ", new=" + lastCommitTime);
        // Note: Commented out to allow replay of historical transactions.
        /*if (this.lastCommitTime != 0L && this.lastCommitTime > lastCommitTime) {

            throw new IllegalStateException("Updated lastCommitTime: old="
                    + this.lastCommitTime + ", new=" + lastCommitTime);
            
        }*/

        this.lastCommitTime = lastCommitTime;
        
    }

    /**
     * The lastCommitTime of the {@link Checkpoint} record from which the
     * {@link BTree} was loaded.
     * <p>
     * Note: Made volatile on 8/2/2010 since it is not otherwise obvious what
     * would guarantee visibility of this field, through I do seem to remember
     * that visibility might be guaranteed by how the BTree class is discovered
     * and returned to the class. Still, it does no harm to make this a volatile
     * read.
     */
    volatile private long lastCommitTime = 0L;// Until the first commit.
    
    final public IDirtyListener getDirtyListener() {
        
        return listener;
        
    }

    final public void setDirtyListener(final IDirtyListener listener) {

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
        
        if (root != null && root.isDirty()) {

            writeNodeRecursive(root);
            
            if(INFO)
                log.info("flushed root: addr=" + root.getIdentity());
            
            return true;
            
        }
        
        return false;

    }

    /**
     * Returns an immutable view of this {@link HTree}. If {@link BTree} is
     * already read-only, then <i>this</i> instance is returned. Otherwise, a
     * read-only {@link BTree} is loaded from the last checkpoint and returned.
     * 
     * @throws IllegalStateException
     *             If the {@link BTree} is dirty.
     * 
     * @todo The {@link Checkpoint} could hold a {@link WeakReference} singleton
     *       to the read-only view loaded from that checkpoint.
     */
    public HTree asReadOnly() {

        if (isReadOnly()) {

            return this;
            
        }

        if(needsCheckpoint())
            throw new IllegalStateException();

		return HTree
				.load(store, checkpoint.getCheckpointAddr(), true/* readOnly */);

    }

//    * Checkpoint operation {@link #flush()}es dirty nodes, the optional
//    * {@link IBloomFilter} (if dirty), the {@link IndexMetadata} (if dirty),
//    * and then writes a new {@link Checkpoint} record on the backing store,
//    * saves a reference to the current {@link Checkpoint} and returns the
//    * address of that {@link Checkpoint} record.
//    * <p>
//    * Note: A checkpoint by itself is NOT an atomic commit. The commit protocol
//    * is at the store level and uses {@link Checkpoint}s to ensure that the
//    * state of the {@link BTree} is current on the backing store.
//    * 
//    * @return The address at which the {@link Checkpoint} record for the
//    *         {@link BTree} was written onto the store. The {@link BTree} can
//    *         be reloaded from this {@link Checkpoint} record.
//    * 
//    * @see #writeCheckpoint2(), which returns the {@link Checkpoint} record
//    *      itself.
    /**
     * {@inheritDoc}
     *       
     * @see #load(IRawStore, long)
     */
    @Override
    final public long writeCheckpoint() {
    
        // write checkpoint and return address of that checkpoint record.
        return writeCheckpoint2().getCheckpointAddr();
        
    }

    /**
     * {@inheritDoc}
     * 
     * @see #load(IRawStore, long)
     */
    @Override
    final public Checkpoint writeCheckpoint2() {
    	
        assertNotTransient();
        assertNotReadOnly();

		/*
		 * Note: Acquiring this lock provides for atomicity of the checkpoint of
		 * the BTree during the commit protocol. Without this lock, users of the
		 * UnisolatedReadWriteIndex could be concurrently modifying the BTree
		 * while we are attempting to snapshot it for the commit.
		 * 
		 * Note: An alternative design would declare a global read/write lock
		 * for mutation of the indices in addition to the per-BTree read/write
		 * lock provided by UnisolatedReadWriteIndex. Rather than taking the
		 * per-BTree write lock here, we would take the global write lock in the
		 * AbstractJournal's commit protocol, e.g., commitNow(). The global read
		 * lock would be taken by UnisolatedReadWriteIndex before taking the
		 * per-BTree write lock. This is effectively a hierarchical locking
		 * scheme and could provide a workaround if deadlocks are found to occur
		 * due to lock ordering problems with the acquisition of the
		 * UnisolatedReadWriteIndex lock (the absence of lock ordering problems
		 * really hinges around UnisolatedReadWriteLocks not being taken for
		 * more than one index at a time.)
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/278
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/284
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/288
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/343
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
		 */
		final Lock lock = writeLock();
		lock.lock();
		try {
            /**
             * Do not permit checkpoint if the index is in an error state.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate
             *      BTree objects if error occurs during eviction </a>
             */
            if (error != null)
                throw new IllegalStateException(ERROR_ERROR_STATE, error);
            
			if (/* autoCommit && */needsCheckpoint()) {

				/*
				 * Flush the btree, write a checkpoint record, and return the
				 * address of that checkpoint record. The [checkpoint] reference
				 * is also updated.
				 */

				return _writeCheckpoint2();

			}

			/*
			 * There have not been any writes on this btree or auto-commit is
			 * disabled.
			 * 
			 * Note: if the application has explicitly invoked writeCheckpoint()
			 * then the returned address will be the address of that checkpoint
			 * record and the BTree will have a new checkpoint address made
			 * restart safe on the backing store.
			 */

			return checkpoint;

		} finally {

			lock.unlock();

		}

    }
    
    /**
	 * Core implementation invoked by {@link #writeCheckpoint2()} while holding
	 * the lock - <strong>DO NOT INVOKE THIS METHOD DIRECTLY</strong>.
	 * 
	 * @return the checkpoint.
	 */
    final private Checkpoint _writeCheckpoint2() {
        
        assertNotTransient();
        assertNotReadOnly();
        
//        assert root != null : "root is null"; // i.e., isOpen().

        // flush any dirty nodes.
        flush();
        
        // pre-condition: all nodes in the tree are clean.
        assert root == null || !root.isDirty();

//        { // TODO support bloom filter?
//            /*
//             * Note: Use the [AbstractBtree#bloomFilter] reference here!!!
//             * 
//             * If that reference is [null] then the bloom filter is either
//             * clean, disabled, or was not configured. For any of those (3)
//             * conditions we will use the address of the bloom filter from the
//             * last checkpoint record. If the bloom filter is clean, then we
//             * will just carry forward its old address. Otherwise the address in
//             * the last checkpoint record will be 0L and that will be carried
//             * forward.
//             */
//            final BloomFilter filter = this.bloomFilter;
//
//            if (filter != null && filter.isDirty() && filter.isEnabled()) {
//
//                /*
//                 * The bloom filter is enabled, is loaded and is dirty, so write
//                 * it on the store now.
//                 */
//
//                filter.write(store);
//
//            }
//            
//        }
        
        if (metadata.getMetadataAddr() == 0L) {
            
        	/*
        	 * Is there an old metadata addr in need of recyling?
        	 */
            if (checkpoint != null) {
            	final long addr = checkpoint.getMetadataAddr();
            	if (addr != IRawStore.NULL)
            		store.delete(addr);
            }
            
            /*
             * The index metadata has been modified so we write out a new
             * metadata record on the store.
             */
            
            metadata.write(store);
            
         }
        
        if (checkpoint != null && getRoot() != null
                && checkpoint.getRootAddr() != getRoot().getIdentity()) {
            
            recycle(checkpoint.getRootAddr());
            
        }

        if (checkpoint != null) {
            
            recycle(checkpoint.getCheckpointAddr());
            
        }
        
        // create new checkpoint record.
        checkpoint = newCheckpoint();
        
        // write it on the store.
        checkpoint.write(store);
        
        if (BigdataStatics.debug||INFO) {
            final String msg = "name=" + metadata.getName()
                    + ", writeQueue{size=" + writeRetentionQueue.size()
                    + ",distinct=" + ndistinctOnWriteRetentionQueue + "} : "
                    + checkpoint;
            if (BigdataStatics.debug)
                System.err.println(msg);
            if (INFO)
                log.info(msg);
        }
        
        // return the checkpoint record.
        return checkpoint;
        
    }

    /**
     * Create a {@link Checkpoint} for a {@link HTree}.
     * <p>
     * The caller is responsible for writing the {@link Checkpoint} record onto
     * the store.
     * <p>
     * The class identified by {@link IndexMetadata#getCheckpointClassName()}
     * MUST declare a public constructor with the following method signature
     * 
     * <pre>
     *   ...( HTree htree )
     * </pre>
     * 
     * @return The {@link Checkpoint}.
     */
    @SuppressWarnings("unchecked")
    final private Checkpoint newCheckpoint() {
        
        try {
            
            @SuppressWarnings("rawtypes")
            final Class cl = Class.forName(metadata.getCheckpointClassName());
            
            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor on the Checkpoint class
             * [cl].
             */
            
            @SuppressWarnings("rawtypes")
            final Constructor ctor = cl.getConstructor(new Class[] {
                    HTree.class //
                    });

            final Checkpoint checkpoint = (Checkpoint) ctor
                    .newInstance(new Object[] { //
                            this //
                    });
            
            return checkpoint;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /*
	 * CRUD API.
	 * 
	 * TODO The hash tree intrinsically supports duplicate keys. This means that
	 * some method semantics must be different. E.g., insert() does not update
	 * an existing tuple for the same key. remove() removes an arbitrary tuple
	 * matching the key, lookup() will return an arbitrary match or null if
	 * there is no match, etc. Also, what corresponds to a lookup scenario in a
	 * BTree will normally be a bag traversal in an HTree requiring an iterator
	 * construct.
	 * 
	 * TODO The hash tree will normally be used with int32 keys, and those keys
	 * will typically be obtained from Object#hashCode(). Special methods should
	 * be provided for this purpose. E.g., ISimpleHTree. Custom serialization
	 * and even a custom mutable leaf data record should be used with this case.
	 * 
	 * TODO insert() return for htree is never old value since always adds() a
	 * new tuple never replaces() an existing tuple.  Further, if the key and
	 * val are equals() then we might even define the semantics as a NOP rather
	 * than appending a duplicate item into the bucket (it's worth thinking on
	 * this further as scanning for equals is more work and sometimes we might
	 * want to allow fully duplicated items into the bucket.).
	 */
    
    /**
     * Convert an int32 hash code key into an <code>unsigned byte[4]</code>.
     * <p>
     * Note: This encoding MUST be consistent with
     * {@link IKeyBuilder#append(int)}.
     */
    private byte[] i2k(final int key) {
        // lexiographic ordering as unsigned int.
        int v = key;
        if (v < 0) {

            v = v - 0x80000000;

        } else {
            
            v = 0x80000000 + v;
            
        }
		final byte[] buf = new byte[4];
		buf[0] = (byte) (v >>> 24);
		buf[1] = (byte) (v >>> 16);
		buf[2] = (byte) (v >>> 8);
		buf[3] = (byte) (v >>> 0);
		return buf;
    }

	// TODO contains with an Object clear needs to test for the Object, not just
	// the key.  Maybe do this as a wrapper similar to BigdataMap?
    public boolean contains(final Object obj) {
        //contains(obj.hashCode());
    	throw new UnsupportedOperationException();
    }

	public boolean contains(final int key) {
		return contains(i2k(key));
	}

	/**
	 * Return <code>true</code> iff there is at least one tuple in the hash tree
	 * having the specified <i>key</i>.
	 * 
	 * @param key
	 *            The key.
	 * @return <code>true</code> iff the hash tree contains at least one tuple
	 *         with that <i>key</i>.
	 * @throws IllegalArgumentException
	 *             if the <i>key</i> is <code>null</code>.
	 * 
	 *             TODO Parallel to insert(), consider a contains() signature
	 *             which permits testing for a specific value as well. Maybe
	 *             in a wrapper class?
	 */
	public boolean contains(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page but do not lazily
			// create new Directory if not present
			final AbstractPage child = current.getChildIfPresent(hashBits);
			if (child == null)
				return false;
			
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, update it.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				if(!bucketPage.contains(key, buddyOffset)) {
					return false;
				}
				return true;
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

//    public void lookup(final Object obj) {
//        lookup(obj.hashCode());
//    }

	/**
	 * Return the first value for the key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The first value for the key -or- <code>null</code> if there are
	 *         no tuples in the index having that key. Note that the return
	 *         value is not diagnostic if the application allows
	 *         <code>null</code> values into the index.
	 */
    public byte[] lookupFirst(final int key) {
    	return lookupFirst(i2k(key));
    }

	/**
	 * Return the first value for the key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The first value for the key -or- <code>null</code> if there are
	 *         no tuples in the index having that key. Note that the return
	 *         value is not diagnostic if the application allows
	 *         <code>null</code> values into the index.
	 */
	public byte[] lookupFirst(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page and ensure a new
			// child is not created on lookup
			final AbstractPage child = current.getChildIfPresent(hashBits);
			if (child == null)
				return null;
			
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, search it for a match.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				return bucketPage.lookupFirst(key, buddyOffset);
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

	/**
	 * Return an iterator which will visit each tuple in the index having the
	 * specified key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The iterator and never <code>null</code>.
	 */
	public ITupleIterator lookupAll(final int key) {
		return lookupAll(i2k(key));
	}

	/**
	 * Return an iterator which will visit each tuple in the index having the
	 * specified key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The iterator and never <code>null</code>.
	 */
	public ITupleIterator lookupAll(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page.
			// Ensure we do not create a child for lookup 
			final AbstractPage child = current.getChildIfPresent(hashBits);
			if (child == null)
				return emptyTupleIterator();
			
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, search it for a match.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				return bucketPage.lookupAll(key);//, buddyOffset);
			} else if (((DirectoryPage)child).isOverflowDirectory()) {
				return ((DirectoryPage) child).getTuples();
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

    private ITupleIterator emptyTupleIterator() {
		return new ITupleIterator() {

			public ITuple next() {
				throw new NoSuchElementException();
			}

			public boolean hasNext() {
				return false;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	public void insert(final Object obj) {
        insert(obj.hashCode(), SerializerUtil.serialize(obj));
    }

	public void insert(final int key, final byte[] val) {
		insert(i2k(key), val);
	}

	/**
	 * Insert a tuple into the hash tree. Tuples with duplicate keys and even
	 * tuples with duplicate keys and values are allowed and will result in
	 * multiple tuples.
	 * 
	 * @param key
	 *            The key.
	 * @param value
	 *            The value.
	 * @return <code>null</code> (always).
	 * 
	 *         TODO If the application wants to restrict the hash tree to such
	 *         that it does not contain duplicate tuples then it must first
	 *         search in the tree for an exact match (key and value). It is
	 *         easier to do that from within the insert logic so expand the
	 *         method signature to pass an insert enum {ALLDUPS,DUPKEYS,NODUPS}.
	 */
	public byte[] insert(final byte[] key, final byte[] value) {

        if (DEBUG)
            log.debug("key=" + BytesUtil.toString(key) + ", value="
                    + Arrays.toString(value));

		if (key == null)
			throw new IllegalArgumentException();
		
		// the current directory page.
		DirectoryPage current = getRoot(); // start at the root.
		
		// #of prefix bits already consumed.
		int prefixLength = 0;// prefix length of the root is always zero.
		
		// buddyOffset into [current].
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		
		while (true) {

			// skip prefixLength bits and then extract globalDepth bits. 
			assert !(prefixLength >= key.length * 8 && !current.isOverflowDirectory());
			
		    final int hashBits = current.getLocalHashCode(key, prefixLength);

		    // find the child directory page or bucket page.
			final AbstractPage child;
			if (current.isOverflowDirectory()) {
				if (!BytesUtil.bytesEqual(key, current.getOverflowKey())) {
					// if not the overflowKey, then insert extra level
					final DirectoryPage pd = current.getParentDirectory();
					
					assert !pd.isOverflowDirectory();
					assert pd.isReadOnly() ? current.isReadOnly() : true;
					
					current = pd._addLevelForOverflow(current);
					// we need to fix this since we have introduced a new level
					// between the old current and its parent directory
					child = current;
					// and frig prefixlength to come back in
					prefixLength -= current.globalDepth;
					
//					if (DEBUG)
//						log.debug("frig prefixLength: " + prefixLength);
				} else {
					child = current.lastChild();
				}
			} else {				
				child = current.getChild(hashBits, buddyOffset);
			}
			
			if (child.isLeaf()) {

				/*
				 * Found the bucket page, update it.
				 * 
				 * We must copyOnWrite now since the insert will otherwise call it
				 * and potentially invalidate the bucketPage reference.
				 */
			    final BucketPage bucketPage = (BucketPage) child.copyOnWrite();
			    
				// Attempt to insert the tuple into the bucket.
				if (!bucketPage.insert(key, value)) {
					if (current.globalDepth == child.globalDepth) {

                        /*
                         * The child is at the same depth as the parent. Either
                         * we can split a directory page which is a parent of
                         * that bucket or we have to add a new level below the
                         * root on the path to that bucket.
                         */

                        if (child.globalDepth == addressBits) {

                        	bucketPage.addLevel();
                        	
                        } else {

                        	current.getParentDirectory().split(buddyOffset, current/* oldChild */);
                           
                        }

                    } else {

                        // globalDepth >= localDepth
                        bucketPage.split();

                    }
		
                    return insert(key,value);

				}
				
				return null; // TODO should be Void return? or depends on enum controlling dups behavior?
				
			}

			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			
			// increase prefix length by the #of address bits consumed by the
			// buddy hash table in the current directory page as we descend.
			prefixLength = prefixLength + current.globalDepth;
			
			// find the offset of the buddy hash table in the child.
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			
			// update current so we can search in the child.
			current = (DirectoryPage) child;
			
		}

	} // insert()
	
	/**
	 * Insert a tuple into the hash tree. Tuples with duplicate keys and even
	 * tuples with duplicate keys and values are allowed and will result in
	 * multiple tuples.
	 * 
	 * @param src
	 *            The source {@link BucketPage} src
	 * @param slot
	 *            The slot in the source {@link BucketPage} whose tuple will be
	 *            inserted (really copied).
	 */
	protected void insertRawTuple(final BucketPage src, final int slot) {

		if (src == null)
			throw new IllegalArgumentException();

		if (slot < 0 || slot >= (1 << addressBits)) // [0:slotsPerPage].
			throw new IllegalArgumentException();

		// the key to insert
		final byte[] key = src.getKeys().get(slot);
		
		if (key == null)
			throw new IllegalArgumentException();
		
		if (DEBUG)
			log.debug("key=" + BytesUtil.toString(key));

		// the current directory page.
		DirectoryPage current = getRoot(); // start at the root.
		
		// #of prefix bits already consumed.
		int prefixLength = 0;// prefix length of the root is always zero.
		
		// buddyOffset into [current].
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		
		while (true) {

			// skip prefixLength bits and then extract globalDepth bits. 
		    final int hashBits = current.getLocalHashCode(key, prefixLength);
			
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			
			if (child.isLeaf()) {

				/*
				 * Found the bucket page, update it.
				 * 
				 * Must ensure we have copyOnWriteVersion since otherwise the bucketPage
				 * later referenced will not be valid
				 */

			    final BucketPage bucketPage = (BucketPage) child.copyOnWrite();
				
				// Attempt to insert the tuple into the bucket.
                if (!bucketPage.insertRawTuple(src, slot, key)) {

                    if (current.globalDepth == child.globalDepth) {

                        /*
                         * The child is at the same depth as the parent. Either
                         * we can split a directory page which is a parent of
                         * that bucket or we have to add a new level below the
                         * root on the path to that bucket.
                         */

                        if (child.globalDepth == addressBits) {

                            // addLevel2(bucketPage);
                        	bucketPage.addLevel();

                        } else {

                        	current.getParentDirectory().split(
                                    buddyOffset, current/* oldChild */);

                        }

                    } else {

                        // globalDepth >= localDepth
                        bucketPage.split();

                    }

                    insertRawTuple(src, slot);
                    return;

				}
				
				return;
				
			}

			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			
			// increase prefix length by the #of address bits consumed by the
			// buddy hash table in the current directory page as we descend.
			prefixLength = prefixLength + current.globalDepth;
			
			// find the offset of the buddy hash table in the child.
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			
			// update current so we can search in the child.
			current = (DirectoryPage) child;
			
		}

	} // insertRawTuple()

	/**
	 * Removes a single entry matching the key supplied.
	 * The HTree provides no guarantee that the order in which a value
	 * with a duplicate key is added will be the order in which it
	 * would be removed - FIFO.  It is more likely to be in LIFO
	 * 
	 * @param key
	 * @return the value removed or null if none found
	 */
	public byte[] remove(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		
		final AbstractPage page = locatePageForKey(key);
		
		final byte[] ret =  page == null ? null : page.removeFirst(key);
		
		if (ret != null) nentries--;
		
		return ret;
	}

	public int removeAll(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		
		final AbstractPage page = locatePageForKey(key);
		
		final int removals =  page == null ? 0 : page.removeAll(key);
		nentries -= removals;
		
		return removals;
	}

	/**
	 * 
	 */
	protected AbstractPage locatePageForKey(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page, ensuring that
			// new Directory is not created if not present
			final AbstractPage child = current.getChildIfPresent(hashBits);
			
			if (child == null || child.isLeaf() || ((DirectoryPage) child).isOverflowDirectory()) {
				return child;
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			current = (DirectoryPage) child;
		}
	}

    public void removeAll() {
		
		DirectoryPage root = getRoot();
		root.removeAll();
				
		newRoot();
	}

	public long rangeCount() {
		
		return nentries;
	
	}

	/**
	 * Pretty print the {@link HTree}.
	 */
	String PP() {
		return PP(true);
	}
	
	String PP(final boolean showBinary) {
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("#nodes=" + nnodes + ", #leaves=" + nleaves + ", #entries="
				+ nentries + "\n");

		root.PP(sb, showBinary);
		
		return sb.toString();
	
	}

	@Override
    public BaseIndexStats dumpPages(final boolean recursive, final boolean visitLeaves) {

        if (!recursive) {

            return new BaseIndexStats(this);

        }

        final HTreePageStats stats = new HTreePageStats();

        getRoot().dumpPages(recursive, visitLeaves, stats);

        return stats;

    }

	/**
	 * Create a new {@link HTree} or derived class. This method works by writing
	 * the {@link IndexMetadata} record on the store and then loading the
	 * {@link HTree} from the {@link IndexMetadata} record.
	 * 
	 * @param store
	 *            The store.
	 * 
	 * @param metadata
	 *            The metadata record.
	 * 
	 * @return The newly created {@link HTree}.
	 * 
	 * @see #load(IRawStore, long, boolean)
	 * 
	 * @throws IllegalStateException
	 *             If you attempt to create two {@link HTree} objects from the
	 *             same metadata record since the metadata address will have
	 *             already been noted on the {@link IndexMetadata} object. You
	 *             can use {@link IndexMetadata#clone()} to obtain a new copy of
	 *             the metadata object with the metadata address set to
	 *             <code>0L</code>.
     * @exception IllegalStateException
     *                if the {@link IndexTypeEnum} in the supplied
     *                {@link IndexMetadata} object is not
     *                {@link IndexTypeEnum#BTree}.
	 */
	public static HTree create(final IRawStore store,
			final HTreeIndexMetadata metadata) {

	    if (metadata.getIndexType() != IndexTypeEnum.HTree) {

            throw new IllegalStateException("Wrong index type: "
                    + metadata.getIndexType());

        }

		if (store == null) {

			// Create an htree which is NOT backed by a persistence store.
			return createTransient(metadata);

		}

        if (metadata.getMetadataAddr() != 0L) {

            throw new IllegalStateException("Metadata record already in use");
            
        }
        
        /*
         * Write metadata record on store. The address of that record is set as
         * a side-effect on the metadata object.
         */
        metadata.write(store);

        /*
         * Create checkpoint for the new H+Tree.
         */
        final Checkpoint firstCheckpoint = metadata.firstCheckpoint();
        
        /*
         * Write the checkpoint record on the store. The address of the
         * checkpoint record is set on the object as a side effect.
         */
        firstCheckpoint.write(store);
        
        /*
         * Load the HTree from the store using that checkpoint record. There is
         * no root so a new root leaf will be created when the HTree is opened.
         */
        return load(store, firstCheckpoint.getCheckpointAddr(), false/* readOnly */);
        
    }

    /**
     * Create a new {@link HTree} or derived class that is fully transient (NO
     * backing {@link IRawStore}).
     * <p>
     * Fully transient {@link HTree}s provide the functionality of a {@link HTree}
     * without a backing persistence store. Internally, reachable nodes and
     * leaves of the transient {@link HTree} use hard references to ensure that
     * remain strongly reachable. Deleted nodes and leaves simply clear their
     * references and will be swept by the garbage collector shortly thereafter.
     * <p>
     * Operations which attempt to write on the backing store will fail.
     * <p>
     * While nodes and leaves are never persisted, the keys and values of the
     * transient {@link HTree} are unsigned byte[]s. This means that application
     * keys and values are always converted into unsigned byte[]s before being
     * stored in the {@link HTree}. Hence if an object that is inserted into
     * the {@link HTree} and then looked up using the {@link HTree} API, you
     * WILL NOT get back the same object reference.
     * <p>
     * Note: CLOSING A TRANSIENT INDEX WILL DISCARD ALL DATA!
     * 
     * @param metadata
     *            The metadata record.
     * 
     * @return The transient {@link HTree}.
     */
    @SuppressWarnings("unchecked")
    public static HTree createTransient(final HTreeIndexMetadata metadata) {
        
        if (metadata.getIndexType() != IndexTypeEnum.HTree) {

            throw new IllegalStateException("Wrong index type: "
                    + metadata.getIndexType());

        }

        /*
         * Create checkpoint for the new HTree.
         */
        final Checkpoint firstCheckpoint = metadata.firstCheckpoint();

        /*
         * Create B+Tree object instance.
         */
        try {

            final Class cl = Class.forName(metadata.getHTreeClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class,//
                    Boolean.TYPE//
                    });

            final HTree htree = (HTree) ctor.newInstance(new Object[] { //
                    null , // store
                    firstCheckpoint, //
                    metadata, //
                    false// readOnly
                    });

            // create the root node.
            htree.reopen();

            return htree;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
    }

	/**
	 * Load an instance of a {@link HTree} or derived class from the store. The
	 * {@link HTree} or derived class MUST declare a constructor with the
	 * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata, boolean readOnly)
     * 
     * </code>
	 * 
	 * @param store
	 *            The store.
	 * @param addrCheckpoint
	 *            The address of a {@link Checkpoint} record for the index.
	 * @param readOnly
	 *            When <code>true</code> the {@link BTree} will be marked as
	 *            read-only. Marking has some advantages relating to the locking
	 *            scheme used by {@link Node#getChild(int)} since the root node
	 *            is known to be read-only at the time that it is allocated as
	 *            per-child locking is therefore in place for all nodes in the
	 *            read-only {@link BTree}. It also results in much higher
	 *            concurrency for {@link AbstractBTree#touch(AbstractNode)}.
	 * 
	 * @return The {@link HTree} or derived class loaded from that
	 *         {@link Checkpoint} record.
	 * 
	 * @throws IllegalArgumentException
	 *             if store is <code>null</code>.
	 */
    @SuppressWarnings("unchecked")
    public static HTree load(final IRawStore store, final long addrCheckpoint,
            final boolean readOnly) {

		if (store == null)
			throw new IllegalArgumentException();
    	
        /*
         * Read checkpoint record from store.
         */
		final Checkpoint checkpoint;
		try {
			checkpoint = Checkpoint.load(store, addrCheckpoint);
		} catch (Throwable t) {
			throw new RuntimeException("Could not load Checkpoint: store="
					+ store + ", addrCheckpoint="
					+ store.toString(addrCheckpoint), t);
		}

		if (checkpoint.getIndexType() != IndexTypeEnum.HTree)
			throw new RuntimeException("Not an HTree checkpoint: " + checkpoint);

		/*
		 * Read metadata record from store.
		 */
		final HTreeIndexMetadata metadata;
		try {
			metadata = (HTreeIndexMetadata) IndexMetadata.read(store,
					checkpoint.getMetadataAddr());
		} catch (Throwable t) {
			throw new RuntimeException("Could not read IndexMetadata: store="
					+ store + ", checkpoint=" + checkpoint, t);
		}

        if (INFO) {

            // Note: this is the scale-out index name for a partitioned index.
            final String name = metadata.getName();

            log.info((name == null ? "" : "name=" + name + ", ")
                    + "readCheckpoint=" + checkpoint);

        }

        /*
         * Create HTree object instance.
         */
        try {

            final Class cl = Class.forName(metadata.getHTreeClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class, //
                    Boolean.TYPE
                    });

            final HTree htree = (HTree) ctor.newInstance(new Object[] { //
                    store,//
                    checkpoint, //
                    metadata, //
                    readOnly
                    });

//            if (readOnly) {
//
//                btree.setReadOnly(true);
//
//            }

            // read the root node.
            htree.reopen();

            return htree;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

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
        
        // getBtreeCounters().bytesReleased += nbytes;
        
        store.delete(addr);
        
        return nbytes;

    }

}
