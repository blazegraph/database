/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IBloomFilter;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndexLocalCounter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.Node;
import com.bigdata.btree.ReadOnlyCounter;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.htree.raba.MutableKeyBuffer;
import com.bigdata.htree.raba.MutableValueBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * An mutable persistence capable extensible hash tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO It should be possible to define an native int32 hash table in
 *          parallel to the unsigned byte[] hash table simply by having an
 *          alternative descent passing an int32 key all the way down and using
 *          the variant of getBits() method which operates on the int32 values.
 *          We could also optimize the storage and retrieval of the int32 keys,
 *          perhaps with a custom mutable bucket page and mutable bucket data
 *          implementation for int32 keys. This optimization is likely to be
 *          quite worth while as the majority of use cases for the hash tree use
 *          int32 keys.
 * 
 *          TODO It is quite possible to define a range query interface for the
 *          hash tree. You have to use an order preserving hash function, which
 *          is external to the HTree implementation. Internally, the HTree must
 *          either double-link the pages or crawl the directory structure.
 * 
 *          TODO The keys should be declared as a computed key based on the data
 *          fields in the record. The {@link HTree} supports arbitrary bit
 *          length keys, but can be optimized for int32 keys easily enough.
 * 
 *          TODO Instrument performance counters for structural modifications,
 *          insert, remove, etc. per {@link BTreeCounters}.
 */
public class HTree extends AbstractHTree 
	implements 
	IIndexLocalCounter,
	ICheckpointProtocol
//	IIndex, 
//  ISimpleBTree//, IAutoboxBTree, ILinearList, IBTreeStatistics, ILocalBTreeView
//  IRangeQuery
{

    private static final transient Logger log = Logger.getLogger(HTree.class);

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
    
    final public long getNodeCount() {
        
        return nnodes;
        
    }

    final public long getLeafCount() {
        
        return nleaves;
        
    }

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
    
    final public Checkpoint getCheckpoint() {

        if (checkpoint == null)
            throw new AssertionError();
        
        return checkpoint;
        
    }
    
    final public long getRecordVersion() {
    	
    	return recordVersion;

    }
    
    final public long getMetadataAddr() {

    	return metadata.getMetadataAddr();

    }
        
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
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/288
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/278
		 */
		final Lock lock = UnisolatedReadWriteIndex.getReadWriteLock(this).writeLock();
		try {

			if (/* autoCommit && */needsCheckpoint()) {

				/*
				 * Flush the btree, write a checkpoint record, and return the
				 * address of that checkpoint record. The [checkpoint] reference
				 * is also updated.
				 */

				return writeCheckpoint();

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

			return checkpoint.getCheckpointAddr();

		} finally {

			lock.unlock();

		}

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
     * Required constructor form for {@link BTree} and any derived subclasses.
     * This constructor is used both to create a new {@link BTree}, and to load
     * a {@link BTree} from the store using a {@link Checkpoint} record.
     * 
     * @param store
     *            The store.
     * @param checkpoint
     *            A {@link Checkpoint} record for that {@link BTree}.
     * @param metadata
     *            The metadata record for that {@link BTree}.
     * @param readOnly
     *            When <code>true</code> the {@link BTree} will be immutable.
     * 
     * @see BTree#create(IRawStore, IndexMetadata)
     * @see BTree#load(IRawStore, long, boolean)
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
                metadata,//
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
	 * ê * checkpoint record. In order for this operation to be atomic, the
	 * caller must be synchronized on the {@link HTree} or otherwise guaranteed
	 * to have exclusive access, e.g., during the ctor or when the {@link HTree}
	 * is mutable and access is therefore required to be single-threaded.
	 */
    private void setCheckpoint(final Checkpoint checkpoint) {

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
    
    final public void setLastCommitTime(final long lastCommitTime) {
        
        if (lastCommitTime == 0L)
            throw new IllegalArgumentException();
        
        if (this.lastCommitTime == lastCommitTime) {

            // No change.
            
            return;
            
        }
        
        if (log.isInfoEnabled())
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
            
            if(log.isInfoEnabled())
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
     * @see #writeCheckpoint2(), which returns the {@link Checkpoint} record
     *      itself.
     *      
     * @see #load(IRawStore, long)
     */
    final public long writeCheckpoint() {
    
        // write checkpoint and return address of that checkpoint record.
        return writeCheckpoint2().getCheckpointAddr();
        
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
     * @return The {@link Checkpoint} record for the {@link BTree} was written
     *         onto the store. The {@link BTree} can be reloaded from this
     *         {@link Checkpoint} record.
     * 
     * @see #load(IRawStore, long)
     */
    final public Checkpoint writeCheckpoint2() {
        
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
             * The index metadata has been modified so we write out a new
             * metadata record on the store.
             */
            
            metadata.write(store);
            
        }
        
        // create new checkpoint record.
        checkpoint = metadata.newCheckpoint(this);
        
        // write it on the store.
        checkpoint.write(store);
        
        if (BigdataStatics.debug||log.isInfoEnabled()) {
            final String msg = "name=" + metadata.getName()
                    + ", writeQueue{size=" + writeRetentionQueue.size()
                    + ",distinct=" + ndistinctOnWriteRetentionQueue + "} : "
                    + checkpoint;
            if (BigdataStatics.debug)
                System.err.println(msg);
            if (log.isInfoEnabled())
                log.info(msg);
        }
        
        // return the checkpoint record.
        return checkpoint;
        
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
     */
    private byte[] i2k(final int key) {
		final byte[] buf = new byte[4];
		buf[0] = (byte) (key >>> 24);
		buf[1] = (byte) (key >>> 16);
		buf[2] = (byte) (key >>> 8);
		buf[3] = (byte) (key >>> 0);
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
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
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
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
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
	 * 
	 * TODO Unit tests for lookupAll(byte[] key)
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
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, search it for a match.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				return bucketPage.lookupAll(key);//, buddyOffset);
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

        if (log.isInfoEnabled())
            log.info("key=" + BytesUtil.toString(key) + ", value="
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
		    final int hashBits = current.getLocalHashCode(key, prefixLength);
			
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			
			if (child.isLeaf()) {

				/*
				 * Found the bucket page, update it.
				 */
			    final BucketPage bucketPage = (BucketPage) child;
			    
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

                        	// addLevel2(bucketPage);
                        	bucketPage.addLevel();
                        	
                        } else {

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
                                    buddyOffset, current/* oldChild */);
                            
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
		
		if (log.isInfoEnabled())
			log.info("key=" + BytesUtil.toString(key));

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
				 */

			    final BucketPage bucketPage = (BucketPage) child;
				
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

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
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

	public byte[] remove(final byte[] key) {
		// TODO Remove 1st match, returning value.
		throw new UnsupportedOperationException();
	}

	public void removeAll(final byte[] key) {
		// TODO Remove all matches for the key.
		throw new UnsupportedOperationException();
	}

	public void removeAll() {
		// TODO Remove all entries (newRoot()).  See BTree for how this is done.
		throw new UnsupportedOperationException();
	}

//    /**
//     * Handle split if buddy bucket is full but localDepth LT globalDepth (so
//     * there is more than one buddy bucket on the page). This will allocate a
//     * new bucket page; update the references in the parent, and then
//     * redistribute buddy buckets among the old and new bucket page. The depth
//     * of the child will be increased by one. As a post-condition, the depth of
//     * the new child will be the same as the then current depth of the original
//     * child. Note that this doubles the size of each buddy bucket, thus always
//     * creating room for additional tuples.
//     * <p>
//     * Note: This is really just doubling the address space for the buddy buckets
//     * on the page.  Since each buddy bucket now has twice as many slots, we have
//     * to move 1/2 of the buddy buckets to a new bucket page.
//     * 
//     * @param parent
//     *            The parent {@link DirectoryPage}.
//     * @param buddyOffset
//     *            The buddyOffset within the <i>parent</i>. This identifies
//     *            which buddy hash table in the parent must be its pointers
//     *            updated such that it points to both the original child and new
//     *            child.
//     * @param oldChild
//     *            The child {@link BucketPage}.
//     * 
//     * @throws IllegalArgumentException
//     *             if any argument is <code>null</code>.
//     * @throws IllegalStateException
//     *             if the depth of the child is GTE the depth of the parent.
//     * @throws IllegalStateException
//     *             if the <i>parent<i/> is read-only.
//     * @throws IllegalStateException
//     *             if the <i>oldChild</i> is read-only.
//     * @throws IllegalStateException
//     *             if the parent of the <i>oldChild</i> is not the given
//     *             <i>parent</i>.
//     */
//    // Note: package private for unit tests.
//    void splitBucketsOnPage(final DirectoryPage parent, final int buddyOffset,
//            final BucketPage oldChild) {
//    	
//		if (parent == null)
//			throw new IllegalArgumentException();
//		if (oldChild == null)
//			throw new IllegalArgumentException();
//		if (oldChild.globalDepth >= parent.globalDepth) {
//			// In this case we have to introduce a new directory page instead.
//			throw new IllegalStateException();
//		}
//		if (buddyOffset < 0)
//			throw new IllegalArgumentException();
//		if (buddyOffset >= (1 << addressBits)) {
//			/*
//			 * Note: This check is against the maximum possible slot index. The
//			 * actual max buddyOffset depends on parent.globalBits also since
//			 * (1<<parent.globalBits) gives the #of slots per buddy and the
//			 * allowable buddyOffset values must fall on an buddy hash table
//			 * boundary.
//			 */
//			throw new IllegalArgumentException();
//		}
//		if(parent.isReadOnly()) // must be mutable.
//			throw new IllegalStateException();
//		if(oldChild.isReadOnly()) // must be mutable.
//			throw new IllegalStateException();
//		if (oldChild.parent != parent.self) // must be same Reference.
//			throw new IllegalStateException();
//		
//		if (log.isInfoEnabled())
//			log.info("parent=" + parent.toShortString() + ", buddyOffset="
//					+ buddyOffset + ", child=" + oldChild.toShortString());
//
//		final int oldDepth = oldChild.globalDepth;
//		final int newDepth = oldDepth + 1;
//
//		// Allocate a new bucket page (globalDepth is increased by one).
//		final BucketPage newBucket = new BucketPage(this, newDepth);
//
//		assert newBucket.isDirty();
//		
//		// Set the parent reference on the new bucket.
//		newBucket.parent = (Reference) parent.self;
//		
//		// Increase global depth on the old page also.
//		oldChild.globalDepth = newDepth;
//
//		nleaves++; // One more bucket page in the hash tree. 
//
//		// update the pointers in the parent.
//		updatePointersInParent(parent, buddyOffset, oldDepth, oldChild,
//				newBucket);
//		
//		// redistribute buddy buckets between old and new pages.
//		redistributeBuddyBuckets(oldDepth, newDepth, oldChild, newBucket);
//
//		// TODO assert invariants?
//		
//	}

	/**
	 * Update pointers in buddy hash table in the parent in order to link the
	 * new {@link AbstractPage} into the parent {@link DirectoryPage}.
	 * <p>
	 * There will be [npointers] slots in the appropriate buddy hash table in
	 * the parent {@link DirectoryPage} which point to the old
	 * {@link AbstactPage}. The upper 1/2 of those pointers will be modified to
	 * point to the new {@link AbstractPage}. The lower 1/2 of the pointers will
	 * be unchanged.
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent must have its pointers
	 *            updated such that it points to both the original child and new
	 *            child.
	 * @param oldDepth
	 *            The depth of the oldBucket before the split.
	 * @param oldChild
	 *            The old {@link AbstractPage}.
	 * @param newChild
	 *            The new {@link AbstractPage}.
	 */
	private void updatePointersInParent(final DirectoryPage parent,
			final int buddyOffset, final int oldDepth,
			final AbstractPage oldChild, final AbstractPage newChild) {

		// #of address slots in the parent buddy hash table.
		final int slotsPerBuddy = (1 << parent.globalDepth);

		// #of pointers in the parent buddy hash table to the old child.
		final int npointers = 1 << (parent.globalDepth - oldDepth);
		
		// Must be at least two slots since we will change at least one.
        if (slotsPerBuddy <= 1)
            throw new AssertionError("slotsPerBuddy=" + slotsPerBuddy);

		// Must be at least two pointers since we will change at least one.
		assert npointers > 1 : "npointers=" + npointers;
		
		// The first slot in the buddy hash table in the parent.
		final int firstSlot = buddyOffset;
		
		// The last slot in the buddy hash table in the parent.
		final int lastSlot = buddyOffset + slotsPerBuddy;

        /*
         * Count pointers to the old child page. There should be [npointers] of
         * them and they should be contiguous.
         * 
         * Note: We can test References here rather than comparing addresses
         * because we know that the parent and the old child are both mutable.
         * This means that their childRef is defined and their storage address
         * is NULL.
         */
		int firstPointer = -1;
		int nfound = 0;
		boolean discontiguous = false;
		for (int i = firstSlot; i < lastSlot; i++) {
			if (parent.childRefs[i] == oldChild.self) {
				if (firstPointer == -1)
					firstPointer = i;
				nfound++;
				if (((MutableDirectoryPageData) parent.data).childAddr[i] != IRawStore.NULL) {
					throw new RuntimeException(
							"Child address should be NULL since child is dirty");
				}
			} else {
				if (firstPointer != -1 && nfound != npointers) {
					discontiguous = true;
				}
			}
		}
		if (firstPointer == -1)
			throw new RuntimeException("No pointers to child");
		if (nfound != npointers)
			throw new RuntimeException("Expected " + npointers
					+ " pointers to child, but found=" + nfound);
		if (discontiguous)
			throw new RuntimeException(
					"Pointers to child are discontiguous in parent's buddy hash table.");

		// Update the upper 1/2 of the pointers to the new bucket.
		for (int i = firstPointer + (npointers >> 1); i < npointers; i++) {

			if (parent.childRefs[i] != oldChild.self)
				throw new RuntimeException("Does not point to old child.");
			
			// update the references to the new bucket.
			parent.childRefs[i] = (Reference) newChild.self;
			
		}
			
	} // updatePointersInParent

//	/**
//	 * Redistribute the buddy buckets.
//	 * <p>
//	 * Note: We are not changing the #of buckets, just their size and the
//	 * page on which they are found. Any tuples in a source bucket will wind up
//	 * in the "same" bucket afterwards, but the page and offset on the page of the
//	 * bucket may have been changed and the size of the bucket will have doubled.
//	 * <p>
//	 * We proceed backwards, moving the upper half of the buddy buckets to the
//	 * new bucket page first and then spreading out the lower half of the source
//	 * page among the new bucket boundaries on the page.
//	 * 
//	 * @param oldDepth
//	 *            The depth of the old {@link BucketPage} before the split.
//	 * @param newDepth
//	 *            The depth of the old and new {@link BucketPage} after the
//	 *            split (this is just oldDepth+1).
//	 * @param oldBucket
//	 *            The old {@link BucketPage}.
//	 * @param newBucket
//	 *            The new {@link BucketPage}.
//	 */
//	private void redistributeBuddyBuckets(final int oldDepth,
//			final int newDepth, final BucketPage oldBucket,
//			final BucketPage newBucket) {
//
//	    assert oldDepth + 1 == newDepth;
//	    
//		// #of slots on the bucket page (invariant given addressBits).
//		final int slotsOnPage = (1 << oldBucket.slotsOnPage());
//
//		// #of address slots in each old buddy hash bucket.
//		final int slotsPerOldBuddy = (1 << oldDepth);
//
//		// #of address slots in each new buddy hash bucket.
//		final int slotsPerNewBuddy = (1 << newDepth);
//
//		// #of buddy tables on the old bucket page.
//		final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;
//
//		// #of buddy tables on the bucket pages after the split.
//		final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;
//
//		final BucketPage srcPage = oldBucket;
//		final MutableKeyBuffer srcKeys = (MutableKeyBuffer) oldBucket.getKeys();
//		final MutableValueBuffer srcVals = (MutableValueBuffer) oldBucket
//				.getValues();
//
//		/*
//		 * Move top 1/2 of the buddy buckets from the child to the new page.
//		 */
//		{
//
//			// target is the new page.
//			final BucketPage dstPage = newBucket;
//			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
//					.getKeys();
//			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
//					.getValues();
//
//			// index (vs offset) of first buddy in upper half of src page.
//			final int firstSrcBuddyIndex = (oldBuddyCount >> 1);
//
//			// exclusive upper bound for index (vs offset) of last buddy in
//			// upper half of src page.
//			final int lastSrcBuddyIndex = oldBuddyCount;
//
//			// exclusive upper bound for index (vs offset) of last buddy in
//			// upper half of target page.
//			final int lastDstBuddyIndex = newBuddyCount;
//
//			// work backwards over buddy buckets to avoid stomping data!
//			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
//			srcBuddyIndex >= firstSrcBuddyIndex; //
//			srcBuddyIndex--, dstBuddyIndex--//
//			) {
//
//				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;
//
//				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;
//
//				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;
//
//				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {
//
//					if (log.isTraceEnabled())
//						log.trace("moving: page(" + srcPage.toShortString()
//								+ "=>" + dstPage.toShortString() + ")"
//								+ ", buddyIndex(" + srcBuddyIndex + "=>"
//								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
//								+ "=>" + dstSlot + ")");
//
//                    // Move the tuple at that slot TODO move metadata also.
//					if(srcKeys.keys[srcSlot] == null)
//					    continue;
//					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
//					dstVals.values[dstSlot] = srcVals.values[srcSlot];
//					srcKeys.keys[srcSlot] = null;
//					srcVals.values[srcSlot] = null;
//                    dstKeys.nkeys++; // one more in that page.
//                    srcKeys.nkeys--; // one less in this page.
//                    dstVals.nvalues++; // one more in that page.
//                    srcVals.nvalues--; // one less in this page.
//
//				}
//
//			}
//
//		}
//
//		/*
//		 * Reposition the bottom 1/2 of the buddy buckets on the old page.
//		 * 
//		 * Again, we have to move backwards through the buddy buckets on the
//		 * source page to avoid overwrites of data which has not yet been
//		 * copied. Also, notice that the buddy bucket at index ZERO does not
//		 * move - it is already in place even though it's size has doubled.
//		 */
//		{
//
//			// target is the old page.
//			final BucketPage dstPage = oldBucket;
//			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
//					.getKeys();
//			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
//					.getValues();
//
//			// index (vs offset) of first buddy in lower half of src page.
//			final int firstSrcBuddyIndex = 0;
//
//			// exclusive upper bound for index (vs offset) of last buddy in
//			// lower half of src page.
//			final int lastSrcBuddyIndex = (oldBuddyCount >> 1);
//
//			// exclusive upper bound for index (vs offset) of last buddy in
//			// upper half of target page (which is also the source page).
//			final int lastDstBuddyIndex = newBuddyCount;
//
//			/*
//			 * Work backwards over buddy buckets to avoid stomping data!
//			 * 
//			 * Note: The slots for first buddy in the lower half of the source
//			 * page DO NOT MOVE. The offset of that buddy in the first page
//			 * remains unchanged. Only the size of the buddy is changed (it is
//			 * doubled, just like all the other buddies on the page).
//			 */
//			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
//			srcBuddyIndex > firstSrcBuddyIndex; // DO NOT move 1st buddy bucket!
//			srcBuddyIndex--, dstBuddyIndex--//
//			) {
//
//				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;
//
//				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;
//
//				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;
//
//				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {
//
//					if (log.isTraceEnabled())
//						log.trace("moving: page(" + srcPage.toShortString()
//								+ "=>" + dstPage.toShortString() + ")"
//								+ ", buddyIndex(" + srcBuddyIndex + "=>"
//								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
//								+ "=>" + dstSlot + ")");
//
//					// Move the tuple at that slot TODO move metadata also.
//                    if(srcKeys.keys[srcSlot] == null)
//                        continue;
//					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
//					dstVals.values[dstSlot] = srcVals.values[srcSlot];
//					srcKeys.keys[srcSlot] = null;
//					srcVals.values[srcSlot] = null;
//					// Note: movement within same page: nkeys/nvals don't change
//
//				}
//
//			}
//
//		}
//
//	}

	    /**
     * Adds a new level above a full {@link BucketPage}. The {@link BucketPage}
     * must be at depth==addressBits (i.e., one buddy bucket on the page and one
     * pointer from the parent into the {@link BucketPage}). Two new
     * {@link BucketPage}s and a new {@link DirectoryPage} are recruited. The
     * new {@link DirectoryPage} is initialized with pointers to the two
     * {@link BucketPage}s and linked into the parent of the original
     * {@link BucketPage}. The new {@link DirectoryPage} will be at maximum
     * depth since it takes the place of the old {@link BucketPage} which was
     * already at maximum depth. The tuples from the old {@link BucketPage} are
     * reindexed, which distributes them between the two new {@link BucketPage}
     * s.
     * 
     * @param oldPage
     *            The full {@link BucketPage}.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the <i>oldPage</i> is not at maximum depth.
     * @throws IllegalStateException
     *             if the parent of the <i>oldPage</i> is not mutable.
     * 
     *             TODO This should really be moved onto BucketPage so the
     *             copy-on-write pattern can be maintained.
     *             <p>
     *             We could pass in the buddy offset on the directory, which
     *             would reduce the amount that we need to scan when locating
     *             the pointer to [oldPage].
     *             <p>
     *             We do not need to be doing copy-on-write on [oldPage]. It is
     *             only [oldPage.parent] which needs to be mutable.
     */
	DirectoryPage addLevel2(final BucketPage oldPage) {
		
		if (oldPage == null)
			throw new IllegalArgumentException();

		if (oldPage.globalDepth != addressBits)
			throw new IllegalStateException();

		if (oldPage.isDeleted())
			throw new IllegalStateException();

        /*
         * Note: This is one of the few gateways for mutation of a leaf via the
         * main btree API (insert, lookup, delete). By ensuring that we have a
         * mutable leaf here, we can assert that the leaf must be mutable in
         * other methods.
         */
        final BucketPage copy = (BucketPage) oldPage.copyOnWrite();

        if (copy != oldPage) {

			/*
			 * This leaf has been copied so delegate the operation to the new
			 * leaf.
			 * 
			 * Note: copy-on-write deletes [this] leaf and delete() notifies any
			 * leaf listeners before it clears the [leafListeners] reference so
			 * not only don't we have to do that here, but we can't since the
			 * listeners would be cleared before we could fire off the event
			 * ourselves.
			 */

			return addLevel2(copy);
			
		}

        // The parent directory page above the old bucket page.
		final DirectoryPage pp = (DirectoryPage) oldPage.getParentDirectory();

		if (pp.isReadOnly())
			throw new IllegalStateException(); // parent must be mutable.
		if(!(pp.data instanceof MutableDirectoryPageData))
			throw new IllegalStateException(); // parent must be mutable (should follow automatically if parent is dirty).

		if (log.isInfoEnabled())
			log.info("bucketPage=" + oldPage.toShortString());
		
		// #of slots on a page.
        final int bucketSlotsPerPage = oldPage.slotsOnPage();
        final int dirSlotsPerPage =  1 << this.addressBits;

		// allocate new nodes and relink the tree.
		final DirectoryPage newParent;
		{

			// 1/2 of the slots will point to each of the new bucket pages.
			final int npointers = dirSlotsPerPage >> 1;

			// The local depth of the new bucket pages.
			final int localDepth = HTreeUtil.getLocalDepth(addressBits,
					addressBits/* globalDepth(newParent) */, npointers);

			newParent = new DirectoryPage(this, addressBits/* globalDepth */);
			nnodes++;

			final BucketPage a = new BucketPage(this, localDepth);
			final BucketPage b = new BucketPage(this, localDepth);
			nleaves++; // Note: only +1 since we will delete the oldPage.

			/*
			 * Replace the pointer to the oldPage in the parent with the new
			 * directory page.
			 */
			{
				final long oldAddr = oldPage.isPersistent() ? oldPage
						.getIdentity() : 0L;
				if (!pp.isDirty()) { 
					/*
					 * Note: I have seen the parent go from mutable to immutable
					 * during an addLevel(). This is presumably because I was
					 * using a small retention queue (10) in a stress test.
					 */
					throw new IllegalStateException(); // parent must be mutable.
				}
				final MutableDirectoryPageData data = (MutableDirectoryPageData) pp.data;
                boolean found = false;
                int np = 0;
				for (int i = 0; i < dirSlotsPerPage; i++) {
				    boolean isPtr = false;
					if (oldPage.isPersistent()) {
						if (data.childAddr[i] == oldAddr)
							isPtr = true; // same address
					} else {
						if (pp.childRefs[i] == oldPage.self)
							isPtr = true; // same reference
					}
					if (isPtr) {
						pp.childRefs[i] = (Reference) newParent.self; // set ref
						data.childAddr[i] = 0L; // clear addr.
						np++;
						found = true;
					} else if(found) {
					    break;
					}
				}
				if (!found)
                    throw new AssertionError();
                if (np != 1)
                    throw new AssertionError("Found np="+np+", but expecting only one.");
			}

			// Set the parent references on the new pages.
			newParent.parent = (Reference) pp.self;
			a.parent = (Reference) newParent.self;
			b.parent = (Reference) newParent.self;

			// Link the new bucket pages into the new parent directory page.
			for (int i = 0; i < dirSlotsPerPage; i++) {
				newParent.childRefs[i] = (Reference) (i < npointers ? a.self
						: b.self);
			}

			if (oldPage.isPersistent()) {
				// delete oldPage.
				store.delete(oldPage.getIdentity());
			}

		}

		/*
		 * Reindex the tuples in the old bucket page
		 * 
		 * Note: This MUST be done as a low level operation in order to avoid
		 * duplicating raw records and in order to preserve per-tuple metadata,
		 * including version timestamps and deleted tuple markers.
		 * 
		 * Note: the page is full. there should be no empty slots (no null
		 * keys).
		 */
		// reindexTuples(oldPage, a, b);
		//final IRaba okeys = oldPage.getKeys();
		//final IRaba ovals = oldPage.getValues();
		for (int i = 0; i < bucketSlotsPerPage; i++) {

			/*
			 * Note: All keys should be non-null.
			 * 
			 * Note: Use okeys.isNull() to do a fast check for a null key, not
			 * okeys.get(i) != null which forces the key to be materialized
			 * (heap churn).
			 */
//			if (okeys.get(i) != null)
			
			insertRawTuple(oldPage, i);
			//newParent.insertRawTuple(okeys.get(i), ovals.get(i), 0);

		}
		
		return newParent;

	}

	/**
	 * Handle split if localDepth LT globalDepth (so there is more than one
	 * buddy directory on the page) but we need to increase the prefix bit
	 * length in order to make a distinction in some child of this directory
	 * page. This doubles the size of each buddy hash table, thus increasing the
	 * prefix length of the paths to the children by one bit.
	 * <p>
	 * This allocates a new directory page; updates the references to the old
	 * directory page in the parent (one half will point to the old directory
	 * page, the other half will point to the new directory page). Since the #of
	 * references to the old page has changed, its local depth is now different
	 * (there are 1/2 as many references to the old directory page, which means
	 * that it's local depth will be increased by one).
	 * <p>
	 * The buddy hash tables on the old directory page are expanded to twice
	 * their original size, and half of them wind up on the new directory page.
	 * All references in a given buddy hash table are to the same child page, so
	 * the empty slots are filled in as appropriate.
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent must be its pointers
	 *            updated such that it points to both the original child and new
	 *            child.
	 * @param oldChild
	 *            The child {@link DirectoryPage} to be split.
	 * 
	 * @throws IllegalArgumentException
	 *             if any argument is <code>null</code>.
	 * @throws IllegalStateException
	 *             if the depth of the child is GTE the depth of the parent.
	 * @throws IllegalStateException
	 *             if the <i>parent<i/> is read-only.
	 * @throws IllegalStateException
	 *             if the <i>oldChild</i> is read-only.
	 * @throws IllegalStateException
	 *             if the parent of the <i>oldChild</i> is not the given
	 *             <i>parent</i>.
	 */
    private void splitDirectoryPage(final DirectoryPage parent,
            final int buddyOffset, final DirectoryPage oldChild) {

        if(true) {
            /*
             * FIXME We need to update this code to handle the directory page
             * not being at maximum depth to work without the concept of buddy
             * buckets.
             */
            throw new UnsupportedOperationException();
        }
        
        if (parent == null)
            throw new IllegalArgumentException();
        if (oldChild == null)
            throw new IllegalArgumentException();
        if (oldChild.globalDepth >= parent.globalDepth) {
            /*
             * In this case we have to introduce a new directory level instead
             * (increasing the height of the tree at that point).
             */
            throw new IllegalStateException();
        }
        if (buddyOffset < 0)
            throw new IllegalArgumentException();
        if (buddyOffset >= (1 << addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if(parent.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if(oldChild.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldChild.parent != parent.self) // must be same Reference.
            throw new IllegalStateException();
        
        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", child=" + oldChild.toShortString());

        final int oldDepth = oldChild.globalDepth;
        final int newDepth = oldDepth + 1;

        // Allocate a new bucket page (globalDepth is increased by one).
        final DirectoryPage newChild = new DirectoryPage(this, newDepth);

        assert newChild.isDirty();
        
        // Set the parent reference on the new bucket.
        newChild.parent = (Reference) parent.self;
        
        // Increase global depth on the old page also.
        oldChild.globalDepth = newDepth;

        nnodes++; // One more directory page in the hash tree. 

        // update the pointers in the parent.
        updatePointersInParent(parent, buddyOffset, oldDepth, oldChild,
                newChild);
        
        // redistribute buddy buckets between old and new pages.
        redistributeBuddyTables(oldDepth, newDepth, oldChild, newChild);

        // TODO assert invariants?
        
    }

	/**
	 * Redistribute the buddy hash tables in a {@link DirectoryPage}.
	 * <p>
	 * Note: We are not changing the #of hash tables, just their size and the
	 * page on which they are found. Any reference in a source buddy hash table
	 * will wind up in the "same" buddy hash table afterwards, but the page and
	 * offset on the page of the buddy hash table may have been changed and the
	 * size of the buddy hash table will have doubled.
	 * <p>
	 * When a {@link DirectoryPage} is split, the size of each buddy hash table
	 * is doubled. The additional slots in each buddy hash table are filled in
	 * by (a) spacing out the old slot entries in each buddy hash table; and (b)
	 * filling in the uncovered slot with a copy of the previous slot.
	 * <p>
	 * We proceed backwards, moving the upper half of the buddy hash tables to
	 * the new directory page first and then spreading out the lower half of the
	 * source page among the new buddy hash table boundaries on the source page.
	 * 
	 * @param oldDepth
	 *            The depth of the old {@link DirectoryPage} before the split.
	 * @param newDepth
	 *            The depth of the old and new {@link DirectoryPage} after the
	 *            split (this is just oldDepth+1).
	 * @param oldDir
	 *            The old {@link DirectoryPage}.
	 * @param newDir
	 *            The new {@link DirectoryPage}.
	 * 
	 * @deprecated with
	 *             {@link #splitDirectoryPage(DirectoryPage, int, DirectoryPage)}
	 */
    private void redistributeBuddyTables(final int oldDepth,
            final int newDepth, final DirectoryPage oldDir,
            final DirectoryPage newDir) {

        assert oldDepth + 1 == newDepth;
        
        // #of slots on the directory page (invariant given addressBits).
        final int slotsOnPage = (1 << addressBits);

        // #of address slots in each old buddy hash table.
        final int slotsPerOldBuddy = (1 << oldDepth);

        // #of address slots in each new buddy hash table.
        final int slotsPerNewBuddy = (1 << newDepth);

        // #of buddy tables on the old bucket directory.
        final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;

        // #of buddy tables on the directory page after the split.
        final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;

        final DirectoryPage srcPage = oldDir;
        final long[] srcAddrs = ((MutableDirectoryPageData) oldDir.data).childAddr;
        final Reference<AbstractPage>[] srcRefs = oldDir.childRefs;

        /*
         * Move top 1/2 of the buddy hash tables from the child to the new page.
         */
        {

            // target is the new page.
            final DirectoryPage dstPage = newDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in upper half of src page.
            final int firstSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of src page.
            final int lastSrcBuddyIndex = oldBuddyCount;

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page.
            final int lastDstBuddyIndex = newBuddyCount;

            // work backwards over buddies to avoid stomping data!
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; //
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot += 2) {

                    if (log.isTraceEnabled())
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot
                        dstAddrs[dstSlot + i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot + i] = srcRefs[srcSlot];
                    }

                }

            }

        }

        /*
         * Reposition the bottom 1/2 of the buddy buckets on the old page.
         * 
         * Again, we have to move backwards through the buddy tables on the
         * source page to avoid overwrites of data which has not yet been
         * copied. Also, notice that the buddy table at index ZERO does not
         * move - it is already in place even though it's size has doubled.
         */
        {

            // target is the old page.
            final DirectoryPage dstPage = oldDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in lower half of src page.
            final int firstSrcBuddyIndex = 0;

            // exclusive upper bound for index (vs offset) of last buddy in
            // lower half of src page.
            final int lastSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page (which is also the source page).
            final int lastDstBuddyIndex = newBuddyCount;

            /*
             * Work backwards over buddy buckets to avoid stomping data!
             * 
             * Note: Unlike with a BucketPage, we have to spread out the data in
             * the slots of the first buddy hash table on the lower half of the
             * page as well to fill in the uncovered slots. This means that we
             * have to work backwards over the slots in each source buddy table
             * to avoid stomping our data.
             */
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; // 
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

//                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                final int lastDstSlot = (dstBuddyIndex + 1) * slotsPerNewBuddy;

                for (int srcSlot = lastSrcSlot-1, dstSlot = lastDstSlot-1; srcSlot >= firstSrcSlot; srcSlot--, dstSlot -= 2) {

                    if (log.isTraceEnabled())
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot.
                        dstAddrs[dstSlot - i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot - i] = srcRefs[srcSlot];
                    }
                    
                }

            }

        }

    }
	
	public long rangeCount() {
		
		return nentries;
	
	}

	/**
	 * Pretty print the {@link HTree}.
	 */
	String PP() {
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("#nodes=" + nnodes + ", #leaves=" + nleaves + ", #entries="
				+ nentries + "\n");

		root.PP(sb);
		
		return sb.toString();
	
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
	 * @see #load(IRawStore, long)
	 * 
	 * @throws IllegalStateException
	 *             If you attempt to create two {@link HTree} objects from the
	 *             same metadata record since the metadata address will have
	 *             already been noted on the {@link IndexMetadata} object. You
	 *             can use {@link IndexMetadata#clone()} to obtain a new copy of
	 *             the metadata object with the metadata address set to
	 *             <code>0L</code>.
	 */
    public static HTree create(final IRawStore store, final IndexMetadata metadata) {

    	if(store == null) {
    	
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
    public static HTree createTransient(final IndexMetadata metadata) {
        
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
     * <i>className</i>(IRawStore store, Checkpoint checkpoint, BTreeMetadata metadata, boolean readOnly)
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

		/*
		 * Read metadata record from store.
		 */
		final IndexMetadata metadata;
		try {
			metadata = IndexMetadata.read(store, checkpoint.getMetadataAddr());
		} catch (Throwable t) {
			throw new RuntimeException("Could not read IndexMetadata: store="
					+ store + ", checkpoint=" + checkpoint, t);
		}

        if (log.isInfoEnabled()) {

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
    
    public final int activeBucketPages() {
    	return root.activeBucketPages();
    }
    
    public final int activeDirectoryPages() {
    	return root.activeDirectoryPages();
    }
    
    public String getPageInfo() {
    	return "Created Pages for " 
		+ addressBits + " addressBits"
		+ ",  directory pages: " + activeDirectoryPages() + " of " + DirectoryPage.createdPages
		+ ", bucket pages: " + activeBucketPages() + " of " + BucketPage.createdPages;
    }

}
