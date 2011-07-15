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
        
		newRoot();

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
             * Read the root node of the HTree.
             */
			root = (DirectoryPage) readNodeOrLeaf(checkpoint.getRootAddr(),
					addressBits);

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
				return bucketPage.lookupAll(key, buddyOffset);
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
                if (!bucketPage.insert(key, value, current/* parent */,
                        buddyOffset)) {

					// TODO if(parent.isReadOnly()) parent = copyOnWrite();
					
					if (current.globalDepth == child.globalDepth) {

                        /*
                         * There is only one buddy hash bucket on the page.
                         * Either we can split a directory page which is a
                         * parent of that bucket or we have to add a new level
                         * below the root on the path to that bucket.
                         */

                        if (child.globalDepth == addressBits) {

                        	addLevel2(bucketPage);
                        	
                        	return insert(key,value);
//							return innerInsertFullBucket(key, value, bucketPage);

                        } else {

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
                                    buddyOffset, current/* oldChild */);
                            throw new UnsupportedOperationException();
                        }

					}

					// globalDepth >= localDepth
					// splitBucketsOnPage(current, buddyOffset, bucketPage);
					current.split(bucketPage);
					
					// Try again. The children have changed.
					continue;
		
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
	private void insertRawTuple(final BucketPage src, final int slot) {

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
				if (!bucketPage.insertRawTuple(src, slot, key,
						current/* parent */, buddyOffset)) {

					// TODO if(parent.isReadOnly()) parent = copyOnWrite();
					
					if (current.globalDepth == child.globalDepth) {

                        /*
                         * There is only one buddy hash bucket on the page.
                         * Either we can split a directory page which is a
                         * parent of that bucket or we have to add a new level
                         * below the root on the path to that bucket.
                         */

						if (child.globalDepth == addressBits) {

							addLevel2(bucketPage); // add a level.

							insertRawTuple(src, slot); // try again
							return;

                        } else {

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
                                    buddyOffset, current/* oldChild */);
                            throw new UnsupportedOperationException();

					    }

					}

					// globalDepth >= localDepth
					splitBucketsOnPage(current, buddyOffset, bucketPage);
					
					// Try again. The children have changed.
					continue;
		
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

//    /**
//     * Handle an insert when the target {@link BucketPage} is full (a single
//     * buddy hash bucket) but can be split (not all keys in the buckets are
//     * duplicates).
//     * 
//     * @param key
//     * @param value
//     * @param prefixLength
//     *            The prefix length to <i>current</i>.
//     * @param current
//     *            The parent of <i>bucketPage</i>. The depth is
//     *            <i>addressBits</i>.
//     * @param bucketPage
//     *            A full {@link BucketPage}. The depth is <i>addressBits</i>.
//     *            All slots a full.
//     * @return
//     */
//	private byte[] innerInsertFullBucket(final byte[] key, final byte[] value,
//			final BucketPage bucketPage) {
//
//    	if (bucketPage == null)
//            throw new IllegalArgumentException();
//
//    	if (bucketPage.globalDepth != addressBits)
//            throw new IllegalStateException();
//
//		final DirectoryPage current = bucketPage.getParentDirectory();
//
//		if (current.globalDepth != addressBits)
//			throw new IllegalStateException();
//
//		/*
//		 * Figure out how much we need to extend the prefix length before we can
//		 * split-and-reindex this bucket page.
//		 * 
//		 * FIXME Probably must recompute each time in the loop since addLevel()
//		 * can recurse through insert() and back into this method.
//		 */
//        final int distinctBitsRequired = bucketPage.distinctBitsRequired();
//
//        if (distinctBitsRequired == -1)
//            throw new AssertionError();
//
//        int n = current.getPrefixLength();
//        
//        while (n < distinctBitsRequired) {
//
//            /*
//             * Find the first directory above the bucket which can be split. If
//             * null, then we will have to add a level instead.
//             */
//            DirectoryPage p = bucketPage.parent.get();
//            DirectoryPage pp;
//            while ((pp = p.getParentDirectory()) != null) {
//
//                if (p.globalDepth < pp.globalDepth) {
//
//                    break;
//
//                }
//
//                p = pp;
//                
//            }
//            
//            if (pp == null) {
//
////                /*
////                 * Add a level; increases prefixLength to bucketPage by one.
////                 */
////
//////                final int buddyOffset = getBuddyOffsetOfDirectoryPageOnPathToFullBucket(
//////                        key, bucketPage.parent.get());
//////
//////                addLevel(bucketPage.parent.get()/* oldParentIsAlwaysRoot */,
//////                        buddyOffset, splitBits);
////
////				/*
////				 * To split the page, we have to introduce a new directory page
////				 * above it and then introduce a sibling bucket page and finally
////				 * re-index the tuples in the bucket page such that they are
////				 * either the original bucket page or its new sibling bucket
////				 * page.
////				 * 
////				 * Note: If all keys on the page are duplicates then we can not
////				 * split the page. Instead we must let the page "overflow."
////				 * BucketPage.insert() handles this condition for us by allowing
////				 * the page to overflow rather than telling us to split the
////				 * page.
////				 * 
////				 * TODO Unit test this to make sure that it is introducing the
////				 * new level along the path to the full bucket page (rather than
////				 * some other path).
////				 */
////
////				// hash bits for the root directory.
////				final int hashBitsForRoot = root
////						.getLocalHashCode(key, 0/* prefixLengthForRootIsZero */);
////
////				// find the child of the root directory page.
////				final AbstractPage childOfRoot = root.getChild(
////						hashBitsForRoot, 0/* buddyOffsetOfRootIsZero */);
////
////				final int buddyOffsetInChild = HTreeUtil.getBuddyOffset(
////						hashBitsForRoot, root.globalDepth,
////						childOfRoot.globalDepth/* localDepthOfChild */);
////
////				// Add a level.
////				addLevel(root/* parent */, buddyOffsetInChild, splitBits);
//            	
//				System.err.println("before addLevel(): distinctBitsRequired="
//						+ distinctBitsRequired + ", n=" + n + "\n" + PP());
//
//				addLevel2(bucketPage);
//				
//				System.err.println("after addLevel(): distinctBitsRequired="
//						+ distinctBitsRequired + ", n=" + n + "\n" + PP());
//
//            } else {
//
//                /*
//                 * Split the directory; increases prefixLength to bucketPage
//                 * one.
//                 */
//
////                final int buddyOffset = getBuddyOffsetOfDirectoryPageOnPathToFullBucket(
////                        key, p);
////
////                splitDirectoryPage(p.parent.get(), buddyOffset, p/*oldChild*/);
//
//            	/*
//            	 * Expectation is that we will always have directory pages at
//            	 * depth := addressBits.
//            	 */
//            	throw new UnsupportedOperationException();
//            	
//            }    
//
//            // adding a level increases the bit depth by addressBits each time.
//            n += addressBits;
//            
//        }
//        
//        return insert(key, value); // TODO Move into outer insert().
//
////        /*
////         * Run up the parent references back to the root. If we find a directory
////         * page whose local depth is LT its parent's global depth then we will
////         * split that directory. Otherwise, we will have to add a new level.
////         */
////        if(splitDirectoryOnPathToFullBucket(key)) {
////            
////            /*
////             * Recursion through the top-level insert().
////             */
////
////            return insert(key, value);
////
////        }
////
////        /*
////         * To split the page, we have to introduce a new directory page above it
////         * and then introduce a sibling bucket page and finally re-index the
////         * tuples in the bucket page such that they are either the original
////         * bucket page or its new sibling bucket page.
////         * 
////         * Note: If all keys on the page are duplicates then we can not split
////         * the page. Instead we must let the page "overflow."
////         * BucketPage.insert() handles this condition for us by allowing the
////         * page to overflow rather than telling us to split the page.
////         * 
////         * TODO Unit test this to make sure that it is introducing the new level
////         * along the path to the full bucket page (rather than some other path).
////         */
////
////        // hash bits for the root directory.
////        final int hashBitsForRoot = current
////                .getLocalHashCode(key, 0/* prefixLengthForRootIsZero */);
////
////        // find the child of the root directory page.
////        final AbstractPage childOfRoot = current
////                .getChild(hashBitsForRoot, 0/* buddyOffsetOfRootIsZero */);
////
////        final int buddyOffsetInChild = HTreeUtil
////                .getBuddyOffset(hashBitsForRoot, root.globalDepth,
////                        childOfRoot.globalDepth/* localDepthOfChild */);
////
////        // Add a level.
////        addLevel(root/* parent */, buddyOffsetInChild, splitBits);
////
////        return insertAfterAddLevel(key, value);
//
//	}
//
//    private int getBuddyOffsetOfDirectoryPageOnPathToFullBucket(final byte[] key,
//            final DirectoryPage sought) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//
//        while (true) {
//
//            if (current == sought) {
//
//                return buddyOffset;
//                
//            }
//
//            // skip prefixLength bits and then extract globalDepth bits.
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//
//            if (child.isLeaf()) {
//
//                // No directory page can be split.
//                throw new RuntimeException();
//
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//
//        }
//
//    }

//    /**
//     * Search the tree along the path to a full {@link BucketPage} (a single
//     * buddy bucket in which at least one key is not a duplicate of the rest).
//     * If we find a {@link DirectoryPage} which can be split (its depth is LT
//     * its parent's depth), then split that {@link DirectoryPage} and return
//     * <code>true</code>. Otherwise return <code>false</code>.
//     * 
//     * @param key
//     *            The original key.
//     * 
//     * @return <code>true</code> if a {@link DirectoryPage} along the path to
//     *         the full {@link BucketPage} was split such that the insert()
//     *         operation can be retried. <code>false</code> if a new level must
//     *         be added instead.
//     */
//    private boolean splitDirectoryOnPathToFullBucket(final byte[] key) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//
//        while (true) {
//
//            // skip prefixLength bits and then extract globalDepth bits.
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//
//            if (child.isLeaf()) {
//
//                // No directory page can be split.
//                return false;
//
//            }
//
//            if (child.globalDepth < current.globalDepth) {
//
//                splitDirectoryPage(current/* parent */, buddyOffset,
//                        (DirectoryPage) child);
//
//                return true;
//
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//
//        }
//
//    }
//
//    /**
//     * Handle an insert after introducing a new level in the tree because a full
//     * {@link BucketPage} could not otherwise be split. This method is
//     * responsible for re-indexing the tuples in that full {@link BucketPage}.
//     * It will then retry the insert (recursively entering the top level insert
//     * method).
//     * 
//     * @param key
//     * @param value
//     * @return
//     */
//	private byte[] insertAfterAddLevel(final byte[] key, final byte[] value) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key) + ", value="
//                    + Arrays.toString(value));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//        
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//        
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//        
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//        
//        while (true) {
//
//            // skip prefixLength bits and then extract globalDepth bits. 
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//            
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//            
//            if (child.isLeaf()) {
//
//                /*
//                 * Found the bucket page, update it.
//                 */
//
//                final BucketPage bucketPage = (BucketPage) child;
//
//                /*
//                 * FIXME This is adding [addressBits] to the [prefixLength] for
//                 * this call. Verify that this is always the right thing to do.
//                 * This is based on some experience with a unit test working
//                 * through a detailed example (insert 1,2,3,4,5) but I have not
//                 * generalized this to a clear rule yet.
//                 */
//                if(!splitAndReindexFullBucketPage(current/* parent */, buddyOffset,
//                        prefixLength + addressBits, bucketPage/* oldBucket */)) {
//                    
//                    throw new AssertionError("Reindex of full bucket fails");
//                    
//                }
//
//                /*
//                 * Outer insert.
//                 * 
//                 * Note: We MUST NOT recursively enter insert() if the sole
//                 * buddy bucket on the page consists entirely of tuples having
//                 * the same key (all duplicate keys). BucketPage.insert()
//                 * handles this condition for us by allowing the page to
//                 * overflow rather than telling us to split the page.
//                 */
//                return insert(key, value);
//                
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//            
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//            
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//            
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//            
//        }
//
//    } // insertAfterAddLevel()
	
	public byte[] remove(final byte[] key) {
		// TODO Remove 1st match, returning value.
		throw new UnsupportedOperationException();
	}

    /**
     * Handle split if buddy bucket is full but localDepth LT globalDepth (so
     * there is more than one buddy bucket on the page). This will allocate a
     * new bucket page; update the references in the parent, and then
     * redistribute buddy buckets among the old and new bucket page. The depth
     * of the child will be increased by one. As a post-condition, the depth of
     * the new child will be the same as the then current depth of the original
     * child. Note that this doubles the size of each buddy bucket, thus always
     * creating room for additional tuples.
     * <p>
     * Note: This is really just doubling the address space for the buddy buckets
     * on the page.  Since each buddy bucket now has twice as many slots, we have
     * to move 1/2 of the buddy buckets to a new bucket page.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param oldChild
     *            The child {@link BucketPage}.
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
    // Note: package private for unit tests.
    void splitBucketsOnPage(final DirectoryPage parent, final int buddyOffset,
            final BucketPage oldChild) {
    	
		if (parent == null)
			throw new IllegalArgumentException();
		if (oldChild == null)
			throw new IllegalArgumentException();
		if (oldChild.globalDepth >= parent.globalDepth) {
			// In this case we have to introduce a new directory page instead.
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
		
		if (log.isInfoEnabled())
			log.info("parent=" + parent.toShortString() + ", buddyOffset="
					+ buddyOffset + ", child=" + oldChild.toShortString());

		final int oldDepth = oldChild.globalDepth;
		final int newDepth = oldDepth + 1;

		// Allocate a new bucket page (globalDepth is increased by one).
		final BucketPage newBucket = new BucketPage(this, newDepth);

		assert newBucket.isDirty();
		
		// Set the parent reference on the new bucket.
		newBucket.parent = (Reference) parent.self;
		
		// Increase global depth on the old page also.
		oldChild.globalDepth = newDepth;

		nleaves++; // One more bucket page in the hash tree. 

		// update the pointers in the parent.
		updatePointersInParent(parent, buddyOffset, oldDepth, oldChild,
				newBucket);
		
		// redistribute buddy buckets between old and new pages.
		redistributeBuddyBuckets(oldDepth, newDepth, oldChild, newBucket);

		// TODO assert invariants?
		
	}

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

	/**
	 * Redistribute the buddy buckets.
	 * <p>
	 * Note: We are not changing the #of buckets, just their size and the
	 * page on which they are found. Any tuples in a source bucket will wind up
	 * in the "same" bucket afterwards, but the page and offset on the page of the
	 * bucket may have been changed and the size of the bucket will have doubled.
	 * <p>
	 * We proceed backwards, moving the upper half of the buddy buckets to the
	 * new bucket page first and then spreading out the lower half of the source
	 * page among the new bucket boundaries on the page.
	 * 
	 * @param oldDepth
	 *            The depth of the old {@link BucketPage} before the split.
	 * @param newDepth
	 *            The depth of the old and new {@link BucketPage} after the
	 *            split (this is just oldDepth+1).
	 * @param oldBucket
	 *            The old {@link BucketPage}.
	 * @param newBucket
	 *            The new {@link BucketPage}.
	 */
	private void redistributeBuddyBuckets(final int oldDepth,
			final int newDepth, final BucketPage oldBucket,
			final BucketPage newBucket) {

	    assert oldDepth + 1 == newDepth;
	    
		// #of slots on the bucket page (invariant given addressBits).
		final int slotsOnPage = (1 << oldBucket.slotsOnPage());

		// #of address slots in each old buddy hash bucket.
		final int slotsPerOldBuddy = (1 << oldDepth);

		// #of address slots in each new buddy hash bucket.
		final int slotsPerNewBuddy = (1 << newDepth);

		// #of buddy tables on the old bucket page.
		final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;

		// #of buddy tables on the bucket pages after the split.
		final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;

		final BucketPage srcPage = oldBucket;
		final MutableKeyBuffer srcKeys = (MutableKeyBuffer) oldBucket.getKeys();
		final MutableValueBuffer srcVals = (MutableValueBuffer) oldBucket
				.getValues();

		/*
		 * Move top 1/2 of the buddy buckets from the child to the new page.
		 */
		{

			// target is the new page.
			final BucketPage dstPage = newBucket;
			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
					.getKeys();
			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
					.getValues();

			// index (vs offset) of first buddy in upper half of src page.
			final int firstSrcBuddyIndex = (oldBuddyCount >> 1);

			// exclusive upper bound for index (vs offset) of last buddy in
			// upper half of src page.
			final int lastSrcBuddyIndex = oldBuddyCount;

			// exclusive upper bound for index (vs offset) of last buddy in
			// upper half of target page.
			final int lastDstBuddyIndex = newBuddyCount;

			// work backwards over buddy buckets to avoid stomping data!
			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
			srcBuddyIndex >= firstSrcBuddyIndex; //
			srcBuddyIndex--, dstBuddyIndex--//
			) {

				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {

					if (log.isTraceEnabled())
						log.trace("moving: page(" + srcPage.toShortString()
								+ "=>" + dstPage.toShortString() + ")"
								+ ", buddyIndex(" + srcBuddyIndex + "=>"
								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
								+ "=>" + dstSlot + ")");

                    // Move the tuple at that slot TODO move metadata also.
					if(srcKeys.keys[srcSlot] == null)
					    continue;
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;
                    dstKeys.nkeys++; // one more in that page.
                    srcKeys.nkeys--; // one less in this page.
                    dstVals.nvalues++; // one more in that page.
                    srcVals.nvalues--; // one less in this page.

				}

			}

		}

		/*
		 * Reposition the bottom 1/2 of the buddy buckets on the old page.
		 * 
		 * Again, we have to move backwards through the buddy buckets on the
		 * source page to avoid overwrites of data which has not yet been
		 * copied. Also, notice that the buddy bucket at index ZERO does not
		 * move - it is already in place even though it's size has doubled.
		 */
		{

			// target is the old page.
			final BucketPage dstPage = oldBucket;
			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
					.getKeys();
			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
					.getValues();

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
			 * Note: The slots for first buddy in the lower half of the source
			 * page DO NOT MOVE. The offset of that buddy in the first page
			 * remains unchanged. Only the size of the buddy is changed (it is
			 * doubled, just like all the other buddies on the page).
			 */
			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
			srcBuddyIndex > firstSrcBuddyIndex; // DO NOT move 1st buddy bucket!
			srcBuddyIndex--, dstBuddyIndex--//
			) {

				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {

					if (log.isTraceEnabled())
						log.trace("moving: page(" + srcPage.toShortString()
								+ "=>" + dstPage.toShortString() + ")"
								+ ", buddyIndex(" + srcBuddyIndex + "=>"
								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
								+ "=>" + dstSlot + ")");

					// Move the tuple at that slot TODO move metadata also.
                    if(srcKeys.keys[srcSlot] == null)
                        continue;
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;
					// Note: movement within same page: nkeys/nvals don't change

				}

			}

		}

	}

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
	 */
	DirectoryPage addLevel2(final BucketPage oldPage) {
		final DirectoryPage newParent;
		
		if (oldPage == null)
			throw new IllegalArgumentException();

		if (oldPage.globalDepth != addressBits)
			throw new IllegalStateException();

		// The parent directory page above the old bucket page.
		final DirectoryPage pp = oldPage.getParentDirectory();

		if (!pp.isDirty())
			throw new IllegalStateException(); // parent must be mutable.

		if (log.isInfoEnabled())
			log.info("bucketPage=" + oldPage.toShortString());
		
		// #of slots on a page.
        final int bucketSlotsPerPage = oldPage.slotsOnPage();
        final int dirSlotsPerPage =  1 << this.addressBits;

		// allocate new nodes and relink the tree.
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
				boolean found = false;
				final long oldAddr = oldPage.isPersistent() ? oldPage
						.getIdentity() : 0L;
				final MutableDirectoryPageData data = (MutableDirectoryPageData) pp.data;
				for (int i = 0; i < dirSlotsPerPage && !found; i++) {
					if (oldPage.isPersistent()) {
						if (data.childAddr[i] == oldAddr)
							found = true; // same address
					} else {
						if (pp.childRefs[i] == oldPage.self)
							found = true; // same reference
					}
					if (found) {
						pp.childRefs[i] = (Reference) newParent.self; // set ref
						data.childAddr[i] = 0L; // clear addr.
					}
				}
				if (!found)
					throw new AssertionError();
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
		final IRaba okeys = oldPage.getKeys();
		final IRaba ovals = oldPage.getValues();
		for (int i = 0; i < bucketSlotsPerPage; i++) {

			// insertRawTuple(oldPage, i);
			if (okeys.get(i) != null)
				newParent.insertRawTuple(okeys.get(i), ovals.get(i), 0);

		}
		
		return newParent;

	}
	
    /**
     * Adds a new {@link DirectoryPage} when we need to split a child but
     * <code>globalDepth == localDepth</code>. The caller must retry the insert
     * after this method makes the structural change.
     * 
     * <h2>Design discussion</h2>
     * 
     * This method must maintain the invariant that the tree of page references
     * for the hash tree is a strict tree. That is, you can not have two
     * different pages each of which points to the same child. This would be a
     * concurrency nightmare as, e.g., splitting the child could require us to
     * propagate updates to multiple parents. However, even with that constraint
     * we have two options.
     * <p>
     * Given addressBits := 2, the following hash tree state is induced by
     * inserting the key sequence (0x01, 0x02, 0x03, 0x04).
     * 
     * <pre>
     * root := [2] (a,c,b,b)
     * a    := [2]   (1,2,3,4)
     * c    := [2]   (-,-,-,-)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * where [x] is the depth of the buddies on the corresponding page and ";"
     * indicates a buddy bucket boundary while "," indicates a tuple boundary.
     * <p>
     * If we then attempt to insert a key which would be directed into (a)
     * 
     * <pre>
     * insert(0x20,...)
     * </pre>
     * 
     * then we must split (a) since depth(a):=2 and depth(root):=2. This will
     * introduce a new directory page (d).
     * 
     * <h3>depth(d) := 1</h3>
     * 
     * This gives us the following post-condition.
     * 
     * <pre>
     * root := [2] (d,d,b,b)
     * d    := [1]   (a,a;c,c)   // two ptrs to (d) so 2 buddies on the page
     * a    := [0]     (1;2;3;4) // depth changes since now 2 ptrs to (a)
     * c    := [0]     (-;-;-;-) // depth changes since now 2 ptrs to (c)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * Regardless of the value of [addressBits], this design gives us
     * [addressBits] buddies on (d) and each buddy has two slots (since the
     * depth of (d) is ONE, each buddy on (d) has a one bit address space and
     * hence uses two slots). The depth(a) and depth(c) will always be reset to
     * ZERO (0) by this design since there will always be TWO pointers to (a)
     * and TWO pointers to (c) in (d). This design provides ONE (1) bit of
     * additional distinctions along the path for which we have exhausted the
     * hash tree address space.
     * 
     * <h3>depth(d) := addressBits</h3>
     * 
     * This gives us the following post-condition.
     * 
     * <pre>
     * root := [2] (a,c,b,b)
     * d    := [2]   (a,a,a,a)   // one ptr to (d) so 1 buddy on the page
     * a    := [0]     (1;2;3;4) // depth changes since now 4 ptrs to (a).
     * c    := [2]   (-,-,-,-)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * In this design, we always wind up with ONE buddy on (d), the depth(d) is
     * [addressBits], and the depth(a) is reset to ZERO(0). This design focuses
     * the expansion in the address space of the hash tree narrowly on the
     * specific key prefix for which we have run out of distinctions and gives
     * us [addressBits] of additional distinctions along that path.
     * 
     * <h3>Conclusion</h3>
     * 
     * Both designs would appear to be valid. Neither one can lead to a
     * situation in which we have multiple parents for a child. In the first
     * design, the one-bit expansion means that we never have pointers to the
     * same child in more than one buddy bucket, and hence they will all be on
     * the same page. In the second design, the depth of the new directory page
     * is already at the maximum possible value so it can not be split again and
     * thus the pointers to the child will always remain on the same page.
     * <p>
     * It seems that the first design has the advantage of growing the #of
     * distinctions more slowly and sharing the new directory page among
     * multiple such distinctions (all keys having the same leading bit). In the
     * second design, we add a full [addressBits] at once to keys having the
     * same [addressBits] leading bits).
     * <p>
     * It would appear that any choice in the inclusive range (1:addressBits) is
     * permissible as in all cases the pointers to (a) will lie within a single
     * buddy bucket. By factoring out the #of additional bits of distinction to
     * be made when we split a directory page, we can defer this design either
     * to construction time (or perhaps even to runtime) decision. I have
     * therefore introduced an additional parameter on the {@link HTree} for
     * this purpose.
     * 
     * @param oldParentIsAlwaysRoot
     *            The parent {@link DirectoryPage}.
     * @param buddyOffsetInChild
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>splitBits</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>splitBits</i> is GT {@link #getAddressBits()}.
     * @throws IllegalStateException
     *             if the depth of the child is GTE the depth of the parent.
     * @throws IllegalStateException
     *             if the <i>parent<i/> is read-only.
     * @throws IllegalStateException
     *             if the <i>oldBucket</i> is read-only.
     * @throws IllegalStateException
     *             if the parent of the <i>oldBucket</i> is not the given
     *             <i>parent</i>.
     * 
     *             FIXME [oldParent] is not required if this is always invoked
     *             with [root] as the [oldParent]. However, we do need to know
     *             which path through the tree is being increased so we can
     *             update the appropriate pointers in the root. This is the
     *             [buddyOffset] of the direct child of the root on the path to
     *             the {@link BucketPage} which which needs to be split.
     *             
     *             @deprecated by the new implementation.
     */
    // Note: package private for unit tests.
    void addLevel(final DirectoryPage oldParentIsAlwaysRoot,
            final int buddyOffsetInChild, final int splitBits) {
        //, final AbstractPage child) {
        
        if (oldParentIsAlwaysRoot == null)
            throw new IllegalArgumentException();
//        if (oldParentIsAlwaysRoot != root)
//            throw new IllegalArgumentException();
//        if (childIsUnused == null)
//            throw new IllegalArgumentException();
//        if (childIsUnused.globalDepth != oldParent.globalDepth) {
//            /*
//             * We only create a new directory page when the global and local
//             * depth are equal.
//             */
//            throw new IllegalStateException();
//        }
        if (buddyOffsetInChild < 0)
            throw new IllegalArgumentException();
        if (buddyOffsetInChild >= (1 << addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if (splitBits <= 0)
            throw new IllegalArgumentException();
        if (splitBits > addressBits)
            throw new IllegalArgumentException();
        if ((buddyOffsetInChild + splitBits) >= (1 << addressBits)) {
            /*
             * [buddyOffset] is the slot index of the first slot for the buddy
             * hash table in the parent. [splitBits] is the #of address bits to
             * copy into the new directory page. Therefore, [buddyOffset +
             * splitBits] must be GTE ZERO (0) and LT [addressBits].
             */
            throw new IllegalArgumentException();
        }
        if (oldParentIsAlwaysRoot.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
//        if (childIsUnused.isReadOnly()) // must be mutable.
//            throw new IllegalStateException();
//        if (childIsUnused.parent != oldParent.self) // must be same Reference.
//            throw new IllegalStateException();

        if (log.isDebugEnabled())
            log.debug("parent=" + oldParentIsAlwaysRoot.toShortString() + ", buddyOffset="
                    + buddyOffsetInChild);// + ", child=" + childIsUnused);

        // Allocate a new directory page. .
        final DirectoryPage newParent = new DirectoryPage(this, splitBits/* globalDepth */);

        // Set the parent Reference on the new dir page to the old dir page.
        newParent.parent = (Reference) oldParentIsAlwaysRoot.self;

        // One more directory page.
        nnodes++;
        
        assert splitBits == newParent.globalDepth;

        // #of buddy hash tables on the new directory page.
        final int nbuddies = (1 << addressBits) / (1 << newParent.globalDepth);
        
        // #of address slots in each buddy hash table for the new dir page.
        final int nslots = (1 << newParent.globalDepth);

        /*
         * This is a nested loop which copies the pointers to the relevant child
         * pages into the new directory page. We then go through and set each of
         * the slots from which we copied a pointer to be a pointer to the new
         * directory page.
         * 
         * The #of pointers to be copied depends on [splitBits] and defines the
         * local depth of the new directory page. If the local depth of the new
         * directory page is to be ONE (1), then we must copy 1/2 of the
         * pointers from the parent. If the local depth of the new directory
         * page is to be [addressBis], then we must copy 1 of the pointers from
         * the parent.
         * 
         * The outer loop visits the slots we need to copy in the parent.
         * 
         * The inner loop fills each the buddy hash table in the new directory
         * with the current pointer from the outer loop.
         */
        {
            final int lastSrc = (buddyOffsetInChild + nbuddies);
            
            // for each pointer to be copied from the parent.
            int dst = 0; // target slot in the new directory page.
            for (int src = buddyOffsetInChild; src < lastSrc; src++) {

                // pointer to be copied.
                final Reference<AbstractPage> ref = oldParentIsAlwaysRoot.childRefs[src];

                // fill the buddy hash table on the new parent with that ptr.
                for (int i = 0; i < nslots; i++) {

                    newParent.childRefs[dst] = ref;

                    dst++;

                }

            }

            /*
             * Replace the pointer to the child page in the old parent with the
             * pointer to the new directory page.
             */
            for (int src = buddyOffsetInChild; src < lastSrc; src++) {

                oldParentIsAlwaysRoot.childRefs[src] = (Reference) newParent.self;

            }

        }

        /*
         * We need to update the parent reference on each page whose pointer was
         * moved into the new directory page and recompute the global depth of
         * the page as well. Both of these pieces of information are transient,
         * so we only do this for pointers to pages that are currently
         * materialized.
         * 
         * The parent of a page whose pointer was moved needs to be updated
         * because the parent is now the new directory page.
         * 
         * The global depth of a page whose pointer was moved needs to be
         * updated since the #of pointers to that page changed. This can be done
         * by counting the #of pointers in any buddy hash table of the new
         * parent to the child. Since all pointers in a buddy hash table on the
         * new parent point to the child page, the #of pointers in a buddy hash
         * table in the new parent is just the #of slots in a buddy hash table
         * for the new parent.
         * 
         * Note: We have to do this for each buddy hash table on the new
         * directory page. 
         */
        {

            int aBuddyOffset = 0;
            for (int i = 0; i < nbuddies; i++) {

                final Reference<AbstractPage> ref = newParent.childRefs[aBuddyOffset];

                final AbstractPage aChild = ref == null ? null : ref.get();

                if (aChild == null) {
                    // Only update materialized pages.
                    continue;
                }
                
                // Each buddy hash table in the new parent was filled by a single
                // pointer so npointers := nslots
                final int npointers = nslots;

                // recompute the local depth of the child page.
                final int localDepth = HTreeUtil.getLocalDepth(addressBits,
                        newParent.globalDepth, npointers);

                // update the cached local depth on the child page.
                aChild.globalDepth = localDepth;

                // Update the parent reference on the child.
                aChild.parent = (Reference) newParent.self;

                aBuddyOffset += nslots;
                
            }
            
        }

    }

    /**
     * Allocate a sibling bucket of a full bucket page (one buddy bucket) and
     * index the tuples in the caller's bucket between the old bucket and the
     * new bucket. This method MUST be invoked immediately after a new level has
     * been added to the {@link HTree} in order to bring the {@link HTree} into
     * an internally consistent state.
     * <p>
     * If the operation can not succeed because the tuples can not be reindexed
     * into the original {@link BucketPage} and a new {@link BucketPage} given
     * the depth of their common parent then this method will return
     * <code>false</code> and the structure of the {@link HTree} WILL NOT be
     * modified. This contract makes it possible to use this method to decide
     * when incremental structural changes (via add level and split directory)
     * have introduced sufficient distinctions to split a full
     * {@link BucketPage}.
     * <p>
     * While this method is "safe" in the sense described above, the
     * {@link HTree} is NOT in a consistent state following an add level
     * operation until the full {@link BucketPage} has been successfully split
     * using this method. The caller is responsible for executing sufficient add
     * level and/or split directory operations such that the full
     * {@link BucketPage} can be split successfully by this method.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must have its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param prefixLength
     *            The bit length of the MSB prefix to the <i>parent</i>
     *            {@link DirectoryPage}.
     * @param oldBucket
     *            The child {@link BucketPage} to be re-indexed.
     * 
     * @return <code>true</code> iff the operation could be carried out without
     *         causing any buddy bucket in either the old {@link BucketPage} or
     *         the new {@link BucketPage} to overflow.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the <i>parent</i> is read-only.
     * @throws IllegalStateException
     *             if the <i>oldBucket</i> is read-only.
     * @throws IllegalStateException
     *             if the parent of the <i>oldBucket</i> is not the given
     *             <i>parent</i>.
     * 
     * @see #addLevel(DirectoryPage, int, int, AbstractPage)
     * 
     * @deprecated no longer used with new {@link #addLevel2(BucketPage)}.
     */
    // Note: package private for unit tests.
    private boolean splitAndReindexFullBucketPage(final DirectoryPage parent,
            final int buddyOffset, final int prefixLength,
            final BucketPage oldBucket) {

        if (parent == null)
            throw new IllegalArgumentException();
        if (oldBucket == null)
            throw new IllegalArgumentException();
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
        if ((buddyOffset + splitBits) >= (1 << addressBits)) {
            /*
             * [buddyOffset] is the slot index of the first slot for the buddy
             * hash table in the parent. [splitBits] is the #of address bits to
             * copy into the new directory page. Therefore, [buddyOffset +
             * splitBits] must be GTE ZERO (0) and LT [addressBits].
             */
            throw new IllegalArgumentException();
        }
        if (prefixLength < 0)
            throw new IllegalArgumentException();
        if (parent.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldBucket.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldBucket.parent != parent.self) // must be same Reference.
            throw new IllegalStateException();
        // TODO There must be ONE (1) reference to the old bucket in the parent.
        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", prefixLength=" + prefixLength
                    + ", oldBucket=" + oldBucket.toShortString());

        final int oldDepth = oldBucket.globalDepth;
        final int newDepth = oldDepth + 1;
        
        // Save a copy of the parent's state.
        final Reference<AbstractPage>[] savedChildRefs;
        final IDirectoryData savedData;
        {
            // Note: This is no longer "free" since is allocating a copy of the parent's data.
            savedChildRefs = new Reference[parent.childRefs.length];

            System.arraycopy(parent.childRefs/* src */, 0/* srcPos */,
                    savedChildRefs/* dest */, 0/* destPos */,
                    parent.childRefs.length/* length */);

            savedData = new MutableDirectoryPageData(addressBits,
                    parent.data);

        }
        
        // Allocate a new bucket page (globalDepth is increased by one).
        final BucketPage newBucket = new BucketPage(this, newDepth);

        assert newBucket.isDirty();
        
        // Set the parent reference on the new bucket.
        newBucket.parent = (Reference) parent.self;
        
        // Increase global depth on the old page also.
        oldBucket.globalDepth = newDepth;

        // One more bucket page in the hash tree.
        nleaves++; 

        // update the pointers in the parent.
        updatePointersInParent(parent, buddyOffset, oldDepth, oldBucket,
                newBucket);

        // attempt to reindex the tuples.
        if (!reindexTuples(parent, buddyOffset, prefixLength, oldBucket,
                newBucket)) {
            /*
             * Reindex failed. Return immediately. NO SIDE EFFECTS ON THE HTREE.
             */
            {

                // Restore child reference[].
                System.arraycopy(savedChildRefs/* src */, 0/* srcPos */,
                        parent.childRefs/* dest */, 0/* destPos */,
                        parent.childRefs.length/* length */);

                // Restore the persistent data record on the parent.
                ((MutableDirectoryPageData)parent.data).copyFrom(savedData);
                
                // Restore bucket depth.
                oldBucket.globalDepth = oldDepth;
                
                // Restore #of leaves in the HTree.
                nleaves--;

            }
            
            return false;
        }

        return true;
        
    }

    /**
     * Re-index the tuples in (a), distributing them between (a) and (b)
     * according to the hash bits which are in play at that level of the
     * {@link HTree}.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param prefixLength
     *            The bit length of the MSB prefix to the <i>parent</i>
     *            {@link DirectoryPage}.
     * @param a
     *            The original bucket.
     * @param b
     *            The new sibling bucket.
     * 
     *            TODO Handle case where the original {@link BucketPage} will
     *            have more than buddy bucket after reindexing (that is, the
     *            parent has a depth LT addressBits).
     * 
     *            TODO This method should return <code>true</code> iff the
     *            indexing operation was successful and should not have a side
     *            effect if the operation could not be completed (because some
     *            buddy bucket would have overflowed).
     *            
     *            @deprecated by the alternative implementation.
     */
    private boolean reindexTuples(final DirectoryPage parent,
            final int buddyOffset, final int prefixLength, final BucketPage a,
            final BucketPage b) {

        if (parent == null)
            throw new IllegalArgumentException();
        if (a == null)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (a.isReadOnly())
            throw new IllegalStateException();
        if (b.isReadOnly())
            throw new IllegalStateException();
        if (a.parent.get() != parent)
            throw new IllegalStateException();
        if (b.parent.get() != parent)
            throw new IllegalStateException();

        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", prefixLength=" + prefixLength
                    + ", oldChild=" + a.toShortString() + ", newChild="
                    + b.toShortString());

        // #of buddy tables on a page (post-condition for both bucket pages).
        final int nbuddies = (1 << addressBits) / (1 << parent.globalDepth);

        // #of address slots in each buddy hash table (post-condition).
        final int slotsPerBuddy = (1 << parent.globalDepth);

        // The #of hash bucket buddies across both bucket pages.
        final int nbins = nbuddies << 1;

        /**
         * A counter per buddy hash bucket. The index into [bins] is the
         * <code>page * nbuddies</code>, where the original page is page:=0 and
         * the new page is page:=1. If any counter exceeds <i>slotsPerBuddy</i>
         * then the reindex operation will fail since the corresponding buddy
         * bucket would overflow. The counter is incremented before we copy the
         * tuple to the target page so we detect an overflow before it occurs.
         */
        final int[] bins = new int[nbins];
        
        // Setup [t] as a temporary page with [a]'s data and clear [a]'s data.
        final BucketPage t = new BucketPage(this, a.globalDepth);//Note: the depth of the temporary BucketPage does not really matter. It could be addressBits since it is ignored by this code.
        {
            final ILeafData tmp = a.data;
            a.data = t.data;
            t.data = tmp;
        }

        /*
         * Visit each tuple which was in [a], figure out which of the two bucket
         * pages it needs to be in, and insert the tuple into the appropriate
         * bucket page.
         */
        
        // #of slots in [a]'s data.
        final IRaba keys = t.data.getKeys();
        final IRaba vals = t.data.getValues();
        final int m = 1 << addressBits; // aka fan-out or branching factor.
        assert m == keys.capacity();
//        // #of tuples inserted into [a] and [b] respectively.
//        int na = 0, nb = 0;
        for (int sourceIndex = 0; sourceIndex < m; sourceIndex++) {

            // Since the bucket is full, no entry should be null.
            assert !keys.isNull(sourceIndex);

            /*
             * Re-insert the tuple
             * 
             * Note: A re-insert via the top-level entry point is logically
             * correct. However, it will: (a) cause the raw record to be
             * materialized and written onto the backing store a second time
             * (which is wasteful in the extreme); (b) have a side-effect on the
             * revision timestamps (if we support them); and (c) cause deleted
             * tuples to "reappear" (if we support delete markers). For all of
             * these reasons, we need to handle the "re-insert" of the tuples
             * at a much lower level in the API.
             */

            // materialize the key.
            final byte[] key = keys.get(sourceIndex);

            /*
             * Figure out whether the tuple would be directed into (a) or (b) by
             * the parent. Then figure out which buddy bucket on the target page
             * will get that tuple.
             */
            final int hashBitsParent = parent.getLocalHashCode(key, prefixLength);

            // Find the child which will get this tuple.
            final AbstractPage child = parent.getChild(hashBitsParent, buddyOffset);
            
            // The child must be a bucket page.
            assert child.isLeaf(); 
            
            // The child be one of the two bucket pages we are working with.
            assert (child == a || child == b);

            // True if the tuple is re-indexed back into (a).
            final boolean isA = child == a;
            
            // The page index component into [bins].
            final int page = isA ? 0 : 1;

            final BucketPage x = (BucketPage) child;

            // find the offset of the buddy hash table in the child.
            final int targetBuddyOffset = HTreeUtil
                    .getBuddyOffset(hashBitsParent, parent.globalDepth,
                            child.globalDepth/* localDepthOfChild */);

            // next free slot in the target buddy bucket.
            final int targetSlotInBuddy = bins[page * nbuddies
                    + targetBuddyOffset]++;

            if (targetSlotInBuddy >= slotsPerBuddy) {

                /*
                 * The buddy hash table has overflowed.
                 * 
                 * Note: Undo the changes to the original bucket page such that
                 * this method does not have a side effect if the index fails.
                 */

                log.warn("Buddy bucket overflow: page=" + page
                        + ", targetBuddyOffset=" + targetBuddyOffset
                        + ", bins=" + Arrays.toString(bins));

                // restore the data for the original page.
                a.data = t.data;
                
                return false;
                
            }
            
            // The index in the child where we will write the tuple.
            final int targetIndexOnPage = targetBuddyOffset * slotsPerBuddy
                    + targetSlotInBuddy;

            assert x.data.getKeys().isNull(targetIndexOnPage);
            
            // Copy the key.
            x.data.getKeys().set(targetIndexOnPage, key);
            
            // Copy the value.
            if (!vals.isNull(sourceIndex))
                x.data.getValues().set(targetIndexOnPage, vals.get(sourceIndex));

            // TODO copy versionTimestamp and delete marker metadata too.

//            // Increment the index of the next target tuple in this child.
//            if (isA)
//                na++;
//            else
//                nb++;
            
        }
        
        // Success
        return true;

    }

	/**
	 * Re-index the tuples in an old {@link BucketPage}, distributing them
	 * between (a) and (b) according to the hash bits which are in play at that
	 * level of the {@link HTree}.
	 * 
	 * @param oldPage
	 *            The old {@link BucketPage} whose tuples will be reindexed.
	 * @param a
	 *            The original bucket.
	 * @param b
	 *            The new sibling bucket.
	 * 
	 * @deprecated No longer used with new {@link #addLevel2(BucketPage)}.
	 */
	private void reindexTuples(final BucketPage oldPage, final BucketPage a,
			final BucketPage b) {

        if (a == null)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (a.isReadOnly())
            throw new IllegalStateException();
        if (b.isReadOnly())
            throw new IllegalStateException();
		if (b.globalDepth != a.globalDepth) {
			/*
			 * Must be at the same depth (these are newly created bucket pages
			 * of a common parent).
			 */
			throw new IllegalStateException();
		}
        final DirectoryPage parent = a.getParentDirectory();
        if (a.parent.get() != parent)
            throw new IllegalStateException();
        if (b.parent.get() != parent)
            throw new IllegalStateException();
		if (parent.globalDepth != addressBits) {
			/*
			 * The parent must be a directory with depth=addressBits, which is a
			 * precondition of the method which invokes this (addLevel()).
			 * Hence, the buddyOffset in the parent will be ZERO (0) since there
			 * is only one buddy in the parent when its depth := addressBits.
			 */
			throw new IllegalStateException();
		}
		// prefixLength to the parent directory page.
		final int prefixLength = parent.getPrefixLength();
		// Note: buddyOffset in parent will be zero per above.
		final int buddyOffset = 0; // since depth==addressBits, only 1 buddy.
		
		if (log.isDebugEnabled())
			log.debug("oldPage=" + oldPage.toShortString() + ", buddyOffset="
					+ buddyOffset + ", prefixLength=" + prefixLength + ", a="
					+ a.toShortString() + ", b=" + b.toShortString());

        // #of buddy buckets on a page (post-condition for both bucket pages).
        final int nbuddies = (1 << addressBits) / (1 << a.globalDepth);

        // #of address slots in each buddy hash bucket (post-condition).
        final int slotsPerBuddy = (1 << a.globalDepth);

        // The #of hash bucket buddies across both bucket pages.
        final int nbins = nbuddies << 1;

        /**
         * A counter per buddy hash bucket. The index into [bins] is the
         * <code>page * nbuddies</code>, where the original page is page:=0 and
         * the new page is page:=1. If any counter exceeds <i>slotsPerBuddy</i>
         * then the reindex operation will fail since the corresponding buddy
         * bucket would overflow. The counter is incremented before we copy the
         * tuple to the target page so we detect an overflow before it occurs.
         */
        final int[] bins = new int[nbins];
        
        final BucketPage t = oldPage;

		/*
		 * Visit each tuple which was in [oldPage], figure out which of the two
		 * bucket pages it needs to be in, and insert the tuple into the
		 * appropriate bucket page.
		 */
        
        final IRaba keys = t.data.getKeys();
        final IRaba vals = t.data.getValues();
        final int m = 1 << addressBits; // aka fan-out or branching factor.
        assert m == keys.capacity();

        for (int sourceIndex = 0; sourceIndex < m; sourceIndex++) {

            // Since the bucket is full, no entry should be null.
            assert !keys.isNull(sourceIndex);

            /*
             * Re-insert the tuple
             * 
             * Note: A re-insert via the top-level entry point is logically
             * correct. However, it will: (a) cause the raw record to be
             * materialized and written onto the backing store a second time
             * (which is wasteful in the extreme); (b) have a side-effect on the
             * revision timestamps (if we support them); and (c) cause deleted
             * tuples to "reappear" (if we support delete markers). For all of
             * these reasons, we need to handle the "re-insert" of the tuples
             * at a much lower level in the API.
             */

            // materialize the key.
            final byte[] key = keys.get(sourceIndex);

			/*
			 * Figure out whether the tuple would be directed into (a) or (b) by
			 * the parent. Then figure out which buddy bucket on the target page
			 * will get that tuple.
			 */
			final int hashBitsParent = parent.getLocalHashCode(key,
					prefixLength);

			// Find the child which will get this tuple.
			final AbstractPage child = parent.getChild(hashBitsParent,
					buddyOffset);

			// The child must be a bucket page.
			assert child.isLeaf();

			// The child be one of the two bucket pages we are working with.
            assert (child == a || child == b);

            // True if the tuple is re-indexed back into (a).
            final boolean isA = child == a;
            
//            // The page index component into [bins].
//            final int page = isA ? 0 : 1;

            final BucketPage x = (BucketPage) child;

            // find the offset of the buddy hash table in the child.
            final int targetBuddyOffset = HTreeUtil
                    .getBuddyOffset(hashBitsParent, parent.globalDepth,
                            child.globalDepth/* localDepthOfChild */);

            // next free slot in the target buddy bucket.
            final int targetSlotInBuddy = bins[targetBuddyOffset]++;

            if (targetSlotInBuddy >= slotsPerBuddy) {

                /*
                 * The buddy hash table has overflowed.
                 * 
                 * Note: Undo the changes to the original bucket page such that
                 * this method does not have a side effect if the index fails.
                 */

				throw new RuntimeException("Buddy bucket overflow: page="
						+ (isA?"a":"b") + ", targetBuddyOffset=" + targetBuddyOffset
						+ ", bins=" + Arrays.toString(bins));

            }
            
			// The index in the child where we will write the tuple.
			final int targetIndexOnPage = targetBuddyOffset + targetSlotInBuddy;

            assert x.data.getKeys().isNull(targetIndexOnPage);
            
            // Copy the key.
            x.data.getKeys().set(targetIndexOnPage, key);
            
            // Copy the value.
            if (!vals.isNull(sourceIndex))
                x.data.getValues().set(targetIndexOnPage, vals.get(sourceIndex));

            // TODO copy versionTimestamp and delete marker metadata too.

        }
        
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
	 * 
	 * @deprecated since directory pages appear to always be at maximum depth
	 *             (depth:=addressBits) there is no reason to ever split a
	 *             directory page.
	 */
    private void splitDirectoryPage(final DirectoryPage parent, final int buddyOffset,
            final DirectoryPage oldChild) {

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

//	/**
//	 * Reindex all tuples spanned by a {@link DirectoryPage}. This method is
//	 * used when the prefixLength is changed, whether by adding a new level or
//	 * by splitting a directory. Both actions can cause the tuples below that
//	 * point in the tree to shift from one buddy bucket to another since a new
//	 * distinction has been introduced in the {@link DirectoryPage}.
//	 * <p>
//	 * This implementation builds a new {@link HTree} sharing the same backing
//	 * store whose root is a {@link DirectoryPage} having the same depth as the
//	 * given {@link DirectoryPage}. The {@link BucketPage}s below the given
//	 * {@link DirectoryPage} are visited and each tuple is inserted into the new
//	 * htree root.
//	 * <p>
//	 * Unlike a normal {@link HTree}, the {@link HTree} into which the tuples
//	 * are being inserted may have a root {@link DirectoryPage} whose depth is
//	 * less than <i>addressBits</i>. This means that we must determine the buddy
//	 * hash table on the root {@link DirectoryPage} into which the key would be
//	 * inserted. This requires us to note the prefixLength to the given
//	 * {@link DirectoryPage} and to compute the buddyOffset into the root
//	 * {@link DirectoryPage} based on that prefix length and the depth of the
//	 * parent of the source {@link DirectoryPage}. Those values can then be
//	 * passed into an appropriate insert() method (they are ZERO(s) for a normal
//	 * {@link HTree}).
//	 * <p>
//	 * A post-order iterator is used to visit the pages spanned by the given
//	 * {@link DirectoryPage}. Each page is released (freed) on the backing store
//	 * after it has been processed.
//	 * <p>
//	 * Note: When more than one add level and/or split directory operation is
//	 * required to transform a tree such that it can accept a new key, all such
//	 * transforms should be carried out and the resulting tree reindexed using
//	 * this method as an after action in order to minimize the amount of work
//	 * required to bring the index back into an internally consistent state.
//	 * 
//	 * @param d
//	 *            The {@link DirectoryPage}.
//	 */
//    void reindexDirectory(final DirectoryPage d) {
//
//		if (log.isInfoEnabled()) {
//			log.info("directoryPage=" + d.toShortString());
//		}
//    	
//    	assert d.isDirty(); // must be mutable (not persistent).
//
//		/*
//		 * Setup a temporary HTree backed by the same persistence store. The
//		 * root of the temporary HTree will be at the same depth as the
//		 * directory page to be reindexed.
//		 * 
//		 * FIXME We need an alternative constructor for the HTree which accepts
//		 * [prefixLength], the depth of the parent of the given directory page,
//		 * and the depth of the given directory page. Those can be stored on
//		 * fields named [rootPrefixLength] (zero for a normal HTree),
//		 * [externalGlobalDepth] (addressBits for a normal HTree), and
//		 * [rootGlobalDepth] (addressBits for a normal HTree).
//		 * 
//		 * FIXME Modify insert(key,val) to use those new fields. It will
//		 * initialize its [prefixLength] from [rootPrefixLength]. It will need
//		 * to compute the [buddyOffset] from the depth of the root (the given
//		 * directory) and the depth of the external parent of the root (the
//		 * parent of the given directory).
//		 * 
//		 * FIXME Update the addLevels() unit test to invoke reindexDirectory()
//		 * after the splitDirectory() call where things are going wrong now. If
//		 * this improves matters, then write more unit tests for
//		 * reindexDirectory().
//		 */
//		final HTree tmp;
//		{
//
//			final int rootPrefixLength = d.getPrefixLength();
//			final DirectoryPage pd = d.getParentDirectory();
//			final int externalPageDepth = pd == null ? 0 : pd.globalDepth;
//
//			final int rootPageDepth = d.globalDepth;
//
//			tmp = new HTree(store, addressBits/* addressBits */, rawRecords,
//					rootPrefixLength, externalPageDepth, rootPageDepth);
//		}
//    	
//    	final Iterator<AbstractPage> itr = d.postOrderIterator();
//    	
//    	while(itr.hasNext()) {
//    		
//    		final AbstractPage page = itr.next();
//    		
//    		if(page.isLeaf()) {
//
//				/*
//				 * Insert into the temporary HTree.
//				 * 
//				 * Note: In order to avoid materializing raw records this needs
//				 * to operate against the actual byte[] value stored in the
//				 * leaf. Going through a higher level API, such as ITuple, will
//				 * cause a raw record to be materialized from the backing
//				 * store!!!
//				 * 
//				 * FIXME If we support additional metadata (version timestamps
//				 * or delete markers) then those need to be handled here as
//				 * well.
//				 */
//    			
//    			final BucketPage bucketPage = (BucketPage)page;
//    			
//    			final IRaba keys = bucketPage.getKeys();
//
//    			final IRaba vals = bucketPage.getValues();
//    			
//				final int slotsPerPage = 1 << addressBits;
//
//				for (int i = 0; i < slotsPerPage; i++) {
//
//					if (keys.isNull(i))
//						continue;
//
//					final byte[] key = keys.get(i);
//					
//					final byte[] val = vals.get(i);
//					
//					tmp.insert(key, val);
//
//				}
//    			
//    		}
//    		
//			if (page.isPersistent()) {
//
//				// delete the source page.
//				store.delete(page.getIdentity());
//
//			}
//
//    	}
//    	
//		/*
//		 * Steal the data for the root of the temporary htree.
//		 */
//
//    	d.data = tmp.root.data;
//		
//    	System.arraycopy(tmp.root.childRefs/* src */, 0/* srcOff */,
//				d.childRefs/* dst */, 0/* dstOff */, tmp.root.childRefs.length/* length */);
//    	
//    	/*
//    	 * link any childRefs to point to new parent
//    	 */
//     	for (int i = 0; i < d.childRefs.length; i++) {
//    		d.childRefs[i].get().parent = (Reference<DirectoryPage>) d.self;
//    	}
//    }
    
//	/**
//	 * Validate pointers in buddy hash table in the parent against the global
//	 * depth as self-reported by the child. By definition, the global depth of
//	 * the child is based on the global depth of the parent and the #of pointers
//	 * to the child in the parent. The {@link AbstractPage#globalDepth} value is
//	 * the cached result of that computation. It can be cross checked by
//	 * counting the actual number of pointers in the parent and comparing that
//	 * with this formula:
//	 * 
//	 * <pre>
//	 * npointers := 1 &lt;&lt; (parent.globalDepth - child.globalDepth)
//	 * </pre>
//	 * 
//	 * This method validates that the cached value of global depth on the child
//	 * is consistent with the #of pointers in the specified buddy hash table of
//	 * the parent. However, this validate depends on the global depth of the
//	 * parent itself being correct.
//	 * <p>
//	 * Note: While other buddy hash tables in the parent can point into the same
//	 * child page. However, due to the buddy offset computation they will not
//	 * point into the same buddy on the same child.
//	 * 
//	 * @param parent
//	 *            The parent {@link DirectoryPage}.
//	 * @param buddyOffset
//	 *            The buddyOffset within the <i>parent</i>. This identifies
//	 *            which buddy hash table in the parent should have references to
//	 *            the child.
//	 * @param child
//	 *            The child {@link AbstractPage}.
//	 */
//	private void validatePointersInParent(final DirectoryPage parent,
//			final int buddyOffset, final AbstractPage child) {
//
//		// #of address slots in the parent buddy hash table.
//		final int slotsPerBuddy = (1 << parent.globalDepth);
//
//		// #of pointers expected in the parent buddy hash table.
//		final int npointers = 1 << (parent.globalDepth - child.globalDepth);
//		
//		// The first slot in the buddy hash table in the parent.
//		final int firstSlot = buddyOffset;
//		
//		// The last slot in the buddy hash table in the parent.
//		final int lastSlot = buddyOffset + slotsPerBuddy;
//
//		if (parent.isDirty() && parent.getIdentity() != IRawStore.NULL)
//			throw new RuntimeException(
//					"Parent address should be NULL since parent is dirty");
//
//		if (child.isDirty() && child.getIdentity() != IRawStore.NULL)
//			throw new RuntimeException(
//					"Child address should be NULL since child is dirty");
//		
//		/*
//		 * Count pointers to the child page. There should be [npointers] of them
//		 * and they should be contiguous. Since the child is materialized (we
//		 * have its reference) we can test pointers. If the child is dirty, then
//		 * all slots in the parent having pointers to the child should be
//		 * associated with NULL addresses for the child. If the child is clean,
//		 * then all slots in the parent having references for the child should
//		 * have the same non-zero address for the child. Also, any slot in the
//		 * parent having the address of a persistent child should have the same
//		 * Reference object, which should be child.self.
//		 */
//		int firstPointer = -1;
//		int nfound = 0;
//		boolean discontiguous = false;
//		for (int i = firstSlot; i < lastSlot; i++) {
//			final boolean slotRefIsChild = parent.childRefs[i] == child.self;
//			final boolean slotAddrIsChild = parent.getChildAddr(i) == child
//					.getIdentity();
//			if (slotAddrIsChild && !slotRefIsChild)
//				throw new RuntimeException(
//						"Child reference in parent should be child since addr in parent is child addr");
//			final boolean slotIsChild = slotRefIsChild || slotAddrIsChild;
//			if (slotIsChild) {
//				if (child.isDirty()) {
//					// A dirty child must have a NULL address in the parent.
//					if (parent.data.getChildAddr(i) != IRawStore.NULL)
//						throw new RuntimeException(
//								"Child address in parent should be NULL since child is dirty");
//				} else {
//					// A clean child must have a non-NULL address in the parent.1
//					if (parent.data.getChildAddr(i) == IRawStore.NULL)
//						throw new RuntimeException(
//								"Child address in parent must not be NULL since child is clean");
//				}
//				if (firstPointer == -1)
//					firstPointer = i;
//				nfound++;
//			} else {
//				if (firstPointer != -1 && nfound != npointers) {
//					discontiguous = true;
//				}
//			}
//		}
//		if (firstPointer == -1)
//			throw new RuntimeException("No pointers to child");
//		if (nfound != npointers) {
//			/*
//			 * This indicates either a problem in maintaining the pointers
//			 * (References and/or addresses) in the parent for the child, a
//			 * problem where child.self is not a unique Reference for the child,
//			 * and/or a problem with the cached [globalDepth] for the child.
//			 */
//			throw new RuntimeException("Expected " + npointers
//					+ " pointers to child, but found=" + nfound);
//		}
//		if (discontiguous)
//			throw new RuntimeException(
//					"Pointers to child are discontiguous in parent's buddy hash table.");
//	} // validatePointersInParent

//	/**
//	 * TODO Scan the entire parent for addresses and References to each child. Any
//	 * time we see the address of a child, then the Reference must be
//	 * [child.self]. Any time we see a reference which is not child.self, then
//	 * the address for that slot must not be the same as the address of the
//	 * child.
//	 * <p>
//	 * Note: Since the child is materialized, any Reference in the parent to
//	 * that child should be [child.self]. Due to the buddy page system, when we
//	 * load a child we need to scan the parent across all buddy hash tables and
//	 * set the Reference on any slot having the address of that child. Likewise,
//	 * when the child becomes dirty, we need to clear the address of the child
//	 * in any slot of any buddy hash table in the parent.
//	 */
//	private void validateChildren(final DirectoryPage parent) {
//		
//	}
	
	public long rangeCount() {
		
		return nentries;
	
	}

	/**
	 * Pretty print the {@link HTree}.
	 */
	String PP() {
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("Total Entries: " + nentries + "\n");
		
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
	 * @exception IllegalStateException
	 *                If you attempt to create two {@link HTree} objects from
	 *                the same metadata record since the metadata address will
	 *                have already been noted on the {@link IndexMetadata}
	 *                object. You can use {@link IndexMetadata#clone()} to
	 *                obtain a new copy of the metadata object with the metadata
	 *                address set to <code>0L</code>.
	 */
    public static HTree create(final IRawStore store, final IndexMetadata metadata) {
        
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

}
