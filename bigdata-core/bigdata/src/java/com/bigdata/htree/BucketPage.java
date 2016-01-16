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
 */
package com.bigdata.htree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IRawRecordAccess;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.raba.MutableKeyBuffer;
import com.bigdata.htree.raba.MutableValueBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.BytesUtil;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * An {@link HTree} bucket page (leaf). The bucket page is comprised of one or
 * more buddy hash buckets. The #of buddy hash buckets is determined by the
 * address bits of the hash tree and the global depth of the bucket page.
 * <p>
 * The entire bucket page is logically a fixed size array of tuples. The
 * individual buddy hash buckets are simply offsets into that logical tuple
 * array. While inserts of distinct keys drive splits, inserts of identical keys
 * do not. Therefore, this simple logical picture is upset by the need for a
 * bucket to hold an arbitrary number of tuples having the same key.
 * <p>
 * Each tuple is represented by a key, a value, and various metadata bits using
 * the {@link ILeafData} API. The tuple keys are always inline within the page
 * and are often 32-bit integers. The tuple values may be either "raw records"
 * on the backing {@link IRawStore} or inline within the page.
 * <p>
 * The {@link ILeafData#getPriorAddr()} and {@link ILeafData#getNextAddr()}
 * fields of the {@link ILeafData} record are reserved by the hash tree to
 * encode the search order for range queries when used in combination with an
 * order preserving hash function.
 * 
 * TODO One way to tradeoff the simplicity of a local tuple array with the
 * requirement to hold an arbitrary number of duplicate keys within a bucket is
 * to split the bucket if it becomes full regardless of whether or not there are
 * duplicate keys.
 * <p>
 * Splitting a bucket doubles its size which causes a new bucket page to be
 * allocated to store 1/2 of the data. If the keys can be differentiated by
 * increasing the local depth, then this is the normal case and the tuples are
 * just redistributed among buddy buckets on the original bucket page and the
 * new bucket page. If the keys are identical but we force a split anyway, then
 * we will still have fewer buckets on the original page and they will be twice
 * as large. The duplicate keys will all wind up in the same buddy bucket after
 * the split, but at least the buddy bucket is 2x larger. This process can
 * continue until there is only a single buddy bucket on the page (the global
 * depth of the parent is the same as the global depth of the buddy bucket). At
 * that point, a "full" page can simply "grow" by permitting more and more
 * tuples into the page (as long as those tuples have the same key). We could
 * also chain overflow pages at that point - it all amounts to the same thing.
 * An attempt to insert a tuple having a different key into a page in which all
 * keys are known to be identical will immediately trigger a split. In this case
 * we could avoid some effort if we modified the directory structure to impose a
 * split since we already know that we have only two distinct keys (the one
 * found in all tuples of the bucket and the new key). When a bucket page
 * reaches this condition of containing only duplicate keys we could of course
 * compress the keys enormously since they are all the same.
 * <p>
 * This works well when we only store the address of the objects in the bucket
 * (rather than the objects themselves, e.g., raw records mode) and choose the
 * address bits based on the expected size of a tuple record. However, we can
 * also accommodate tuples with varying size (think binding sets) with in page
 * locality if split the buddy bucket with the most bytes when an insert into
 * the page would exceed the target page size. This looks pretty much like the
 * case above, except that we split buddy buckets based on not only whether
 * their alloted #of slots are filled by tuples but also based on the data on
 * the page.
 * 
 * TODO Delete markers will also require some thought. Unless we can purge them
 * out at the tx commit point, we can wind up with a full bucket page consisting
 * of a single buddy bucket filled with deleted tuples all having the same key.
 * 
 * TODO Explore cracking. Concerning cracking, we need to be careful about the
 * thread-safety guarantee. If we did cracking for a mutation operation, that
 * would be Ok since we are single threaded. However, we could not do cracking
 * for a read operation even against a mutable HTree since we allow concurrent
 * read operations as long as there is no writer.
 */
class BucketPage extends AbstractPage implements ILeafData, IRawRecordAccess {

	/**
	 * The data record. {@link MutableBucketData} is used for all mutation
	 * operations. {@link ReadOnlyLeafData} is used when the {@link BucketPage}
	 * is made persistent. A read-only data record is automatically converted
	 * into a {@link MutableBucketData} record when a mutation operation is
	 * requested.
	 * <p>
	 * Note: This is package private in order to expose it to {@link HTree}.
	 */
	ILeafData data;
	
	@Override
	public AbstractFixedByteArrayBuffer data() {
		return data.data();
	}

   @Override
	public boolean getDeleteMarker(final int index) {
		return data.getDeleteMarker(index);
	}

   @Override
	public int getKeyCount() {
		return data.getKeyCount();
	}

   @Override
	public IRaba getKeys() {
		return data.getKeys();
	}

   @Override
	public long getMaximumVersionTimestamp() {
		return data.getMaximumVersionTimestamp();
	}

   @Override
	public long getMinimumVersionTimestamp() {
		return data.getMinimumVersionTimestamp();
	}

   @Override
	public long getNextAddr() {
		return data.getNextAddr();
	}

   @Override
	public long getPriorAddr() {
		return data.getPriorAddr();
	}

   @Override
	public long getRawRecord(int index) {
		return data.getRawRecord(index);
	}

	// public int getSpannedTupleCount() {
	// return data.getSpannedTupleCount();
	// }

   @Override
	public int getValueCount() {
		return data.getValueCount();
	}

   @Override
	public IRaba getValues() {
		return data.getValues();
	}

   @Override
	public long getVersionTimestamp(final int index) {
		return data.getVersionTimestamp(index);
	}

   @Override
	public boolean hasDeleteMarkers() {
		return data.hasDeleteMarkers();
	}

   @Override
	public boolean hasRawRecords() {
		return data.hasRawRecords();
	}

   @Override
	public boolean hasVersionTimestamps() {
		return data.hasVersionTimestamps();
	}

   @Override
	public boolean isCoded() {
		return data.isCoded();
	}

   @Override
	public boolean isDoubleLinked() {
		return data.isDoubleLinked();
	}

   @Override
	public boolean isLeaf() {
		return data.isLeaf();
	}

   @Override
	public boolean isReadOnly() {
		return data.isReadOnly();
	}

	/**
	 * Create a new empty bucket.
	 * 
	 * @param htree
	 *            A reference to the owning {@link HTree}.
	 * @param globalDepth
	 *            The size of the address space (in bits) for each buddy hash
	 *            table on a directory page. The global depth of a node is
	 *            defined recursively as the local depth of that node within its
	 *            parent. The global/local depth are not stored explicitly.
	 *            Instead, the local depth is computed dynamically when the
	 *            child will be materialized by counting the #of pointers to the
	 *            the child in the appropriate buddy hash table in the parent.
	 *            This local depth value is passed into the constructor when the
	 *            child is materialized and set as the global depth of the
	 *            child.
	 */
	BucketPage(final HTree htree, final int globalDepth) {

		super(htree, true/* dirty */, globalDepth);
		
		data = new MutableBucketData(//
				htree.bucketSlots, // fan-out
				htree.versionTimestamps,//
				htree.deleteMarkers,//
				htree.rawRecords//
		);

	}

	/**
	 * Deserialization constructor - {@link #globalDepth} MUST be set by the
	 * caller.
	 * 
	 * @param htree
	 * @param addr
	 * @param data
	 */
	BucketPage(final HTree htree, final long addr, final ILeafData data) {

		super(htree, false/* dirty */, 0/*unknownGlobalDepth*/);

		this.data = data;
		
        setIdentity(addr);

	}

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node (must be immutable).
     * 
     * @see AbstractPage#copyOnWrite()
     */
    protected BucketPage(final BucketPage src) {

        super(src);

        assert !src.isDirty();
        assert src.isReadOnly();
//        assert src.isPersistent();

        // steal/clone the data record.
		this.data = src.isReadOnly() ? new MutableBucketData(src.slotsOnPage(),
				src.data) : src.data;

        // clear reference on source.
        src.data = null;

//        /*
//         * Steal/copy the keys.
//         * 
//         * Note: The copy constructor is invoked when we need to begin mutation
//         * operations on an immutable node or leaf, so make sure that the keys
//         * are mutable.
//         */
//        {
//
////            nkeys = src.nkeys;
//
//            if (src.getKeys() instanceof MutableKeyBuffer) {
//
//                keys = src.getKeys();
//
//            } else {
//
//                keys = new MutableKeyBuffer(src.getBranchingFactor(), src
//                        .getKeys());
//
//            }
//
//            // release reference on the source node.
////            src.nkeys = 0;
//            src.keys = null;
//            
//        }
//
////        /*
////         * Steal the values[].
////         */
////
////        // steal reference and clear reference on the source node.
////        values = src.values;
//
//        /*
//         * Steal/copy the values[].
//         * 
//         * Note: The copy constructor is invoked when we need to begin mutation
//         * operations on an immutable node or leaf, so make sure that the values
//         * are mutable.
//         */
//        {
//
//            if (src.values instanceof MutableValueBuffer) {
//
//                values = src.values;
//
//            } else {
//
//                values = new MutableValueBuffer(src.getBranchingFactor(),
//                        src.values);
//
//            }
//
//            // release reference on the source node.
//            src.values = null;
//            
//        }
//
//        versionTimestamps = src.versionTimestamps;
//        
//        deleteMarkers = src.deleteMarkers;
        
//        // Add to the hard reference queue.
//        btree.touch(this);

    }

    /**
	 * Return <code>true</code> if there is at lease one tuple in the buddy hash
	 * bucket for the specified key.
	 * 
	 * @param key
	 *            The key.
	 * @param buddyOffset
	 *            The offset within the {@link BucketPage} of the buddy hash
	 *            bucket to be searched.
	 * 
	 * @return <code>true</code> if a tuple is found in the buddy hash bucket
	 *         for the specified key.
	 */
	boolean contains(final byte[] key, final int buddyOffset) {

		if (key == null)
			throw new IllegalArgumentException();

		/*
		 * Use search to locate key, buddy offset is ignored for BucketPage
		 */	
		final int index = getKeys().search(key);
		return index >= 0;
	}

	/**
	 * There is no reason why the number of slots in a BucketPage should be the
	 * same as the number in a DirectoryPage.
	 * 
	 * @return number of slots available in this BucketPage
	 */
	final int slotsOnPage() {
		return htree.bucketSlots;
		// return 1 << htree.addressBits;
	}

	/**
	 * Return the first value found in the buddy hash bucket for the specified
	 * key.
	 * 
	 * @param key
	 *            The key.
	 * @param buddyOffset
	 *            The offset within the {@link BucketPage} of the buddy hash
	 *            bucket to be searched.
	 * 
	 * @return The value associated with the first tuple found in the buddy hash
	 *         bucket for the specified key and <code>null</code> if no such
	 *         tuple was found. Note that the return value is not diagnostic if
	 *         the application allows <code>null</code> values into the index.
	 */
	final byte[] lookupFirst(final byte[] key, final int buddyOffset) {
		final int index = lookupIndex(key);
		
		if (index == -1)
			return null;
		
		if (hasRawRecords()) {
			final long addr = getRawRecord(index);
			
			if (addr != IRawStore.NULL)
				return getBytes(readRawRecord(addr));
		}

		return getValues().get(index);
	}
	
	/**
	 * @param buf
	 * @return a byte array representing the data view of the ByteBuffer
	 */
	final byte[] getBytes(final ByteBuffer buf) {

		if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0
				&& buf.limit() == buf.capacity()) {

			/*
			 * Return the backing array.
			 */

			return buf.array();

		}

		/*
		 * Copy the expected data into a byte[] using a read-only view on the
		 * buffer so that we do not mess with its position, mark, or limit.
		 */
		final byte[] a;
		{

			final ByteBuffer buf2 = buf.asReadOnlyBuffer();

			final int len = buf2.remaining();

			a = new byte[len];

			buf2.get(a);

		}

		return a;

	}

	final int lookupIndex(final byte[] key) {

		if (key == null)
			throw new IllegalArgumentException();

		/*
		 * Locate the first unassigned tuple in the buddy bucket.
		 * 
		 */
		final IRaba keys = getKeys();
		
		final int si = keys.search(key);
		
		return si < 0 ? -1 : si;
	}

	/**
	 * Return an iterator which will visit each tuple in the buddy hash bucket
	 * for the specified key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return An iterator which will visit each tuple in the buddy hash table
	 *         for the specified key and never <code>null</code>.
	 * 
	 *         TODO Specify the contract for concurrent modification both here
	 *         and on the {@link HTree#lookupAll(byte[])} methods.
	 */
//	 * @param buddyOffset
//	 *            The offset within the {@link BucketPage} of the buddy hash
//	 *            bucket to be searched.
	final ITupleIterator lookupAll(final byte[] key) {// final int buddyOffset) {

		return new BuddyBucketTupleIterator(key, this);//, buddyOffset);

	}

	/**
	 * Insert the tuple into the buddy bucket.
	 * 
	 * @param key
	 *            The key (all bits, all bytes).
	 * @param val
	 *            The value (optional).
	 * 
	 * @return <code>false</code> iff the buddy bucket must be split.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>key</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if <i>parent</i> is <code>null</code>.
	 * @throws IndexOutOfBoundsException
	 *             if <i>buddyOffset</i> is out of the allowed range.
	 */
	boolean insert(final byte[] key, final byte[] val) {

		if (key == null)
			throw new IllegalArgumentException();

		if (parent == null)
			throw new IllegalArgumentException();

		// #of slots on the page.
		final int slotsOnPage = slotsOnPage();

        /*
         * Note: This is one of the few gateways for mutation of a leaf via the
         * main btree API (insert, lookup, delete). By ensuring that we have a
         * mutable leaf here, we can assert that the leaf must be mutable in
         * other methods.
         */
        final BucketPage copy = (BucketPage) copyOnWrite();

		// assert copy.dirtyHierarchy();

		if (copy != this) {

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

			return copy.insert(key, val);

		}
		
		// convert to raw record if necessary
		final byte[] ival = checkRawRecord(val);

		final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
		if (keys.nkeys < keys.capacity()) {
			int insIndex;
			// Check if an overflow BucketPage (by checking parent)
			// and if so, then just append to end
			if (this.getParentDirectory().isOverflowDirectory()) {
				insIndex = keys.nkeys;
			} else {
				insIndex = keys.search(key);
			}
			if (insIndex < 0) {
				insIndex = -insIndex - 1;
			} else if (TRACE){
				log.trace("Insert duplicate key");
			}
			
			((MutableBucketData) data).insert(insIndex, key, ival, ival != val);
			
			((HTree) htree).nentries++;
			
			return true;
		}
			
		/*
		 * Now we have to figure out whether or not all keys are duplicates.
		 */
		boolean identicalKeys = true;
		for (int i = 0; i < slotsOnPage; i++) {
			if (!BytesUtil.bytesEqual(key, keys.get(i))) {
				identicalKeys = false;
				break;
			}
		}
		if (!identicalKeys) {
			/*
			 * Force a split since it is possible to redistribute some tuples.
			 */
			return false;
		}
		
		/*
		 * Rather than overflow a BucketPage by some chaining structure it
		 * turns out to be a lot simpler to introduce a new DirectoryPage
		 * for this bucketPage since the serialization and dirty protocols
		 * need not change at all. 
		 */
//		final EvictionProtection protect = new EvictionProtection(this);
//		try {
			/**
			 * In any event:
			 * 		create new bucket page and insert key/value
			 */
			final BucketPage newPage = new BucketPage((HTree) htree, globalDepth);			
			((HTree) htree).nleaves++;

			final DirectoryPage pd = getParentDirectory();
			if (pd.isOverflowDirectory()) { // already handles blobs
				assert globalDepth == htree.addressBits;
				pd._addChild(newPage); // may result in extra level insertion
			} else {
				if (pd.getLevel() * htree.addressBits > key.length * 8)
					throw new AssertionError();
				
				// Must ensure that there is only a single reference to this BucketPage
				// and that the "active" page is for the overflow key
				pd._ensureUniqueBucketPage(key, this.self);
				globalDepth = htree.addressBits;
				newPage.globalDepth = htree.addressBits;
				
	            final DirectoryPage blob = new DirectoryPage((HTree) htree,
	                    key,// overflowKey
	                    pd.getOverflowPageDepth());
				// now add in blob
				pd.replaceChildRef(this.self, blob);
	
				blob._addChild(this);
				blob._addChild(newPage); // Directories MUST have at least 2 slots!
				
			}
			
			newPage.insert(key, val);
			
			// assert (1 << (htree.addressBits - this.globalDepth)) == getParentDirectory().countChildRefs(this);
			// assert (1 << (htree.addressBits - newPage.globalDepth)) == getParentDirectory().countChildRefs(newPage);
			
			assert dirtyHierarchy();
//		} finally {
//			protect.release();
//		}

		return true;
	}

	/**
	 * Checks to see if the value supplied should be converted to a raw record, and
	 * if so converts it.
	 * 
	 * @param val - value to be checked
	 * @return the value to be used, converted to raw record reference if required
	 */
	private byte[] checkRawRecord(final byte[] val) {
		if (hasRawRecords() && val != null && val.length > htree.getMaxRecLen()) {

			// write the value on the backing store.
			final long naddr = htree.writeRawRecord(val);

			// convert to byte[].
			return ((HTree) htree).encodeRecordAddr(naddr);
		} else {
			return val;
		}

	}
	
	/**
	 * Insert used when addLevel() is invoked to copy a tuple from an existing
	 * bucket page into another bucket page. This method is very similar to
	 * {@link #insert(byte[], byte[], DirectoryPage, int)}. The critical
	 * differences are: (a) it correctly handles raw records (they are not
	 * materialized during the copy); (b) it correctly handles version counters
	 * and delete markers; and (c) the #of tuples in the index is unchanged.
	 * 
	 * @param srcPage
	 *            The source {@link BucketPage}.
	 * @param srcSlot
	 *            The slot in that {@link BucketPage} having the tuple to be
	 *            copied.
	 * @param key
	 *            The key (already materialized).
	 * @param parent
	 *            The parent {@link DirectoryPage} and never <code>null</code>
	 *            (this is required for the copy-on-write pattern).
	 * @param buddyOffset
	 *            The offset into the child of the first slot for the buddy hash
	 *            table or buddy hash bucket.
	 * 
	 * @return <code>false</code> iff the buddy bucket must be split.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>key</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if <i>parent</i> is <code>null</code>.
	 * @throws IndexOutOfBoundsException
	 *             if <i>buddyOffset</i> is out of the allowed range.
	 */
	boolean insertRawTuple(final BucketPage srcPage, final int srcSlot,
			final byte[] key) {//, final int buddyOffset) {
		
		if (key == null)
			throw new IllegalArgumentException();

        /*
         * Note: This is one of the few gateways for mutation of a leaf via the
         * main btree API (insert, lookup, delete). By ensuring that we have a
         * mutable leaf here, we can assert that the leaf must be mutable in
         * other methods.
         */
        final BucketPage copy = (BucketPage) copyOnWrite();

        if (copy != this) {

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

            return copy.insertRawTuple(srcPage, srcSlot, key);//, buddyOffset);

        }
        
        // just fit somewhere in page
		final int slotsOnPage = slotsOnPage();
        final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
        final MutableValueBuffer vals = (MutableValueBuffer) getValues();
        for (int i = 0; i < slotsOnPage; i++) {
            if (keys.isNull(i)) {
                keys.nkeys++;
                keys.keys[i] = key;
                vals.nvalues++;
                
                // Note: DOES NOT Materialize a raw record!!!!
                vals.values[i] = srcPage.getValues().get(srcSlot);
                
                // deleteMarkers
                if (srcPage.hasDeleteMarkers()) {
                	((MutableBucketData) data).deleteMarkers[i] = srcPage.getDeleteMarker(srcSlot);
                }
                
                // version timestamp
                if (srcPage.hasVersionTimestamps()) {
                	((MutableBucketData) data).versionTimestamps[i] = srcPage.getVersionTimestamp(srcSlot);
                }
                
                // copy raw record info
                if (srcPage.hasRawRecords() && srcPage.getRawRecord(srcSlot) != IRawStore.NULL) {
                	((MutableBucketData) data).rawRecords[i] = true;
                }
                
                // do not increment on raw insert, since this is only ever
                // (for now) a re-organisation

                // insert Ok.
                return true;
            }

        } // next slot on page

        /*
         * The page is full. Now we have to figure out whether or not all keys
         * are duplicates.
         */
		boolean identicalKeys = true;
		for (int i = 0; i < slotsOnPage; i++) {
			if (!BytesUtil.bytesEqual(key, keys.get(i))) {
				identicalKeys = false;
				break;
			}
		}
		if (!identicalKeys) {
			/*
			 * Force a split since it is possible to redistribute some tuples.
			 */
			return false;
		}

        /*
         * Note: insertRawTuple() is invoked when we split a bucket page.
         * Therefore, it is not possible that a bucket page to which we must
         * redistribute a tuple could be full. If it were full, then we would
         * not split it. If we split it, then we can not wind up with more
         * tuples in the target bucket page than were present in the original
         * bucket page.
         */

		throw new AssertionError();

	}

	/**
	 * Return an iterator visiting all the non-deleted, non-empty tuples on this
	 * {@link BucketPage}.
	 */
	ITupleIterator tuples() {

		return new InnerBucketPageTupleIterator(IRangeQuery.DEFAULT);

	}

	/**
	 * Visits the non-empty tuples in each {@link BucketPage} visited by the
	 * source iterator.
	 */
	private class InnerBucketPageTupleIterator<E> implements ITupleIterator<E> {

		private final int slotsPerPage = slotsOnPage();

		private int nextNonEmptySlot = 0;

		private final Tuple<E> tuple;

		InnerBucketPageTupleIterator(final int flags) {

			// look for the first slot.
			if (findNextSlot()) {

				this.tuple = new Tuple<E>(htree, flags);

			} else {

				// Nothing will be visited.
				this.tuple = null;

			}

		}

		/**
		 * Scan to the next non-empty slot in the current {@link BucketPage}.
		 * 
		 * @return <code>true</code> iff there is a non-empty slot on the
		 *         current {@link BucketPage}.
		 */
		private boolean findNextSlot() {
			final IRaba keys = getKeys();
			final int size = keys.size();
			for (; nextNonEmptySlot < size; nextNonEmptySlot++) {
				if (keys.isNull(nextNonEmptySlot))
					continue;
				return true;
			}
			// The current page is exhausted.
			return false;
		}

		public boolean hasNext() {

			return nextNonEmptySlot < getKeys().size();

		}

		public ITuple<E> next() {
			if (!hasNext())
				throw new NoSuchElementException();
			// Copy the data for the current tuple into the Tuple buffer.
			tuple.copy(nextNonEmptySlot, BucketPage.this);
			/*
			 * Advance to the next slot on the current page. if there is non,
			 * then the current page reference will be cleared and we will need
			 * to fetch a new page in hasNext() on the next invocation.
			 */
			nextNonEmptySlot++; // skip past the current tuple.
			findNextSlot(); // find the next non-null slot (next tuple).
			// Return the Tuple buffer.
			return tuple;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Visits this leaf if unless it is not dirty and the flag is true, in which
	 * case the returned iterator will not visit anything.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<AbstractPage> postOrderNodeIterator(
			final boolean dirtyNodesOnly, final boolean nodesOnly) {

		if (dirtyNodesOnly && !isDirty()) {

			return EmptyIterator.DEFAULT;

		} else if (nodesOnly) {

			return EmptyIterator.DEFAULT;

		} else {

			return new SingleValueIterator(this);

		}

	}

	@Override
	public void PP(final StringBuilder sb, final boolean showBinary) {

		sb.append(PPID() + " [" + globalDepth + "] " + indent(getLevel()));

		sb.append("("); // start of address map.

		// #of buddy tables on a page.
		// final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);
		final int nbuddies = 1;

		// #of address slots in each buddy hash table.
		// final int slotsPerBuddy = (1 << globalDepth);
		final int slotsPerBuddy = slotsOnPage();

		for (int i = 0; i < nbuddies; i++) {

			if (i > 0) // buddy boundary marker
				sb.append(";");

			for (int j = 0; j < slotsPerBuddy; j++) {

				if (j > 0) // slot boundary marker.
					sb.append(",");

				final int slot = i * slotsPerBuddy + j;
				if (slot > 0 && slot % 16 == 0)
					sb.append("\n----------" + indent(getLevel()));
				
				sb.append(PPVAL(slot, showBinary));

			}

		}

		sb.append(")"); // end of tuples

		sb.append("\n");

	}

	/**
	 * Pretty print a value from the tuple at the specified slot on the page.
	 * 
	 * @param index
	 *            The slot on the page.
	 * 
	 * @return The pretty print representation of the value associated with the
	 *         tuple at that slot.
	 */
	private String PPVAL(final int index, final boolean showBinary) {

		if (index >= getKeys().size())
			return "-";

		if (index > getKeys().capacity()) {
			throw new RuntimeException("index="+index+", keys.size="+getKeys().size()+", keys.capacity="+getKeys().capacity());
		}
		
		final byte[] key = getKeys().get(index);

		final String keyStr = showBinary ? BytesUtil.toString(key) + "("
				+ BytesUtil.toBitString(key) + ")" : BytesUtil.toString(key);

		final String valStr;

		if (false/* showValues */) {

			final long addr;
			if (hasRawRecords()) {
				addr = getRawRecord(index);
			} else {
				addr = IRawStore.NULL;
			}

			if (addr != IRawStore.NULL) {
				// A raw record
				valStr = "@" + htree.getStore().toString(addr);
			} else {
				final byte[] value = getValues().get(index);

				valStr = BytesUtil.toString(value);
			}
		} else {

			valStr = null;

		}

		if (valStr == null) {

			return keyStr;

		}

		return keyStr + "=>" + valStr;

	}

	/**
	 * Human readable representation of the {@link ILeafData} plus transient
	 * information associated with the {@link BucketPage}.
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder();

		sb.append(super.toString());

		sb.append("{ isDirty=" + isDirty());

		sb.append(", isDeleted=" + isDeleted());

		sb.append(", addr=" + identity);

		final DirectoryPage p = (parent == null ? null : parent.get());

		sb.append(", parent=" + (p == null ? "N/A" : p.toShortString()));
		sb.append(", globalDepth=" + getGlobalDepth());
		sb.append(", nbuddies=" + (1 << htree.addressBits) / (1 << globalDepth));
		sb.append(", slotsPerBuddy=" + (1 << globalDepth));
		if (data == null) {

			// No data record? (Generally, this means it was stolen by copy on
			// write).
			sb.append(", data=NA}");

			return sb.toString();

		}

		sb.append(", nkeys=" + getKeyCount());

		// sb.append(", minKeys=" + minKeys());
		//
		// sb.append(", maxKeys=" + maxKeys());

		DefaultLeafCoder.toString(this, sb);

		sb.append("}");

		return sb.toString();

	}

   @Override
	protected boolean dump(final Level level, final PrintStream out,
			final int height, final boolean recursive, final boolean materialize) {

		final boolean debug = level.toInt() <= Level.DEBUG.toInt();

		// Set to false iff an inconsistency is detected.
		boolean ok = true;

		if (parent == null || parent.get() == null) {
			out.println(indent(height) + "ERROR: parent not set");
			ok = false;
		}

		if (globalDepth > parent.get().globalDepth) {
			out.println(indent(height)
					+ "ERROR: localDepth exceeds globalDepth of parent");
			ok = false;
		}

		/*
		 * TODO Count the #of pointers in each buddy hash table of the parent
		 * to each buddy bucket in this bucket page and verify that the
		 * globalDepth on the child is consistent with the pointers in the
		 * parent.
		 * 
		 * TODO The same check must be performed for the directory page to
		 * cross validate the parent child linking pattern with the transient
		 * cached globalDepth fields.
		 */

		if (debug || !ok) {

			out.println(indent(height) + toString());

		}

		return ok;

	}

   @Override
   public void dumpPages(final boolean recursive, final boolean visitLeaves,
         final HTreePageStats stats) {

      if (!visitLeaves)
         return;

      stats.visit(htree, this);

   }
	
	/**
	 * From the current bit resolution, determines how many extra bits are
	 * required to ensure the current set of bucket values can be split.
	 * <p>
	 * The additional complexity of determining whether the page can really be
	 * split is left to the parent. A new directory, covering the required
	 * prefixBits would initially be created with depth 1. But if the specified
	 * bit is discriminated within buddy buckets AND other bits do not further
	 * separate the buckets then the depth of the directory will need to be
	 * increased before the bucket page can be split.
	 * 
	 * @return bit depth increase from current offset required -or-
	 *         <code>-1</code> if it is not possible to split the page no matter
	 *         how many bits we have.
	 */
	int distinctBitsRequired() {
		final int currentResolution = getPrefixLength(); // start offset of this
															// page
		int testPrefix = currentResolution + 1;

		final IRaba keys = data.getKeys();
		final int nkeys = keys.size();

		int maxPrefix = 0;
		for (int t = 1; t < nkeys; t++) {
			final byte[] k = keys.get(t);
			final int klen = k == null ? 0 : k.length;
			maxPrefix = maxPrefix > klen ? maxPrefix : klen;
		}
		maxPrefix *= 8; // convert max bytes to max bits

		assert nkeys > 1;

		while (testPrefix < maxPrefix) {
			final boolean bitset = BytesUtil.getBit(keys.get(0), testPrefix);
			for (int t = 1; t < nkeys; t++) {
				final byte[] k = keys.get(t);
				if (bitset != (k == null ? false : BytesUtil.getBit(
						keys.get(t), testPrefix))) {
					return testPrefix - currentResolution;
				}
			}
			testPrefix++;
		}

		return -1;
	}

//	/**
//	 * To insert in a BucketPage must handle split
//	 * 
//	 * @see com.bigdata.htree.AbstractPage#insertRawTuple(byte[], byte[], int)
//	 */
//	void insertRawTuple(final byte[] key, final byte[] val, final int buddy) {
//		final int slotsPerBuddy = slotsOnPage(); // (1 << htree.addressBits);
//		final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
//		final MutableValueBuffer vals = (MutableValueBuffer) getValues();
//
//		if (true) {
//			// just fit somewhere in page
//			for (int i = 0; i < slotsPerBuddy; i++) {
//				if (keys.isNull(i)) {
//					keys.nkeys++;
//					keys.keys[i] = key;
//					vals.nvalues++;
//					vals.values[i] = val;
//					// TODO deleteMarker:=false
//					// TODO versionTimestamp:=...
//					// do not increment on raw insert, since this is only ever
//					// (for now) a re-organisation
//					// ((HTree)htree).nentries++;
//					// insert Ok.
//					return;
//				}
//			}
//		} else { // if mapping buddy explicitly
//			final int buddyStart = buddy * slotsPerBuddy;
//			final int lastSlot = buddyStart + slotsPerBuddy;
//
//			for (int i = buddyStart; i < lastSlot; i++) {
//				if (keys.isNull(i)) {
//					keys.nkeys++;
//					keys.keys[i] = key;
//					vals.nvalues++;
//					setValue(i, val);
//					// TODO deleteMarker:=false
//					// TODO versionTimestamp:=...
//					((HTree) htree).nentries++;
//					// insert Ok.
//					return;
//				}
//			}
//		}
//
//		// unable to insert
//		final DirectoryPage np;
//		if (globalDepth == htree.addressBits) {
//			// max depth so add level
//			np = ((HTree) htree).addLevel2(this);
//		} else {
//			// otherwise split page by asking parent to split and re-inserting
//			// values
//
//			np = getParentDirectory();
//			np.split(this); // will re-insert tuples from original page
//		}
//		
//		np.insertRawTuple(key, val, 0);
//	}
//
//	// setValue() is unused. only reference is in insertRawTuple (above).
//	private void setValue(final int entryIndex, final byte[] newval) {
//
//		// Tunnel through to the mutable object.
//        final MutableBucketData data = (MutableBucketData) this.data;
//		
//        /*
//		 * Update the entry on the leaf.
//		 */
//		if (hasRawRecords()) {
//
//			/*
//			 * Note: If the old value was a raw record, we need to delete
//			 * that raw record now.
//			 * 
//			 * Note: If the new value will be a raw record, we need to write
//			 * that raw record onto the store now and save its address into
//			 * the values[] raba.
//			 */
//			final long oaddr = getRawRecord(entryIndex);
//
//			if(oaddr != IRawStore.NULL) {
//				
//				htree.deleteRawRecord(oaddr);
//				
//			}
//			
//			final long maxRecLen = htree.getMaxRecLen();
//			
//			if (newval != null && newval.length > maxRecLen) {
//
//				// write the value on the backing store.
//				final long naddr = htree.writeRawRecord(newval);
//
//				// save its address in the values raba.
//				data.vals.values[entryIndex] = ((HTree) htree)
//						.encodeRecordAddr(naddr);
//				
//				// flag as a raw record.
//				data.rawRecords[entryIndex] = true;
//
//			} else {
//				
//				data.vals.values[entryIndex] = newval;
//			
//				data.rawRecords[entryIndex] = false;
//				
//			}
//			
//		} else {
//
//			data.vals.values[entryIndex] = newval;
//			
//		}
//
//	}
	
	/**
	 * Convenience method returns the byte[] for the given index in the leaf. If
	 * the tuple at that index is a raw record, then the record is read from the
	 * backing store. More efficient operations should be performed when copying
	 * the value into a tuple.
	 * 
	 * @param leaf
	 *            The leaf.
	 * @param index
	 *            The index in the leaf.
	 * 
	 * @return The data.
	 * 
	 * @see AbstractTuple#copy(int, ILeafData)
	 */
    public byte[] getValue(final int index) {
    	
		if (!hasRawRecords()) {
		
			return getValues().get(index);
			
		}
		
		final long addr = getRawRecord(index);
		
		if( addr == IRawStore.NULL) {

			return getValues().get(index);

		}
		
		final ByteBuffer tmp = htree.readRawRecord(addr);
		
		if (tmp.hasArray() && tmp.arrayOffset() == 0 && tmp.position() == 0
				&& tmp.limit() == tmp.capacity()) {
			/*
			 * Return the backing array.
			 */
			return tmp.array();
		}

		/*
		 * Copy the data into a byte[].
		 */

		final int len = tmp.remaining();

		final byte[] a = new byte[len];

		tmp.get(a);

		return a;

    }

    @Override
	final public ByteBuffer readRawRecord(long addr) {
		
		return htree.readRawRecord(addr);

	}

	/*
	 * TODO When writing a method to remove a key/value, the following logic
	 * should be applied to delete the corresponding raw record on the backing
	 * store when the tuple is deleted.
	 */
//	/*
//	 * If the tuple was associated with a raw record address, then delete
//	 * the raw record from the backing store.
//	 * 
//	 * Note: The general copy-on-write contract of the B+Tree combined with
//	 * the semantics of the WORM, RW, and scale-out persistence layers will
//	 * ensure the actual delete of the raw record is deferred until the
//	 * commit point from which the tuple was deleted is no longer visible.
//	 */
//	if (data.hasRawRecords()) {
//
//		final long addr = data.getRawRecord(entryIndex);
//
//		if (addr != IRawStore.NULL) {
//
//			btree.deleteRawRecord(addr);
//
//		}
//
//	}

    /**
     * Split the bucket page into two, updating the pointers in the parent
     * accordingly.
     */
	void split() {
	    
        /*
         * Note: This is one of the few gateways for mutation of a BucketPage
         * via the main htree API (insert, lookup, delete). By ensuring that we
         * have a mutable directory here, we can assert that the directory must
         * be mutable in other methods.
         */
        final BucketPage copy = (BucketPage) copyOnWrite();

        if (copy != this) {

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

            copy.split();
            return;
            
        }

        /*
         * DO THE WORK HERE
         * 
         * FIXME We are doing too much work here since the BucketPage which is
         * being split does NOT need to be made mutable. However, this follows
         * the established copy-on-write pattern by starting from the leaf.
         * Revisit and optimize this once we have a stable eviction pattern for
         * the HTree.
         */
        getParentDirectory()._splitBucketPage(this);
        
	}
	
	void addLevel() {
		/**
		 * Ensure parent directory is mutable - at present this requires making
		 * a Leaf mutable
		 */
        final BucketPage copy = (BucketPage) copyOnWrite();
        if (copy != this) {
        	copy.addLevel();
        	return;
        }
    	getParentDirectory()._addLevel(this);
	}

	@Override
	boolean isClean() {
		return !isDirty();
	}

   @Override
	public int removeAll(final byte[] key) {
		if (isReadOnly()) {
			BucketPage copy = (BucketPage) copyOnWrite(getIdentity());
			
			return copy.removeAll(key);
		}
		
		// non-optimal
		int ret = 0;
		while (removeFirst(key) != null) ret++;
		
		return ret;
	}
	
	/**
	 * Must check for rawRecords and remove the references.
	 */
   @Override
	final public byte[] removeFirst(final byte[] key) {

      if (isReadOnly()) {
			final BucketPage copy = (BucketPage) copyOnWrite(getIdentity());
			
			return copy.removeFirst(key);
		}
		
		final int index = lookupIndex(key);
		
		if (index == -1)
			return null;
		
		// get byte[], reading rawRecord if necessary
		final long addr = hasRawRecords() ? getRawRecord(index) : IRawStore.NULL;
		final byte[] ret;
		if (addr != IRawStore.NULL) {
			ret = getBytes(htree.readRawRecord(addr));
			
			htree.deleteRawRecord(addr);					
		} else {
			ret = data.getValues().get(index);
		}
		
		// Now remove reference from data
		((MutableBucketData) data).remove(index);
		
		
		return ret;
		
	}

	/**
	 * Since the BucketPage orders its keys the first key will be the
	 * "lowest" in sort order.
	 * 
	 * @return first key value
	 */
	public byte[] getFirstKey() {
		
	   assert data.getKeys().size() > 0;
		
		return data.getKeys().get(0);
	}

}
