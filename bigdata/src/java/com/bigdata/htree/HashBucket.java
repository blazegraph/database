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
 * Created on Nov 29, 2010
 */
package com.bigdata.htree;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.data.IAbstractNodeDataCoder;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.data.IBucketData;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;

/**
 * A (very) simple hash bucket. The bucket holds N int32 keys.
 * 
 * @todo The hash of the key should be part of the ITuple interface so it can be
 *       passed along based on the application level encoding of the key.
 * 
 * @todo Support out-of-line representations of the key and/or value for a tuple
 *       when they are large. The definition of "large" can be a configuration
 *       value for the index metadata. For example, 1/4 of the target page size
 *       or (1k assuming a target page size of 4k). It should also be possible
 *       to specify that the value is always out of line (this corresponds to
 *       the common practice in a relational database of indexing into a
 *       persistent heap rather than the perfect indices with their inline data
 *       which we use for RDF statements).
 *       <p>
 *       The easiest way to do this is to treat the key and value separately and
 *       write them as raw records onto the backing store if they exceed the
 *       configured threshold. For the B+Tree, we can not readily move the key
 *       out of line since we need it for search, but it is easy to do this for
 *       the HTree. (For now, I suggest that we live with the constraint that
 *       the key can not be moved out of line for the B+Tree.) For both index
 *       structures, it is easy to move the value out of line. The tuple
 *       metadata will stay inline regardless.
 *       <p>
 *       In order to resolve out of line keys and/or values the
 *       {@link ILeafData} will need access to the {@link IRawStore} reference.
 *       This may require an API change to {@link IRaba} and/or
 *       {@link IAbstractNodeDataCoder} (the latter also needs to be modified to
 *       work with {@link IDirectoryData} records) in order to made the
 *       {@link IRawStore} reference available when the record is serialized
 *       and/or deserialized.
 *       <p>
 *       When the tuple is deleted, the raw record reference for its key and/or
 *       value must also be deleted.
 *       <p>
 *       During a bulk index build, the raw record must be copied to the target
 *       index store, e.g., an {@link IndexSegment} using an
 *       {@link IOverflowHandler}.
 */
public class HashBucket extends AbstractHashPage<HashBucket>//
//		implements IBucketData// 
{

	private final transient static Logger log = Logger
			.getLogger(HashBucket.class);
	
	/**
	 * The #of hash code bits which are in use by this {@link HashBucket}.
	 * <p>
	 * Note: There are <code>2^(globalBits-localBits)</code> dictionary entries
	 * which address a given page. Initially, globalBits := 1 and localBits :=
	 * 0. For these values, we have <code>2^(1-0) == 2</code> references to the
	 * initial page of the hash table.
	 * 
	 * @todo If we need to examine this when we change the size of the address
	 *       space then it makes more sense to have this as local metadata in
	 *       the address table than as local data in the bucket (the latter
	 *       would require us to visit each bucket when expanding the address
	 *       space). This only needs to be 4 bits to express values in [0:31].
	 * 
	 * @todo When overflow buckets are chained together, does each bucket have
	 *       {@link #localHashBits}? If they do, then we need to make sure that
	 *       all buckets in the chain are updated. If {@link #localHashBits} is
	 *       only marked on the first bucket in the chain then we need to
	 *       correctly ignore it on overflow buckets.
	 * 
	 * @todo adjusting this dirties the bucket (unless the #of local bits its
	 *       stored in the address table entry, but that increases the in-memory
	 *       burden of the address table).
	 */
	private int localHashBits;

	/**
	 * The #of keys stored in the bucket. The keys are stored in a dense array.
	 * For a given {@link #size}, the only indices of the array which have any
	 * data are [0:{@link #size}-1].
	 */
	int size;

	/**
	 * The user data for the bucket.
	 * 
	 * @todo IRaba keys plus IRaba vals.
	 */
	final int[] data;

	protected void setLocalHashBits(final int localHashBits) {
		
		this.localHashBits = localHashBits;
		
	}

	public int getLocalHashBits() {
		return localHashBits;
	}

	/**
	 * Human friendly representation.
	 */
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("{localHashBits=" + getLocalHashBits());
		sb.append(",size=" + size);
		sb.append(",values={");
		for (int i = 0; i < size; i++) {
			if (i > 0)
				sb.append(',');
			sb.append(Integer.toString(data[i]));
		}
		sb.append("}}");
		return sb.toString();
	}

	/**
	 * Create a new mutable bucket.
	 * 
	 * @param htbl
	 * @param localHashBits
	 * @param bucketSize
	 */
	public HashBucket(final HashTree htbl,
			final int localHashBits, final int bucketSize) {

		super(htbl, true/* dirty */);

		if (localHashBits < 0 || localHashBits > 32)
			throw new IllegalArgumentException();

		if (bucketSize <= 0)
			throw new IllegalArgumentException();

		this.localHashBits = localHashBits;

		this.data = new int[bucketSize];

		// one more bucket.
		htbl.nbuckets++;
		
	}

	/**
	 * Return <code>true</code> if the bucket contains the key.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @return <code>true</code> if the key was found in the bucket.
	 * 
	 * @todo passing in the hash code here makes sense when the bucket stores
	 *       the hash values, e.g., if we always do that or if we have an out of
	 *       bucket reference to a raw record because the tuple did not fit in
	 *       the bucket.
	 */
	public boolean contains(final int h, final int key) {

		for (int i = 0; i < size; i++) {

			if (data[i] == key)
				return true;

		}

		return false;

	}

	/**
	 * Type safe enumeration reports on the various outcomes when attempting to
	 * insert a tuple into a page.
	 * 
	 * @todo The problem with this enumeration (or with using a return code per
	 *       the following javadoc) is that page splits are going to be deferred
	 *       until the page is evicted unless there is an aspect of the split
	 *       function which decides based on the #of tuples on the page. If a
	 *       the split function reports that the page is over capacity when it
	 *       is evicted, then we need to decide whether to split the page, chain
	 *       an overflow page, or use a larger capacity page.
	 *       <p>
	 *       What we should do is scan the page if an insert would fail (or if
	 *       the serialization of the page would fail) and determine what local
	 *       depth we would need to successfully split the page (e.g., no more
	 *       than 70% of the items would be in any prefix at a given depth).
	 *       That can be used to guide the decision to use overflow pages or
	 *       expand the directory.
	 *       <p>
	 *       What are some fast techniques for counting the #of bits which we
	 *       need to make the necessary distinctions in the bucket? Should we
	 *       build a trie over the hash codes?
	 */
	private static enum InsertEnum {
		/**
		 * The tuple was inserted successfully into this page.
		 * 
		 * @todo This could be reported as ZERO (0), which is an indication that
		 *       NO expansions where required to insert the tuple into the page.
		 */
		OK,
		/**
		 * The insert failed because the page is full. Further, the tuple has
		 * the same key value as all other tuples on the page. Therefore, either
		 * the insert must be directed into an overflow page or the page size
		 * must be allowed to increase.
		 * 
		 * @todo This could be reported as {@link Integer#MAX_VALUE}, which is
		 *       an indication that infinite expansions will not make it
		 *       possible to insert the key into this page (e.g., an overflow
		 *       page is required). [Alternatively, this could report the 
		 *       necessary page size if we allow the page size to expand.]
		 */
		KEYS_ARE_IDENTICAL,
		/**
		 * The insert failed because the page is full. Further, the hash
		 * associated with the tuple is the same as the hash for all other keys
		 * on the page. In this case, the insert operation will eventually
		 * succeed if the address space is expanded (one or more times).
		 * 
		 * @todo This could be reported as the #of bits which are in common for
		 *       the keys in this page. That could be used to determine how many
		 *       expansions would be required before the key could be inserted.
		 *       [If KEYS_ARE_IDENTICAL is handled by reporting the necessary
		 *       page size, then this could report the #of hash bits which are
		 *       identical using a negative integer (flipping the sign).]
		 */
		HASH_IS_IDENTICAL;
	}
	
	/**
	 * Insert the key into the bucket (duplicates are allowed). It is an error
	 * if the bucket is full.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @return <code>false</code> iff the bucket must be split.
	 * 
	 * @todo The caller needs to be careful that [h] is the full hash code for
	 *       the key. Normally this is not a problem, but we sometimes wind up
	 *       with masked off hash codes, especially during splits and merges,
	 *       and those must not be passed in here.
	 */
	public void insert(final int h, final int key) {

		if (size == data.length) {

			/*
			 * The bucket must be split, potentially recursively.
			 * 
			 * Note: Splits need to be triggered based on information which is
			 * only available to the bucket when it considers the insert of a
			 * specific tuple, including whether the tuple is promoted to a raw
			 * record reference, whether the bucket has deleted tuples which can
			 * be compacted, etc.
			 * 
			 * @todo I need to figure out where the control logic goes to manage
			 * the split. If the bucket handles splits, then we need to pass in
			 * the table reference.
			 */

			// split the bucket and insert the record (recursive?)
			split(key, this);

			/*
			 * Insert the key into the expanded hash table (this will insert
			 * into either the old or the new bucket, depending on the hash code
			 * for the key).
			 * 
			 * FIXME There are a variety of special conditions which need to be
			 * handled by insert(), especially all keys have the same value or
			 * the same int32 hash code or the tuple is too large for the
			 * bucket. Those conditions all need to be handled before requested
			 * a split. Since insert() has to handle all of this, it is also
			 * responsible for re-attempting the key insertion after the split.
			 * 
			 * The next step is to handle cases where splitting the bucket once
			 * does not result in a bucket with sufficient space for the new
			 * key. There are actually two cases here: (1) the hash codes of the
			 * keys are distinct, so if we double the address space enough times
			 * the insert will succeed; (2) the hash codes of the keys are
			 * identical, so no amount of expansion of the address space will
			 * permit the insert to succeed and an overflow page must be used.
			 * For (1) we can also chose to use an overflow page in order to
			 * prevent run away expansion of the address space.
			 * 
			 * This class needs to be converted to use persistence and to use an
			 * IRaba for keys/values. For the sake of the unit tests, it needs
			 * to be parameterized for the overflow versus expand decision and
			 * the IRaba for the keys needs to be defined such that we have a
			 * guaranteed split when there are three integer keys (or a split
			 * function could be used to make this decision based on more
			 * general criteria). [Could also use a pure- append binary raba w/
			 * compacting if the raba is full and there are deleted tuples.]
			 */
			if (log.isDebugEnabled())
				log.debug("retrying insert: key=" + key);

			/*
			 * @todo This can recurse until the address space reaches the
			 * maximum possible address space and then throw an exception. The
			 * code should be modified to use a decision function for growing
			 * the page, chaining an overflow page, or splitting the page (when
			 * it would cause the address space to be doubled).
			 */
			htbl.insert(key);
			
//			{
//				// the hash value of the key.
//				final int h = htbl.hash(key);
//				// the address of the bucket for that hash code.
//				final int addr = htbl.getRoot().addrOf(h);
//				// the bucket for that address.
//				final SimpleBucket btmp = htbl.getBucketAtStoreAddr(addr);
//				if (btmp.insert(h, key)) {
//					// insert was successful.
//					return;
//				}
//				/*
//				 */
//			
//				log
//						.fatal("Split of bucket did not map space available for new key: key="
//								+ key + ", table=" + htbl.dump());
//				
//				throw new UnsupportedOperationException();
//				
//			}
			
			return;
			
		}

		data[size++] = key;

        // one more entry in the index.
        htbl.nentries++;

	}

	/**
	 * Delete a tuple having the specified key. If there is more than one such
	 * tuple, then a random tuple having the key is deleted.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @todo return the delete tuple.
	 */
	public boolean delete(final int h, final int key) {

		for (int i = 0; i < size; i++) {

			if (data[i] == key) {

				// #of tuples remaining beyond this point.
				final int length = size - i - 1;

				if (length > 0) {

					// Keep the array dense by copying down by one.
					System.arraycopy(data, i + 1/* srcPos */, data/* dest */,
							i/* destPos */, length);

				}

				size--;

		        // one less entry in the index.
		        htbl.nentries--;

				return true;

			}

		}

		return false;

	}

	/**
	 * The #of entries in the bucket.
	 */
	public int getEntryCount() {

		return size;

	}

	/**
	 * Visit the entries in any order.
	 */
	public Iterator<Integer/* key */> getEntries() {

		return new EntryIterator();

	}

	/**
	 * Visits the entries in the page.
	 */
	private class EntryIterator implements Iterator<Integer> {

		private int current = 0;

		private EntryIterator() {

		}

		@Override
		public boolean hasNext() {
			return current < size;
		}

		@Override
		public Integer next() {
			return data[current++];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public void delete() throws IllegalStateException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * Split the bucket, adjusting the address map iff necessary. How this
	 * proceeds depends on whether the hash #of bits used in the bucket is equal
	 * to the #of bits used to index into the bucket address table. There are
	 * two cases:
	 * <p>
	 * Case 1: If {@link #globalHashBits} EQ the
	 * {@link HashBucket#localHashBits}, then the bucket address table is out
	 * of space and needs to be resized.
	 * <p>
	 * Case 2: If {@link #globalHashBits} is GT
	 * {@link HashBucket#localHashBits}, then there will be at least two
	 * entries in the bucket address table which point to the same bucket. One
	 * of those entries is relabeled. The record is then inserted based on the
	 * new #of hash bits to be considered. If it still does not fit, then either
	 * handle by case (1) or case (2) as appropriate.
	 * <p>
	 * Note that records which are in themselves larger than the bucket size
	 * must eventually be handled by: (A) using an overflow record; (B) allowing
	 * the bucket to become larger than the target page size (using a larger
	 * allocation slot or becoming a blob); or (C) recording the tuple as a raw
	 * record and maintaining only the full hash code of the tuple and its raw
	 * record address in the bucket (this would allow us to automatically
	 * promote long literals out of the hash bucket and a similar approach might
	 * be used for a B+Tree leaf, except that a long key will still cause a
	 * problem [also, this implies that deleting a bucket or leaf on the
	 * unisolated index of the RWStore might require a scan of the IRaba to
	 * identify blob references which must also be deleted, so it makes sense to
	 * track those as part of the bucket/leaf metadata).
	 * 
	 * @param h
	 *            The key which triggered the split.
	 * @param bold
	 *            The bucket lacking sufficient room for the key which triggered
	 *            the split.
	 * 
	 * @todo caller will need an exclusive lock if this is to be thread safe.
	 * 
	 * @todo Overflow buckets (or oversize buckets) are required when all hash
	 *       bits considered by the local bucket are the same, when all keys in
	 *       the local bucket are the same, and when the record to be inserted
	 *       is larger than the bucket. In order to handle these cases we may
	 *       need to more closely integrate the insert/split logic since
	 *       detecting some of these cases requires transparency into the
	 *       bucket.
	 * 
	 *       FIXME The caller could decide to switch to a larger page size or
	 *       chain overflow pages together in order to increase storage
	 *       utilization or handle buckets having large populations of identical
	 *       keys (or keys with the same int32 hash code). [This decision must
	 *       be made before we decide to split.]
	 * 
	 *       FIXME The caller should handle the promotion of large tuples to raw
	 *       records when they are inserted, so we do not need to handle that
	 *       here either.
	 */
	private void split(final int key, final HashBucket bold) {

		final int globalHashBits = htbl.getGlobalHashBits();
		
		if (log.isDebugEnabled())
			log.debug("globalBits=" + globalHashBits + ",localHashBits="
					+ bold.getLocalHashBits() + ",key=" + key);

		if (globalHashBits < bold.getLocalHashBits()) {
			// This condition should never arise.
			throw new AssertionError();
		}

		if (globalHashBits == bold.getLocalHashBits()) {
			/*
			 * The address table is out of space and needs to be resized.
			 */
			htbl.getRoot().doubleAddressSpaceAndSplitBucket(key, bold);
			// fall through
		}

		if (globalHashBits > bold.getLocalHashBits()) {
			/*
			 * Split the bucket.
			 */
			htbl.getRoot().splitBucket(key, bold);
			// fall through.
		}

	}

	/*
	 * IBucketData
	 */
	
	public int getHash(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getLengthMSB() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/*
	 * IAbstractNodeData
	 */
	
	public boolean hasVersionTimestamps() {
		// TODO Auto-generated method stub
		return false;
	}

	public AbstractFixedByteArrayBuffer data() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getKeyCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public IRaba getKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getMaximumVersionTimestamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMinimumVersionTimestamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getSpannedTupleCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isCoded() {
		// TODO Auto-generated method stub
		return false;
	}

	final public boolean isLeaf() {
		
		return true;
		
	}

	/**
	 * The result depends on the implementation. The {@link HashBucket} will be
	 * mutable when it is first created and is made immutable when it is
	 * persisted. If there is a mutation operation, the backing
	 * {@link IBucketData} is automatically converted into a mutable instance.
	 */
    final public boolean isReadOnly() {
        
//        return data.isReadOnly();
		// TODO Auto-generated method stub
		return false;
        
    }

    /*
	 * ILeafData
	 */
	
	public boolean getDeleteMarker(int index) {
		// TODO Auto-generated method stub
		return false;
	}

	public long getNextAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getPriorAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getValueCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public IRaba getValues() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getVersionTimestamp(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean hasDeleteMarkers() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isDoubleLinked() {
		// TODO Auto-generated method stub
		return false;
	}

}
