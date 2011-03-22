/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Dec 2, 2010
 */
package com.bigdata.htree.data;

import java.security.MessageDigest;
import java.util.Iterator;

import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.data.ILeafData;

/**
 * Interface for the data record of a hash bucket. The hash bucket extends the
 * B+Tree leaf data record interface. A hash bucket page may be shared by
 * multiple directory entries (this is one of the principle tenants of
 * extendible hashing). However, the bucket is just a bucket to each such
 * directory entry. There is no sense of offset addressing into the shared
 * bucket.
 * <p>
 * The {@link ILeafData#getPriorAddr()} and {@link ILeafData#getNextAddr()}
 * fields of the {@link ILeafData} record are reserved by the hash tree to
 * encode the search order for range queries when used in combination with an
 * order preserving hash function.
 * <p>
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public interface IBucketData extends ILeafData {

//	/**
//	 * The local depth of the hash bucket.
//	 * <p>
//	 * Note: This information is encoded by the parent hash directory.
//	 */
//	int getLocalDepth();

	/**
	 * {@inheritDoc}
	 * <p>
	 * For clarification, this returns the #of entries in the hash bucket (not
	 * just the number of distinct keys).
	 */
	int getKeyCount();
	
	/**
	 * The length (in bits) of the MSB prefix shared by the hash values of the
	 * keys on this page.
	 */
	int getLengthMSB();

	/**
	 * Return the hash value of the key at the given index.
	 * 
	 * @param index
	 *            The index of the key.
	 * 
	 * @return The hash value of that key.
	 * 
	 * @deprecated This is just the int32 key for that tuple. It should be
	 *             generalized as a byte[]. This will require us to generalize
	 *             the handle of the LSB and MSB bits as well, all of which must
	 *             be efficient and compact. For a {@link HTree}, we need to
	 *             declare the bits length the key. That might be part of the
	 *             {@link Checkpoint} or the {@link HTree}'s
	 *             {@link IndexMetadata}. {@link HTree} should be usable with
	 *             int32 hash codes, int64, and {@link MessageDigest} based hash
	 *             codes, which are 20+ bytes as well as with a fixed prefix of
	 *             a {@link MessageDigest} for shorter keys which still have
	 *             strongly random distributions, plus order-preserving hash
	 *             codes.
	 */
	int getHash(int index);

	/**
	 * Return an {@link Iterator} which visits the index of each entry in the
	 * hash bucket having the given hash code.
	 * 
	 * @param h
	 *            The hash code.
	 * 
	 * @todo Note: There is a design tradeoff between autoboxing of the
	 *       <code>int</code> index and allowing the {@link IBucketData} class
	 *       to encapsulate the iterator pattern together with any setup which
	 *       can be done once per scan for a given hash code. For example, using
	 *       a BitInputStream. The iterator allows us to amortize the cost of
	 *       that setup, but we pay for the autoboxing of the index values.
	 *       However, autobox primitives tend to be quite cheap as they are
	 *       rapidly reclaimed by GC.
	 *       <p>
	 *       It is possible to implement an extension interface which returns
	 *       the [int] index without autoboxing. If this method signature is
	 *       modified to return that interface then the implementation can avoid
	 *       autoboxing.
	 * 
	 * @deprecated Since hash codes will be unsigned byte[] keys, this will have
	 *             to be visit a read-only byte[] slice or a copy of the byte[]
	 *             key. We already have methods for this. Those methods (and the
	 *             leaf coder) might be more compact, e.g., for int32 keys,
	 *             since we code a (short) fixed length key in more simply.
	 */
	Iterator<Integer> hashIterator(int h);
	
//	/**
//	 * The storage address of the last overflow page in the overflow chain.
//	 * <p>
//	 * Note: Regardless of the hashing scheme, either larger pages or overflow
//	 * pages are required when all keys in a hash bucket are identical. The
//	 * advantage of overflow pages over larger pages (which can be converted
//	 * into a series of linked pages depending on the persistence store) is that
//	 * only the desired page needs to be read/written. The disadvantage is that
//	 * it requires more IO to query a bucket which has only one or two overflow
//	 * pages and that we must manage the eventual compaction and unlinking of
//	 * overflow pages as tuples are deleted.
//	 */
//	long getOverflowAddr();
	
}
