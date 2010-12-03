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

import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.data.IAbstractNodeDataCoder;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.rawstore.IRawStore;

/**
 * Interface for the data record of a hash bucket.
 * <p>
 * The hash bucket extends the B+Tree leaf node page. However, hash buckets must
 * have the HASH_KEYS flag enabled and SHOULD NOT use prefix compression unless
 * (a) an order preserving hash function is used; and (b) the tuples are in key
 * order within the page.
 * <p>
 * The hash values of the keys in the bucket will have a shared prefix (when
 * using an MSB hash prefix) which corresponds to the globalDepth of the path
 * through the hash tree leading to this bucket less the localDepth of this
 * bucket. It is therefore possible (in principle) to store only the LSB bits of
 * the hash values in the page and reconstruct the hash values using the MSB
 * bits from the path through the hash tree. In order to be able to reconstruct
 * the full hash code key based solely on local information, the MSB bits can be
 * written out once and the LSB bits can be written out once per tuple. Testing
 * the hash value of a key may then be done considering only the LSB bits of the
 * hash value. This storage scheme also has the advantage that the hash value is
 * not restricted to an int32 and is therefore compatible with the use of
 * cryptographic hash functions. (If hash values are stored in a B+Tree leaf
 * they will not shared this prefix property and can not be compressed in this
 * manner).
 * <p>
 * The {@link ILeafData#getPriorAddr()} and {@link ILeafData#getNextAddr()}
 * fields of the {@link ILeafData} record are reserved by the hash tree to
 * encode the search order for range queries when used in combination with an
 * order preserving hash function.
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

//	/**
//	 * The total bit length of the hash values of the keys.
//	 */
//	int getHashBitLength();
	
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
	 * @return The hash value of that key.
	 */
	int getHash(int index);
	
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
