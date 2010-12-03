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
 * Created on Dec 2, 2010
 */
package com.bigdata.htree.data;

import java.lang.ref.Reference;

import com.bigdata.io.IDataRecordAccess;
import com.bigdata.rawstore.IRawStore;

/**
 * Interface for the data record of a hash directory. A hash directory provides
 * an address space. In a hash table, all of the children are buckets. In a hash
 * tree, children may be either buckets or directories. If the hash tree is
 * balanced, then all children on the same level are of the same type (either
 * buckets or directories). If the hash tree is unbalanced, then children may be
 * of either type on a given level.
 * <p>
 * The hash directory provides an index which maps a subset of the bits in a
 * hash value onto a directory entry. The directory entry provides the storage
 * address of the child page to which a lookup with that hash value would be
 * directed. The directory entry also indicates whether the child is a bucket or
 * another directory. This requires 1-bit per directory entry, which amounts to
 * an overhead of 3% when compared to a record which manages to encode the
 * bucket / directory distinction into the storage address.
 * <p>
 * The number of entries in a hash directory is a function of the globalDepth of
 * that directory: <code>entryCount := 2^globalDepth</code>. The globalDepth of
 * a child directory is its localDepth in the parent directory. While the
 * localDepth of a <em>persistent</em> child may be computed by scanning and
 * counting the #of references to that child, the copy-on-write policy used to
 * support MVCC for the hash tree requires that the storage address of a dirty
 * child is undefined. Therefore, the localDepth MUST be explicitly stored in
 * the directory record. Assuming 32-bit hash codes, this is a cost of 4 bits
 * per directory entry which amounts to a 11% overhead when compared to a record
 * which manages to encode that information using a scan of the directory
 * entries.
 * <p>
 * By far the largest storage cost associated with a directory page are the
 * addresses of the child pages. Bigdata uses <code>long</code> addresses for
 * the {@link IRawStore} interface. However, it is possible to get by with int32
 * addresses when using the RWStore.
 * <p>
 * Finally, bigdata uses checksums on all data records. Therefore the maximum
 * space available on a 4k page is actually 4096-4 := 4094 bytes. [Yikes! This
 * means that we can not store power of 2 addresses efficiently. That means that
 * we really need to use a compressed dictionary representation in order to have
 * efficient storage utilization with good fan out.]
 */
public interface IDirectoryData extends IDataRecordAccess {

	/**
	 * Return the storage address of the child page.
	 * 
	 * @param index
	 *            The index into the hash directory.
	 * 
	 * @return The address of the page at that index -or- ZERO (0L) if the child
	 *         is not persistent (including when the child has become dirty
	 *         since it address will not be known until the next time the page
	 *         is written out on the store).
	 */
	long getAddress(int index);

	/**
	 * Return <code>true</code> if the child page at the given index is a hash
	 * bucket (versus a hash directory).
	 * 
	 * @param index
	 *            The index into the hash directory.
	 * 
	 * @return <code>true</code> if the child page is a hash bucket and
	 *         <code>false</code> if it is a hash directory.
	 */
	boolean isBucket(int index);

	/**
	 * Return the local depth of the child page. This may be used to decide the
	 * #of directory entries which reference that child using the formula
	 * <code>2 ^ (globalDepth - localDepth)</code>, where globalDepth is the
	 * globalDepth of this directory page.
	 * <p>
	 * Note: If the hash directory is setup with the MSB as the first bit in the
	 * hash prefix, then the local depth may be computed by a scan of the
	 * directory counting the #of directory entries which point to the same
	 * child page (when the child page is dirty, you instead must count the #of
	 * entries having the same {@link Reference}). Thus using a MSB prefix
	 * policy makes it possible to compute the local depth rather than storing
	 * it.
	 * 
	 * @param index
	 *            The child page.
	 * 
	 * @return The local depth of that child page.
	 */
	int getLocalDepth(int index);
	
}
