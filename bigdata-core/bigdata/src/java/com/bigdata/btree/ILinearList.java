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
 * Created on Feb 14, 2007
 */

package com.bigdata.btree;

/**
 * Interface for methods that return or accept an ordinal index into the entries
 * in the B+-Tree. The semantics of this interface are build over the #of
 * spanned tuples for each child as recorded within each node of the B+Tree.
 * This provides a fast means to compute the linear index into the B+Tree of any
 * given tuple. However, this interface is only available for a local B+Tree
 * object (versus scale-out) since the spanned tuple count metadata is not exact
 * across shards. Further, when delete markers are used, the deleted tuples
 * remain in the B+Tree and the {@link ILinearList} interface will continue to
 * count them until they have been purged. Thus deleting a tuple does not change
 * the {@link #indexOf(byte[])} keys after that tuple, {@link #keyAt(long)} can
 * return the key for a deleted tuple, and {@link #valueAt(long)} will return
 * <code>null</code> if the tuple at that index is marked as deleted within the
 * B+Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ILinearList {

    /**
     * Lookup the index position of the key.
     * <p>
     * Note that {@link #indexOf(byte[])} is the basis for implementing the
     * {@link IRangeQuery} interface.
     * 
     * @param key
     *            The key.
     * 
     * @return The index of the search key, if found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be found it it were
     *         inserted into the btree without intervening mutations. Note that
     *         this guarantees that the return value will be >= 0 if and only if
     *         the key is found. When found the index will be in [0:nentries).
     *         Adding or removing entries in the tree may invalidate the index.
     *         <P>
     *         <code>pos = -(pos+1)</code> will convert an insertion point to
     *         the index at which the <i>key</i> would be found if it were
     *         inserted - this is also the index of the predecessor of <i>key</i>
     *         in the index.
     * 
     * 
     * @see #keyAt(long)
     * @see #valueAt(long)
     */
    public long indexOf(byte[] key);

    /**
     * Return the key for the identified entry. This performs an efficient
     * search whose cost is essentially the same as
     * {@link ISimpleBTree#lookup(byte[])}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The key at that index position.
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than or equal to the #of entries.
     * 
     * @see #indexOf(byte[])
     * @see #valueAt(long)
     */
    public byte[] keyAt(long index);

    /**
     * Return the value for the identified entry. This performs an efficient
     * search whose cost is essentially the same as
     * {@link ISimpleBTree#lookup(byte[])}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The value at that index position -or- <code>null</code> if
     *         there is a deleted entry at that index position then
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than or equal to the #of entries.
     * 
     * @see #indexOf(byte[])
     * @see #keyAt(long)
     */
    public byte[] valueAt(long index);
    
}
