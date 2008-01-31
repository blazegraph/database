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
 * Created on Jan 19, 2007
 */

package com.bigdata.btree;

/**
 * Interface for operations on an ordered set of keys. Each key is a variable
 * length unsigned byte[]. Keys are considered to be <em>immutable</em>,
 * though this is NOT enforced. Several aspects of the code assume that a byte[]
 * key is NOT modified once it has been created. This makes it possible to copy
 * references to keys rather than allocating new byte[]s and copying the data.
 * There are mutable and immutable implementations of this interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBuffer {
    
    /**
     * The #of defined keys.
     */
    public int getKeyCount();

    /**
     * Return the key at the specified index.  Whenever possible, the reference
     * to the key is returned.  When necessary, a new byte[] will be allocated
     * and returned to the caller.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     *            
     * @return The key.
     */
    public byte[] getKey(int index);

    /**
     * Return a mutable instance. If the instance is mutable, returns
     * <code>this</code>.
     * 
     * @exception IllegalArgumentException
     *                if the capacity is less than the #of defined keys.
     */
    public MutableKeyBuffer toMutableKeyBuffer();
    
    /**
     * A human readable representation of the keys.
     */
    public String toString();

    /**
     * <p>
     * Search for the given <i>searchKey</i> in the key buffer.
     * </p>
     * <p>
     * Each time it is invoked, this method either returns the index of the
     * child (for a node) or the entry (for a leaf) with that search key, or the
     * insert position for that search key. When invoked by a node, the insert
     * position is translated to identify the child that spans the search key.
     * When invoked by a leaf, the insert position is interpreted as either a
     * key not found or a key found.
     * </p>
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return index of the search key, if it is found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be inserted. Note
     *         that this guarantees that the return value will be >= 0 if and
     *         only if the key is found.
     * 
     * @exception IllegalArgumentException
     *                if the searchKey is null.
     */
    public int search(byte[] searchKey);

    /**
     * Return the largest leading prefix shared by all keys.
     */
    public byte[] getPrefix();
    
    /**
     * The length of the leading prefix shared by all keys.
     */
    public int getPrefixLength();

    /**
     * The maximum #of keys that may be held in the buffer (its capacity).
     */
    public int getMaxKeys();
    
    /**
     * True iff the key buffer can not contain another key.
     */
    public boolean isFull();

}
