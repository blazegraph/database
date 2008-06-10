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
 * FIXME (a) the prefix compression form should be serialization only so the
 * {@link ImmutableKeyBuffer} should be deprecated. This will leave
 * {@link MutableKeyBuffer} (copies references) and there should be a
 * CompactingKeyBuffer as well (copies bytes into its buffer, uses a journal
 * style allocator, and compacts when it would overflow).
 * <p>
 * (De-)serialization needs to be decoupled from the choice of how the key
 * buffer gets implemented, so that means that we are not going to be able to
 * read in a raw byte[] and "just" process the record directly. Instead, there
 * needs to be an API for populating the key buffer when it is de-serialized.
 * Since there is no longer an immutable version we can just add(byte[]) or
 * add(byte[],off,len) for each key read from the serialized form - this will
 * let us do dictionary style compression without requiring that we keep the
 * leaf compressed in memory.
 * <p>
 * The only hope in this for a raw record read and processing would be to have
 * the node or leaf state and its keys/children/value state all in the same
 * CompactingBuffer and have an API suitable for partitioning the buffer into
 * some fixed size regions (values that are overwritten when they are replaced)
 * and a variable length region that is used to store keys, values, timestamps,
 * and child addresses and compacted as necessary.
 * <p>
 * Even that is not really a "raw record" since the data are still
 * de-serialized, e.g., undoing dictionary compression or other techniques as
 * applied to keys, values, etc. It is really a technique to reduce the #of
 * distinct references in the heap and to reduce the amount of allocation by
 * copying data into a CompactingBuffer. In this way it is very much like the
 * GOM data record and it might be possible to reuse that as the basis for this.
 * In that case it was more efficient to operate with a long[] than a byte[]. In
 * fact, an int[] might be best for the 32bit generation while a long[] would be
 * better for 64bit machines. For GOM, we have a lot of long values so that
 * long[] had a bit of an edge in that way as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBuffer extends IRandomAccessByteArray {
    
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
     * key not found or a key found. You can translate an insert position into a
     * tuple index using
     * </p>
     * 
     * <pre>
     * entryIndex = -entryIndex - 1
     * </pre>
     * 
     * <p>
     * or just
     * </p>
     * 
     * <pre>
     * entryIndex = -entryIndex
     * </pre>
     * 
     * if you are looking for the first key after the searchKey.
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
     * True iff the key buffer can not contain another key.
     */
    public boolean isFull();

//    /**
//     * @todo methods are being added to this API as the btree implementation
//     *       becomes decoupled from the {@link MutableKeyBuffer} so that we can
//     *       also try out a CompactingKeyBuffer implementation.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public interface IMutableKeyBuffer {
//        
//        
//    }
    
}
