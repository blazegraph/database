/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 19, 2007
 */

package com.bigdata.btree;

/**
 * Interface for operations on an ordered set of keys. Each key is a variable
 * length unsigned byte[]. Keys are considered to be <em>immutable</em>,
 * though this is NOT enforced. Several aspects of the code assume that a byte[]
 * key is NOT modified once it has been created. This makes it possible to copu
 * references to keys rather than allocating new byte[]s and copying the data.
 * There are mutable and immutable implementations of this interface.
 * 
 * @todo This code is optimized for very long variable length unsigned byte[]s.
 *       It will be much less efficient that direct comparison when the key is a
 *       simple int or long integer value (including a float or double which can
 *       be expressed in the int and long spaces respectively). Provide an
 *       alternative realization of the btree that is efficient for these
 *       special cases for use in, e.g., an embedded object database
 *       application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBuffer {

//    /**
//     * The minimum #of allowed keys. When the instance is immutable, this value
//     * still provides the minimum #of keys that are allowed in a mutable
//     * instance.
//     */
//    public int getMinKeyCount();
    
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
     * Return a new mutable instance. When possible, the new instance will share
     * references to the same keys.
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
