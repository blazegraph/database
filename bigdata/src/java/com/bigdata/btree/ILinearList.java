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
 * Created on Feb 14, 2007
 */

package com.bigdata.btree;

/**
 * Interface for methods that return or accept an ordinal index into the entries
 * in the B+-TRee.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
     * 
     * @see #keyAt(int)
     * @see #valueAt(int)
     */
    public int indexOf(byte[] key);

    /**
     * Return the key for the identified entry. This performs an efficient
     * search whose cost is essentially the same as {@link #lookup(Object)}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The key at that index position (not a copy).
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     * 
     * @see #indexOf(Object)
     * @see #getValue(int)
     */
    public byte[] keyAt(int index);
    
    /**
     * Return the value for the identified entry. This performs an efficient
     * search whose cost is essentially the same as {@link #lookup(Object)}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The value at that index position.
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     * 
     * @see #indexOf(Object)
     * @see #keyAt(int)
     */
    public Object valueAt(int index);
    
}
