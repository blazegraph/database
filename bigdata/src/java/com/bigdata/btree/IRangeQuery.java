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
 * Interface for range count and range query operations (non-batch api).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRangeQuery {
    
    /**
     * Return an iterator that visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @see #entryIterator(), which visits all entries in the btree.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     * 
     * @todo define behavior when the toKey is less than the fromKey.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey);

    /**
     * Return the #of entries in a half-open key range. The fromKey and toKey
     * need not be defined in the btree. This method computes the #of entries in
     * the half-open range exactly using {@link AbstractNode#indexOf(Object)}.
     * The cost is equal to the cost of lookup of the both keys.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The #of entries in the half-open key range. This will be zero if
     *         <i>toKey</i> is less than or equal to <i>fromKey</i> in the
     *         total ordering.
     * 
     * @todo this could overflow an int32 for a scale out database.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey);

}
