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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.store;

import java.util.Iterator;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.util.KeyOrder;

/**
 * An interface that operations on a triple pattern using the most efficient
 * statement index for that triple pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAccessPath {

    /**
     * The triple pattern (s, p, o).
     * <p>
     * Note: A value of {@link ITripleStore#NULL} indicates a wildcard.
     */
    public long[] getTriplePattern();
    
    /**
     * The index that is the most efficient for the triple pattern.
     */
    public IIndex getIndex();

    /**
     * Identifies the statement index that was choosen and hence the natural
     * order in which the statements would be visited by {@link #iterator()}.
     */
    public KeyOrder getKeyOrder();

    /**
     * The maximum #of statements that could be returned for the specified
     * triple pattern.
     * <p>
     * Note: This is an upper bound since {@link IIsolatableIndex} indices
     * will report entries that have been deleted but not yet purged from
     * the index in the range count. If the index does not support isolation
     * then this will be an exact count.
     */
    public int rangeCount();

    /**
     * The raw iterator for traversing the selected index within the key
     * range implied by the triple pattern specified to the ctor.
     */
    public IEntryIterator rangeQuery();

    /**
     * An iterator visiting {@link SPO}s using the natural order of the index
     * selected for the triple pattern.
     * 
     * @return The iterator.
     * 
     * @todo modify the iterator to support {@link ISPOIterator#remove()} and
     *       state here that it will do so.
     */
    public ISPOIterator iterator();

    /**
     * An iterator visiting {@link SPO}s using the natural order of the index
     * selected for the triple pattern.
     * 
     * @param limit
     *            The maximum #of {@link SPO}s that will be visited.
     * 
     * @param capacity
     *            The maximum capacity for the buffer used by the iterator. When
     *            ZERO(0), a default capacity will be used. When a <i>limit</i>
     *            is specified, the capacity will never exceed the <i>limit</i>.
     * 
     * @return The iterator.
     * 
     * @todo modify the iterator to support {@link ISPOIterator#remove()} and
     *       state here that it will do so.
     */
    public ISPOIterator iterator(int limit, int capacity);

    /**
     * Performs an efficient scan of a statement index returning the distinct
     * term identifiers found in the first key component for the
     * {@link IAccessPath}. Depending on the {@link KeyOrder} for the
     * {@link IAccessPath}, this will be the term identifiers for the distinct
     * subjects, predicates, or objects in the KB.
     * 
     * @return The distinct term identifiers in the first key component for the
     *         statement index associated with this {@link IAccessPath}. The
     *         term identifiers are in ascending order (this is the order in
     *         which they are read from the index).
     */
    public Iterator<Long> distinctTermScan();

    /**
     * Remove all statements selected by the triple pattern (batch, parallel,
     * chunked, NO truth maintenance).
     * <p>
     * Note: This does NOT perform truth maintenance. Statements are removed
     * regardless of their {@link StatementEnum} value.
     * 
     * @return The #of statements that were removed.
     */
    public int removeAll();
    
}
