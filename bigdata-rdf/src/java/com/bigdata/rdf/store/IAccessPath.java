/*

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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.store;

import java.util.Iterator;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOFilter;
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
public interface IAccessPath extends Iterable<SPO> {

    /**
     * The triple pattern (s, p, o).
     * <p>
     * Note: A value of {@link IRawTripleStore#NULL} indicates a wildcard.
     */
    public long[] getTriplePattern();
    
//    /**
//     * The index that is the most efficient for the triple pattern.
//     */
//    public IIndex getStatementIndex();

    /**
     * Identifies the statement index that was choosen and hence the natural
     * order in which the statements would be visited by {@link #iterator()}.
     */
    public KeyOrder getKeyOrder();

    /**
     * True iff the access path is empty (there are no matches for the triple
     * pattern). This is more conclusive than {@link #rangeCount()} since you
     * MAY have a non-zero range count when the key range is in fact empty
     * (there may be "deleted" index entries within the key range).
     */
    public boolean isEmpty();
    
    /**
     * The maximum #of statements that could be returned for the specified
     * triple pattern.
     * <p>
     * Note: This is an upper bound since scale-out indices use delete markers
     * and therefore will report entries that have been deleted but not yet
     * purged from the index in the range count. If the index does not support
     * delete markers then this will be an exact count.
     */
    public long rangeCount();

    /**
     * The raw iterator for traversing the selected index within the key range
     * implied by the triple pattern specified to the ctor.
     * 
     * @todo for scale-out version, the optional {@link ISPOFilter} should be
     *       sent to the data service.
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
     * @param filter
     *            An optional filter. When non-<code>null</code>, only
     *            statements matching this filter will be visited.
     * 
     * @return The iterator.
     */
    public ISPOIterator iterator(ISPOFilter filter);

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
     * @param filter
     *            An optional filter. When non-<code>null</code>, only
     *            statements matching this filter will be visited.
     * 
     * @return The iterator.
     * 
     * @todo modify the iterator to support {@link ISPOIterator#remove()} and
     *       state here that it will do so.
     */
    public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter);

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
     * 
     * @todo modify to accept a term filter so that we can choose to include or
     *       exclude literals based on an examination of the term identifier.
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

    /**
     * Remove all statements selected by the triple pattern (batch, parallel,
     * chunked, NO truth maintenance).
     * <p>
     * Note: This does NOT perform truth maintenance. Statements are removed
     * regardless of their {@link StatementEnum} value.
     * 
     * @param An
     *            optional filter. When non-<code>null</code>, only matching
     *            statements are removed.
     * 
     * @return The #of statements that were removed.
     */
    public int removeAll(ISPOFilter filter);
    
}
