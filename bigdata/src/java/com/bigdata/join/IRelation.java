/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 23, 2008
 */

package com.bigdata.join;

import com.bigdata.btree.IRangeQuery;

/**
 * An abstraction corresponding to a set of elements using some schema.
 * <p>
 * 
 * @todo A triple store is a relation with N access paths, one for each
 *       statement index. The term2id and id2term indices are a 2nd relation.
 *       The full text index is a third relation. When querying a focusStore and
 *       a db, the focusStore will have the same relation class (the triples)
 *       and the index classes declared (SPO, POS, OSP) but the relation
 *       instance and the indices will be distinct from those associated with
 *       the main db.
 * 
 * FIXME An {@link IRelation} introduces a dependency on access to the data. if
 * {@link IPredicate} knows its owning {@link IRelation} then rules can not be
 * written outside of the context of the data. it might be better to create a
 * binding between the predicates in a rule and the data source(s). that binding
 * really needs to be symbolic since the data sources can not be passed by value
 * when within a distributed JOIN. Instead they need to be things like the
 * namespace of the scale out triple store. And if they are symbolic then we
 * need a means to convert them to functional objects that can materialize
 * access paths that actually read or write on the appropriate relation.
 * 
 * FIXME Is there no relation in the head or is this how we handle query (no
 * relation, results are written onto a buffer for eventual read by a client aka
 * SELECT) vs write (write on the relation via a suitable buffering mechanism
 * aka INSERT, UPDATE, DELETE)?
 * 
 * @todo Note that a different subset of bindings might be used when feeding
 *       another JOIN.
 * 
 * @todo the buffer will require access to the {@link IBigdataClient} when
 *       feeding another JOIN using the distributed federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param E
 *            The generic type for the elements in the relation.
 */
public interface IRelation<E> extends IAccessPathFactory<E> {

    /**
     * The #of elements in the relation.
     * 
     * @param exact
     *            When <code>true</code> an exact count is reported. An exact
     *            count will require a key-range scan if delete markers are in
     *            use, in which case it will be more expensive. See
     *            {@link IRangeQuery}.
     * 
     * @todo depending on this for fixed point termination is simpler but
     *       potentially less efficient than reporting from the various write
     *       methods whether any elements in the relation were modified.
     */
    public long getElementCount(boolean exact);
    
}
