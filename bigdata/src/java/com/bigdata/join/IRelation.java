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

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

/**
 * An abstraction corresponding to a set of elements using some schema.
 * 
 * FIXME An {@link IRelation} introduces a dependency on access to the data. if
 * {@link IPredicate} knows its owning {@link IRelation} then rules can not be
 * written outside of the context of the data. it might be better to create a
 * binding between the predicates in a rule and the data source(s). that binding
 * really needs to be symbolic since the data sources can not be passed by value
 * when within a distributed JOIN. Instead they need to be things like the
 * timestamp for the view (including the case of a checkpoint that evolves when
 * bringing a rule set to fixed point) and the namespace of the scale out triple
 * store so that we can locate the various index partitions. And if they are
 * symbolic then we need a means to convert them to functional objects that can
 * materialize access paths that actually read or write on the appropriate
 * relation.
 * <p>
 * We need to bind the relation to the protocol for accessing that relation
 * whenever we want to request anything for the relation. That binding needs to
 * be (re-)established when reading rules from a file or when a data service
 * receives a (part of a) rule for remote execution as part of a distributed
 * JOIN.
 * <p>
 * It is perfectly reasonable for rules to write on a temporary index and there
 * should be a facility for creating and destroying temporary indices. Perhaps
 * they should be placed into their own namespace, e.g., "#x" would be a
 * temporary index named "x" (could support scale-out) and "##x" would be a
 * data-service local temporary index named "x". "x" by itself is a normal
 * index. Normally, such temporary indices should be scoped to something like a
 * transaction but transaction support is not yet finished.
 * <p>
 * There MUST be a way to name a view of two relations to support
 * {@link RelationFusedView}. The view has to be described before we are able
 * to resolve the relations to their functional objects.
 * 
 * FIXME Allow the head of a rule to return [null] for getRelation() so that
 * query via rules can work even when there is no specific relation that
 * corresponds to the head of the rule?
 * <P>
 * Is there no relation in the head or is this how we handle query (no relation,
 * results are written onto a buffer for eventual read by a client aka SELECT)
 * vs write (write on the relation via a suitable buffering mechanism aka
 * INSERT, UPDATE, DELETE)?
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
public interface IRelation<E> {

    /**
     * The #of elements in the relation.
     * 
     * @param exact
     *            When <code>true</code> an exact count is reported. An exact
     *            count will require a key-range scan if delete markers are in
     *            use, in which case it will be more expensive. See
     *            {@link IRangeQuery}.
     * 
     * @todo Depending on this for fixed point termination is simpler but MUCH
     *       less efficient than reporting from the various write methods
     *       whether any elements in the relation were modified. (We need to
     *       request an exact count in the general case since entailments may
     *       have overwritten deleted tuples and we would miss that with exact :=
     *       false).
     */
    long getElementCount(boolean exact);

    /**
     * Return the best {@link IAccessPath} for a relation given a predicate with
     * zero or more unbound variables.
     * <p>
     * If there is an {@link IIndex} that directly corresponeds to the natural
     * order implied by the variable pattern on the predicate then the access
     * path should use that index. Otherwise you should choose the best index
     * given the constraints and make sure that the {@link IAccessPath}
     * incorporates additional filters that will allow you to filter out the
     * irrelevant {@link ITuple}s during the scan - this is very important when
     * the index is remote!
     * <p>
     * If there are any {@link IPredicateConstraint}s then the access path MUST
     * incorporate those constraints such that only elements that satisify the
     * constraints may be visited.
     * <p>
     * Whether the constraints arise because of the lack of a perfect index for
     * the access path or because they were explicitly specified for the
     * {@link IPredicate}, those constraints should be translated into
     * constraints imposed on the underlying {@link ITupleIterator} and sent
     * with it to be evaluated local to the data.
     * 
     * @param predicate
     *            The constraint on the elements to be visited.
     * 
     * @return The best {@link IAccessPath} for that {@link IPredicate}.
     */
    IAccessPath<E> getAccessPath(IPredicate<E> predicate);
    
}
