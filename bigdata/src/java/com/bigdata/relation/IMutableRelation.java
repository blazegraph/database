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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation;

import com.bigdata.btree.BTree;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;

/**
 * A mutable {@link IRelation}. The relation must maintain any secondary
 * indices under mutation.
 * <p>
 * The methods declared by this interface return a "mutation count" - the
 * mutation count MUST be exact and MUST NOT count overwrites that do not change
 * the state of the tuple (the same key and value). The mutation counts are used
 * to determine the fixed point for closure of a rule set. If they do not follow
 * this contract then the closure operation will not terminate!
 * <p>
 * If fact, it is MUCH more efficient if an implementation avoids the overwrite
 * of an element with identical data. All index writes (including overwrites)
 * add data to the {@link AbstractJournal} backing the mutable {@link BTree}
 * absorbing writes for an index. An "overwrite" thus incurs more IO and will
 * trigger overflow for the journal earlier than if overwrite were avoided. In
 * contrast, while you have a lock on the mutable index you can test for a
 * pre-existing key and compare the serialized byte[] values with relatively
 * little cost (zero additional IO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the [E]lements of the relation.
 */
public interface IMutableRelation<E> extends IRelation<E>, IMutableResource<IRelation<E>> {

    /**
     * Write elements on the relation.
     * 
     * @param itr
     *            An iterator visiting the elements to be written.
     * 
     * @return The #of elements that were actually written on the relation.
     */
    public long insert(IChunkedOrderedIterator<E> itr);

    /**
     * Remove elements from the relation.
     * 
     * @param itr
     *            An iterator visiting the elements to be removed. Existing
     *            elements in the relation having a key equal to the key formed
     *            from the visited elements will be removed from the relation.
     * 
     * @return The #of elements that were actually removed from the relation.
     */
    public long delete(IChunkedOrderedIterator<E> itr);

    /*
     * @todo update is notional. it has not been implemented yet (you can use
     * delete+insert). i suspect that the implementation will eventually involve
     * the "transform" being specified as an extension to the rule, e.g.,
     * 
     * update [relation] set z=foo from [relation] (x,y,bar)
     * 
     * if the update causes a change in the key for either the primary index or
     * any secondary index for the relation then it must be realized as a
     * delete+insert since the changes to the underlying tuples might not be
     * local (e.g., they could be in an different index partition, on a
     * different data service, on even on a different host). such non-local
     * changes will not be atomic unless you use a full transaction or
     * read-behind from a last known consistent commit point.
     * 
     * @todo the RDF DB faces exactly this problem with truth maintenance (the
     * problem is somewhat more severe since inference against an unstable KB
     * state can magnify any inconsistencies).
     */
    
//    /**
//     * Update elements on the relation.
//     * <p>
//     * The implemention must locate each element in the relation and, if found,
//     * update its state using the <i>transform</i>. It is an error if the
//     * transformed element has a different "primary key" than the visited
//     * element.
//     * <p>
//     * Note: While UPDATE is often realized as "DELETE + INSERT" within the same
//     * transaction, the advantage of this formulation is that is one-half of the
//     * cost since each element in the relation is visited only once. However, if
//     * you need to update parts of the primary key then "DELETE + INSERT" is the
//     * way to go since each the "delete" and the "insert" may operate on
//     * different tuples located in different parts of the index. For scale-out
//     * indices, those tuples can even lie on different machines.
//     * 
//     * @param itr
//     *            An iterator visiting the elements selected for update.
//     * @param transform
//     *            A transform that produces the new state for each visited
//     *            element.
//     * 
//     * @return The #of elements that were actually modified in the relation.
//     */
//    public long update(IChunkedOrderedIterator<E> itr,
//            ITransform<E> transform);
//
//    /**
//     * A transform that produces a new state from the given element. The state
//     * change is typically constrained such that the "primary key" is immutable.
//     * How a primary key is defined is {@link IRelation} specific.
//     * 
//     * @todo Declarative transforms could be written to support SQL like
//     *       UPDATEs.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <E>
//     * 
//     * @see IMutableRelation#update(IChunkedOrderedIterator, ITransform)
//     */
//    public interface ITransform<E> {
//        
//        public E transform(E e);
//        
//    }

}
