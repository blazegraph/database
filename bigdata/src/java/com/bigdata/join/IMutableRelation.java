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

package com.bigdata.join;

/**
 * A mutable {@link IRelation}. The relation must maintain any secondary
 * indices under mutation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMutableRelation<E> extends IRelation<E> {

    /**
     * Write elements on the relation.
     * <p>
     * Note: It is more efficient if an implementation can easily avoid
     * overwrite of an element with identical data. Overwrite adds data to the
     * journal backing the mutable B+Tree and thus incurs more IO and will
     * trigger overflow for the journal earlier than if overwrite were avoided.
     * In contrast, while you have a lock on the mutable index you can test for
     * a pre-existing key and compare the serialized byte[] values with
     * relatively little cost.
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
    public long remove(IChunkedOrderedIterator<E> itr);

    /**
     * Update elements on the relation.
     * <p>
     * The implemention must locate each element in the relation and, if found,
     * update its state using the <i>transform</i>. It is an error if the
     * transformed element has a different "primary key" than the visited
     * element.
     * <p>
     * Note: While UPDATE is often realized as "DELETE + INSERT" within the same
     * transaction, the advantage of this formulation is that is one-half of the
     * cost since each element in the relation is visited only once. However, if
     * you need to update parts of the primary key then "DELETE + INSERT" is the
     * way to go since each the "delete" and the "insert" may operate on
     * different tuples located in different parts of the index. For scale-out
     * indices, those tuples can even lie on different machines.
     * 
     * @param itr
     *            An iterator visiting the elements selected for update.
     * @param transform
     *            A transform that produces the new state for each visited
     *            element.
     * 
     * @return The #of elements that were actually modified in the relation.
     */
    public long update(IChunkedOrderedIterator<E> itr,
            ITransform<E> transform);

    /**
     * A transform that produces a new state from the given element. The state
     * change is typically constrained such that the "primary key" is immutable.
     * How a primary key is defined is {@link IRelation} specific.
     * 
     * @todo Declarative transforms could be written to support SQL like
     *       UPDATEs.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     * 
     * @see IMutableRelation#update(IChunkedOrderedIterator, ITransform)
     */
    public interface ITransform<E> {
        
        public E transform(E e);
        
    }

}
