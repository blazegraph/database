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
import com.bigdata.striterator.IChunkedOrderedIterator;

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

}
