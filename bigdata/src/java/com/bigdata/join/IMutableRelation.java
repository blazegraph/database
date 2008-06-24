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
 * A mutable relation.
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

}
