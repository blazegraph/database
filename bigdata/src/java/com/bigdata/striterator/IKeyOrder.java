/**

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
 * Created on Jan 26, 2007
 */

package com.bigdata.striterator;

import java.util.Comparator;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.keys.IKeyBuilder;

/**
 * An interface representing the natural traversal orders for the different
 * indices for some class of relation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements in the relation.
 */
public interface IKeyOrder<E> {

    /**
     * Return the #of elements in the key for this natural order.
     */
    int getKeyArity();
    
    /**
     * Return the comparator that places elements into the natural order for the
     * associated index.
     */
    Comparator<E> getComparator();

    /**
     * The base name for the index.
     */
    String getIndexName();

    /**
     * Return the index of the slot in the {@link IPredicate} which appears at
     * the specified position in the key.
     * 
     * @param keyPos
     *            The index into the key that is being generated.
     * 
     * @return The index of the slot in the {@link IPredicate}.
     */
    int getKeyOrder(final int keyPos);

    /*
     * New methods.
     */

    /**
     * Return the key for an element of the relation.
     * 
     * @param keyBuilder
     *            The object which will be used to construct the key.
     * @param element
     *            An element for the associated relation.
     *            
     * @return The key for the index associated with this {@link IKeyOrder}.
     */
    byte[] getKey(IKeyBuilder keyBuilder, E element);

    /**
     * Return the inclusive lower bound which would be used for a query against
     * this {@link IKeyOrder} for the given {@link IPredicate}.
     * 
     * @param keyBuilder
     *            The object which will be used to construct the key.
     * @param predicate
     *            A predicate describing bound and unbound fields for the key.
     * 
     * @return The key corresponding to the inclusive lower bound for a query
     *         against that {@link IPredicate}.
     */
    byte[] getFromKey(IKeyBuilder keyBuilder, IPredicate<E> predicate);

    /**
     * Return the exclusive upper bound which would be used for a query against
     * this {@link IKeyOrder} for the given {@link IPredicate}.
     * 
     * @param keyBuilder
     *            The object which will be used to construct the key.
     * @param predicate
     *            A predicate describing bound and unbound fields for the key.
     * 
     * @return The key corresponding to the exclusive upper bound for a query
     *         against that {@link IPredicate}.
     */
    byte[] getToKey(IKeyBuilder keyBuilder, IPredicate<E> predicate);

}
