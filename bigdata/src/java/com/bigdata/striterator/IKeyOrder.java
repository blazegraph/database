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

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.relation.rule.IPredicate;

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
    public int getKeyArity();
    
    /**
     * Return the comparator that places elements into the natural order for the
     * associated index.
     */
    public Comparator<E> getComparator();

    /**
     * The base name for the index.
     */
    public String getIndexName();

    /**
     * Return the index of the slot in the {@link IPredicate} which appears at
     * the specified position in the key.
     * 
     * @param keyPos
     *            The index into the key that is being generated.
     * 
     * @return The index of the slot in the {@link IPredicate}.
     */
    public int getKeyOrder(final int keyPos);

    /*
     * New methods.
     */
    
//    /**
//     * Return the inclusive lower bound which would be used for a query against
//     * this {@link IKeyOrder} for the given {@link IPredicate}.
//     */
//    byte[] getFromKey(IKeyBuilder keyBuilder, IPredicate<ISPO> predicate);
//
//    /**
//     * Return the exclusive upper bound which would be used for a query against
//     * this {@link IKeyOrder} for the given {@link IPredicate}.
//     */
//    byte[] getToKey(IKeyBuilder keyBuilder, IPredicate<ISPO> predicate);
//
//    /**
//     * Encode a key for the index.
//     * 
//     * @param keyBuilder
//     *            The object used to encode an unsigned byte[].
//     * @param e
//     *            An element for the owning relation type.
//     * 
//     * @return The encoded key.
//     */
//    byte[] encodeKey(IKeyBuilder keyBuilder, E e);
//
//    /**
//     * Decode the key into an {@link SPO}. The {@link StatementEnum} and the
//     * optional SID will not be decoded, since it is carried in the B+Tree
//     * value. However, if the {@link SPOKeyOrder} is a quad order then the
//     * {@link SPO#c()} will be bound.
//     * 
//     * @param keyOrder
//     *            The natural order of the key.
//     * @param key
//     *            The key.
//     * 
//     * @return The decoded key.
//     */
//    E decodeKey(byte[] key);

}
