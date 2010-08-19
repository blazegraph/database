/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 19, 2010
 */

package com.bigdata.striterator;

import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;

/**
 * Abstract base class provides default behavior for generating keys for a given
 * index order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractKeyOrder<E> implements IKeyOrder<E> {

    /**
     * This implementation should work fine unless you need to override the
     * manner in which a bound value in the {@link IPredicate} is converted into
     * a key.
     * <p>
     * {@inheritDoc}
     */
    public byte[] getFromKey(final IKeyBuilder keyBuilder,
            final IPredicate<E> predicate) {

        keyBuilder.reset();

        final int keyArity = getKeyArity(); // use the key's "arity".

        boolean noneBound = true;

        for (int i = 0; i < keyArity; i++) {

            final IVariableOrConstant<?> term = predicate.get(getKeyOrder(i));

            // Note: term MAY be null for the context position.
            if (term == null || term.isVar())
                break;

            /*
             * Note: If you need to override the default IKeyBuilder behavior do
             * it here.
             */
            appendKeyComponent(keyBuilder, i, term.get());

            noneBound = false;

        }

        return noneBound ? null : keyBuilder.getKey();

    }

    /**
     * Encodes an value into the key. This implementation uses the default
     * behavior of {@link IKeyBuilder}. If you need to specialize how a value
     * gets encoded into the key then you can override this method.
     */
    protected void appendKeyComponent(final IKeyBuilder keyBuilder,
            final int index, final Object keyComponent) {

        keyBuilder.append(keyComponent);

    }
    
    public byte[] getToKey(final IKeyBuilder keyBuilder,
            final IPredicate<E> predicate) {

        final byte[] from = getFromKey(keyBuilder, predicate);

        return from == null ? null : SuccessorUtil.successor(from);

    }

}
