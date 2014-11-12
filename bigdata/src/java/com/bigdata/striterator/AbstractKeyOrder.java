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

import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class provides default behavior for generating keys for a given
 * index order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractKeyOrder<E> implements IKeyOrder<E> {

    /**
     * {@inheritDoc}
     * 
     *       FIXME This needs to be reconciled with
     *       {@link ITupleSerializer#serializeKey(Object)}. For example, this
     *       does not play well with the {@link DefaultTupleSerializer}.
     */
	@Override
    public byte[] getKey(final IKeyBuilder keyBuilder, final E element) {
        
        keyBuilder.reset();

        final int keyArity = getKeyArity(); // use the key's "arity".

        for (int i = 0; i < keyArity; i++) {

            /*
             * Note: If you need to override the default IKeyBuilder behavior do
             * it in the invoked method.
             */
            appendKeyComponent(keyBuilder, i, ((IElement) element)
                    .get(getKeyOrder(i)));

        }

        return keyBuilder.getKey();
        
    }

	@Override
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
             * it in the invoked method.
             */
            appendKeyComponent(keyBuilder, i, term.get());

            noneBound = false;

        }

        final byte[] key = noneBound ? null : keyBuilder.getKey();

        return key;
        
    }

	@Override
    public byte[] getToKey(final IKeyBuilder keyBuilder,
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
             * it in the invoked method.
             */
            appendKeyComponent(keyBuilder, i, term.get());

            noneBound = false;

        }

        final byte[] key = noneBound ? null : keyBuilder.getKey();

        return key == null ? null : SuccessorUtil.successor(key);

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

	/**
	 * Return the {@link IKeyOrder} values having key components in their first
	 * N positions that correspond to the N constants bound on the given
	 * predicate.
	 * 
	 * @param pred
	 *            The predicate
	 * @param src
	 *            The set of possible {@link IKeyOrder}s to filter.
	 * 
	 * @return An iterator visiting only those {@link IKeyOrder}s that have a
	 *         prefix key composed entirely of contants bound in the
	 *         {@link IPredicate}.
	 * 
	 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
	 *      query is slow </a>
	 * 
	 *      FIXME Keep or drop?
	 */
	@SuppressWarnings("unchecked")
	public static <E> Iterator<IKeyOrder<E>> getFilteredKeyOrderIterator(
			final IPredicate<E> pred,//
			final Iterator<IKeyOrder<E>> src//
	) {

		final int keyArity = pred.arity();

		// The #of variables in the as-bound predicate.
		final int nvars = BOpUtility.getArgumentVariableCount(pred);

		// The #of constants in the as-bound predicate.
		final int nconst = keyArity - nvars;

		if (nconst < 0) {
			// If there are more variables than the key arity then something is
			// wrong.
			throw new RuntimeException("Wrong keyArity? keyArity=" + keyArity
					+ ", but nvars=" + nvars + " for pred=" + pred);

		}

		IStriterator itr = new Striterator(src);

		itr.addFilter(new Filter() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean isValid(final Object obj) {

				@SuppressWarnings({ "rawtypes" })
				final IKeyOrder<E> keyOrder = (IKeyOrder) obj;

				if (keyOrder.getKeyArity() != keyArity) {
					// The caller got something wrong.
					throw new IllegalArgumentException();
				}

				/*
				 * The key order will be accepted IFF all positions in the
				 * predicate that are bound as constants appear within the first
				 * [nconst] positions in the key order.
				 */

				// keyPos is the ordered position of a component in the key.
				for (int keyPos = 0; keyPos < nconst; keyPos++) {

					// Find the position in the predicate for that key position.
					final int j = keyOrder.getKeyOrder(keyPos);

					if (pred.get(j).isVar()) {

						// The predicate has variable in that key position.
						return false;

					}

				} // next key component.

				return true;

			}
		});

		return itr;

	}

}
