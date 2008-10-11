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
 * Created on Aug 4, 2008
 */

package com.bigdata.btree.filter;

import java.util.Iterator;

import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.filter.LookaheadTupleFilter.ILookaheadTupleIterator;

/**
 * Abstract base class for an {@link ITupleFilter} that transforms the data type
 * of the keys and/or values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param E
 *            The generic type for the objects materialized from the source
 *            tuples.
 * @param F
 *            The generic type for the objects that can be materialized from the
 *            output tuples.
 * 
 * @todo better encapsulation for patterns with a 1:1 transform (one tuple in,
 *       one out) and patterns with a M:1 (many in, one out) and 1:M (1 in, many
 *       out)?
 */
abstract public class TupleTransformer<E,F> implements ITupleFilter<F> {

    /** The serialization provider for the transformed tuples. */
    final protected ITupleSerializer<? extends Object/*key*/,F/*val*/> tupleSer;

    /**
     * @param tupleSer
     *            The serialization provider for the transformed tuples.
     */
    public TupleTransformer(
            ITupleSerializer<? extends Object/* key */, F/* value */> tupleSer) {
        
        if (tupleSer == null)
            throw new IllegalArgumentException();

        this.tupleSer = tupleSer;

    }
    
    /**
     * @param src
     *            The source iterator.
     */
    @SuppressWarnings("unchecked")
    public ITupleIterator<F> filter(Iterator src) {

        // layer in one-step lookahead.
        src = new LookaheadTupleFilter().filter((ITupleIterator<E>)src);
        
        // the transformer.
        return newTransformer((ILookaheadTupleIterator<E>)src);
        
    }

    /**
     * Method responsible for creating a new instance of the iterator that reads
     * from the lookahead source whose tuples are of the source type and visits
     * the transformed tuples.
     */
    abstract protected ITupleIterator<F> newTransformer(ILookaheadTupleIterator<E> src);
    
//    /**
//     * Return <code>true</code> iff another tuple of the transformed type can
//     * be assembled from the source iterator.
//     * 
//     * @param src
//     *            The source iterator.
//     */
//    abstract protected boolean hasNext(ILookaheadTupleIterator<E> src);
//    
//    /**
//     * Implementation should consume one or more tuples from the source,
//     * returning a new tuple of the target generic type.
//     * 
//     * @param src
//     *            The source iterator.
//     * 
//     * @return The transformed tuple.
//     */
//    abstract protected ITuple<F> next(ILookaheadTupleIterator<E> src);

//    /**
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <E>
//     * @param <F>
//     */
//    static private class Transformerator<E, F> implements ITupleIterator<F> {
//        
////        private final ILookaheadTupleIterator<E> src;
////        private final TupleTransformer<E, F> filter;
//        private final ITupleIterator<F> transform;
//
//        /**
//         * 
//         * @param src
//         * @param filter
//         */
//        protected Transformerator(ITupleIterator<F> transform) {
//
//            if (transform == null)
//                throw new IllegalArgumentException();
//
//            this.transform = transform;
//
//        }
//
//        public ITuple<F> next() {
//        
//            return transform.next();
//            
//        }
//
//        public boolean hasNext() {
//
//            return transform.hasNext();
//            
//        }
//
//        public void remove() {
//
//            throw new UnsupportedOperationException();
//            
//        }
//
//    }

}
