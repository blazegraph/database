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
 * Created on Jun 19, 2008
 */

package com.bigdata.join;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.SuccessorUtil;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class for type-specific {@link IAccessPath} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractAccessPath<E> implements IAccessPath<E> {

    private final IPredicate<E> predicate;
    private final IKeyOrder<E> keyOrder;
    private final IIndex ndx;
    private final int flags;

    /**
     * The filter derived from the {@link IPredicateConstraint}.
     */
    final private ITupleFilter filter;

    private boolean didInit = false;
    private byte[] fromKey;
    private byte[] toKey;

    public IKeyOrder<E> getKeyOrder() {
        
        return keyOrder;
        
    }
    
    /**
     * 
     * @param predicate
     *            The constraints on the access path.
     * @param keyOrder
     *            The order in which the elements would be visited for this
     *            access path.
     * @param ndx
     *            The index on which the access path is reading.
     * @param flags
     *            The default {@link IRangeQuery} flags.
     * 
     * @todo This needs to be more generalized so that you can use a index that
     *       is best without being optimal by specifying a low-level filter to
     *       be applied to the index. When the predicate also specifies a filter
     *       constraint then that must be layer on top of this lower-level
     *       constaint.
     */
    protected AbstractAccessPath(IPredicate<E> predicate,
            IKeyOrder<E> keyOrder, IIndex ndx, int flags) {

        if (predicate == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        if (ndx == null)
            throw new IllegalArgumentException();
        
        this.predicate = predicate;

        this.keyOrder = keyOrder;
        
        this.ndx = ndx;

        this.flags = flags;

        final IPredicateConstraint<E> constraint = predicate.getConstraint();

        if (constraint == null) {

            this.filter = null;

        } else {

            this.filter = new ITupleFilter() {
                
                private static final long serialVersionUID = 1L;

                public void add(ITupleFilter filter) {

                    throw new UnsupportedOperationException();
                    
                }

                @SuppressWarnings("unchecked")
                public boolean isValid(ITuple tuple) {
                    
                    E e = (E)tuple.getValue();
                    
                    return constraint.accept(e);
                    
                }

                public void rewrite(ITuple tuple) {
                    
                    throw new UnsupportedOperationException();
                    
                }

            };

        }
        
    }

    /**
     * Required post-ctor initialization.
     */
    protected void init() {
        
        this.fromKey = getFromKey();

        this.toKey = getToKey();

    }
    
    public IPredicate<E> getPredicate() {
        
        assertInitialized();
        
        return predicate;
        
    }

    public boolean isEmpty() {

        final IChunkedIterator<E> itr = iterator(1,1);
        
        try {
            
            return itr.hasNext();
            
        } finally {
            
            itr.close();
            
        }
        
    }

    public IChunkedOrderedIterator<E> iterator() {
        
        return iterator(0,0);
        
    }

    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> iterator(int limit, int capacity) {

        // @todo optimizations for point tests and small limits.
        return new ChunkedWrappedIterator<E>(new Striterator(rangeIterator(capacity,
                flags, filter)).addFilter(new Resolver() {

                    private static final long serialVersionUID = 0L;

                    @Override
                    protected Object resolve(Object arg0) {

                        final ITuple tuple = (ITuple) arg0;

                        return tuple.getObject();

                    }
                }));
            
    }

    /**
     * @throws IllegalStateException
     *             unless {@link #init()} has been invoked.
     */
    final protected void assertInitialized() {

        if (!didInit)
            throw new IllegalStateException();
        
    }
    
    /**
     * Return the key corresponding to the inclusive lower bound for the
     * {@link IAccessPath}.
     * 
     * @return The inclusive lower bound -or- <code>null</code> if there is no
     *         lower bound.
     * 
     * @todo does this work as a default impl? if so then improve the javadoc.
     */
    protected byte[] getFromKey() {

        if (fromKey == null) {

            fromKey = ndx.getIndexMetadata().getTupleSerializer().serializeKey(
                    predicate);

        }

        return fromKey;
        
    }
    
    /**
     * Return the key corresponding to the exclusive upper bound for the
     * {@link IAccessPath}.
     * 
     * @return The exclusive upper bound -or- <code>null</code> if there is no
     *         upper bound.
     * 
     * @todo does this work as a default impl? if so then improve the javadoc.
     */
    protected byte[] getToKey() {

        final byte[] fromKey = getFromKey();
        
        if (fromKey == null)
            
            toKey = null;
        
        else {
        
            toKey = SuccessorUtil.successor(fromKey);
            
        }

        return toKey;

    }
    
    public long rangeCount() {

        assertInitialized();
        
        // Note: for an exact count you must also apply the optional [filter] to the range iterator.
        return ndx.rangeCount(fromKey, toKey);
        
    }

    public ITupleIterator<E> rangeIterator() {

        return rangeIterator(0/* capacity */, flags, filter);
        
    }
    
    @SuppressWarnings({ "unchecked" })
    protected ITupleIterator<E> rangeIterator(int capacity,int flags, ITupleFilter filter) {

        assertInitialized();
        
        return ndx.rangeIterator(fromKey, toKey, capacity, flags, filter);
        
    }

    /**
     * This implementation removes all tuples that would be visited by the
     * access path from the backing index. If you are maintaining multiple
     * indices then you MUST override this method to remove the data from each
     * of those indices.
     */
    public long removeAll() {

        /*
         * Remove everything in the key range. Do not materialize keys or
         * values.
         */
        final ITupleIterator itr = rangeIterator(0/* capacity */,
                IRangeQuery.REMOVEALL, filter);

        long n = 0;

        while (itr.hasNext()) {

            itr.next();

            n++;

        }

        return n;

    }

}
