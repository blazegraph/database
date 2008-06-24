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
 * Created on Oct 31, 2007
 */

package com.bigdata.join;

import com.bigdata.btree.FusedEntryIterator;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A read-only fused view of two access paths obtained for the same
 * {@link IPredicate} constraint in two different databases (this is used for
 * truth maintenance when reading on the union of a focus store and the
 * database).
 * 
 * FIXME review impl and write tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AccessPathFusedView<E> implements IAccessPath<E> {

    private final IAccessPath<E> path1;
    private final IAccessPath<E> path2;
    
    /**
     * 
     */
    public AccessPathFusedView(IAccessPath<E> path1, IAccessPath<E> path2) {
        
        if (path1 == null)
            throw new IllegalArgumentException();
        
        if (path2 == null)
            throw new IllegalArgumentException();
        
        if (path1 == path2)
            throw new IllegalArgumentException();
        
        final IPredicate<E> p1 = path1.getPredicate();
        
        final IPredicate<E> p2 = path2.getPredicate();
        
        if (p1.arity() != p2.arity())
            throw new IllegalArgumentException();

        final int arity = p1.arity();
        
        for(int i=0; i<arity; i++) {
            
            if (!p1.get(i).equals(p2.get(i))) {

                throw new IllegalArgumentException();
                
            }
            
        }
        
        this.path1 = path1;
        
        this.path2 = path2;
        
    }

    public IPredicate<E> getPredicate() {

        return path1.getPredicate();
        
    }

    public boolean isEmpty() {

        return path1.isEmpty() && path2.isEmpty();
        
    }

    public long rangeCount() {

        // Note: this is the upper bound.
        
        // @todo check for overflow on Long#Max_value
        
        return path1.rangeCount() + path2.rangeCount();
        
    }

    // FIXME These need to be raised in the API so that they can be passed down.
    final private int flags = IRangeQuery.DEFAULT;
    final private ITupleFilter filter = null;
    
    public ITupleIterator<E> rangeIterator() {

        return rangeIterator(0,flags,filter);
        
    }
    
    protected ITupleIterator<E> rangeIterator(int capacity, int flags, ITupleFilter filter) {

        /*
         * @todo The modification to drive the range iterator capacity, flags
         * and filter through everywhere might have broken FusedEntryIterator
         * for this call in the case where the source iterators are not
         * requesting either the keys or the values.
         * 
         * FIXME This MUST specify ALLVERSIONS for the source iterators in order
         * for the fused view to be able to recognize a deleted index entry and
         * discard a historical undeleted entry later in the predence order for
         * the view.
         */
        return new FusedEntryIterator(
                false, // ALLVERSIOSN
                new ITupleIterator[] {
                path1.rangeIterator(),//
                path2.rangeIterator()//
                }
        );
                
    }

    public IChunkedOrderedIterator<E> iterator() {

        return iterator(0,0);

    }

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

    public long removeAll() {
        
        throw new UnsupportedOperationException();
        
    }

    public IKeyOrder<E> getKeyOrder() {

        return path1.getKeyOrder();
        
    }

}
