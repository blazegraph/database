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

package com.bigdata.relation.accesspath;

import com.bigdata.btree.FusedEntryIterator;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.relation.rule.IPredicate;

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

    private final AbstractAccessPath<E> path1;
    private final AbstractAccessPath<E> path2;
    
    /**
     * 
     */
    public AccessPathFusedView(AbstractAccessPath<E> path1,
            AbstractAccessPath<E> path2) {
        
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

//        // assume the flags from the first access path.
//        this.flags = path1.flags;
        
    }

    public IPredicate<E> getPredicate() {

        return path1.getPredicate();
        
    }

    public boolean isEmpty() {

        return path1.isEmpty() && path2.isEmpty();

    }

    /**
     * Note: You can not get an exact range count for a view.
     * 
     * @throws UnsupportedOperationException
     *             if <code>exact == true</code>.
     * 
     * @todo an exact range count for a view could be written. It would have to
     *       use two iterators (like a {@link FusedView}) that progressed in
     *       sync so that duplicates could be detected. This means a full key
     *       range scan for both source access paths.
     */
    public long rangeCount(boolean exact) {

        if (exact) {

            throw new UnsupportedOperationException();

        }

        // @todo check for overflow on Long#Max_value

        return path1.rangeCount(exact) + path2.rangeCount(exact);

    }

//    final private int flags;

    /**
     * @throws UnsupportedOperationException
     *             always.
     *             
     * @todo this could be implemented with a variant (or relaxed form) of
     *       {@link FusedView}.
     */
    public IIndex getIndex() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public ITupleIterator<E> rangeIterator() {

        return rangeIterator(0/*capacity*/);
        
    }
    
    private ITupleIterator<E> rangeIterator(final int capacity) {

        return new FusedEntryIterator<E>(
                path1.flags, // assume the flags from the first access path.
                false, // we do not want to see the deleted tuples.
                new ITupleIterator[] {
                path1.rangeIterator(capacity,path1.flags,path1.filter),//
                path2.rangeIterator(capacity,path2.flags,path2.filter)//
                }
        );
                
    }

    public IChunkedOrderedIterator<E> iterator() {

        return iterator(0, 0);

    }

    /**
     * @todo optimizations for point tests and small limits. See
     *       {@link AbstractAccessPath#iterator(int, int) for impl details.
     */
    public IChunkedOrderedIterator<E> iterator(int limit, int capacity) {

        return new ChunkedWrappedIterator<E>(new Striterator(rangeIterator(
                capacity)).addFilter(new Resolver() {

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
