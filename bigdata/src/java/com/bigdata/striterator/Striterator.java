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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

import com.bigdata.service.ndx.PartitionedTupleIterator;

/**
 * Streaming iterator pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <I>
 * @param <E>
 */
public class Striterator<I extends Iterator<E>, E> implements IStriterator<I,E> {

    /**
     * Set to the source iterator by the ctor. This exists against the
     * possibility that some patterns will require access to the source iterator
     * as well. One example is the partitioned tuple iterator which needs to
     * know the key for last tuple visited by the source iterator in order to
     * advance the aggregate iterator efficiently to the next chunk without
     * re-scanning tuples.
     * 
     * @todo backport this concept into the {@link PartitionedTupleIterator}
     */
    protected final I realSource;
    
    /**
     * Set to the source iterator by the ctor and then replaced each time we
     * wrap the source iterator with another {@link IFilter}. This is always
     * the iterator that realizes the stacked filter semantics.
     */
    protected I src;
    
    /**
     * @param src The source iterator.
     */
    public Striterator(final I src) {

        if (src == null)
            throw new IllegalArgumentException();
        
        this.realSource = this.src = src;
        
    }

    /**
     * Wraps the enumeration as an iterator.
     * 
     * <strong>The constructor must be overridden for derived classes that
     * specialize the type of the iterator.</strong>
     * 
     * @param srcEnum
     *            The source enumeration.
     */
    @SuppressWarnings("unchecked")
    public Striterator(final Enumeration<E> srcEnum) {
        
        this((I)new Iterator<E>() {

            public boolean hasNext() {
                return srcEnum.hasMoreElements();
            }

            public E next() {
                return srcEnum.nextElement();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        });

    }

    @SuppressWarnings("unchecked")
    public IStriterator<I, E> addFilter(final IFilter<I, ?, E> filter) {

        src = (I)filter.filter((I) src);

        return this;

    }

    public IStriterator<I, E> addInstanceOfFilter(final Class<E> cls) {

        return addFilter(new Filter<I, E>(cls) {

            private static final long serialVersionUID = 1L;

            @Override
            protected boolean isValid(E e) {

                return cls.isInstance(e);

            }
            
        });
        
    }

    public IStriterator<I, E> append(I src) {

        return addFilter(new Appender<I, E>(src));
        
    }

    /**
     * Exclude elements found in the set.
     * 
     * @param set
     *            The set of elements to be excluded.
     *            
     * @return The filtered iterator.
     */
    public IStriterator<I,E> exclude(Set<E> set) {
        // TODO Auto-generated method stub
        return null;
    }

    // @todo use temporary store for scalable set to filter the instances.
    public IStriterator<I,E> makeUnique() {
        // TODO Auto-generated method stub
        return null;
    }

    public IStriterator<I,E> map(Object client, Method method) {
        // TODO Auto-generated method stub
        return null;
    }

    final public boolean hasMoreElements() {

        return src.hasNext();
        
    }

    final public E nextElement() {
        
        return src.next();
        
    }
    
    final public boolean hasNext() {

        return src.hasNext();
        
    }

    final public E next() {
        
        return src.next();
        
    }

    /**
     * Unsupported operation.
     * <p>
     * Extreme care must be taken when implementing striterator patterns that
     * support removal in order to ensure that the current iterator element is
     * removed from the iteration source. In the general case, it may not be
     * possible to implement {@link #remove()} for striterator patterns.
     * <p>
     * When the striterator is also an {@link IChunkedIterator} then
     * {@link #remove()} semantics face an additional difficulty. If the source
     * iterator is being consumed a {@link IChunkedIterator#nextChunk() chunk}
     * at a time, then {@link #remove()} only has the semantics of removing the
     * last element in the chunk.
     * 
     * @throws UnsupportedOperationException
     *             For the above reasons, {@link #remove()} is NOT supported for
     *             chunked striterator patterns and this method will always
     *             throw an {@link UnsupportedOperationException}.
     */
    public void remove() {

        throw new UnsupportedOperationException();
        
    }

}
