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
 * Created on May 28, 2008
 */

package com.bigdata.btree;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;

/**
 * A {@link SortedSet} backed by a B+Tree.
 * <p>
 * Note: The {@link BigdataMap} has the same concurrency constraints as the
 * {@link BTree} - it is single-threaded for writes and allows concurrent
 * readers.
 * <p>
 * Note: The {@link BigdataSet} is actually flyweight wrapper around a
 * {@link BigdataMap} whose keys and values are both formed from the values
 * stored in this {@link SortedSet}.
 * <p>
 * Note: Both {@link Set#equals(Object)} and {@link Set#hashCode()} are VERY
 * expensive, but that is how they are defined.
 * 
 * @param E
 *            The generic type for the elements stored in the set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSet<E> extends AbstractSet<E> implements SortedSet<E> {

    /**
     * Text of the error message used when there are more than
     * {@link Integer#MAX_VALUE} entries.
     */
    protected static transient final String ERR_TOO_MANY = "Too many entries";
    
    private final BigdataMap<E, E> map;

    public BigdataSet(AbstractBTree ndx) {

        map = new BigdataMap<E, E>(ndx);

    }

    /**
     * Ctor used to wrap a key-range of a {@link BigdataMap}.
     * 
     * @param map
     *            The map which will impose the key-range constraints.
     */
    BigdataSet(BigdataMap<E, E> map) {

        if (map == null)
            throw new IllegalArgumentException();

        this.map = map;
        
    }

    public boolean add(E key) {
        
        E tmp = map.put(key, key);
        
        // true if this set did not already contain the specified element.
        return tmp == null;
        
    }

    public boolean remove(Object key) {
        
        E tmp = map.remove(key);
        
        // true if the set contained the specified element. 
        return tmp != null;

    }

    public void clear() {

        map.clear();
        
    }

    public boolean contains(Object key) {

        return map.containsKey(key);
        
    }

    public boolean isEmpty() {

        return map.isEmpty();
        
    }

    /**
     * The #of index entries. When there are more than {@link Integer#MAX_VALUE}
     * entries then this method will report {@link Integer#MAX_VALUE} entries.
     * If the backing index supports delete markers then an index scan will be
     * performed in order to count the #of non-deleted index entries.
     */
    public int size() {

        return map.size();
        
    }

    /**
     * The #of non-deleted entries in the map.
     * 
     * @param exactCount
     *            When <code>true</code> the result will be an exact count,
     *            which will require a full key-range scan if delete markers are
     *            enabled for the index.
     * 
     * @return The #of entries in the map. When <i>exactCount</i> is
     *         <code>false</code> and delete markers are being used, then this
     *         will be an upper bound.
     * 
     * @see IRangeQuery#rangeCount(byte[], byte[])
     */
    public long rangeCount(boolean exactCount) {
        
        return map.rangeCount(exactCount);
        
    }
    
    public Iterator<E> iterator() {

        return map.keySet().iterator();
        
    }

    /**
     * @todo override with implementation using chunked ordered writes.
     */
    public boolean addAll(Collection<? extends E> c) {

        return super.addAll(c);
        
    }

    /**
     * @todo override with implementation using chunked ordered reads.
     */
    public boolean containsAll(Collection<?> c) {

        return super.containsAll(c);
        
    }

    /**
     * @todo override with implementation using chunked ordered writes.
     */
    public boolean removeAll(Collection<?> c) {

        return super.removeAll(c);
        
    }

    /**
     * @todo override with implementation using chunked ordered writes.
     */
    public boolean retainAll(Collection<?> c) {

        return super.retainAll(c);
        
    }

    /**
     * There is no means available to specify a {@link Comparator} for the
     * {@link SortedSet}. Application keys are first converted into
     * <strong>unsigned</strong> byte[] keys using the configured
     * {@link ITupleSerializer} for the backing B+Tree. The index order is
     * directly determined by those keys.
     * 
     * @return Always returns <code>null</code>.
     */
    final public Comparator<? super E> comparator() {

        return null;
        
    }

    /**
     * Note: This is written using an {@link ITupleIterator} in order to decode
     * the entry.
     */ 
    @SuppressWarnings("unchecked")
    public E first() {

        final IIndex ndx = map.getIndex();
        
        final ITupleIterator itr = ndx.rangeIterator(map.fromKey, map.toKey,
                1/* capacity */, IRangeQuery.DEFAULT, null/* filter */);
        
        if(!itr.hasNext()) {
            
            throw new NoSuchElementException();
            
        }
        
        return (E) map.tupleSer.deserialize(itr.next());
        
    }

    /**
     * Note: This is written using an {@link ITupleIterator} in order to decode
     * the entry.
     * 
     * FIXME Implement the reverse scan feature for {@link ITupleIterator}.
     */ 
    @SuppressWarnings("unchecked")
    public E last() {

        final IIndex ndx = map.getIndex();
        
        final ITupleIterator itr = ndx.rangeIterator(map.fromKey, map.toKey,
                1/* capacity */, IRangeQuery.DEFAULT|IRangeQuery.REVERSE, null/* filter */);
        
        if(!itr.hasNext()) {
            
            throw new NoSuchElementException();
            
        }
        
        return (E) map.tupleSer.deserialize(itr.next());

    }

    public SortedSet<E> headSet(E toKey) {

        return new BigdataSet<E>((BigdataMap<E, E>) map.headMap(toKey));

    }

    public SortedSet<E> subSet(E fromKey, E toKey) {

        return new BigdataSet<E>((BigdataMap<E, E>) map.subMap(fromKey, toKey));

    }

    public SortedSet<E> tailSet(E fromKey) {

        return new BigdataSet<E>((BigdataMap<E, E>) map.tailMap(fromKey));

    }

}
