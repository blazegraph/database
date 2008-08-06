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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * A flyweight {@link SortedMap} wrapping a B+Tree.
 * <p>
 * Note: The {@link BigdataMap} has the same concurrency constraints as the
 * {@link BTree} - it is single-threaded for writes and allows concurrent
 * readers.
 * <p>
 * Note: The total order of the {@link BigdataMap} is completely determined by
 * {@link ITupleSerializer#serializeKey(Object)}. There is NO concept of a
 * {@link Comparator}. The {@link ITupleSerializer} is responsible for coercing
 * application keys into variable length <strong>unsigned</strong> byte[]s
 * which are the keys for the underlying B+Tree. The order for the B+Tree is the
 * natural order for the <strong>unsigned byte[]</strong>s. {@link KeyBuilder}
 * supports the generation of unsigned byte[]s from various kinds of Java
 * primitives and Unicode {@link String}s and is typically used to write the
 * {@link ITupleSerializer#serializeKey(Object)} method.
 * <p>
 * Note: The coercion of the application keys into unsigned byte[]s is not
 * typesafe unless you either consistently use a strongly typed instance of this
 * class or specify an {@link ITupleSerializer} for the backing B+Tree that only
 * allows application keys that are instances of acceptable classes. This issue
 * is more critical for keys than for values since the keys define the total
 * index order and the default coercion rules for keys are provided by
 * {@link KeyBuilder#asSortKey(Object)} which does not attenpt to partition the
 * key space by the application key type (keys are not safely polymorphic by
 * default).
 * <p>
 * Note: Both {@link Map#equals(Object)} and {@link Map#hashCode()} are VERY
 * expensive, but that is how they are defined.
 * 
 * @param K
 *            The generic type for the keys stored in the map.
 * @param V
 *            The generic type for the values stored in the map.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Refactor until this will support both local and scale-out indices.
 * Right now it is restricted to just local / monotlithic indices. This is
 * mostly because the {@link ILocalBTree} interface is not declared by
 * {@link IIndex}.
 */
public class BigdataMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V> {

    /** The backing B+Tree. */
    final AbstractBTree ndx;

    /** true iff the index supports delete markers. */
    final boolean deleteMarkers;
    
    /** Serializes keys and values and de-serializes tuples. */
    final ITupleSerializer tupleSer;

    /** The optional inclusive lower bound. */
    final byte[] fromKey;

    /** The optional exclusive upper bound. */
    final byte[] toKey;
    
    /**
     * The backing index.
     */
    public AbstractBTree getIndex() {
        
        return ndx;
        
    }

    /**
     * Wrap an index as a {@link SortedMap}.
     * 
     * @param ndx
     *            The index.
     */
    public BigdataMap(AbstractBTree ndx) {

        this(ndx, null/* fromKey */, null/* toKey */);
        
    }
    
    /**
     * Ctor that imposes a key-range constraint.
     * 
     * @param ndx
     *            The backing index.
     * @param fromKey
     *            The optional inclusive lower bound.
     * @param toKey
     *            The optional exclusive upper bound.
     */
    BigdataMap(AbstractBTree ndx, byte[] fromKey, byte[] toKey) {

        if (ndx == null)
            throw new IllegalArgumentException();

        this.ndx = ndx;

        final IndexMetadata md = ndx.getIndexMetadata();
        
        this.deleteMarkers = md.getDeleteMarkers();
        
        this.tupleSer = md.getTupleSerializer();
        
        this.fromKey = fromKey;

        this.toKey = toKey;
        
    }
    
    /**
     * This method imposes an optional key-range restriction on sub-map
     * operations.
     * 
     * @param x
     *            The key.
     * 
     * @param allowUpperBound
     *            <code>true</code> iff the <i>key</i> represents an
     *            inclusive upper bound and thus must be allowed to be LTE to
     *            the right separator key for the index partition. For example,
     *            this would be <code>true</code> for the <i>toKey</i>
     *            parameter on rangeCount or rangeIterator methods.
     * 
     * @return <code>true</code> always.
     * 
     * @throws IllegalArgumentException
     *             if the <i>key</i> is <code>null</code>
     * @throws RuntimeException
     *             if the <i>key</i> does not lie within the legal key range.
     */
    private boolean rangeCheck(final Object key, boolean allowUpperBound) {

        if (key == null) {

            // null keys are not allowed.
            throw new IllegalArgumentException();
            
        }

        return rangeCheck(tupleSer.serializeKey(key),allowUpperBound);
        
    }
    
    private boolean rangeCheck(final byte[] key, boolean allowUpperBound) {

        if (key == null) {

            // null keys are not allowed.
            throw new IllegalArgumentException();
            
        }
        
        if (fromKey == null && toKey == null) {
            
            /*
             * Note: Avoids generating the key for the range check if there is
             * no key range constraint.
             */
            
            return true;
            
        }

        /*
         * Serialize the key so that we can range check it.
         * 
         * Note: Range checks for the B+Tree are always defined in terms of the
         * unsigned byte[] keys.
         */
        
        final byte[] k = tupleSer.serializeKey(key);

        if (BytesUtil.compareBytes(k, fromKey) < 0) {

            throw new RuntimeException("KeyBeforeRange: key="
                    + BytesUtil.toString(k));

        }

        if (toKey != null ) {
            
            final int ret = BytesUtil.compareBytes(k, toKey);
            
            if (allowUpperBound) {

                if (ret <= 0) {

                    // key less than or equal to the exclusive upper bound.

                } else {
                    
                    throw new RuntimeException("KeyAfterRange: key="
                            + BytesUtil.toString(k) + ", allowUpperBound="
                            + allowUpperBound);
                }

            } else {

                if (ret < 0) {

                    // key strictly less than the exclusive upper bound.
                    
                } else {
                    
                    throw new RuntimeException("KeyAfterRange: key="
                            + BytesUtil.toString(k) + ", allowUpperBound="
                            + allowUpperBound);
                }
                
            }

        }
        
        // key lies within the legal range.
        return true;
        
    }
    
    public void clear() {

        if (fromKey == null && toKey == null) {

            // replace the root leaf.
            
            ndx.removeAll();

        } else {

            // remove everything in the key range.
            
            ndx.rangeIterator(fromKey, toKey, 0/* capacity */,
                    IRangeQuery.REMOVEALL, null/*filter*/);
            
        }
        
    }

    /**
     * @todo override with implementation using ordered writes.
     * 
     * Note: Key range checks are being imposed by {@link #put(Object, Object)}.
     */
    public void putAll(Map<? extends K, ? extends V> t) {

        super.putAll(t);
        
//        Iterator<Map.Entry<K,V>> itr = src.entrySet().iterator();
//        
//        while(itr.hasNext()) {
//            
//            Map.Entry<K,V> entry = itr.next();
//            
//            final K key = entry.getKey();
//
//            final V val = entry.getValue();
//            
//            ndx.insert(key, val);
//            
//        }
        
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {

        rangeCheck(key, false/*allowUpperBound*/);

        return (V) ndx.lookup(key);
        
    }

    @SuppressWarnings("unchecked")
    public V put(K key, V val) {

        rangeCheck(key, false/* allowUpperBound */);
        
        return (V) ndx.insert(key, val);
        
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {

        rangeCheck(key, false/* allowUpperBound */);

        if(deleteMarkers) {

            final ITuple tuple = ndx.insert(tupleSer.serializeKey(key),
                    null/* val */, true/* delete */, 0L/* timestamp */,
                    ndx.writeTuple);

            if (tuple.isDeletedVersion()) {
                
                // The previous version was deleted.
                return null;
                
            }
            
            return (V) tupleSer.deserialize(tuple);
            
        } else {

            return (V) ndx.remove(key);
            
        }
        
    }

    public boolean containsKey(Object key) {

        rangeCheck(key, false/* allowUpperBound */);

        return ndx.contains(key);
        
    }

    /**
     * Note: This performs an index scan (since the values of the map are
     * unordered) but stops as soon as a match is found.
     */
    public boolean containsValue(Object value) {

        final byte[] val = tupleSer.serializeVal(value);

        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey,
                0/* capacity */, IRangeQuery.VALS/* flags */,
                new FilterConstructor().addFilter(new TupleFilter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(ITuple tuple) {

                        return BytesUtil.bytesEqual(tuple.getValue(), val);

                    }
                }));

        while(itr.hasNext()) {

            // stop as soon as a match is found.
            return true;
            
        }
        
        // no match was found.
        return false;
        
    }

    /**
     * The #of index entries. When there are more than {@link Integer#MAX_VALUE}
     * entries then this method will report {@link Integer#MAX_VALUE} entries.
     * If the backing index supports delete markers then an index scan will be
     * performed in order to count the #of non-deleted index entries.
     */
    public int size() {

        final long n = rangeCount(true/*exact*/);

        if (n > Integer.MAX_VALUE) {

            return Integer.MAX_VALUE;
            
        }
        
        return (int) n;
        
    }

    public boolean isEmpty() {

        if (ndx instanceof AbstractBTree
                && ((AbstractBTree) ndx).getEntryCount() == 0) {

            // Fast test for an empty index.
            
            return true;
        
        }
        
        /*
         * Otherwise perform an index scan but stop as soon as we find at least
         * one index entry.
         * 
         * @todo this idiom should be place on IIndex#isEmpty().
         */
        
        final ITupleIterator itr = ndx
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.NONE/* flags */, null/* filter */);

        while(itr.hasNext()) {

            /*
             * Stop as soon as an index entry is found - we do not even need to
             * resolve the tuple.
             */
            
            return false;
            
        }
        
        // nothing found.
        return true;
        
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
        
        if (exactCount && ndx.getIndexMetadata().getDeleteMarkers()) {

            /*
             * The use of delete markers means that index entries are not
             * removed immediately but rather a delete flag is set. This is
             * always true for the scale-out indices because delete markers are
             * used to support index partition views. It is also true for
             * indices that can support transactions regardless or whether or
             * not the database is using scale-out indices. In any case, if you
             * want an exact range count when delete markers are in use then you
             * need to actually visit every tuple in the index, which is what
             * this code does. Note that the [flags] are 0 since we do not need
             * either the KEYS or VALS. We are just interested in the #of tuples
             * that the iterator is willing to visit.
             */

            long n = 0L;

            final Iterator itr = ndx.rangeIterator(fromKey, toKey,
                    0/* capacity */, 0/* flags */, null/* filter */);

            while (itr.hasNext()) {

                itr.next();
                
                n++;

            }

            return n;

        }

        /*
         * Either an exact count is not required or delete markers are not in
         * use and therefore rangeCount() will report the exact count.
         */

        return ndx.rangeCount(fromKey, toKey);

    }
    
    /*
     * SortedMap.
     */
    
    /**
     * There is no means available to specify a {@link Comparator} for the
     * {@link SortedMap}. Application keys are first converted into
     * <strong>unsigned</strong> byte[] keys using the configured
     * {@link ITupleSerializer} for the backing B+Tree. The index order is
     * directly determined by those keys.
     * 
     * @return Always returns <code>null</code>.
     */
    final public Comparator<? super K> comparator() {

        return null;
        
    }

    /**
     * 
     * @throws UnsupportedOperationException
     *             if the {@link ITupleSerializer} does not implement the
     *             optional {@link ITupleSerializer#deserializeKey(ITuple)}
     *             method.
     */
    @SuppressWarnings("unchecked")
    public K firstKey() {

        final byte[] key = firstInternalKey();
        
        if (key == null)
            throw new NoSuchElementException();

        // Note: The tuple must be defined and non-delete per firstInternalKey().
        final ITuple tuple = ndx.lookup(key, ndx.lookupTuple.get());
        
        assert tuple != null;

        assert !tuple.isDeletedVersion();
        
        return (K) tupleSer.deserializeKey(tuple);
        
    }

    /**
     * 
     * @throws UnsupportedOperationException
     *             if the {@link ITupleSerializer} does not implement the
     *             optional {@link ITupleSerializer#deserializeKey(ITuple)}
     *             method.
     */
    @SuppressWarnings("unchecked")
    public K lastKey() {
        
        final byte[] key = lastInternalKey();
        
        if (key == null)
            throw new NoSuchElementException();

        // Note: The tuple must be defined and non-delete per lastInternalKey().
        final ITuple tuple = ndx.lookup(key, ndx.lookupTuple.get());
        
        assert tuple != null;

        assert !tuple.isDeletedVersion();
        
        return (K) tupleSer.deserializeKey(tuple);
                
    }
    
    /**
     * The <strong>unsigned byte[]</strong> representation of the first key.
     * 
     * @return The first key or <code>null</code> if there are no keys. (More
     *         precisely, if there are no undeleted tuples within the current
     *         key range constraint.)
     */
    public byte[] firstInternalKey() {

        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey,
                1/* capacity */, IRangeQuery.DEFAULT, null/* filter */);

        if (!itr.hasNext()) {

            return null;

        }

        return itr.next().getKey();

    }

    /**
     * The <strong>unsigned byte[]</strong> representation of the last key.
     * 
     * @return The last key or <code>null</code> if there are no keys. (More
     *         precisely, if there are no undeleted tuples within the current
     *         key range constraint.)
     */
    public byte[] lastInternalKey() {

        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey,
                1/* capacity */, IRangeQuery.DEFAULT | IRangeQuery.REVERSE,
                null/* filter */);
        
        if(!itr.hasNext()) {
            
            return null;
            
        }
        
        return itr.next().getKey();

    }

    /**
     * A {@link SortedMap} view onto the backing B+Tree which imposes the
     * specified key-range restriction.
     */
    public SortedMap<K, V> headMap(K toKey) {

        final byte[] k = tupleSer.serializeKey(toKey);
        
        rangeCheck(k, false/*allowUpperBound*/);
        
        return new BigdataMap<K, V>(ndx, k, fromKey);
        
    }

    /**
     * A {@link SortedMap} view onto the backing B+Tree which imposes the
     * specified key-range restriction.
     */
    public SortedMap<K, V> subMap(K fromKey, K toKey) {

        final byte[] kf = tupleSer.serializeKey(fromKey);
        
        final byte[] kt = tupleSer.serializeKey(toKey);

        rangeCheck(kf, false/*allowUpperBound*/);

        rangeCheck(kt, true/*allowUpperBound*/);

        return new BigdataMap<K, V>(ndx, kf, kt);
        
    }

    /**
     * A {@link SortedMap} view onto the backing B+Tree which imposes the
     * specified key-range restriction.
     */
    public SortedMap<K, V> tailMap(K fromKey) {
        
        final byte[] k = tupleSer.serializeKey(fromKey);

        rangeCheck(k, false/*allowUpperBound*/);

        return new BigdataMap<K, V>(ndx, toKey, k);
        
    }

    /**
     * Note: {@link #keySet()} and {@link #values()} both depend on this method.
     */
    public Set<Map.Entry<K, V>> entrySet() {

        return new EntrySet();
        
    }

    /**
     * Implementation supporting {@link BigdataMap#entrySet()}.
     * 
     * @todo override methods that are being overriden by {@link BigdataSet} for
     *       efficiency (basically those that should be using ordered reads or
     *       ordered writes).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class EntrySet extends AbstractSet<Map.Entry<K,V>> {

        @Override
        public Iterator<java.util.Map.Entry<K, V>> iterator() {

            return new EntrySetIterator(ndx.rangeIterator(fromKey, toKey));
            
        }

        @Override
        public int size() {

            return BigdataMap.this.size();
            
        }
        
    }

    /**
     * Iterator visiting {@link Map.Entry} objects in support of {@link EntrySet}. 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class EntrySetIterator implements Iterator<Map.Entry<K, V>> {

        private final ITupleIterator src;
        
        EntrySetIterator(ITupleIterator src) {

            if (src == null)
                throw new IllegalArgumentException();

            this.src = src;
            
        }
        
        public boolean hasNext() {

            return src.hasNext();
            
        }

        public java.util.Map.Entry<K, V> next() {

            if (!src.hasNext())
                throw new NoSuchElementException();
            
            return new Entry(src.next());
            
        }

        public void remove() {

            src.remove();
            
        }
        
    }
    
    /**
     * Note: Since the same {@link ITuple} instance is returned for each tuple
     * by many {@link ITupleIterator}s there is a side-effect that invalidates
     * the last visited {@link Map.Entry} object. This side-effect could be
     * removed by eagerly materializing the key and value from the ITuple rather
     * than doing it lazily but that maps the {@link Map} interface either blow
     * up (the key can not be recovered), do more work for cases where the key
     * is going to be stored in the value, and work efficiently only for those
     * cases where the key can be directly decoded, e.g., int, long, float,
     * double, etc.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class Entry implements Map.Entry<K, V> {

        private final ITuple tuple;
        private K key;
        private V val;
        
        public Entry(ITuple tuple) {
            
            if (tuple == null)
                throw new IllegalArgumentException();
            
            this.tuple = tuple;
            
        }
        
        @SuppressWarnings("unchecked")
        public K getKey() {
            
            if(key == null) {
            
                key = (K) tupleSer.deserializeKey(tuple);
                
            }
            
            return key;
            
        }

        @SuppressWarnings("unchecked")
        public V getValue() {
            
            if(val == null) {
                
                val = (V) tupleSer.deserialize(tuple);
                
            }
            
            return val;
            
        }

        public V setValue(V value) {
        
            this.val = value;
            
            return BigdataMap.this.put(getKey(),value);

        }
        
    }
    
}
