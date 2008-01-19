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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.util.NoSuchElementException;

/**
 * Visits the values of a {@link Leaf} in the external key ordering. There is
 * exactly one value per key for a leaf node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EntryIterator implements IEntryIterator {

    private final Leaf leaf;

    private final Tuple tuple;
    
    private final IEntryFilter filter;
    
    private int index = 0;

    private int lastVisited = -1;

//    private final byte[] fromKey;
//    
//    private final byte[] toKey;

    // first index to visit.
    private final int fromIndex;

    // first index to NOT visit.
    private final int toIndex;

    public EntryIterator(Leaf leaf) {

        this(leaf, new Tuple(), null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple) {

        this(leaf, tuple, null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey) {

        this(leaf, tuple, fromKey, toKey, null);

    }
    
    /**
     * 
     * @param leaf
     *            The leaf whose entries will be traversed (required).
     * @param tuple
     *            Used to hold the output values (required).
     * @param fromKey
     *            The first key whose entry will be visited or <code>null</code>
     *            if the lower bound on the key traversal is not constrained.
     * @param toKey
     *            The first key whose entry will NOT be visited or
     *            <code>null</code> if the upper bound on the key traversal is
     *            not constrained.
     * @param flags
     *            Flags specifying whether the keys and/or values will be
     *            materialized.
     * @param filter
     *            An optional filter used to test and exclude elements from the
     *            iteration.
     * 
     * @exception IllegalArgumentException
     *                if fromKey is given and is greater than toKey.
     */
    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey,
            IEntryFilter filter) {

        assert leaf != null;

        assert tuple != null;

        this.leaf = leaf;
        
        this.tuple = tuple;

//        this.fromKey = fromKey; // may be null (no lower bound).
//        
//        this.toKey = toKey; // may be null (no upper bound).
        
        this.filter = filter; // MAY be null.
        
        { // figure out the first index to visit.

            int fromIndex;

            if (fromKey != null) {

                fromIndex = leaf.keys.search(fromKey);

                if (fromIndex < 0) {

                    fromIndex = -fromIndex - 1;

                }

            } else {

                fromIndex = 0;

            }

            this.fromIndex = fromIndex;

        }

        { // figure out the first index to NOT visit.

            int toIndex;

            if (toKey != null) {

                toIndex = leaf.keys.search(toKey);

                if (toIndex < 0) {

                    toIndex = -toIndex - 1;

                }

            } else {

                toIndex = leaf.nkeys;

            }

            this.toIndex = toIndex;

        }

        if (fromIndex > toIndex) {
            
            throw new IllegalArgumentException("fromKey > toKey");
            
        }
        
        // starting index is the lower bound.
        index = fromIndex;
        
        if (tuple != null && tuple.needKeys
                && leaf.keys instanceof ImmutableKeyBuffer) {

            /*
             * Immutable key buffers break the key into a shared prefix and a
             * per-key remainder. We copy the shared prefix into the buffer and
             * set the mark on the buffer. We then rewind to the mark for each
             * visited key and append the remainder such that the full key is
             * materialized in the buffer without doing any allocations on the
             * heap.
             */

            final ImmutableKeyBuffer keys = ((ImmutableKeyBuffer) leaf.keys);

            // reset the buffer.
            tuple.kbuf.reset();

            // copy the shared prefix into the buffer.
            tuple.kbuf.put(keys.buf, 0, keys.getPrefixLength());
            
            // set the mark - we will reuse the shared prefix for each visited key.
            tuple.kbuf.mark();
                
        }
        
    }

    public boolean hasNext() {

        if(filter == null) {
            
            return index >= fromIndex && index < toIndex;
            
        }

        while(index >= fromIndex && index < toIndex) {
         
            if (filter.isValid(leaf.values[index])) {
                
                return true;
                
            }
            
            index++;

        }
        
        return false;
        
    }

    public Object next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;

        // #of entries visited by the iterator.
        tuple.nvisited++;

        if (tuple.needKeys) {

            // tuple.key = leaf.keys.getKey(lastVisited);
            if (leaf.keys instanceof MutableKeyBuffer) {

                // reference to the current key.
                final byte[] key = ((MutableKeyBuffer) leaf.keys).keys[lastVisited];

                // copy the key data into the buffer.
                tuple.kbuf.reset().put(key);

            } else {

                final ImmutableKeyBuffer keys = ((ImmutableKeyBuffer) leaf.keys);

                // rewind to the end of the shared key prefix.
                tuple.kbuf.rewind();

                // copy the data for the remainder of the current key.
                tuple.kbuf.put(keys.buf, keys.offsets[lastVisited], keys
                        .getRemainderLength(lastVisited));

            }

        }

        if (tuple.needVals) {

            // the current value. @todo when copying values into the leaf this will need to copy the value out!
            final Object val = filter == null ? leaf.values[lastVisited] : filter
                    .resolve(leaf.values[lastVisited]); 
            
            tuple.val = val;
            
            return val;
            
        }
        
        // Note: if values not requested then next() always returns null.
        return null;
        
    }

    public Object getValue() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        if(!tuple.needVals) {
            
            throw new UnsupportedOperationException();
            
        }
        
        return tuple.val;
        
    }
    
    public ITuple getTuple() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }

        return tuple;
        
    }
    
    public byte[] getKey() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }

        if(!tuple.needKeys) {
            
            throw new UnsupportedOperationException();
            
        }
        
//        if(tuple != null && tuple.needKeys) {
        
            // return the key from the tuple.
            
            return tuple.getKey();
            
//        }
//        
//        return leaf.keys.getKey(lastVisited);
        
    }
    
    /**
     * FIXME Support removal. This is tricky because we have to invoke
     * copy-on-write if the leaf is not dirty. Since copy-on-write can cause the
     * leaf and its parents to be cloned, and since the entry must be removed in
     * the mutable copy, this means that the {@link Leaf} that we have may be
     * the wrong object. The simplest way to handle that is to re-start the
     * iterator from the current key and then invoke delete on that entry.
     * However, that is not that simple :-) <br>
     * The other twist with removal is that it can cause the leaf to underflow,
     * which results in a structural change in the btree.
     * 
     * @todo Also support update of the value during traversal. This is simpler
     *       since it does not lead to structural changes (the #of entries in
     *       the leaf does not change), but it has the same problems with
     *       needing to invoke copy-on-write if the leaf is not dirty.
     * 
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
