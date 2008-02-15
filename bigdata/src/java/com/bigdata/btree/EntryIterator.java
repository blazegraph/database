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
    
    private int index;

    private int lastVisited = -1;

//    private final byte[] fromKey;
//    
//    private final byte[] toKey;

    // first index to visit.
    private final int fromIndex;

    // first index to NOT visit.
    private final int toIndex;

    public EntryIterator(Leaf leaf) {

        this(leaf, new Tuple(IRangeQuery.DEFAULT), null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple) {

        this(leaf, tuple, null, null, null);

    }

    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey) {

        this(leaf, tuple, fromKey, toKey, null/*filter*/);

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
        
//        if (tuple != null && tuple.needKeys
//                && leaf.keys instanceof ImmutableKeyBuffer) {
//
//            /*
//             * Immutable key buffers break the key into a shared prefix and a
//             * per-key remainder. We copy the shared prefix into the buffer and
//             * set the mark on the buffer. We then rewind to the mark for each
//             * visited key and append the remainder such that the full key is
//             * materialized in the buffer without doing any allocations on the
//             * heap.
//             */
//
//            final ImmutableKeyBuffer keys = ((ImmutableKeyBuffer) leaf.keys);
//
//            // reset the buffer.
//            tuple.kbuf.reset();
//
//            // copy the shared prefix into the buffer.
//            tuple.kbuf.put(keys.buf, 0, keys.getPrefixLength());
//            
//            // set the mark - we will reuse the shared prefix for each visited key.
//            tuple.kbuf.mark();
//                
//        }
        
    }

    /**
     * Examines the entry at {@link #index}. If it passes the criteria for an
     * entry to visit then return true. Otherwise increment the {@link #index}
     * until either all entries in this leaf have been exhausted -or- the an
     * entry is identified that passes the various criteria.
     * <p>
     * Note: Criteria include (a) the optional {@link IEntryFilter}; and (b)
     * deleted entries are skipped unless {@link IRangeQuery#DELETED} has
     * been specified.
     */
    public boolean hasNext() {

//        if(filter == null) {
//            
//            return index >= fromIndex && index < toIndex;
//            
//        }

        for( ; index >= fromIndex && index < toIndex; index++) {
         
            /*
             * Skip deleted entries unless specifically requested.
             */
            if (leaf.hasDeleteMarkers()
                    && ((tuple.flags() & IRangeQuery.DELETED) == 0)
                    && leaf.deleteMarkers[index]) {
                
                // skipping a deleted version.
                
                continue;
                
            }
            
            if (filter != null) {

                /*
                 * Copy the value from the index entry that is to be considered
                 * by the filter into a buffer and then expose that buffer as an
                 * input stream so that the filter can read the value.
                 * 
                 * Note: The filter is considering a value that is a successor
                 * in the leaf order of the last tuple returned by next() (if
                 * any). We can not reuse the tuple to expose the value to the
                 * filter since it is already in use exposing the last visited
                 * value to the iterator consumer.
                 */
                
                if (tmp == null) {

                    // temp tuple copies all available data about the index entry.
                    tmp = new Tuple(IRangeQuery.KEYS|IRangeQuery.VALS);

                }
                
                // copy data from the index entry.
                tmp.copy(index, leaf);
                
                if (!filter.isValid(tmp)) {

                    // skip entry rejected by the filter.

                    continue;

                }

            }

            // entry @ index is next to visit.
            
            return true;
            
        }

        // nothing left to visit in this leaf.
        
        return false;
        
    }
    
    /**
     * Used iff an {@link IEntryFilter} was specified.
     */
    private Tuple tmp = null;
    
    public ITuple next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;

        tuple.copy(lastVisited,leaf);
        
        return tuple;
        
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
     * @todo Also support update of the value during traversal? This has the
     *       same problems with needing to invoke copy-on-write if the leaf is
     *       not dirty.
     * 
     * @see ChunkedRangeIterator, which does support removal using a batch
     *      delete-behind approach
     * 
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
