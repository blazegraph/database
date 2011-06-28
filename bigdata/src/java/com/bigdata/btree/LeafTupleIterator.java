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
public class LeafTupleIterator<E> implements ITupleIterator<E> {

    private final Leaf leaf;

    private final AbstractTuple<E> tuple;
    
    private int index;

    private int lastVisited = -1;

//    private final byte[] fromKey;
//    
//    private final byte[] toKey;

    // first index to visit.
    private final int fromIndex;

    // first index to NOT visit.
    private final int toIndex;

    private final boolean hasDeleteMarkers;
    
    private final boolean visitDeleted;
    
    public LeafTupleIterator(Leaf leaf) {

        this(leaf, new Tuple<E>(leaf.btree, IRangeQuery.DEFAULT), null, null);

    }

    public LeafTupleIterator(final Leaf leaf, final AbstractTuple<E> tuple) {

        this(leaf, tuple, null, null);

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
     * 
     * @exception IllegalArgumentException
     *                if fromKey is given and is greater than toKey.
     */
    public LeafTupleIterator(final Leaf leaf, final AbstractTuple<E> tuple,
            final byte[] fromKey, final byte[] toKey) {

        assert leaf != null;

        assert tuple != null;

        this.leaf = leaf;
        
        this.tuple = tuple;

        this.hasDeleteMarkers = leaf.hasDeleteMarkers();

        this.visitDeleted = (tuple.flags() & IRangeQuery.DELETED) != 0;

//        this.fromKey = fromKey; // may be null (no lower bound).
//        
//        this.toKey = toKey; // may be null (no upper bound).
        
        { // figure out the first index to visit.

            int fromIndex;

            if (fromKey != null) {

                fromIndex = leaf.getKeys().search(fromKey);

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

                toIndex = leaf.getKeys().search(toKey);

                if (toIndex < 0) {

                    toIndex = -toIndex - 1;

                }

            } else {

                toIndex = leaf.getKeyCount();

            }

            this.toIndex = toIndex;

        }

        if (fromIndex > toIndex) {
            
            throw new IllegalArgumentException("fromKey > toKey");
            
        }
        
        // starting index is the lower bound.
        index = fromIndex;
        
    }

    /**
     * Examines the entry at {@link #index}. If it passes the criteria for an
     * entry to visit then return true. Otherwise increment the {@link #index}
     * until either all entries in this leaf have been exhausted -or- the an
     * entry is identified that passes the various criteria.
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
            if (hasDeleteMarkers && !visitDeleted
                    && leaf.getDeleteMarker(index)) {

                // skipping a deleted version.
                
                continue;
                
            }

            // entry @ index is next to visit.
            
            return true;
            
        }

        // nothing left to visit in this leaf.
        
        return false;
        
    }
    
    public ITuple<E> next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;

        tuple.copy(lastVisited, leaf);
        
        return tuple;
        
    }

    /**
     * This operation is not supported.
     * <p>
     * Note: There are two ways in which you can achieve the semantics of
     * {@link #remove()}. One is to use an {@link ITupleCursor}, which
     * correctly handles traversal with concurrent modification. The other is to
     * use a {@link AbstractChunkedTupleIterator}, which buffers the tuples
     * first and then does a "delete" behind in order to avoid concurrent
     * modification during traversal.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
