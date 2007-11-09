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

    private final EntryFilter filter;
    
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

        this(leaf, null, null, null, null);

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
     *            The leaf whose entries will be traversed.
     * @param tuple
     *            Used to hold the output values.
     * @param fromKey
     *            The first key whose entry will be visited or <code>null</code>
     *            if the lower bound on the key traversal is not constrained.
     * @param toKey
     *            The first key whose entry will NOT be visited or
     *            <code>null</code> if the upper bound on the key traversal is
     *            not constrained.
     * @param filter
     *            An optional filter used to test and exclude elements from the
     *            iteration.
     * 
     * @exception IllegalArgumentException
     *                if fromKey is given and is greater than toKey.
     */
    public EntryIterator(Leaf leaf, Tuple tuple, byte[] fromKey, byte[] toKey,
            EntryFilter filter) {

        assert leaf != null;

        this.leaf = leaf;
        
        this.tuple = tuple; // MAY be null.

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
        
        if( tuple != null ) {

            /*
             * eagerly set the key/value on the tuple for a side-effect style
             * return.
             */
            tuple.key = leaf.keys.getKey(lastVisited);
            
            tuple.val = filter == null ? leaf.values[lastVisited] : filter
                    .resolve(leaf.values[lastVisited]);
            
            return tuple.val;
            
        }
        
        return filter == null ? leaf.values[lastVisited] : filter
                .resolve(leaf.values[lastVisited]);
        
    }

    public Object getValue() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return filter == null ? leaf.values[lastVisited] : filter
                .resolve(leaf.values[lastVisited]);
        
    }
    
    public byte[] getKey() {
        
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return leaf.keys.getKey(lastVisited);
        
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
