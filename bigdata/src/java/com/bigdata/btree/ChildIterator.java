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
 * Visits the direct children of a {@link Node} in the external key ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class ChildIterator implements INodeIterator {

    private final Node node;

    private int index = 0;

    private int lastVisited = -1;
    
//    private final byte[] fromKey;
//    
//    private final byte[] toKey;

    // first index to visit.
    private final int fromIndex;

    // first index to NOT visit.
    private final int toIndex;
    
    public ChildIterator(Node node) {

        this(node,null,null);
        
    }
    
    /**
     * 
     * @param node
     *            The node whose children will be traversed.
     * @param fromKey
     *            The first key whose child will be visited or <code>null</code>
     *            if the lower bound on the key traversal is not constrained.
     * @param toKey
     *            The first key whose child will NOT be visited or
     *            <code>null</code> if the upper bound on the key traversal is
     *            not constrained.
     * 
     * @exception IllegalArgumentException
     *                if fromKey is given and is greater than toKey.
     */
    public ChildIterator(Node node, byte[] fromKey, byte[] toKey) {

        assert node != null;

        this.node = node;

//        this.fromKey = fromKey; // may be null (no lower bound).
//        
//        this.toKey = toKey; // may be null (no upper bound).

        { // figure out the first index to visit.

            int fromIndex;

            if (fromKey != null) {

                fromIndex = node.findChild(fromKey);

            } else {

                fromIndex = 0;

            }

            this.fromIndex = fromIndex;

        }

        {    /*
             * figure out the last index to visit.
             * 
             * Note: Unlike a leaf, we do visit the child in which the toKey
             * would be found.  This is necessary in order to ensure that we
             * visit any keys in leaves spanned by the child that are less
             * than the toKey.
             */

            int toIndex;

            if (toKey != null) {

                toIndex = node.findChild(toKey);

                if (fromIndex > toIndex) {
                    /*
                     * Note: we test for from/to key out of order _before_
                     * incrementing the toIndex.
                     */                    
                    throw new IllegalArgumentException("fromKey > toKey");
                    
                }
                
                // +1 so that we visit the child in which toKey would be found!
                toIndex++;
                
            } else {

                // Note: nchildren == nkeys+1 for a Node.
                toIndex = node.getKeyCount() + 1;

            }

            this.toIndex = toIndex;

        }

        // starting index is the lower bound.
        index = fromIndex;

    }

    public boolean hasNext() {

        return index >= fromIndex && index < toIndex;

    }

    public AbstractNode next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;
        
        return node.getChild(lastVisited);
        
    }

    public AbstractNode getNode() {
    
        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return node.getChild(lastVisited);

    }

    public Object getKey() {

        if( lastVisited == -1 ) {
            
            throw new IllegalStateException();
            
        }
        
        return node.keys.get(lastVisited);
        
    }
    
    /**
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
