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

import java.lang.ref.Reference;
import java.util.NoSuchElementException;

/**
 * Visits the direct dirty children of a {@link Node} in the external key
 * ordering. Since dirty nodes are always resident this iterator never forces a
 * child to be loaded from the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class DirtyChildIterator implements INodeIterator {

    private final Node node;
    
    /**
     * The index of the next child to return.
     */
    private int index = 0;
    
    /**
     * The index of the last child that was returned to the caller via
     * {@link #next()}.
     */
    private int lastVisited = -1;

    /**
     * The next child to return or null if we need to scan for the next child.
     * We always test to verify that the child is in fact dirty in
     * {@link #next()} since it may have been written out between
     * {@link #hasNext()} and {@link #next()}.
     */
    private AbstractNode child = null;

    /**
     * 
     * @param node
     *            The node whose direct dirty children will be visited in key
     *            order.
     */
    public DirtyChildIterator(Node node) {

        assert node != null;

        this.node = node;
        
    }

    /**
     * @return true iff there is a dirty child having a separator key greater
     *         than the last visited dirty child at the moment that this method
     *         was invoked. If this method returns <code>true</code> then an
     *         immediate invocation of {@link #next()} will succeed. However,
     *         that guarentee does not hold if intervening code forces the
     *         scheduled dirty child to be written onto the store.
     */
    public boolean hasNext() {

        /*
         * If we are only visiting dirty children, then we need to test the
         * current index. If it is not a dirty child, then we need to scan until
         * we either exhaust the children or find a dirty index.
         */

        if( child != null && child.isDirty() ) {
        
            /*
             * We have a child reference and it is still dirty.
             */
            return true;
            
        }
        
        for( ; index <= node.nkeys; index++ ) {
            
            Reference<AbstractNode> childRef = node.childRefs[index];
            
            if( childRef == null ) continue;
            
            child = childRef.get();
            
            if( child == null ) continue;
            
            if( ! child.isDirty() ) continue;

            /*
             * Note: We do NOT touch the hard reference queue here since the
             * DirtyChildrenIterator is used when persisting a node using a
             * post-order traversal. If a hard reference queue eviction drives
             * the serialization of a node and we touch the hard reference queue
             * during the post-order traversal then we break down the semantics
             * of HardReferenceQueue#append(...) as the eviction does not
             * necessarily cause the queue to reduce in length.
             */
//            /*
//             * Touch the child so that it will not be a candidate for eviction
//             * to the store.
//             */ 
//            node.btree.touch(node);
            
            break;
            
        }
        
        return index <= node.nkeys;

    }

    public AbstractNode next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        assert child != null;

        assert child.isDirty();

        AbstractNode tmp = child;

        // advance the index where the scan will start next() time.
        lastVisited = index++;

        // clear the reference to force another scan.
        child = null;

        return tmp;

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
        
        return node.keys.getKey(lastVisited);
        
    }

    /**
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
