/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 15, 2006
 */
package com.bigdata.objndx;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Visits the direct dirty children of a {@link Node} in the external key
 * ordering. Since dirty nodes are always resident this iterator never forces a
 * child to be loaded from the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class DirtyChildIterator implements Iterator<AbstractNode>,
        IKeyVisitor<AbstractNode> {

    private final Node node;
    
    /**
     * The index of the next child to return.
     */
    private int index = 0;

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
            
            WeakReference<AbstractNode> childRef = node.childRefs[index];
            
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
        index++;

        // clear the reference to force another scan.
        child = null;

        return tmp;

    }

    // FIXME either implement getKey() or do not implement that interface.
    public Object getKey() {

        throw new UnsupportedOperationException();

    }

    /**
     * @exception UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

}
