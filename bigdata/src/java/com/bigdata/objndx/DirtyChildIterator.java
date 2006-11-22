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
 * <p>
 * Visits the direct dirty children of a node in the external key ordering. This
 * iterator does NOT force children to be loaded from disk if the are not
 * resident since dirty nodes are always resident.
 * </p>
 * <p>
 * In order to guarentee that a node will still be dirty by the time that the
 * caller visits it the iterator must touch the node (or hold a hard reference),
 * thereby placing it into the appropriate hard reference queue and incrementing
 * its reference counter. Evictions do NOT cause IO when the reference is
 * non-zero, so the node will not be made persistent as a result of other node
 * touches. However, the node can still be made persistent if the caller
 * explicitly writes the node onto the store.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Write tests for this, including the fact that the child must remain
 * strongly reachable.
 */
class DirtyChildIterator implements Iterator<AbstractNode> {

    private final Node node;
    
    /**
     * The index of the next child to return.
     */
    private int index = 0;

    /**
     * The next child to return or null if we need to scan for the next child.
     * We use a hard reference to make sure that a dirty child can not be
     * written out incrementally and have its reference swept in between a call
     * to {@link #hasNext()} and the corresponding call to {@link #next()}.
     */
    private AbstractNode child = null;

    /**
     * 
     * @param node
     *            The node whose direct children will be visited in key order.
     */
    public DirtyChildIterator(Node node) {

        assert node != null;

        this.node = node;
        
    }

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
            
            if( child.isDirty() ) continue;
            
        }
        
        return index <= node.nkeys;

    }

    public AbstractNode next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        if( child == null ) {
            
            return node.getChild(index++);
            
        } else {
            
            AbstractNode tmp = child;
            
            child = null;
            
            return tmp;
            
        }
        
    }

    public void remove() {

        throw new UnsupportedOperationException();

    }

}