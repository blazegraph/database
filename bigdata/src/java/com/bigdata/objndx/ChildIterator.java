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
    
    private final byte[] fromKey;
    
    private final byte[] toKey;

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

        this.fromKey = fromKey; // may be null (no lower bound).
        
        this.toKey = toKey; // may be null (no upper bound).

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
                toIndex = node.nkeys + 1;

            }

            this.toIndex = toIndex;

        }

        // starting index is the lower bound.
        index = fromIndex;

    }

    public boolean hasNext() {

        return index >= fromIndex && index < toIndex;

    }

    public IAbstractNode next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;
        
        return node.getChild(lastVisited);
        
    }

    public IAbstractNode getNode() {
    
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
