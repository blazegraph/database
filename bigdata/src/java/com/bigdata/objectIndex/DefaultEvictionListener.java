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
 * Created on Nov 17, 2006
 */
package com.bigdata.objectIndex;

import com.bigdata.cache.HardReferenceQueue;

/**
 * Hard reference cache eviction listener writes the node or leaf onto the
 * persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultEvictionListener implements
        IEvictionListener {

    public void evicted(HardReferenceQueue<PO> cache, PO ref) {

        AbstractNode node = (AbstractNode) ref;

        /*
         * Decrement the reference counter.  When it reaches zero (0) we will
         * evict the node or leaf iff it is dirty.
         */
        
        if (--node.referenceCount == 0) {

            if (node.isDirty()) {

                if (node.isLeaf()) {

                    /*
                     * A leaf is written out directly.
                     */
                    node.btree.writeNodeOrLeaf(node);

                } else {

                    /*
                     * A non-leaf node must be written out using a post-order
                     * traversal so that all dirty children are written through
                     * before the dirty parent. This is required in order to
                     * assign persistent identifiers to the dirty children.
                     */
                    node.btree.writeNodeRecursive(node);

                }

                // Verify the object is now persistent.
                assert !ref.dirty;
                assert ref.identity != PO.NULL;

            }

        }

    }

}
