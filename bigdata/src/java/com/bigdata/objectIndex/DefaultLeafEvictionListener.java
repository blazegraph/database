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

import com.bigdata.cache.HardReferenceCache;

/**
 * Hard reference cache eviction listener writes the node or leaf onto the
 * persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultLeafEvictionListener implements
        ILeafEvictionListener {

    public void evicted(HardReferenceCache<PO> cache, PO ref) {

        if( ref.isDirty() ) {

            AbstractNode node = (AbstractNode) ref;
            
            /*
             * Scan at least as many queue entries as their are levels in the
             * btree.
             */
            final int nscan = Math.max(cache.nscan(), node.btree.height+1);

            /*
             * Scan the head of the hard reference queue to make sure that we do
             * NOT write out a node that has been recently scanned or modified.
             */
            if( cache.scanHead(nscan,ref)) {
                
                /*
                 * Since the reference appears in the head of the hard reference
                 * queue we do NOT write out the node. Writing out the node
                 * would cause it to become immutable, but it is critical that
                 * we do NOT force a node to become immutable precisely at the
                 * time when we are preparing to insert or split that node. If
                 * we do not make this test here then the act of considering a
                 * node may cause it to be appended to the queue and thereby
                 * potentially force the node reference to be evicted from the
                 * queue. Without this test, this scenario can result in the
                 * node being evicted and becoming immutable exactly when we
                 * need to write on it!
                 * 
                 * Note: This also reduce IOs in cases where we are very likely
                 * to need to use the node again soon and avoids triggering
                 * copy-on-write if we do need to reuse the node.  Thus this is
                 * very much a win-win test.
                 */

                return;
                
            }
            
            if( node.isLeaf() ) {
            
                /*
                 * A leaf is written out directly.
                 */
                node.btree.writeNodeOrLeaf(node);
                
            } else {
                
                /*
                 * A non-leaf node must be written out using a post-order
                 * traversal so that all dirty children are written through
                 * before the dirty parent.  This is required in order to
                 * assign persistent identifiers to the dirty children.
                 */
                node.btree.writeNodeRecursive(node);
                
            }

            // Verify the object is now persistent.
            assert ! ref.dirty;
            assert ref.identity != PO.NULL;
            
        }

    }

}