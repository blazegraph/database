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
 * Created on Nov 17, 2006
 */
package com.bigdata.btree;

import com.bigdata.cache.IHardReferenceQueue;

/**
 * Hard reference cache eviction listener writes a dirty node or leaf onto the
 * persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultEvictionListener implements
        IEvictionListener {

    public void evicted(final IHardReferenceQueue<PO> cache, final PO ref) {

        final AbstractNode<?> node = (AbstractNode<?>) ref;

        /*
         * Decrement the reference counter. When it reaches zero (0) we will
         * evict the node or leaf iff it is dirty.
         * 
         * Note: The reference counts and the #of distinct nodes or leaves on
         * the writeRetentionQueue are not exact for a read-only B+Tree because
         * neither synchronization nor atomic counters are used to track that
         * information.
         */

        if (--node.referenceCount > 0) {
            
            return;

            
        }

        final AbstractBTree btree = node.btree;

        // Note: This assert can be violated for a read-only B+Tree since there
        // is less synchronization.
        assert btree.isReadOnly() || btree.ndistinctOnWriteRetentionQueue > 0;

        btree.ndistinctOnWriteRetentionQueue--;
        
        if (node.deleted) {

            /*
             * Deleted nodes are ignored as they are evicted from the queue.
             */

            return;

        }

        // this does not permit transient nodes to be coded.
        if (node.dirty && btree.store != null) {
//            // this causes transient nodes to be coded on eviction.
//            if (node.dirty) {
            
            if (node.isLeaf()) {

                /*
                 * A leaf is written out directly.
                 */
                
                btree.writeNodeOrLeaf(node);

            } else {

                /*
                 * A non-leaf node must be written out using a post-order
                 * traversal so that all dirty children are written through
                 * before the dirty parent. This is required in order to
                 * assign persistent identifiers to the dirty children.
                 */

                btree.writeNodeRecursive(node);

            }

            // is a coded data record.
            assert node.isCoded();
            
            // no longer dirty.
            assert !node.dirty;
            
            if (btree.store != null) {
             
                // object is persistent (has assigned addr).
                assert ref.identity != PO.NULL;
                
            }
            
        } // isDirty

        // This does not insert into the cache.  That is handled by writeNodeOrLeaf.
//        if (btree.globalLRU != null) {
//
//            /*
//             * Add the INodeData or ILeafData object to the global LRU, NOT the
//             * Node or Leaf.
//             * 
//             * Note: The global LRU touch only occurs on eviction from the write
//             * retention queue. This is nice because it limits the touches on
//             * the global LRU, which could otherwise be a hot spot. We do a
//             * touch whether or not the node was persisted since we are likely
//             * to return to the node in either case.
//             */
//
//            final IAbstractNodeData delegate = node.getDelegate();
//
//            assert delegate != null : node.toString();
//
//            assert delegate.isCoded() : node.toString();
//
//            btree.globalLRU.add(delegate);
//
//        }

    }

}
