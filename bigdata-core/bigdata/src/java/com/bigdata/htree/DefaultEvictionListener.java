/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.htree;

import com.bigdata.btree.EvictionError;
import com.bigdata.btree.IEvictionListener;
import com.bigdata.btree.PO;
import com.bigdata.cache.IHardReferenceQueue;

/**
 * Hard reference cache eviction listener writes a dirty node or leaf onto the
 * persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultEvictionListener implements
        IEvictionListener {

//	private static final Logger log = Logger.getLogger(DefaultEvictionListener.class);

	@Override
    public void evicted(final IHardReferenceQueue<PO> cache, final PO ref) {

        final AbstractPage node = (AbstractPage) ref;

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

//        final AbstractHTree htree = node.htree;
//
//        final BTreeCounters counters = htree.getBtreeCounters();
//
//        counters.queueEvict.incrementAndGet();
//        
//        if (--node.referenceCount > 0) {
//            
//            return;
//            
//        }
//
//        counters.queueEvictNoRef.incrementAndGet();

        doEviction(node);

	}
	
	private void doEviction(final AbstractPage node) {
        
        final AbstractHTree htree = node.htree;
        
        if (htree.error != null) {
            /**
             * This occurs if an error was detected against a mutable view of
             * the index (the unisolated index view) and the caller has not
             * discarded the index and caused it to be reloaded from the most
             * recent checkpoint.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/1005"> Invalidate
             *      BTree objects if error occurs during eviction </a>
             */
            throw new IllegalStateException(AbstractHTree.ERROR_ERROR_STATE,
                    htree.error);
        }

        try {

        // Note: This assert can be violated for a read-only B+Tree since there
        // is less synchronization.
        assert htree.isReadOnly() || htree.ndistinctOnWriteRetentionQueue > 0;

        htree.ndistinctOnWriteRetentionQueue--;
        
        if (node.isDeleted()) {

            /*
             * Deleted nodes are ignored as they are evicted from the queue.
             */

            return;

        }

        // this does not permit transient nodes to be coded.
        if (node.isDirty() && htree.store != null) {
//            // this causes transient nodes to be coded on eviction.
//            if (node.dirty) {

//            counters.queueEvictDirty.incrementAndGet();
            
//			if (log.isDebugEnabled())
//				log.debug("Evicting: " + (node.isLeaf() ? "leaf" : "node")
//						+ " : " + node.toShortString());
        	
            if (node.isLeaf()) {

                /*
                 * A leaf is written out directly.
                 */
                
                htree.writeNodeOrLeaf(node);

            } else {

                /*
                 * A non-leaf node must be written out using a post-order
                 * traversal so that all dirty children are written through
                 * before the dirty parent. This is required in order to
                 * assign persistent identifiers to the dirty children.
                 */

                htree.writeNodeRecursive(node);

            }

            // is a coded data record.
            assert node.isCoded();
            
            // no longer dirty.
            assert !node.isDirty();
            
            if (htree.store != null) {
             
                // object is persistent (has assigned addr).
                assert node.getIdentity() != PO.NULL;
                
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

        } catch (Throwable e) {
            
            if (!htree.readOnly) {

                /**
                 * If the btree is mutable and an eviction fails, then the index
                 * MUST be discarded.
                 * 
                 * @see <a href="http://trac.blazegraph.com/ticket/1005">
                 *      Invalidate BTree objects if error occurs during eviction
                 *      </a>
                 */
                
                htree.error = e;

                // Throw as Error.
                throw new EvictionError(e);

            }

            // Launder the throwable.
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;

            throw new RuntimeException(e);

        }

	}

}
