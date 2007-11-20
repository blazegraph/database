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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;

/**
 * Hard reference cache eviction listener writes a dirty node or leaf onto the
 * persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultEvictionListener implements
        IEvictionListener {

    /**
     * Log for eviction of dirty leaves and nodes. 
     */
    public static final Logger log = Logger.getLogger(DefaultEvictionListener.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();
    
    public void evicted(HardReferenceQueue<PO> cache, PO ref) {

        AbstractNode node = (AbstractNode) ref;

        /*
         * Decrement the reference counter.  When it reaches zero (0) we will
         * evict the node or leaf iff it is dirty.
         */
        
        if (--node.referenceCount == 0) {

            final AbstractBTree btree = node.btree;
            
            assert btree.ndistinctOnWriteRetentionQueue > 0;

            btree.ndistinctOnWriteRetentionQueue--;
            
            if( node.deleted ) {
                
                /*
                 * Deleted nodes are ignored as the are evicted from the queue.
                 */

                if( DEBUG ) log.debug("ignoring deleted");
                
                return;
                
            }
            
            if (node.dirty) {
                
                if (node.isLeaf()) {

                    /*
                     * A leaf is written out directly.
                     */
                    
                    if(INFO) log.info("Evicting dirty leaf: "+node);
                    
                    btree.writeNodeOrLeaf(node);

                } else {

                    /*
                     * A non-leaf node must be written out using a post-order
                     * traversal so that all dirty children are written through
                     * before the dirty parent. This is required in order to
                     * assign persistent identifiers to the dirty children.
                     */

                    if(INFO) log.info("Evicting dirty node: "+node);
                    
                    btree.writeNodeRecursive(node);

                }

                // Verify the object is now persistent.
                assert !ref.dirty;
                assert ref.identity != PO.NULL;

                if(btree.readRetentionQueue!=null) {
                    
                    btree.readRetentionQueue.append(ref);
                    
                }
                
            }

        }

    }

}
