/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 4, 2008
 */

package com.bigdata.sparse;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.sparse.TPS.TPV;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Utility class for choosing timestamps for the {@link SparseRowStore} on the
 * server.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TimestampChooser implements IRowStoreConstants {

    /**
     * Choose the timestamp for {@link TPV} tuples to written on the sparse row
     * store.
     * <p>
     * Note: Revisions written with the same timestamp as a pre-existing column
     * value will overwrite the existing column value rather than causing new
     * revisions with their own distinct timestamp to be written. There is
     * therefore a choice for "auto" vs "auto-unique" for timestamps.
     * <p>
     * Note: Timestamps generated locally on the server will be consistent
     * within a row, and all revisions of column values for the same row will
     * always be in the same index partition and hence on the same server. This
     * means that we can use locally assigned timestamp as unique timestamps.
     * However, time could go backwards if there is a failover to another server
     * for the partition and the other server has a different clock time. This
     * is resolved by choosing a timestamp assigned by the global
     * {@link ITimestampService}.
     * 
     * @param timestamp
     *            When <i>timestamp</> is {@link IRowStoreConstants#AUTO_TIMESTAMP}
     *            the timestamp is the local system time. When <i>timestamp</i>
     *            is {@link IRowStoreConstants#AUTO_TIMESTAMP_UNIQUE} a federation
     *            wide unique timestamp is assigned by the
     *            {@link ITimestampService}. Otherwise the caller's
     *            <i>timestamp</i> is returned.
     */
    static public long chooseTimestamp(final IIndex ndx, final long timestamp) {

        if (timestamp == AUTO_TIMESTAMP) {

            return System.currentTimeMillis();

        } else if (timestamp == AUTO_TIMESTAMP_UNIQUE) {

            /*
             * The BTree that is absorbing writes.
             */
            final BTree mutableBTree = ((ILocalBTreeView) ndx)
                    .getMutableBTree();

            if(mutableBTree.getStore() instanceof TemporaryRawStore) {
                
                /*
                 * Use a unique timestamp for the local machine since a
                 * temporary store is not visible outside of that context.
                 */

                return MillisecondTimestampFactory.nextMillis();
                
            }
            
            /*
             * The backing store will be some kind of AbstractJournal - either
             * a Journal or a ManagedJournal.
             */

            final AbstractJournal journal = (AbstractJournal) mutableBTree
                    .getStore();

            /*
             * This will be locally unique for a Journal and federation-wide
             * unique for a ManagedJournal. In the former case the timestamp is
             * assigned locally. In the latter case it will use a robust method
             * to discover the timestamp service and obtain a federation-wide
             * unique timestamp.
             */

//            try {

                return journal.getLocalTransactionManager().nextTimestamp();
                
//            } catch(IOException ex) {
//                
//                /*
//                 * Note: Declared for RMI interoperability.
//                 */
//                
//                throw new RuntimeException(ex);
//                
//            }
            
        } else {
    
            // return the caller's value.
            return timestamp;
            
        }

    }
    
}
