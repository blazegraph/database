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

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.sparse.TPS.TPV;

/**
 * Utility class for choosing timestamps for the {@link SparseRowStore} on the
 * server.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TimestampChooser {

    /**
     * Choose the timestamp for new {@link TPV} tuples written on the sparse row
     * store. When <i>timestamp</> is {@link SparseRowStore#AUTO_TIMESTAMP} the
     * timestamp is just the local system time. When <i>timestamp</i> is
     * {@link SparseRowStore#AUTO_TIMESTAMP_UNIQUE} the timestamp is assigned by
     * the {@link ILocalTransactionManager}. Otherwise the caller's
     * <i>timestamp</i> is returned.
     * <p>
     * Note: Revisions written with the same timestamp as a pre-existing column
     * value will overwrite the existing column value rather that causing new
     * revisions with their own distinct timestamp to be written. There is
     * therefore a choice for "auto" vs "auto-unique" for timestamps.
     * 
     * @todo Timestamps can be locally generated on the server since they must
     *       be consistent solely within a row, and all revisions of column
     *       values for the same row will always be in the same index partition
     *       and hence on the same server.
     *       <P>
     *       The only way in which time could go backward is if there is a
     *       failover to another server for the partition and the other server
     *       has a different clock time. If the server clocks are kept
     *       synchronized then this should not be a problem.
     */
    static public long chooseTimestamp(IIndex ndx, long timestamp) {

        if (timestamp == SparseRowStore.AUTO_TIMESTAMP) {

            timestamp = System.currentTimeMillis();

        } else if (timestamp == SparseRowStore.AUTO_TIMESTAMP_UNIQUE) {

            final AbstractJournal journal = ((AbstractJournal) ((AbstractBTree) ndx)
                    .getStore());

            final ILocalTransactionManager transactionManager = journal
                    .getLocalTransactionManager();

            timestamp = transactionManager.nextTimestampRobust();

        }

        return timestamp;

    }

}
