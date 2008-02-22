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
package com.bigdata.mdi;

import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;

/**
 * Abstract base class for tasks that build {@link IndexSegment}(s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write test suite for executing partition task schedules.
 * 
 * @todo add result to persistent schedule outcome so that this is restart safe.
 * 
 * @todo once the {@link IndexSegment} is ready the metadata index needs to be
 *       updated to reflect that the {@link IndexSegment} is live and the views
 *       that rely on the partition on the old journal need to be invalidated so
 *       new views utilize the new {@link IndexSegment} rather than the data on
 *       the old journal.
 * 
 * @todo the old journal is not available for release until all partitions for
 *       all indices have been evicted. we need to track that in a restart safe
 *       manner.
 * 
 * @todo try performance with and without checksums and with and without record
 *       compression.
 */
abstract public class AbstractPartitionTask extends AbstractTask {

    /**
     * 
     */
    public AbstractPartitionTask(IConcurrencyManager concurrencyManager,
            long tx, String name) {

        super(concurrencyManager, tx, name);
        
    }
    
}
