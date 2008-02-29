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
 * Created on Feb 29, 2008
 */

package com.bigdata.resources;

import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.mdi.MetadataIndex;

/**
 * Task joins one or more index partitions and should be invoked when their is
 * strong evidence that the index partitions have shrunk enough to warrant their
 * being combined into a single index partition. The index partitions MUST be
 * partitions of the same scale-out index and MUST be siblings (their left and
 * right separators must cover a continuous interval).
 * <p>
 * The task reads from the lastCommitTime of the old journal and builds a single
 * {@link IndexSegment} from the merged read of the source index partitions as
 * of that timestamp and returns a {@link JoinResult}.
 * 
 * @see UpdateJoinIndexPartition, which performs the atomic update of the view
 *      definitions on the live journal and the {@link MetadataIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinIndexPartitionTask extends AbstractTask {

    /**
     * 
     */
    private final ResourceManager resourceManager;

    /**
     * @param resourceManager 
     * @param concurrencyManager
     * @param lastCommitTime
     * @param resource
     */
    protected JoinIndexPartitionTask(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, long lastCommitTime,
            String[] resources) {

        super(concurrencyManager, -lastCommitTime, resources);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

    }

    @Override
    protected Object doTask() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }
    
}
