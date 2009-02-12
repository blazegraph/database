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
 * Created on Feb 6, 2009
 */

package com.bigdata.resources;

import java.util.UUID;

import com.bigdata.journal.TimestampUtility;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.Split;

/**
 * Splits the tail of an index partition and optionally submits a task to move
 * the tail to a target data service specified by the caller.
 * <p>
 * The split point is choosen by locating the right-most non-leaf node. The key
 * range which would enter that node is placed within the new right-sibling
 * index partition. The rest of the key range is placed within the new
 * left-sibling index partition.
 * <p>
 * The tail split operation is extremely fast since it only redefines the index
 * partitions. However, it require RMI to the metadata index to do that and
 * therefore should not be done during synchronous overflow in order to reduce
 * the possibility for errors during that operation.
 * <p>
 * The right-sibling of tail split is a prime candidate for a move since there
 * is an expectation that it will continue to be hot for writes. Therefore the
 * caller has an opportunity when specifying a tail split to also specify that
 * the new right-sibling index partition will be moved onto a caller specified
 * data service.
 * 
 * @todo unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitTailTask extends AbstractPrepareTask {

    private final ViewMetadata vmd;

    /**
     * When non-<code>null</code> the new right-sibling (the tail) will be
     * moved to the specified data service after the split.
     */
    private final UUID moveTarget;
    
    /**
     * @param vmd
     * @param moveTarget
     *            When non-<code>null</code> the new right-sibling (the tail)
     *            will be moved to the specified data service after the split.
     */
    public SplitTailTask(final ViewMetadata vmd, final UUID moveTarget) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);

        this.vmd = vmd;
        
        if (vmd.pmd == null) {

            throw new IllegalStateException("Not an index partition.");

        }

        if (vmd.pmd.getSourcePartitionId() != -1) {

            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + vmd.pmd.getSourcePartitionId());

        }

        this.moveTarget = moveTarget;
        
    }

    @Override
    protected void clearRefs() {

        vmd.clearRef();
        
    }

    @Override
    protected Object doTask() throws Exception {
        
        final Event e = new Event(resourceManager.getFederation(),
                new EventResource(vmd.indexMetadata),
                OverflowActionEnum.TailSplit, OverflowActionEnum.TailSplit
                        + (moveTarget != null ? "+" + OverflowActionEnum.Move
                                : "") + "(" + vmd.name + ") : " + vmd
                        + ", moveTarget=" + moveTarget).start();

        try {

            final SplitResult result;
            try {
                
                /*
                 * Split into head (most) and tail. both will be new index
                 * partitions.
                 */

                final Split[] splits = SplitUtility.tailSplit(resourceManager,
                        vmd.getBTree());

                // validate the splits before processing them.
                SplitUtility.validateSplits(vmd.getBTree(), splits);

                result = SplitUtility.buildSplits(vmd, splits, e);

            } finally {

                /*
                 * We are done building index segments from the source index
                 * partition view so we clear our references for that view.
                 */

                clearRefs();

            }
        
            // Do the atomic update
            SplitIndexPartitionTask.doSplitAtomicUpdate(resourceManager, vmd,
                    result, OverflowActionEnum.TailSplit,
                    resourceManager.indexPartitionTailSplitCounter, e);
            
            if (moveTarget != null) {
             
                /*
                 * Note: Unlike a normal move where there are writes on the old
                 * journal, all the data for the rightSibling is in an index
                 * segment that we just built and new writes MAY be buffered on
                 * the live journal. Therefore we use a different entry point
                 * into the MOVE operation, one which does not copy over the
                 * data from the old journal.
                 */

                /*
                 * The name of the post-split rightSibling (this is the source
                 * index partition for the move operation).
                 */
                final String rightSiblingName = DataService
                        .getIndexPartitionName(vmd.indexMetadata.getName(),
                                result.splits[1].pmd.getPartitionId());

                /*
                 * Obtain a new partition identifier for the partition that will
                 * be created when we move the rightSibling to the target data
                 * service.
                 */
                final int newPartitionId = resourceManager
                        .nextPartitionId(vmd.indexMetadata.getName());
                
                // register the new index partition.
                final MoveResult moveResult = MoveIndexPartitionTask
                        .registerNewPartitionOnTargetDataService(
                                resourceManager, moveTarget, rightSiblingName,
                                newPartitionId, e);
            
                /*
                 * Move the buffered writes since the tail split and go live
                 * with the new index partition.
                 */
                MoveIndexPartitionTask.moveBufferedWritesAndGoLive(
                        resourceManager, moveResult, e);
                
            }
            
            // Done.
            return result;
                        
        } finally {
            
            e.end();
            
        }
        
    }
    
}
