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

import com.bigdata.btree.BTreeCounters;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
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
 * 
 * @todo do this as a variant on move? The right-sibling of tail split is a
 *       prime candidate for a move since there is an expectation that it will
 *       continue to be hot for writes. Therefore the caller has an opportunity
 *       when specifying a tail split to also specify that the new right-sibling
 *       index partition will be moved onto a caller specified data service.
 * 
 * @todo unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitTailTask extends AbstractPrepareTask {

    private final long lastCommitTime;
    
    private final ViewMetadata vmd;

    /**
     * When non-<code>null</code> the new right-sibling (the tail) will be
     * moved to the specified data service after the split.
     */
    private final UUID moveTarget;
    
    /**
     * The event corresponding to the split action.
     */
    private final Event e;
    
    /**
     * @param resourceManager
     * @param lastCommitTime
     * @param name
     * @param vmd
     * @param moveTarget
     *            When non-<code>null</code> the new right-sibling (the tail)
     *            will be moved to the specified data service after the split.
     */
    public SplitTailTask(final ResourceManager resourceManager,
            final long lastCommitTime, final String name,
            final ViewMetadata vmd, final UUID moveTarget) {

        super(resourceManager, lastCommitTime, name);

        this.lastCommitTime = lastCommitTime;

        this.vmd = vmd;
        
        if (!vmd.name.equals(name))
            throw new IllegalArgumentException();
        
        if (vmd.pmd == null) {

            throw new IllegalStateException("Not an index partition.");

        }

        if (vmd.pmd.getSourcePartitionId() != -1) {

            throw new IllegalStateException(
                    "Split not allowed during move: sourcePartitionId="
                            + vmd.pmd.getSourcePartitionId());

        }

        this.moveTarget = moveTarget;

        final BTreeCounters counters = vmd.getBTree().btreeCounters;

        this.e = new Event(
                resourceManager.getFederation(),
                vmd.name,
                OverflowActionEnum.TailSplit,
                OverflowActionEnum.TailSplit
                        + "("
                        + name
                        + ") : "
                        + vmd
                        + " : #tailSplit="
                        + counters.tailSplit
                        + ", #leafSplit="
                        + counters.leavesSplit
                        + ", ratio="
                        + (counters.tailSplit / (double) counters.leavesSplit + 1));
        
    }

    @Override
    protected void clearRefs() {

        vmd.clearRef();
        
    }

    @Override
    protected Object doTask() throws Exception {
        
        try {

            final SplitResult result;
            final Split[] splits;
            try {
                
                /*
                 * Split into head (most) and tail. both will be new index
                 * partitions.
                 */

                splits = SplitUtility
                        .tailSplit(resourceManager, vmd.getBTree());

                // validate the splits before processing them.
                SplitUtility.validateSplits(vmd.getBTree(), splits);

                result = SplitUtility.buildSplits(resourceManager, vmd,
                        lastCommitTime, splits);

            } finally {

                /*
                 * We are done building index segments from the source index
                 * partition view so we clear our references for that view.
                 */

                clearRefs();

            }
        
            // Do the atomic update
            SplitIndexPartitionTask.doSplitAtomicUpdate(resourceManager, vmd,
                    splits, result, OverflowActionEnum.TailSplit,
                    resourceManager.indexPartitionTailSplitCounter, e);
            
            if (moveTarget != null) {
             
                // the name of the newly created rightSibling index partition.
                final String rightSiblingName = DataService
                        .getIndexPartitionName(vmd.indexMetadata.getName(),
                                splits[1].pmd.getPartitionId());

                /*
                 * The partitionId that will be used when the rightSibling is
                 * moved to the target data service (moving it will cause a new
                 * index partition to be defined using this partitionId).
                 */
                final int newPartitionId = resourceManager
                        .nextPartitionId(rightSiblingName);

                /*
                 * FIXME Unlike a normal move where there are writes on the old
                 * journal, all the data for the rightSibling is in an index
                 * segment that we just built and new writes MAY be buffered on
                 * the live journal. Therefore we need a different entry point
                 * into the MOVE operation, one which does not copy over the
                 * data from the old journal.
                 */

//                new MoveIndexPartitionTask(resourceManager,
//                        rightSiblingRegisterTime, rightSiblingName,
//                        rightSiblingVMD, moveTarget, newPartitionId).call();

                throw new UnsupportedOperationException();
                
            }
            
            // Done.
            return result;
                        
        } finally {
            
            e.end();
            
        }
        
    }
    
}
