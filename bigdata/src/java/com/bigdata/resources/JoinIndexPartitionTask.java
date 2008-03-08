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

import java.util.Arrays;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.service.DataService;

/**
 * Task joins one or more index partitions and should be invoked when their is
 * strong evidence that the index partitions have shrunk enough to warrant their
 * being combined into a single index partition. The index partitions MUST be
 * partitions of the same scale-out index and MUST be siblings (their left and
 * right separators must cover a continuous interval).
 * <p>
 * The task reads from the lastCommitTime of the old journal and builds a single
 * {@link BTree} from the merged read of the source index partitions as of that
 * timestamp and returns a {@link JoinResult}.
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
     *            The names of the index partitions to be join. These names MUST
     *            be given in natural ordering of the left separator keys for
     *            those index partitions.
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
        
        final String[] names = getResource();
        
        // _clone_ the index metadata for the first of the siblings.
        final IndexMetadata newMetadata = getIndex(names[0]).getIndexMetadata().clone();
        
        if (newMetadata.getPartitionMetadata() == null) {
            
            throw new RuntimeException("Not an index partition: "+names[0]);
            
        }
        
        /*
         * Make a note of the expected left separator for the next partition.
         * The first time through the loop this is just the left separator for
         * the 1st index partition that to be joined.
         * 
         * Note: Do this _before_ we clear the partition metadata.
         */
        byte[] leftSeparator = newMetadata.getPartitionMetadata().getLeftSeparatorKey();
        
        /*
         * clear the partition metadata before we create the index so that it
         * will not report range check errors on the data that we copy in.
         */
        newMetadata.setPartitionMetadata(null);
        
        /*
         * Create B+Tree on which all data will be merged. This B+Tree is
         * created on the _live_ journal. It will be inaccessible to anyone
         * until it is registered. Until then we will just pass along the
         * checkpoint address (obtained below).
         */
        final BTree btree = BTree.create(resourceManager.getLiveJournal(), newMetadata);

        // the partition metadata for each partition that is being merged.
        final LocalPartitionMetadata[] oldpmd = new LocalPartitionMetadata[names.length];
        
        // consider each resource in order.
        for(int i=0; i<names.length; i++) {

            final String name = names[i];
            
            final IIndex src = getIndex(name);

            /*
             * Validate partition of same index
             */
            
            final IndexMetadata sourceIndexMetadata = src.getIndexMetadata();
            
            if(!newMetadata.getIndexUUID().equals(sourceIndexMetadata.getIndexUUID())) {
                
                throw new RuntimeException(
                        "Partition for the wrong index? : names="
                                + Arrays.toString(names));
                
            }
            
            final LocalPartitionMetadata pmd = sourceIndexMetadata.getPartitionMetadata();

            if (pmd == null) {

                throw new RuntimeException("Not an index partition: " + names[i]);

            }

            /*
             * Validate that this is a rightSibling by checking the left
             * separator of the index partition to be joined against the
             * expected left separator.
             */ 
            if (!BytesUtil.bytesEqual(leftSeparator,pmd.getLeftSeparatorKey())) {

                throw new RuntimeException("Partitions out of order: names="
                        + Arrays.toString(names) + ", have="
                        + Arrays.toString(oldpmd) + ", found=" + pmd);
                
            }
            
            oldpmd[i] = pmd;
                        
            /*
             * Copy all data into the new btree.
             */
            
            final long ncopied = btree.rangeCopy(src, null, null);
            
            log.info("Copied " + ncopied + " index entries from " + name);
            
            // the new left separator.
            leftSeparator = pmd.getRightSeparatorKey();
            
        }
        
        /*
         * Set index partition.
         * 
         * Note: A new index partitionId is assigned by the metadata server.
         * 
         * Note: The leftSeparator is the leftSeparator of the first joined
         * index partition and the rightSeparator is the rightSeparator of the
         * last joined index partition.
         * 
         * Note: All data for the new index partition is in this B+Tree which we
         * just created. Therefore only the journal itself gets listed as a
         * resource for the index partition view.
         */
        
        final String scaleOutIndexName = newMetadata.getName();
        
        final int partitionId = resourceManager.getMetadataService().nextPartitionId(scaleOutIndexName);

        newMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                partitionId,//
                oldpmd[0].getLeftSeparatorKey(),//
                oldpmd[names.length-1].getRightSeparatorKey(),//
                new IResourceMetadata[]{//
                    // Note: the live journal.
                    getJournal().getResourceMetadata()//
                },//
                // new history line.
                "join("+Arrays.toString(names)+") "
                ));
        
        /*
         * Set the updated index metadata on the btree (required for it to be
         * available on reload).
         */
        
        btree.setIndexMetadata(newMetadata.clone());
        
        final long checkpointAddr = btree.writeCheckpoint();
        
        /*
         * Note: We pass in the name of the new index partition and the names of
         * the old index partitions. We will need an exclusive lock on all of
         * those resources so that we can register the former and drop the
         * latter in an atomic operation. We will update the metadata index
         * within that atomic operation so that the total change over is atomic.
         */
        return new JoinResult( DataService.getIndexPartitionName(scaleOutIndexName,
                partitionId), newMetadata, checkpointAddr, names );
        
    }
    
}
