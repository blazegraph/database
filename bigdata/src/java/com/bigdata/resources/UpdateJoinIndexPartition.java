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

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * Task performs an atomic update of the index partition view definitions on the
 * live journal and the {@link MetadataIndex}, thereby putting into effect the
 * changes made by a {@link JoinIndexPartitionTask}.
 * <p>
 * This task obtains an exclusive lock on the new index partition and on all of
 * the index partions on the live journal that are being joined. It then copies
 * all writes absorbed by the index partitions that are being since the overflow
 * onto the new index partition and atomically (a) drops the old index
 * partitions; (b) registers the new index partition; and (c) updates the
 * metadata index to reflect the join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateJoinIndexPartition extends AbstractTask {

    private final ResourceManager resourceManager;
    private final JoinResult result;
    
    /**
     * @param concurrencyManager
     * @param startTime
     * @param resource
     *            All resources (both the new index partition arising from the
     *            join and the old index partitions which have continued to
     *            receive writes that need to be copied into the new index
     *            partition and then dropped).
     * @param result
     */
    public UpdateJoinIndexPartition(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, String[] resource,
            JoinResult result) {
        
        super(concurrencyManager, ITx.UNISOLATED, resource);

        if (resourceManager== null)
            throw new IllegalArgumentException();

        if (result == null)
            throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;
        
        this.result = result;
        
    }

    @Override
    protected Object doTask() throws Exception {
   
        /*
         * Load the btree from the live journal that already contains all data
         * from the source index partitions to the merge as of the
         * lastCommitTime of the old journal.
         * 
         * In order to make this btree complete we will now copy in any writes
         * absorbed by those index partitions now that we have an exclusive lock
         * on everyone on the new journal.
         */ 
        final BTree btree = getJournal().getIndex(result.checkpointAddr); 
        
        assert btree != null;
        
        final String scaleOutIndexName = btree.getIndexMetadata().getName();
        
        final int njoined = result.oldnames.length;
        
        final PartitionLocator[] oldLocators = new PartitionLocator[njoined];
        
        for(int i=0; i<njoined; i++) {
            
            final String name = result.oldnames[i];
            
            IIndex src = getIndex(name);
            
            assert src != null;
            
            // same scale-out index.
            assert btree.getIndexMetadata().getIndexUUID() == src.getIndexMetadata().getIndexUUID();
         
            final LocalPartitionMetadata pmd = src.getIndexMetadata().getPartitionMetadata(); 
            
            oldLocators[i] = new PartitionLocator(
                    pmd.getPartitionId(),
                    resourceManager.getDataServiceUUIDs(),
                    pmd.getLeftSeparatorKey(),
                    pmd.getRightSeparatorKey()
                    );
            
            // copy in all data.
            btree.rangeCopy(src, null, null);
            
            // drop the old index partition.
            getJournal().dropIndex(name);
            
        }
        
        // register the new index partition.
        getJournal().registerIndex(result.name, btree);

        final LocalPartitionMetadata pmd = btree.getIndexMetadata().getPartitionMetadata();
        
        assert pmd != null;
        
        final PartitionLocator newLocator = new PartitionLocator(
                pmd.getPartitionId(),
                resourceManager.getDataServiceUUIDs(),
                pmd.getLeftSeparatorKey(),
                pmd.getRightSeparatorKey()
                );
        
        resourceManager.getMetadataService().joinIndexPartition(scaleOutIndexName, oldLocators, newLocator);
        
        return null;
        
    }

}
