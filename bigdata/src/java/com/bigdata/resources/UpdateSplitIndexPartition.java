package com.bigdata.resources;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.Split;

/**
 * An {@link ITx#UNISOLATED} operation that splits the live index using the
 * same {@link Split} points, generating new index partitions with new
 * partition identifiers. The old index partition is deleted as a
 * post-condition. The new index partitions are registered as a
 * post-condition. Any data that was accumulated in the live index on the
 * live journal is copied into the appropriate new {@link BTree} for the new
 * index partition on the live journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateSplitIndexPartition extends AbstractTask {
    
    /**
     * 
     */
    private final ResourceManager resourceManager;

    protected final SplitResult splitResult;
    
    public UpdateSplitIndexPartition(
            ResourceManager resourceManager, IConcurrencyManager concurrencyManager, String resource,
            SplitResult splitResult) {

        super(concurrencyManager, ITx.UNISOLATED, resource);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (splitResult == null)
            throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;

        this.splitResult = splitResult;
        
    }

    @Override
    protected Object doTask() throws Exception {

        // The name of the scale-out index.
        final String scaleOutIndexName = splitResult.indexMetadata.getName();
        
        // the name of the source index.
        final String name = getOnlyResource();
        
        // this is the live journal since this task is unisolated.
        final AbstractJournal journal = getJournal();
        
        /*
         * Note: the source index is the BTree on the live journal that has
         * been absorbing writes since the last overflow.  This is NOT a fused
         * view.  All we are doing is re-distributing the writes onto the new
         * splits of the index partition.
         */
        final BTree src = (BTree) resourceManager.getIndexOnStore(name, ITx.UNISOLATED, journal); 
        
        /*
         * Locators for the new index partitions.
         */

        final LocalPartitionMetadata oldpmd = (LocalPartitionMetadata) src
                .getIndexMetadata().getPartitionMetadata();

        final Split[] splits = splitResult.splits;
        
        final PartitionLocator[] locators = new PartitionLocator[splits.length];

        for (int i = 0; i < splits.length; i++) {

            // new metadata record (cloned).
            final IndexMetadata md = src.getIndexMetadata().clone();

            final LocalPartitionMetadata pmd = (LocalPartitionMetadata) splits[i].pmd;

            assert pmd.getResources() == null : "Not expecting resources for index segment: "
                    + pmd;
            
            /*
             * form locator for the new index partition for this split..
             */
            final PartitionLocator locator = new PartitionLocator(
                    pmd.getPartitionId(),//
                    /*
                     * This is the set of failover services for this index
                     * partition. The first element of the array is always
                     * the data service on which this resource manager is
                     * running. The remainder of elements in the array are
                     * the failover services.
                     * 
                     * @todo The index partition data will be replicated at
                     * the byte image level for the live journal.
                     * 
                     * @todo New index segment resources will be replicated
                     * as well.
                     * 
                     * @todo Once the index partition data is fully
                     * replicated we update the metadata index.
                     */
                    resourceManager.getDataServiceUUIDs(),//
                    pmd.getLeftSeparatorKey(),//
                    pmd.getRightSeparatorKey()//
                    );
            
            locators[i] = locator;
            
            /*
             * Update the view definition.
             */
            md.setPartitionMetadata(new LocalPartitionMetadata(
                    pmd.getPartitionId(),//
                    pmd.getLeftSeparatorKey(),//
                    pmd.getRightSeparatorKey(),//
                    new IResourceMetadata[] {//
                        /*
                         * Resources are (a) the new btree; and (b) the new
                         * index segment.
                         */
                        journal.getResourceMetadata(),
                        splitResult.buildResults[i].segmentMetadata
                    },
                    /* 
                     * Note: history is record of the split.
                     */
                    pmd.getHistory()
                    ));
            
            // create new btree.
            final BTree btree = BTree.create(journal, md);

            // the new partition identifier.
            final int partitionId = pmd.getPartitionId();
            
            // name of the new index partition.
            final String name2 = DataService.getIndexPartitionName(scaleOutIndexName, partitionId);
            
            // register it on the live journal.
            journal.registerIndex(name2, btree);
            
            // lower bound (inclusive) for copy.
            final byte[] fromKey = pmd.getLeftSeparatorKey();
            
            // upper bound (exclusive) for copy.
            final byte[] toKey = pmd.getRightSeparatorKey();
            
            // copy all data in this split from the source index.
            final long ncopied = btree.rangeCopy(src, fromKey, toKey);
            
            log.info("Copied " + ncopied
                    + " index entries from the live index " + name
                    + " onto " + name2);
            
        }

        // drop the source index (the old index partition).
        journal.dropIndex(name);

        /*
         * Notify the metadata service that the index partition has been
         * split.
         * 
         * @todo Is it possible for the dataServiceUUIDs[] that we have
         * locally to be out of sync with the one in the locator on the
         * metadata service? If so, then the metadata service needs to
         * ignore that field when validating the oldLocator that we form
         * below.
         */
        resourceManager.getMetadataService().splitIndexPartition(
                src.getIndexMetadata().getName(),//
                new PartitionLocator(//
                        oldpmd.getPartitionId(), //
                        resourceManager.getDataServiceUUIDs(), //
                        oldpmd.getLeftSeparatorKey(),//
                        oldpmd.getRightSeparatorKey()//
                        ),
                locators);
        
        return null;
        
    }
    
}