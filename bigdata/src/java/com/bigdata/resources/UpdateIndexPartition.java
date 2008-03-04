package com.bigdata.resources;

import java.util.Arrays;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;

/**
 * Task updates the definition of an index partition such that the specified
 * index segment is used in place of any older index segments and any
 * journal last commitTime is less than or equal to the createTime of the
 * new index segment.
 * <p>
 * The use case for this task is that you have just done an overflow on a
 * journal, placing empty indices on the new journal and defining their
 * views to read from the new index and the old journal. Then you built an
 * index segment from the last committed state of the index on the old
 * journal. Finally you use this task to update the view on the new journal
 * such that the index now reads from the new index segment rather than the
 * old journal.
 * <p>
 * Note: this implementation only works with a full compacting merge
 * scenario. It does NOT handle the case when multiple index segments are
 * required to complete the index partition view. the presumption is that
 * the new index segment was built from the fused view as of the last
 * committed state on the old journal, not just from the {@link BTree} on
 * the old journal.
 * <h2>Pre-conditions</h2>
 * 
 * <ol>
 * 
 * <li> The view is comprised of:
 * <ol>
 * 
 * <li>the live journal</li>
 * <li>the previous journal</li>
 * <li>an index segment having data for some times earlier than the old
 * journal (optional) </li>
 * </ol>
 * </li>
 * 
 * <li> The createTime on the live journal MUST be GT the createTime on the
 * previous journal (it MUST be newer).</li>
 * 
 * <li> The createTime of the new index segment MUST be LTE the
 * firstCommitTime on the live journal. (The new index segment should have
 * been built from a view that did not read on the live journal. In fact,
 * the createTime on the new index segment should be exactly the
 * lastCommitTime on the oldJournal.)</li>
 * 
 * <li> The optional index segment in the view MUST have a createTime LTE to
 * the createTime of the previous journal. (That is, it must have been from
 * a prior overflow operation and does not include any data from the prior
 * journal.)
 * </ol>
 * 
 * <h2>Post-conditions</h2>
 * 
 * The view is comprised of:
 * <ol>
 * <li>the live journal</li>
 * <li>the new index segment</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateIndexPartition extends AbstractTask {

    /**
     * 
     */
    private final ResourceManager resourceManager;

    final protected SegmentMetadata segmentMetadata;

    /**
     * @param concurrencyManager
     * @param resource
     * @param resourceManager TODO
     */
    protected UpdateIndexPartition(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, String resource,
            SegmentMetadata segmentMetadata) {

        super(concurrencyManager, ITx.UNISOLATED, resource);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

        this.segmentMetadata = segmentMetadata;

    }

    @Override
    protected Object doTask() throws Exception {

        // the live journal.
        final AbstractJournal journal = getJournal();

        // live index
        final BTree btree = journal.getIndex(getOnlyResource());

        assert btree != null : "Expecting index: "+getOnlyResource();
        
        // clone the current metadata record for the live index.
        final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();

        /*
         * This is the old index partition definition.
         */
        final LocalPartitionMetadata oldpmd = indexMetadata
                .getPartitionMetadata();

        // Check pre-conditions.
        {

            assert oldpmd != null : "Not an index partition: "
                    + getOnlyResource();

            final IResourceMetadata[] oldResources = oldpmd.getResources();

            assert oldResources.length == 2 || oldResources.length == 3 : "Expecting either 2 or 3 resources: "
                    + Arrays.toString(oldResources);

            assert oldResources[0].getUUID().equals(
                    journal.getRootBlockView().getUUID()) : "Expecting live journal to the first resource: "
                    + oldResources;

            // the old journal's resource metadata.
            final IResourceMetadata oldJournalMetadata = oldResources[1];
            assert oldJournalMetadata != null;
            assert oldJournalMetadata instanceof JournalMetadata;

            // live journal must be newer.
            assert journal.getRootBlockView().getCreateTime() > oldJournalMetadata
                    .getCreateTime();

            // new index segment build from a view that did not include data from the live journal.
            assert segmentMetadata.getCreateTime() < journal.getRootBlockView()
                    .getFirstCommitTime();

            if (oldResources.length == 3) {

                // the old index segment's resource metadata.
                final IResourceMetadata oldSegmentMetadata = oldResources[2];
                assert oldSegmentMetadata != null;
                assert oldSegmentMetadata instanceof SegmentMetadata;

                assert oldSegmentMetadata.getCreateTime() <= oldJournalMetadata
                        .getCreateTime();

            }

        }

        // new view definition.
        final IResourceMetadata[] newResources = new IResourceMetadata[] {
                journal.getResourceMetadata(), segmentMetadata };

        // describe the index partition.
        indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(oldpmd
                .getPartitionId(),//
                oldpmd.getLeftSeparatorKey(),//
                oldpmd.getRightSeparatorKey(),//
                newResources //
                ));

        // update the metadata associated with the btree.
        btree.setIndexMetadata(indexMetadata);

        // verify that the btree recognizes that it needs to be checkpointed.
        assert btree.needsCheckpoint();

        return null;

    }

}
