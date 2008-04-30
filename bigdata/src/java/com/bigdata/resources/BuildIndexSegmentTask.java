package com.bigdata.resources;

import java.io.File;

import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;

/**
 * Task builds an {@link IndexSegment} from the fused view of an index partition
 * as of some historical timestamp. This task is typically applied after an
 * {@link IResourceManager#overflow(boolean, boolean)} in order to produce a
 * compact view of the index as of the lastCommitTime on the old journal. As its
 * last action, this task submits a {@link AtomicUpdateBuildIndexSegmentTask}
 * which replaces the view with one defined by the current {@link BTree} on the
 * journal and the newly built {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BuildIndexSegmentTask extends AbstractResourceManagerTask {

    final protected long lastCommitTime;

    final protected File outFile;

    final protected byte[] fromKey;

    final protected byte[] toKey;

    /**
     * 
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     */
    public BuildIndexSegmentTask(ResourceManager resourceManager,
            long lastCommitTime,
            String name, File outFile) {

        this(resourceManager, lastCommitTime, name, 
                outFile, null/* fromKey */, null/* toKey */);

    }

    /**
     * 
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     */
    public BuildIndexSegmentTask(ResourceManager resourceManager,
            long lastCommitTime,
            String name, File outFile, byte[] fromKey, byte[] toKey) {

        super(resourceManager, -lastCommitTime/*historical read*/, name);

        this.lastCommitTime = lastCommitTime;

        if (outFile == null)
            throw new IllegalArgumentException();

        this.outFile = outFile;

        this.fromKey = fromKey;

        this.toKey = toKey;

    }

    /**
     * Build an {@link IndexSegment} from an index partition.
     * 
     * @return The {@link BuildResult}.
     */
    public Object doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();
        
        // the name under which the index partition is registered.
        final String name = getOnlyResource();

        log.info("src="+name);
        
        // The source view.
        final IIndex src = getIndex(name);

//        if (src instanceof BTree) {
//
//            // pre-condition.
//            log.warn("View is only a B+Tree: name=" + name
//                    + ", pmd=" + src.getIndexMetadata().getPartitionMetadata());
//            
//        }
        
        // Build the index segment.
        final BuildResult result = resourceManager.buildIndexSegment(name, src,
                outFile, lastCommitTime, fromKey, toKey);

        // task will update the index partition view definition.
        final AbstractTask task = new AtomicUpdateBuildIndexSegmentTask(resourceManager,
                concurrencyManager, name, result);

        log.info("src="+name+", will run atomic update task");

        // submit task and wait for it to complete @todo config timeout?
        concurrencyManager.submit(task).get();

        return result;

    }

    /**
     * The result of an {@link BuildIndexSegmentTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class BuildResult extends AbstractResult {

        /**
         * The metadata describing the generated {@link IndexSegment}.
         */
        public final SegmentMetadata segmentMetadata;

        /**
         * 
         * @param name
         *            The name under which the processed index partition was
         *            registered (this is typically different from the name of
         *            the scale-out index).
         * @param indexMetadata
         *            The index metadata object for the processed index as of
         *            the timestamp of the view from which the
         *            {@link IndexSegment} was generated.
         * @param segmentMetadata
         *            The metadata describing the generated {@link IndexSegment}.
         */
        public BuildResult(String name, IndexMetadata indexMetadata,
                SegmentMetadata segmentMetadata) {

            super(name, indexMetadata);
            
            if (segmentMetadata == null) {

                throw new IllegalArgumentException();
                
            }

            this.segmentMetadata = segmentMetadata;

        }

        public String toString() {
            
            return "BuildResult{name="+name+"}";
            
        }

    }
    
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
     * <li>the previous journal (optional)</li>
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
     * @todo modify to support view consisting of more than one historical
     *       component so that we can do incremental builds (just the buffered
     *       writes) as well as full builds (the index view). Incremental build
     *       index segments need to be marked as such in the {@link BuildResult}
     *       and would only replace the old journal rather than all historical
     *       entries in the view.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class AtomicUpdateBuildIndexSegmentTask extends AbstractResourceManagerTask {

        final protected BuildResult buildResult;
        
        /**
         * @param resourceManager 
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicUpdateBuildIndexSegmentTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                BuildResult buildResult) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if(buildResult == null) {
                
                throw new IllegalArgumentException();
                
            }

            this.buildResult = buildResult;

            assert resource.equals( buildResult.name );
            
        }

        @Override
        protected Object doTask() throws Exception {

            final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

            log.info("Begin: name="+getOnlyResource()+", newSegment="+segmentMetadata);

            /*
             * Open the unisolated B+Tree on the live journal that is absorbing
             * writes. We are going to update its index metadata.
             * 
             * Note: I am using AbstractTask#getIndex(String name) so that the
             * concurrency control logic will notice the changes to the BTree
             * and cause it to be checkpointed if this task succeeds normally.
             */
            final IIndex view = getIndex(getOnlyResource());

            if(view instanceof BTree) {
                
                /*
                 * Note: there is an expectation that this is not a simple BTree
                 * because this the build task is supposed to be invoked after
                 * an overflow event, and that event should have re-defined the
                 * view to include the BTree on the new journal plus the
                 * historical view.
                 * 
                 * One explanation for finding a simple view here is that the
                 * view was a simple BTree on the old journal and the data was
                 * copied from the old journal into the new journal and then
                 * someone decided to do a build even through a copy had already
                 * been done. However, this is not a very good explanation since
                 * we try to avoid doing a build if we have already done a copy!
                 */
                
                log.warn("View is only a B+Tree: name="
                        + buildResult.name + ", pmd="
                        + view.getIndexMetadata().getPartitionMetadata());
                
            }
            
            // The live B+Tree.
            final BTree btree = (BTree)(view instanceof FusedView?((FusedView)view).getSources()[0]:view);

            assert btree != null : "Expecting index: "+getOnlyResource();
            
            // clone the current metadata record for the live index.
            final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();

            /*
             * This is the old index partition definition.
             */
            final LocalPartitionMetadata oldpmd = indexMetadata.getPartitionMetadata();

            // Check pre-conditions.
            {

                if (oldpmd == null) {
                 
                    throw new IllegalStateException("Not an index partition: "
                            + getOnlyResource());
                    
                }

                final IResourceMetadata[] oldResources = oldpmd.getResources();

                if (!oldResources[0].getUUID().equals(
                        getJournal().getRootBlockView().getUUID())) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to the first resource: "
                                    + oldResources + ", but found "
                                    + oldResources);
                    
                }

                /*
                 * Note: I have commented out a bunch of pre-condition tests that are not 
                 * valid for histories such as:
                 * 
                 * history=create() register(0) split(0) copy(entryCount=314)
                 * 
                 * This case arises when there are not enough index entries written on the
                 * journal after a split to warrant a build so the buffered writes are just
                 * copied to the new journal. The resources in the view are:
                 * 
                 * 1. journal
                 * 2. segment
                 * 
                 * And this update will replace the segment. 
                 */
                        
//                // the old journal's resource metadata.
//                final IResourceMetadata oldJournalMetadata = oldResources[1];
//                assert oldJournalMetadata != null;
//                assert oldJournalMetadata instanceof JournalMetadata : "name="
//                        + getOnlyResource() + ", old pmd=" + oldpmd
//                        + ", segmentMetadata=" + buildResult.segmentMetadata;
    //
//                // live journal must be newer.
//                assert journal.getRootBlockView().getCreateTime() > oldJournalMetadata
//                        .getCreateTime();

                // new index segment build from a view that did not include data from the live journal.
                assert segmentMetadata.getCreateTime() < getJournal().getRootBlockView()
                        .getFirstCommitTime();

//                if (oldResources.length == 3) {
    //
//                    // the old index segment's resource metadata.
//                    final IResourceMetadata oldSegmentMetadata = oldResources[2];
//                    assert oldSegmentMetadata != null;
//                    assert oldSegmentMetadata instanceof SegmentMetadata;
    //
//                    assert oldSegmentMetadata.getCreateTime() <= oldJournalMetadata
//                            .getCreateTime();
    //
//                }

            }

            // new view definition.
            final IResourceMetadata[] newResources = new IResourceMetadata[] {
                    // the live journal.
                    getJournal().getResourceMetadata(),
                    // the newly build index segment.
                    segmentMetadata
                    };

            // describe the index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    oldpmd.getPartitionId(),//
                    oldpmd.getLeftSeparatorKey(),//
                    oldpmd.getRightSeparatorKey(),//
                    newResources, //
                    oldpmd.getHistory()+
                    "replaceHistory(lastCommitTime="
                            + segmentMetadata.getCreateTime() + ",segment="
                            + segmentMetadata.getUUID() + ") "
                    ));

            // update the metadata associated with the btree
            btree.setIndexMetadata(indexMetadata);

            log.info("Updated view: name=" + getOnlyResource() + ", pmd="
                    + indexMetadata.getPartitionMetadata());
            
            /*
             * Verify that the btree recognizes that it needs to be
             * checkpointed.
             * 
             * Note: The atomic commit point is when this task commits.
             */
            assert btree.needsCheckpoint();

            // notify successful index partition build.
            resourceManager.buildCounter.incrementAndGet();
            
            return null;

        }

    }

}
