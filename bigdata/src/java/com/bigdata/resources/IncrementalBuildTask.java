package com.bigdata.resources;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;

/**
 * Task builds an {@link IndexSegment} from the mutable {@link BTree} for an
 * index partition as of some historical timestamp and then atomically updates
 * the view (aka an incremental build).
 * <p>
 * Note: As its last action, this task submits a
 * {@link AtomicUpdateIncrementalBuildTask} which replaces the view with one
 * defined by the current {@link BTree} on the journal and the newly built
 * {@link IndexSegment}.
 * <p>
 * Note: If the task fails, then the output {@link IndexSegment} will be
 * deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IncrementalBuildTask extends
        AbstractResourceManagerTask<BuildResult> {

    final protected long lastCommitTime;

    final protected File outFile;

    /**
     * 
     * @param resourceManager
     * @param lastCommitTime
     *            The lastCommitTime of the journal whose view of the index
     *            you wish to capture in the generated {@link IndexSegment}.
     * @param name
     *            The name of the index.
     * @param outFile
     *            The file on which the {@link IndexSegment} will be
     *            written.
     */
    public IncrementalBuildTask(final ResourceManager resourceManager,
            final long lastCommitTime, final String name, final File outFile) {

        super(resourceManager, TimestampUtility
                .asHistoricalRead(lastCommitTime), name);

        this.lastCommitTime = lastCommitTime;

        if (outFile == null)
            throw new IllegalArgumentException();

        this.outFile = outFile;

    }

    /**
     * Build an {@link IndexSegment} from the compacting merge of an index
     * partition.
     * 
     * @return The {@link BuildResult}.
     */
    public BuildResult doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();
        
        // the name under which the index partition is registered.
        final String name = getOnlyResource();

        // The source view.
        final BTree src = ((ILocalBTreeView)getIndex(name)).getMutableBTree();

        // The UUID for the scale-out index.
        final UUID indexUUID = src.getIndexMetadata().getIndexUUID();
        
        if (INFO) {

            log.info("src=" + name + ", counter=" + src.getCounter().get()
                    + ", checkpoint=" + src.getCheckpoint());

        }
        
        // Build the index segment.
        final BuildResult result = resourceManager.buildIndexSegment(name, src,
                outFile, false/* compactingMerge */, lastCommitTime,
                null/* fromKey */, null/* toKey */);

        /*
         * @todo error handling should be inside of the atomic update task since
         * it has more visibility into the state changes and when we can no
         * longer delete the new index segment.
         */
        try {
            
            // task will update the index partition view definition.
            final AbstractTask<Void> task = new AtomicUpdateIncrementalBuildTask(
                    resourceManager, concurrencyManager, name, indexUUID,
                    result);

            if (INFO)
                log.info("src=" + name + ", will run atomic update task");

            // submit task and wait for it to complete @todo config timeout?
            concurrencyManager.submit(task).get();

        } catch (Throwable t) {

            // delete the generated index segment.
            resourceManager.deleteResource(result.segmentMetadata.getUUID(),
                    false/* isJournal */);

            // re-throw the exception
            throw new Exception(t);

        }

        return result;

    }

    /**
     * <p>
     * The source is an {@link IndexSegment} that was built from the mutable
     * {@link BTree} associated with the lastCommitTime on old journal of some
     * index partition. What we are doing is replacing the role of that
     * {@link BTree} on the closed out journal with the {@link IndexSegment}.
     * Note that the {@link IndexSegment} contains the same data as the
     * {@link BTree} as of the lastCommitTime. The new view (as defined by this
     * task) will be selected when the desired view is GTE the lastCommitTime.
     * The old view will be used whenever the desired view is LT the
     * lastCommitTime.
     * </p>
     * 
     * <pre>
     * journal A
     * view={A,...}
     * ---- sync overflow begins ----
     * create journal B
     * view={B,A,...}
     * Begin incremental build of segment from A (just the BTree state as identified by the lastCommitTime)
     * ---- sync overflow ends ----
     * ... build continues ...
     * ... writes against view={B,A,...} are written on B.
     * ... index segment S0 complete (based on A).
     * ... 
     * atomic update task runs: view={B,S0,...}
     * ... writes continue.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class AtomicUpdateIncrementalBuildTask extends
            AbstractAtomicUpdateTask<Void> {

        final protected BuildResult buildResult;
        
        /**
         * @param resourceManager 
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicUpdateIncrementalBuildTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult) {

            super(resourceManager, ITx.UNISOLATED, resource, indexUUID);

            if(buildResult == null)
                throw new IllegalArgumentException();

            this.buildResult = buildResult;

            assert resource.equals( buildResult.name );
            
        }

        /**
         * <p>
         * Atomic update.
         * </p>
         * 
         * @return <code>null</code>
         */
        @Override
        protected Void doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

            if (INFO)
                log.info("Begin: name=" + getOnlyResource() + ", newSegment="
                        + segmentMetadata);

            /*
             * Open the unisolated B+Tree on the live journal that is absorbing
             * writes. We are going to update its index metadata.
             * 
             * Note: I am using AbstractTask#getIndex(String name) so that the
             * concurrency control logic will notice the changes to the BTree
             * and cause it to be checkpointed if this task succeeds normally.
             */
            final ILocalBTreeView view = (ILocalBTreeView)getIndex(getOnlyResource());

            // make sure that we are working with the same index.
            assertSameIndex(view.getMutableBTree());
            
            if(view instanceof BTree) {
                
                /*
                 * Note: there is an expectation that this is not a simple BTree
                 * because this the build task is supposed to be invoked after
                 * an overflow event, and that event should have re-defined the
                 * view to include the BTree on the new journal plus the
                 * historical view.
                 * 
                 * One explanation for finding a simple view here is that the
                 * old index was deleted and a new one created in its place. We
                 * check that above.
                 */
                
                throw new RuntimeException("View is only a B+Tree: name="
                        + buildResult.name + ", pmd="
                        + view.getIndexMetadata().getPartitionMetadata());
                
            }
            
            // The live B+Tree.
            final BTree btree = view.getMutableBTree();
            
            if (INFO)
                log.info("src=" + getOnlyResource() + ", counter="
                        + view.getCounter().get() + ", checkpoint="
                        + btree.getCheckpoint());

            assert btree != null : "Expecting index: "+getOnlyResource();
            
            // clone the current metadata record for the live index.
            final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();

            /*
             * This is the index partition definition on the live index - the
             * one that will be replaced with a new view as the result of this
             * atomic update.
             */
            final LocalPartitionMetadata currentpmd = indexMetadata.getPartitionMetadata();

            // Check pre-conditions.
            final IResourceMetadata[] currentResources = currentpmd.getResources();
            {

                if (currentpmd == null) {
                 
                    throw new IllegalStateException("Not an index partition: "
                            + getOnlyResource());
                    
                }

                /*
                 * verify that there are at least two resources in the current
                 * view:
                 * 
                 * 1. currentResources[0] is the mutable BTree on the live
                 * journal
                 * 
                 * 2. currentResources[1] is the mutable BTree on the old
                 * journal (since closed out for writes so it is no longer
                 * mutable).
                 */
                
                if (currentResources.length < 2) {
                    
                    throw new IllegalStateException(
                            "Expecting at least 2 resources in the view: "
                                    + Arrays.toString(currentResources));
                    
                }
                
                if (!currentResources[0].getUUID().equals(
                        getJournal().getRootBlockView().getUUID())) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to be the first resource: "
                                    + currentResources);
                    
                }
                
                /*
                 * verify that the 2nd resource in the view is the BTree on the
                 * old journal [this only verifies that the 2nd resource in the
                 * view is also on a journal].
                 */
                if (!currentResources[1].isJournal()) {
                 
                    throw new IllegalStateException(
                            "Expecting live journal to be the first resource: "
                                    + currentResources);
                    
                }

                /*
                 * verify that the new index segment was built from a view that
                 * did not include data from the live journal.
                 */
                assert segmentMetadata.getCreateTime() < getJournal().getRootBlockView()
                        .getFirstCommitTime();
                
            }

            // new view definition.
            final IResourceMetadata[] newResources;
            {
                
                final List<IResourceMetadata> newView = new LinkedList<IResourceMetadata>();

                // the live journal.
                newView.add(getJournal().getResourceMetadata());

                // the newly built index segment.
                newView.add(segmentMetadata);

                // the rest of the components of the old view.
                for (int i = 2; i < currentResources.length; i++) {

                    newView.add(currentResources[i]);

                }

                newResources = (IResourceMetadata[]) newView
                        .toArray(new IResourceMetadata[] {});

            }

            // describe the index partition.
            indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                    currentpmd.getPartitionId(),//
                    currentpmd.getSourcePartitionId(),//
                    currentpmd.getLeftSeparatorKey(),//
                    currentpmd.getRightSeparatorKey(),//
                    newResources, //
                    currentpmd.getHistory()+
                    "incrementalBuild"//
                    +"(lastCommitTime="+ segmentMetadata.getCreateTime()//
                    +",segment="+ segmentMetadata.getUUID()//
                    +",counter="+btree.getCounter().get()//
                    +",oldResources="+Arrays.toString(currentResources)
                    +") "
                    ));
            
            // update the metadata associated with the btree
            btree.setIndexMetadata(indexMetadata);

            if (INFO)
                log.info("Updated view: name=" + getOnlyResource() + ", pmd="
                        + indexMetadata.getPartitionMetadata()
                        + "\noldResources=" + Arrays.toString(currentResources)
                        + "\nnewResources=" + Arrays.toString(newResources));
            
            /*
             * Verify that the btree recognizes that it needs to be
             * checkpointed.
             * 
             * Note: The atomic commit point is when this task commits.
             */
            assert btree.needsCheckpoint();

            // notify successful index partition build.
            resourceManager.indexPartitionBuildCounter.incrementAndGet();
            
            return null;

        }

    }

}
