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
 * Created on Feb 21, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.mdi.AbstractPartitionTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.mdi.ResourceState;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.DataService;
import com.bigdata.sparse.SparseRowStore;

/**
 * @todo Work through build, compacting merge, merge+split at the
 *       {@link DataService} layer, get the {@link MetadataIndex} to update and
 *       clients to notice those updates, and then try out on the RDF database.
 *       I can set the split initially in terms of the #of index entries in an
 *       index partition, e.g., 10 or 100M. I can also simplify by only
 *       implementing the compacting merges and splits, leaving joins for later.
 *       I will need some means to choose the host onto which to put an index
 *       partition, but that can be (partitionId MOD nhosts) at first. Likewise
 *       I can defer failover support. I will need to move index partitions
 *       around, at least when new partitions are first created, or they will
 *       just jamb up on the first machine(s). The initial indices can be
 *       distributed onto one host each, so that is 5-12 hosts depending on
 *       whether justifications, full text search, or a quad store is being
 *       used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMergeTasks extends AbstractResourceManagerTestCase {

//    /**
//     * Branching factor used for temporary file(s).
//     */
//    private final int tmpFileBranchingFactor = Bytes.kilobyte32*4;

    /**
     * 
     */
    public TestMergeTasks() {
        super();

    }

    /**
     * @param arg0
     */
    public TestMergeTasks(String arg0) {
        super(arg0);
    }

    /**
     * Test generates an {@link IndexSegment} from (typically) a historical
     * {@link BTree} holding writes not yet in an {@link IndexSegment} and which
     * is part of the view of some index partition. After the operation the new
     * {@link IndexSegment} MAY be used as a replacement for the source
     * {@link BTree} in the index partition view. This change needs to be
     * recorded in the {@link MetadataIndex} before clients will being reading
     * from the new view using the new {@link IndexSegment}.
     * 
     * @todo call this a BuildTask?
     */
    public void test_nonCompactingMerge() {
        
        fail("write tests");
        
    }
    
    /**
     * Test generates an {@link IndexSegment} from a (typically historical)
     * fused view of an index partition. The resulting {@link IndexSegment} is a
     * complete replacement for the historical view but does not possess any
     * deleted index entries. Typically the {@link IndexSegment} will be used to
     * replace the current index partition definition such that the resources
     * that were the inputs to the view from which the {@link IndexSegment} was
     * built are no longer required to read on that view. This change needs to
     * be recorded in the {@link MetadataIndex} before clients will being
     * reading from the new view using the new {@link IndexSegment}.
     * 
     * @throws IOException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     * @todo test variant supporting split where a constained key-range and the
     *       target partition identifier are specified. in this use case we
     *       split the source view into two output views from which we will then
     *       assemble the new index partitions and finally register the split
     *       with the metadata index.  Call this variant a MergeSplitTask since
     *       it needs different inputs to the ctor       
     */
    public void test_compactingMerge() throws IOException, InterruptedException, ExecutionException {

        final ResourceManager rmgr = resourceManager;
        
        final String name = "testIndex";

        /*
         * Register the index.
         */
        final UUID indexUUID = UUID.randomUUID();
        {
        
            IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);
            
            // must be an index partition.
            indexMetadata.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                    0, // partitionId (arbitrary since no metadata index).
                    new UUID[]{UUID.randomUUID()},// data services (arbitrary since not scale-out)
                    new IResourceMetadata[]{resourceManager.getLiveJournal().getResourceMetadata()},
                    new byte[]{}, //leftSeparator
                    null // rightSeparator
                    ));
            
            // submit task to register the index and wait for it to complete.
            concurrencyManager.submit(new RegisterIndexTask(concurrencyManager,name,indexMetadata)).get();
        
        }
        
        /*
         * Populate the index with some data.
         */
        final BTree groundTruth = BTree.create(new SimpleMemoryRawStore(),new IndexMetadata(indexUUID));
        {
            
            final int nentries = 10;
            
            final byte[][] keys = new byte[nentries][];
            final byte[][] vals = new byte[nentries][];

            Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(i);

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i],vals[i]);
                                
            }

            IIndexProcedure proc = BatchInsertConstructor.RETURN_NO_VALUES
                    .newInstance(nentries, 0/* offset */, keys, vals);

            // submit the task and wait for it to complete.
            concurrencyManager.submit(
                    new IndexProcedureTask(concurrencyManager, ITx.UNISOLATED,
                            name, proc)).get();
            
        }

        /*
         * Force overflow causing an empty btree to be created for that index on
         * a new journal and the view definition in the new btree to be updated.
         */
        
        // createTime of the old journal.
        final long createTime0 = rmgr.getLiveJournal().getRootBlockView().getCreateTime();
        // uuid of the old journal.
        final UUID uuid0 = rmgr.getLiveJournal().getRootBlockView().getUUID();
        
        // force overflow onto a new journal.
        rmgr.doOverflow();
        
        // lookup the old journal again using its createTime.
        final AbstractJournal oldJournal = rmgr.getJournal(createTime0);
        assertEquals("uuid",uuid0,oldJournal.getRootBlockView().getUUID());
        assertNotSame("closeTime",0L,oldJournal.getRootBlockView().getCloseTime());
        
        // create time for the new journal.
        final long createTime1 = rmgr.getLiveJournal().getRootBlockView().getCreateTime();
        // uuid of the new journal.
        final UUID uuid1 = rmgr.getLiveJournal().getRootBlockView().getUUID();
        
        // run merge task.
        final SegmentMetadata segmentMetadata;
        {
            
            /*
             * Note: The task start time is a historical read on the final
             * committed state of the old journal. This means that the generated
             * index segment will have a createTime EQ to the lastCommitTime on
             * the old journal. This also means that it will have been generated
             * from a fused view of all data as of the final commit state of the
             * old journal.
             */ 
            final long startTime = -oldJournal.getRootBlockView().getLastCommitTime();
        
            // task to run.
            final AbstractTask task = new MergeTask(concurrencyManager,startTime,name);
            
            // submit task and await result (metadata describing the new index segment).
            segmentMetadata = (SegmentMetadata) concurrencyManager.submit(task).get();
            
            System.err.println(segmentMetadata.toString());

            // verify file exists.
            assertTrue(new File(segmentMetadata.getFile()).exists());
            
            // verify createTime == lastCommitTime on the old journal. 
            assertEquals("createTime", oldJournal.getRootBlockView()
                    .getLastCommitTime(), segmentMetadata.getCreateTime());
            
        }
        
        // verify segment has all data in the groundTruth btree.
        {
            
            IndexSegmentFileStore segStore = new IndexSegmentFileStore(new File(segmentMetadata.getFile()));
            
            IndexSegment seg = segStore.load();
            
            AbstractBTreeTestCase.assertSameBTree(groundTruth, seg);
            
        }

        // run task that re-defines the index partition view.
        {

            AbstractTask task = new UpdateIndexPartition(concurrencyManager,
                    name, segmentMetadata);

            // run task, await completion.
            concurrencyManager.submit(task).get();
            
        }
        
        /*
         * 
         * @todo verify the updated definition of the live view.
         */

        /*
         * verify same data from ground truth and the new view (using btree
         * helper classes for this).
         */
        {
            
            final IIndex actual = localTransactionManager.getIndex(name,
                    ITx.UNISOLATED);
            
            AbstractBTreeTestCase.assertSameBTree(groundTruth, actual);
            
        }

        /* @todo verify that can delete the old journal and still read on the
         * new view against ground truth.
         * 
         * @todo verify a repeat of the operation where we again do an overflow,
         * merge task, and update the view - this checks the path where the view
         * already includes an index segment.
         * 
         * @todo verify that we do not generate a new index segment if there have
         * been no writes on a named index on a given journal.  I need to figure
         * out how to test for that.  This case applies to the resource manager's
         * thread that runs these tasks automatically.
         */

        fail("write tests");

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
    public static class UpdateIndexPartition extends AbstractTask {

        final protected SegmentMetadata segmentMetadata;
        
        /**
         * @param concurrencyManager
         * @param resource
         */
        protected UpdateIndexPartition(IConcurrencyManager concurrencyManager,
                String resource, SegmentMetadata segmentMetadata) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            this.segmentMetadata = segmentMetadata;
            
        }

        @Override
        protected Object doTask() throws Exception {

            // the live journal.
            final AbstractJournal journal = getJournal();
            
            // live index
            final BTree btree = journal.getIndex(getOnlyResource());
            
            // clone the current metadata record for the live index.
            final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();
            
            /*
             * This is the old index partition definition.
             */
            final PartitionMetadataWithSeparatorKeys oldpmd = indexMetadata
                    .getPartitionMetadata();

            // Check pre-conditions.
            {

                assert oldpmd != null : "Not an index partition: "+getOnlyResource();

                final IResourceMetadata[] oldResources = oldpmd.getResources();

                assert oldResources.length == 2 || oldResources.length == 3 : "Expecting either 2 or 3 resources: "
                        + oldResources;

                assert oldResources[0].getUUID().equals(journal
                        .getRootBlockView().getUUID()) : "Expecting live journal to the first resource: "
                        + oldResources;

                // the old journal's resource metadata.
                final IResourceMetadata oldJournalMetadata = oldResources[1];
                assert oldJournalMetadata != null;
                assert oldJournalMetadata instanceof JournalMetadata;
                
                // live journal must be newer.
                assert journal.getRootBlockView().getCreateTime() > oldJournalMetadata.getCreateTime();

                // new index segment build from a view that did not include data from the live journal.
                assert segmentMetadata.getCreateTime() < journal
                        .getRootBlockView().getFirstCommitTime();
                
                if (oldResources.length == 3) {
                    
                    // the old index segment's resource metadata.
                    final IResourceMetadata oldSegmentMetadata = oldResources[2];
                    assert oldSegmentMetadata != null;
                    assert oldSegmentMetadata instanceof SegmentMetadata;

                    assert oldSegmentMetadata.getCreateTime() <= oldJournalMetadata.getCreateTime();
                    
                }
                
            }

            // new view definition.
            final IResourceMetadata[] newResources = new IResourceMetadata[] {
                    journal.getResourceMetadata(),
                    segmentMetadata
            };

            // describe the index partition.
            indexMetadata
                    .setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                            oldpmd.getPartitionId(),//
                            oldpmd.getDataServices(),//
                            newResources,//
                            oldpmd.getLeftSeparatorKey(),//
                            oldpmd.getRightSeparatorKey()//
                    ));
            
            // update the metadata associated with the btree.
            btree.setIndexMetadata(indexMetadata);
            
            // verify that the btree recognizes that it needs to be checkpointed.
            assert btree.needsCheckpoint();
            
            return null;
            
        }
        
    }
    
    /**
     * Task builds an {@link IndexSegment} from an index partition. When the key
     * range is unspecified (null, null) the index segment will contain all data
     * in the source index partition (a full merge). If the index partition is
     * to be split into two index partitions then the key range should be
     * specified as (null, splitKey) for the left sibling and (splitKey, null)
     * for the right sibling. This can be generalized into an N-way split simply
     * by choosing N-1 useful keys.
     * 
     * FIXME Most recent thinking on merge rules - they have to be written
     * differently depending on the nature of the index. (a) The
     * {@link SparseRowStore} puts timestamps in the keys, so you can prune
     * history explicitly each time an index segment is built by copying in only
     * those timestamp-property-values that you want to retain. (b) A fully
     * isolated index uses timestamps on index entries and the transaction
     * manager will apply a policy determining when to delete behind old
     * resources, but only the most current value is ever copied into an index
     * segment. (c) An index with only delete markers supports scale-out and
     * history policies are managed on the resource level, just like a fully
     * isolated index, so only the most recent value is copied into an index
     * segment during a build. A zero history policy can be used for (c) in
     * which case resources that are no longer part of the current view are
     * immediately available for delete (in the case of an index segment; in the
     * case of a journal it is available for delete once no index partition has
     * data on that journal). The file system zones are an application of (c)
     * where the zone (set of index partitions with a common policy) causes
     * resources to be maintained for some amount of time (there is no way to
     * handle "some number of versions" outside of the sparse row store with the
     * timestamps in the keys, just age of the resources). If isolated and
     * unisolated indices will be mixed on a journal then the retention policy
     * for that journal needs to be the maximum (AND) of the recommendations of
     * the individual policies.
     * 
     * @todo A flag may be used to specify that deleted versions should not
     *       appear in the generated view (but note that the transaction manager
     *       or history policy generally will make this decision).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME The choice of keys MUST be informed by metadata ideally attached to
     * the metadata index. Basically, we need an {@link ISplitRule} that "knows"
     * when a key would split an indivisable unit, such as a logical row in a
     * sparse row store. The rule could be more sophisticated and "prefer"
     * certain split points, but there are some split points that are hard
     * constraints. The same "rule" could potentially be used to count logical
     * "rows" for the {@link SparseRowStore} row iterator.
     * 
     * FIXME A merge rule that knows about version timestamps and deletion
     * markers. The output {@link IndexSegment} will contain timestamps and
     * deletion markers and support isolation.
     * 
     * FIXME write a "split" task that finds the split points for an index
     * partition and then runs N merge tasks, one per split point. this is very
     * straight forward. work through simple scenarios such as dynamically
     * splitting the indices for an RDF database and periodically performing
     * compacting merges of the index partitions (there are two reasons to do a
     * compacting merge - one is to move data onto the read-only index segments
     * with their optimized storage and higher branching factors; the other is
     * to purge deleted versions).
     * 
     * @todo test both the split and the merge tasks and get some sense of their
     *       performance, but note the limits on the development platform
     *       (laptop vs server).
     * 
     * @todo Once this is in hand, consider again what it takes to achieve an
     *       "atomic" split or "atomic" merge. For example, if we can take the
     *       "live" btree on the journal and atomically rename it (or otherwise
     *       set up its last commit point as a btree readable by another name)
     *       and describe the index partition as a view including the old "live"
     *       btree and a new "live" btree then we can continue to absorb writes
     *       on the new view while doing the split on the old view. Once the
     *       split completes we can split up any data accumulated on the new
     *       btree among the new index partitions and then atomically delete the
     *       new btree putting the new index partitions into service.
     * 
     * @todo since partition identifiers need to be assigned by the MDI this
     *       implies that all splits or joins must first be pre-declared at the
     *       MDI (but somehow not yet in use) so as to reserve the partition
     *       identifiers.
     * 
     * @todo It is an error to supply a <i>fromKey</i> that is not also the
     *       separatorKey for the partition, however the code does not detect
     *       this error.
     * 
     * @todo It is possible for the partition to be redefined (joined with a
     *       sibling or split into two partitions). In this case any already
     *       scheduled operations MUST abort and new operations with the correct
     *       separator keys must be scheduled.
     * 
     * @todo do I need a full timestamp for the index entries in order to
     *       support history policies based on age? If the timestamp is the time
     *       at which the commit group begins then the timestamps can be
     *       compressed readily using a dictionary since there will tend to be
     *       very few distinct timestamps per leaf.
     */
     public static class MergeTask extends AbstractPartitionTask {

        /**
         * Varient performs a full compacting merge when the index partition is
         * NOT being split. The result is a single {@link IndexSegment}
         * containing all data in the source index partition view.
         */
        public MergeTask(IConcurrencyManager concurrencyManager, long tx,
                String name) {

            super(concurrencyManager, tx, name);
            
        }

        /**
         * Build an {@link IndexSegment} from a key range corresponding to an
         * index partition.
         * 
         * @return The {@link IResourceMetadata} describing the new
         *         {@link IndexSegment}.
         */
        public Object doTask() throws Exception {

            // @todo should probably be from the index metadata.
            final int branchingFactor = 4096;
            
            // The source view.
            final IIndex src = getIndex(getOnlyResource());
            
            // metadata for that index / index partition.
            final IndexMetadata metadata = src.getIndexMetadata();

            /*
             * This MUST be the timestamp of the commit record from for the
             * source view. The startTime specified for the task has exactly the
             * correct semantics since you MUST choose the source view by
             * choosing the startTime!
             */
            final long commitTime = Math.abs( startTime );
            
            // the file to be generated.
            final File outFile = getResourceManager().getIndexSegmentFile(metadata);

            // Note: truncates nentries to int.
            final int nentries = (int) src.rangeCount(null,null);
            
            /*
             * Note: Delete markers are ignored so they will NOT be transferred to
             * the new index segment (compacting merge).
             */
            final IEntryIterator itr = src
                    .rangeIterator(null, null, 0/* capacity */,
                            IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

            // Build index segment.
            final IndexSegmentBuilder builder = new IndexSegmentBuilder(//
                    outFile, //
                    getResourceManager().getTmpDir(), //
                    nentries,//
                    itr, //
                    branchingFactor,//
                    metadata,//
                    commitTime//
            );

            // notify the resource manager so that it can find this file.
            ((ResourceManager)resourceManager).addResource(builder.segmentUUID, outFile);

            /*
             * Describe the index segment.
             */
            return new SegmentMetadata(//
                    "" + outFile, //
                    outFile.length(),//
                    ResourceState.Live,//
                    builder.segmentUUID,//
                    commitTime//
                    );

        }
        
    }

    public static class MergeSplitTask extends AbstractPartitionTask {

        protected final int branchingFactor;
        protected final String name;
        protected final int partId;
        protected final byte[] fromKey;
        protected final byte[] toKey;

        /**
         * Performs a full merge when the index partition is being split
         * and is invoked for each (fromKey,toKey) pair that define the output
         * index partitions. The result is a single index segment containing all
         * the data for the output index partition having the given key range.
         * 
         * @param partId
         *            The unique partition identifier to be assigned to the new
         *            partition.
         * @param branchingFactor
         *            The branching factor for the new {@link IndexSegment}.
         * @param fromKey
         *            The first key that would be accepted into that partition
         *            (aka the separator key for that partition).<br>
         *            Note: Failure to use the actual separatorKeys for the
         *            partition will result in changing the separator key in a
         *            manner that is incoherent! The change arises since the
         *            updates are stored in the metadata index based on the
         *            supplied <i>fromKey</i>.
         * @param toKey
         *            The first key that would NOT be accepted into that
         *            partition (aka the separator key for the right sibling
         *            partition and <code>null</code> iff there is no right
         *            sibling).
         */
        public MergeSplitTask(IConcurrencyManager concurrencyManager, long tx,
                String name, int partId, int branchingFactor, byte[] fromKey,
                byte[] toKey) {

            super(concurrencyManager, tx, name);

            this.name = name;
            this.partId = partId;
            this.branchingFactor = branchingFactor;
            this.fromKey = fromKey;
            this.toKey = toKey;

        }
        
        /**
         * Build an {@link IndexSegment} from a key range corresponding to an
         * index partition.
         */
        public Object doTask() throws Exception {

            // The source view.
            final IIndex src = getIndex(name);
            
            // metadata for that index / index partition.
            final IndexMetadata metadata = src.getIndexMetadata();

            /*
             * @todo review assumptions - this MUST be the timestamp of the
             * commit record from for the source view. In fact, it probably has
             * different semantics and a method should be exposed on
             * AbstractTask to return the necessary timestamp. This value is
             * written onto the generated index segment and MUST correspond to
             * the timestamp associated with the updated partition metadata. So
             * another should for this is to have the caller specify it and to
             * use a historical read when building the index segment.
             */
            final long commitTime = startTime;
            
            // the file to be generated.
            final File outFile = getResourceManager().getIndexSegmentFile(metadata);

            // Note: truncates nentries to int.
            final int nentries = (int) src.rangeCount(fromKey, toKey);
            
            /*
             * Note: If a full compacting merge is to performed then the input
             * is the fused view of an index partition as of some timestamp and
             * the output is an equivalent compact view. Old index entries and
             * delete markers are dropped when building the compact view.
             * 
             * Otherwise delete markers need to be preserved, in which case we
             * MUST choose an iterator that visits delete markers so that they
             * can be propagated to the index segment.
             */
            final IEntryIterator itr = src.rangeIterator(fromKey, toKey,
                    0/* capacity */, IRangeQuery.KEYS | IRangeQuery.VALS
                            | IRangeQuery.DELETED, null/* filter */);

            // Build index segment.
            final IndexSegmentBuilder builder = new IndexSegmentBuilder(//
                    outFile, //
                    getJournal().tmpDir, //
                    nentries,//
                    itr, //
                    branchingFactor,//
                    metadata,//
                    commitTime//
//                    src.getNodeSerializer().getValueSerializer(), 
//                    useChecksum,
//                    src.isIsolatable(),
//                    recordCompressor,
//                    errorRate,
//                    src.getIndexUUID()
                    );

            /*
             * Describe the index segment.
             * 
             * @todo return only the SegmentMetadata?
             */
            final IResourceMetadata[] resources = new SegmentMetadata[] { //
                    
                    new SegmentMetadata(//
                            "" + outFile, //
                            outFile.length(),//
                            ResourceState.New,//
                            builder.segmentUUID,//
                            commitTime
                            )//
                    
            };

            return resources;
            
        }
        
    }

    //    /**
//     * Abstract base class for compacting merge tasks.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    abstract static class AbstractMergeTask extends AbstractPartitionTask {
//
//        protected final boolean fullCompactingMerge;
//
//        /**
//         * A compacting merge of two or more resources.
//         * 
//         * @param segId
//         *            The output segment identifier.
//         * 
//         * @param fullCompactingMerge
//         *            True iff this will be a full compacting merge.
//         */
//        protected AbstractMergeTask(MasterJournal master, String name,
//                UUID indexUUID, int branchingFactor, double errorRate,
//                int partId, byte[] fromKey, byte[] toKey,
//                boolean fullCompactingMerge) {
//            
//            super(master, name, indexUUID, branchingFactor, errorRate, partId,
//                    fromKey, toKey);
//            
//            this.fullCompactingMerge = fullCompactingMerge;
//            
//        }
//        
//        /**
//         * Compacting merge of two or more resources - deletion markers are
//         * preserved iff {@link #fullCompactingMerge} is <code>false</code>.
//         * 
//         * @todo make sure that deletion markers get removed from the view as
//         *       part of the index segment merger logic (choose the most recent
//         *       write for each key, but if it is a delete then remove the entry
//         *       from the merge so that it does not appear in the temporary file
//         *       that is the input to the {@link IndexSegmentBuilder}).
//         */
//        public Object call() throws Exception {
//            
//            // tmp file for the merge process.
//            File tmpFile = File.createTempFile("merge", ".tmp", journal.tmpDir);
//
//            tmpFile.deleteOnExit();
//
//            // output file for the merged segment.
//            File outFile = journal.getSegmentFile(name, partId);
//
//            IResourceMetadata[] resources = getResources();
//            
//            AbstractBTree[] srcs = new AbstractBTree[resources.length];
//            
//            for(int i=0; i<srcs.length; i++) {
//
//                // open the index - closed by the weak value cache.
//                srcs[i] = journal.getIndex(name, resources[i]);
//                
//            }
//            
//            final IValueSerializer valSer = srcs[0].getNodeSerializer()
//                    .getValueSerializer();
//            
//            // merge the data from the btree on the slave and the index
//            // segment.
//            MergedLeafIterator mergeItr = new IndexSegmentMerger(
//                    tmpFileBranchingFactor, srcs).merge();
//            
//            // build the merged index segment.
//            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
//                    null, mergeItr.nentries, new MergedEntryIterator(mergeItr),
//                    branchingFactor, valSer, useChecksum, recordCompressor,
//                    errorRate, indexUUID);
//
//            // close the merged leaf iterator (and release its buffer/file).
//            // @todo this should be automatic when the iterator is exhausted but
//            // I am not seeing that.
//            mergeItr.close();
//
//            /*
//             * @todo Update the metadata index for this partition. This needs to
//             * be an atomic operation so we have to synchronize on the metadata
//             * index or simply move the operation into the MetadataIndex API and
//             * let it encapsulate the problem.
//             * 
//             * We mark the earlier index segment as "Dead" for this partition
//             * since it has been replaced by the merged result.
//             * 
//             * Note: it is a good idea to wait until you have opened the merged
//             * index segment, and even until it has begun to serve data, before
//             * deleting the old index segment that was an input to that merge!
//             * The code currently waits until the next time a compacting merge
//             * is performed for the partition and then deletes the Dead index
//             * segment.
//             */
//
//            final MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
//            
//            final PartitionMetadata pmd = mdi.get(fromKey);
//
//            // #of live segments for this partition.
//            final int liveCount = pmd.getLiveCount();
//            
//            // @todo assuming compacting merge each time.
//            if(liveCount!=1) throw new UnsupportedOperationException();
//            
//            // new segment definitions.
//            final IResourceMetadata[] newSegs = new IResourceMetadata[2];
//
//            // assume only the last segment is live.
//            final SegmentMetadata oldSeg = (SegmentMetadata) pmd.getResources()[pmd
//                    .getResources().length - 1];
//            
//            newSegs[0] = new SegmentMetadata(oldSeg.getFile(), oldSeg.size(),
//                    ResourceState.Dead, oldSeg.getUUID());
//
//            newSegs[1] = new SegmentMetadata(outFile.toString(), outFile
//                    .length(), ResourceState.Live, builder.segmentUUID);
//            
//            mdi.put(fromKey, new PartitionMetadata(0, pmd.getDataServices(), newSegs));
//            
//            return null;
//            
//        }
//    
//        /**
//         * The resources that comprise the view in reverse timestamp order
//         * (increasing age).
//         */
//        abstract protected IResourceMetadata[] getResources();
//        
//    }
//    
//    /**
//     * Task builds an {@link IndexSegment} using a compacting merge of two
//     * resources having data for the same partition. Common use cases are a
//     * journal and an index segment or two index segments. Only the most recent
//     * writes will be retained for any given key (duplicate suppression). Since
//     * this task does NOT provide a full compacting merge (it may be applied to
//     * a subset of the resources required to materialize a view for a commit
//     * time), deletion markers MAY be present in the resulting
//     * {@link IndexSegment}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class MergeTask extends AbstractMergeTask {
//
//        private final IResourceMetadata[] resources;
//
//        /**
//         * A compacting merge of two or more resources.
//         * 
//         * @param srcs
//         *            The source resources, which must be given in reverse
//         *            timestamp order (increasing age).
//         * @param segId
//         *            The output segment identifier.
//         */
//        public MergeTask(MasterJournal master, String name, UUID indexUUID,
//                int branchingFactor, double errorRate, int partId,
//                byte[] fromKey, byte[] toKey, IResourceMetadata[] resources
//                ) {
//
//            super(master, name, indexUUID, branchingFactor, errorRate, partId,
//                    fromKey, toKey, false);
//            
//            this.resources = resources;
//            
//        }
//
//        protected IResourceMetadata[] getResources() {
//            
//            return resources;
//            
//        }
//        
//    }
//    
//    /**
//     * Task builds an {@link IndexSegment} using a full compacting merge of all
//     * resources having data for the same partition as of a specific commit
//     * time. Only the most recent writes will be retained for any given key
//     * (duplicate suppression). Deletion markers wil NOT be present in the
//     * resulting {@link IndexSegment}.
//     * <p>
//     * Note: A full compacting merge does not necessarily result in only a
//     * single {@link IndexSegment} for a partition since (a) it may be requested
//     * for a historical commit time; and (b) subsequent writes may have
//     * occurred, resulting in additional data on either the journal and/or new
//     * {@link IndexSegment} that do not participate in the merge.
//     * <p>
//     * Note: in order to perform a full compacting merge the task MUST read from
//     * all resources required to provide a consistent view for a partition
//     * otherwise lost deletes may result.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class FullMergeTask extends AbstractMergeTask {
//
//        private final long commitTime;
//
//        /**
//         * A full compacting merge of an index partition.
//         * 
//         * @param segId
//         *            The output segment identifier.
//         * 
//         * @param commitTime
//         *            The commit time for the view that is the input to the
//         *            merge operation.
//         */
//        public FullMergeTask(MasterJournal master, String name, UUID indexUUID,
//                int branchingFactor, double errorRate, int partId,
//                byte[] fromKey, byte[] toKey, long commitTime) {
//
//            super(master, name, indexUUID, branchingFactor, errorRate, partId,
//                    fromKey, toKey, true);
//
//            this.commitTime = commitTime;
//
//        }
//
//        protected IResourceMetadata[] getResources() {
//            
//            final PartitionedIndexView oldIndex = ((PartitionedIndexView) journal
//                    .getIndex(name, commitTime));
//            
//            final IResourceMetadata[] resources = oldIndex.getResources(fromKey);
//            
//            return resources;
//
//        }
//        
//    }

}
