/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.scaleup;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executors;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentMerger;
import com.bigdata.btree.RecordCompressor;
import com.bigdata.btree.IndexSegmentMerger.MergedEntryIterator;
import com.bigdata.btree.IndexSegmentMerger.MergedLeafIterator;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.isolation.Value;
import com.bigdata.rawstore.Bytes;

/**
 * Abstract base class for tasks that build {@link IndexSegment}(s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write test suite for executing partition task schedules.
 * 
 * @todo add a
 *       {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}
 *       that will be used to run these tasks and modify the shutdown logic
 *       for the master to also shutdown that thread.
 * 
 * @todo add result to persistent schedule outcome so that this is restart
 *       safe.
 * 
 * @todo once the {@link IndexSegment} is ready the metadata index needs to
 *       be updated to reflect that the indexsegment is live and the views
 *       that rely on the partition on the old journal need to be
 *       invalidated so new views utilize the new indexsegment rather than
 *       the data on the old journal.
 * 
 * @todo the old journal is not available for release until all partitions
 *       for all indices have been evicted. we need to track that in a
 *       restart safe manner.
 * 
 * @todo parameterize useChecksum, recordCompressor.
 * 
 * @todo try performance with and without checksums and with and without
 *       record compression.
 */
abstract public class AbstractPartitionTask implements
        IPartitionTask {

    protected final MasterJournal master;
    /**
     * Branching factor used for generated {@link IndexSegment}(s). 
     */
    protected final int branchingFactor;
    protected final double errorRate;
    protected final String name;
    protected final UUID indexUUID;
    protected final int partId;
    protected final byte[] fromKey;
    protected final byte[] toKey;

    /**
     * Branching factor used for temporary file(s).
     */
    protected final int tmpFileBranchingFactor = Bytes.kilobyte32*4;

    /**
     * When true, pre-record checksum are generated for the output
     * {@link IndexSegment}.
     */
    protected final boolean useChecksum = false;
    
    /**
     * When non-null, a {@link RecordCompressor} will be applied to the
     * output {@link IndexSegment}.
     */
    protected final RecordCompressor recordCompressor = null;
        
    /**
     * 
     * @param master
     *            The master.
     * @param name
     *            The index name.
     * @param branchingFactor
     *            The branching factor to be used on the new
     *            {@link IndexSegment}(s).
     * @param errorRate
     *            The error rate for the bloom filter for the new
     *            {@link IndexSegment}(s) -or- zero(0d) if no bloom filter
     *            is desired.
     * @param partId
     *            The unique partition identifier for the partition.
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
     * 
     * @todo It is an error to supply a <i>fromKey</i> that is not also the
     *       separatorKey for the partition, however the code does not
     *       detect this error.
     * 
     * @todo It is possible for the partition to be redefined (joined with a
     *       sibling or split into two partitions). In this case any already
     *       scheduled operations MUST abort and new operations with the
     *       correct separator keys must be scheduled.
     */
    public AbstractPartitionTask(MasterJournal master, String name,
            UUID indexUUID, int branchingFactor, double errorRate, int partId,
            byte[] fromKey, byte[] toKey) {

        this.master = master;
        this.branchingFactor = branchingFactor;
        this.errorRate = errorRate;
        this.name = name;
        this.indexUUID = indexUUID;
        this.partId = partId;
        this.fromKey = fromKey;
        this.toKey = toKey;

    }

    /**
     * 
     * @todo update the metadata index (synchronized) and invalidate
     *       existing partitioned index views that depend on the source
     *       index.
     *       
     * @param resources
     */
    protected void updatePartition(IResourceMetadata[] resources) {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Task builds an {@link IndexSegment} for a partition from data on a
     * historical read-only {@link SlaveJournal}. When the {@link IndexSegment}
     * is ready the metadata index is updated to make the segment "live" and
     * existing views are notified that the source data on the partition must be
     * invalidated in favor of the new {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo this must use a merge rule that knows about deletion markers and is
     *       only usable when the input is an {@link UnisolatedBTree}. The
     *       output {@link IndexSegment} will contain timestamps and deletion
     *       markers and support isolation.
     */
    public static class BuildTask extends AbstractPartitionTask {

        private final IResourceMetadata src;
        
        /**
         * 
         * @param src
         *            The source for the build operation. Only those entries in
         *            the described key range will be used.
         */
        public BuildTask(MasterJournal master, String name, UUID indexUUID,
                int branchingFactor, double errorRate, int partId,
                byte[] fromKey, byte[] toKey, IResourceMetadata src) {
            
            super(master, name, indexUUID, branchingFactor, errorRate, partId,
                    fromKey, toKey);
            
            this.src = src;
            
        }
        
        /**
         * Build an {@link IndexSegment} from a key range corresponding to an
         * index partition.
         * 
         * @todo this needs to use the rangeIterator on the root of the btree so
         *       that the {@link Value}s will be visible, including both
         *       version counters and deletion markers.
         */
        public Object call() throws Exception {

            AbstractBTree src = master.getIndex(name,this.src);
            
            File outFile = master.getSegmentFile(name, partId);

            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
                    master.tmpDir, src.rangeCount(fromKey, toKey), src
                            .rangeIterator(fromKey, toKey), branchingFactor,
                    src.getNodeSerializer().getValueSerializer(), useChecksum,
                    recordCompressor, errorRate, indexUUID);

            IResourceMetadata[] resources = new SegmentMetadata[] { new SegmentMetadata(
                    "" + outFile, outFile.length(), ResourceState.New,
                    builder.segmentUUID) };

            updatePartition(resources);
            
            return null;
            
        }
        
    }

    /**
     * Abstract base class for compacting merge tasks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract static class AbstractMergeTask extends AbstractPartitionTask {

        protected final boolean fullCompactingMerge;

        /**
         * A compacting merge of two or more resources.
         * 
         * @param segId
         *            The output segment identifier.
         * 
         * @param fullCompactingMerge
         *            True iff this will be a full compacting merge.
         */
        protected AbstractMergeTask(MasterJournal master, String name,
                UUID indexUUID, int branchingFactor, double errorRate,
                int partId, byte[] fromKey, byte[] toKey,
                boolean fullCompactingMerge) {
            
            super(master, name, indexUUID, branchingFactor, errorRate, partId,
                    fromKey, toKey);
            
            this.fullCompactingMerge = fullCompactingMerge;
            
        }
        
        /**
         * Compacting merge of two or more resources - deletion markers are
         * preserved iff {@link #fullCompactingMerge} is <code>false</code>.
         * 
         * @todo make sure that deletion markers get removed from the view as
         *       part of the index segment merger logic (choose the most recent
         *       write for each key, but if it is a delete then remove the entry
         *       from the merge so that it does not appear in the temporary file
         *       that is the input to the {@link IndexSegmentBuilder}).
         */
        public Object call() throws Exception {
            
            // tmp file for the merge process.
            File tmpFile = File.createTempFile("merge", ".tmp", master.tmpDir);

            tmpFile.deleteOnExit();

            // output file for the merged segment.
            File outFile = master.getSegmentFile(name, partId);

            IResourceMetadata[] resources = getResources();
            
            AbstractBTree[] srcs = new AbstractBTree[resources.length];
            
            for(int i=0; i<srcs.length; i++) {

                // open the index - closed by the weak value cache.
                srcs[i] = master.getIndex(name, resources[i]);
                
            }
            
            final IValueSerializer valSer = srcs[0].getNodeSerializer()
                    .getValueSerializer();
            
            // merge the data from the btree on the slave and the index
            // segment.
            MergedLeafIterator mergeItr = new IndexSegmentMerger(
                    tmpFileBranchingFactor, srcs).merge();
            
            // build the merged index segment.
            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
                    null, mergeItr.nentries, new MergedEntryIterator(mergeItr),
                    branchingFactor, valSer, useChecksum, recordCompressor,
                    errorRate, indexUUID);

            // close the merged leaf iterator (and release its buffer/file).
            // @todo this should be automatic when the iterator is exhausted but
            // I am not seeing that.
            mergeItr.close();

            /*
             * @todo Update the metadata index for this partition. This needs to
             * be an atomic operation so we have to synchronize on the metadata
             * index or simply move the operation into the MetadataIndex API and
             * let it encapsulate the problem.
             * 
             * We mark the earlier index segment as "Dead" for this partition
             * since it has been replaced by the merged result.
             * 
             * Note: it is a good idea to wait until you have opened the merged
             * index segment, and even until it has begun to serve data, before
             * deleting the old index segment that was an input to that merge!
             * The code currently waits until the next time a compacting merge
             * is performed for the partition and then deletes the Dead index
             * segment.
             */

            final MetadataIndex mdi = master.getSlave().getMetadataIndex(name);
            
            final PartitionMetadata pmd = mdi.get(fromKey);

            // #of live segments for this partition.
            final int liveCount = pmd.getLiveCount();
            
            // @todo assuming compacting merge each time.
            if(liveCount!=1) throw new UnsupportedOperationException();
            
            // new segment definitions.
            final IResourceMetadata[] newSegs = new IResourceMetadata[2];

            // assume only the last segment is live.
            final SegmentMetadata oldSeg = (SegmentMetadata)pmd.resources[pmd.resources.length-1];
            
            newSegs[0] = new SegmentMetadata(oldSeg.filename, oldSeg.nbytes,
                    ResourceState.Dead, oldSeg.uuid);

            newSegs[1] = new SegmentMetadata(outFile.toString(), outFile
                    .length(), ResourceState.Live, builder.segmentUUID);
            
            mdi.put(fromKey, new PartitionMetadata(0, pmd.dataServices, newSegs));
            
            return null;
            
        }
    
        /**
         * The resources that comprise the view in reverse timestamp order
         * (increasing age).
         */
        abstract protected IResourceMetadata[] getResources();
        
    }
    
    /**
     * Task builds an {@link IndexSegment} using a compacting merge of two
     * resources having data for the same partition. Common use cases are a
     * journal and an index segment or two index segments. Only the most recent
     * writes will be retained for any given key (duplicate suppression). Since
     * this task does NOT provide a full compacting merge (it may be applied to
     * a subset of the resources required to materialize a view for a commit
     * time), deletion markers MAY be present in the resulting
     * {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MergeTask extends AbstractMergeTask {

        private final IResourceMetadata[] resources;

        /**
         * A compacting merge of two or more resources.
         * 
         * @param srcs
         *            The source resources, which must be given in reverse
         *            timestamp order (increasing age).
         * @param segId
         *            The output segment identifier.
         */
        public MergeTask(MasterJournal master, String name, UUID indexUUID,
                int branchingFactor, double errorRate, int partId,
                byte[] fromKey, byte[] toKey, IResourceMetadata[] resources
                ) {

            super(master, name, indexUUID, branchingFactor, errorRate, partId,
                    fromKey, toKey, false);
            
            this.resources = resources;
            
        }

        protected IResourceMetadata[] getResources() {
            
            return resources;
            
        }
        
    }
    
    /**
     * Task builds an {@link IndexSegment} using a full compacting merge of all
     * resources having data for the same partition as of a specific commit
     * time. Only the most recent writes will be retained for any given key
     * (duplicate suppression). Deletion markers wil NOT be present in the
     * resulting {@link IndexSegment}.
     * <p>
     * Note: A full compacting merge does not necessarily result in only a
     * single {@link IndexSegment} for a partition since (a) it may be requested
     * for a historical commit time; and (b) subsequent writes may have
     * occurred, resulting in additional data on either the journal and/or new
     * {@link IndexSegment} that do not participate in the merge.
     * <p>
     * Note: in order to perform a full compacting merge the task MUST read from
     * all resources required to provide a consistent view for a partition
     * otherwise lost deletes may result.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class FullMergeTask extends AbstractMergeTask {

        private final long commitTime;

        /**
         * A full compacting merge of an index partition.
         * 
         * @param segId
         *            The output segment identifier.
         * 
         * @param commitTime
         *            The commit time for the view that is the input to the
         *            merge operation.
         */
        public FullMergeTask(MasterJournal master, String name, UUID indexUUID,
                int branchingFactor, double errorRate, int partId,
                byte[] fromKey, byte[] toKey, long commitTime) {

            super(master, name, indexUUID, branchingFactor, errorRate, partId,
                    fromKey, toKey, true);

            this.commitTime = commitTime;

        }

        protected IResourceMetadata[] getResources() {
            
            final PartitionedIndexView oldIndex = ((PartitionedIndexView) master
                    .getIndex(name, commitTime));
            
            final IResourceMetadata[] resources = oldIndex.getResources(fromKey);
            
            return resources;

        }
        
    }

//    /**
//     * Task builds an {@link IndexSegment} by joining an existing
//     * {@link IndexSegment} for a partition with an existing
//     * {@link IndexSegment} for either the left or right sibling of that
//     * partition.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class JoinTask implements IPartitionTask {
//        
//    }
//    /**
//     * Task splits an {@link IndexSegment} into two new {@link IndexSegment}s.
//     * This task is executed when a partition is split in order to breakdown the
//     * {@link IndexSegment} for that partition into data for the partition and
//     * its new left/right sibling.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class SplitTask implements IPartitionTask {
//        
//    }
    
//    /**
//     * Update the metadata index to reflect the split of one index segment
//     * into two index segments.
//     * 
//     * @param separatorKey
//     *            Requests greater than or equal to the separatorKey (and
//     *            less than the next largest separatorKey in the metadata
//     *            index) are directed into seg2. Requests less than the
//     *            separatorKey (and greated than any proceeding separatorKey
//     *            in the metadata index) are directed into seg1.
//     * @param seg1
//     *            The metadata for the index segment that was split.
//     * @param seg2
//     *            The metadata for the right sibling of the split index
//     *            segment in terms of the key range of the distributed
//     *            index.
//     */
//    public void split(Object separatorKey, PartitionMetadata md1, PartitionMetadata md2) {
//        
//    }
//
//    /**
//     * @todo join of index segment with left or right sibling. unlike the
//     *       nodes of a btree we merge nodes whenever a segment goes under
//     *       capacity rather than trying to redistribute part of the key
//     *       range from one index segment to another.
//     */
//    public void join() {
//        
//    }

}
