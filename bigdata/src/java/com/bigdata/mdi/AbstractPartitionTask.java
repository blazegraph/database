/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.mdi;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.Executors;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rawstore.Bytes;
import com.bigdata.sparse.SparseRowStore;

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
abstract public class AbstractPartitionTask extends AbstractTask {

//    protected final MasterJournal journal;
    
    /**
     * Branching factor used for temporary file(s).
     */
    protected final int tmpFileBranchingFactor = Bytes.kilobyte32*4;

//    /**
//     * When true, pre-record checksum are generated for the output
//     * {@link IndexSegment}.
//     */
//    protected final boolean useChecksum = false;
    
//    /**
//     * When non-null, a {@link RecordCompressor} will be applied to the
//     * output {@link IndexSegment}.
//     */
//    protected final RecordCompressor recordCompressor = null;
        
    /**
     * Return the desired filename for a segment in a partition of a named
     * index.
     * 
     * @param name
     *            The name of the index.
     * @param partId
     *            The unique within index partition identifier - see
     *            {@link PartitionMetadata#partId}.
     * 
     * @todo munge the index name so that we can support unicode index names in
     *       the filesystem.
     * 
     * @todo use leading zero number format for the partitionId and the
     *       segmentId in the filenames.
     */
    protected File getSegmentFile(String name,int partId) {

//        File parent = getPartitionDirectory(name,partId);
        
//        return new File(journal.getFile().getParent(), //
//                "index-" + name + "-" + partId + Options.SEG//
//                );
        
// if(!parent.exists() && !parent.mkdirs()) {
//            
//            throw new RuntimeException("Could not create directory: "+parent);
//            
//        }
//        

        File parent = journal.getFile().getParentFile();
        
        try {

            String basename = "index_" + name + "_" + partId + Options.SEG;
            
            return File.createTempFile(basename, Options.SEG, parent);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
//    /**
//     * The directory in which files for the parition should be located.
//     * 
//     * @param name
//     *            The index name.
//     * @param partId
//     *            The partition identifier.
//     *            
//     * @return The directory in which files for the partition should be placed.
//     */
//    protected File getPartitionDirectory(String name,int partId) {
//        
//        return new File(getIndexDirectory(name),
//                "part" + partId
//                );
//
//    }
//
//    /**
//     * The name of the directory in which the partitions for the named index
//     * should be located.
//     * 
//     * @param name
//     *            The index name.
//     * @return
//     */
//    protected File getIndexDirectory(String name) {
//        
//        return new File(segmentDir, name);
//        
//    }
//    
//    /**
//     * Return a unique name for a new slave journal file.
//     */
//    protected File getNextJournalFile() {
//        
//        final File file;
//        
//        try {
//
//            file = File.createTempFile(basename,Options.JNL, journalDir);
//            
//            if (!file.delete()) {
//
//                throw new AssertionError("Unable to delete new slave file");
//
//            }
//            
//            return file;
//            
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//    }

    /**
     * 
     * @param journal
     *            The master.
     * @param name
     *            The index name.
     */
    public AbstractPartitionTask(Journal journal, String name) {

        /*
         * @todo does not have to be unisolated - we can run against a
         * historical commit record for a btree if we re-define the view
         * appropriately.
         */

        super(journal, ITx.UNISOLATED, false/* readOnly */, name);
        
    }
    
    /**
     * Factory for iterators allowing an application choose both the #of splits
     * and the split points. The implementation should examine the
     * {@link IIndex}, returning a sequence of keys that are acceptable index
     * partition separators for the {@link IIndex}.
     * <p>
     * Various kinds of indices have constraints on the keys that may be used as
     * index partition separators. For example, a {@link SparseRowStore}
     * constrains its index partition separators such that
     * 
     * @see IIndex
     * @see 
     * @see PartitionMetadataWithSeparatorKeys
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface ISplitFactory extends Serializable {
        
        /**
         * Create and return an {@link Iterator} that will return a sequence of
         * zero or more keys that identify the leftSeparator of index partitions
         * to be created from the given index.
         * <p>
         * Note: The factory is given a reference to an {@link IIndex}, which
         * is typically a {@link FusedView} of an index partition. Fused views
         * do NOT implement the {@link AbstractBTree#keyAt(int)} method which
         * relies on a total ordering rather than a merge of total orderings. As
         * a result the {@link Iterator}s returned by the factory typically
         * need to perform a key range scan to identify suitable index partition
         * separators.
         * <p>
         * An index partition separator is a key that forms the
         * <em>leftSeparator</em> key for that index partition. The
         * leftSeparator describes the first key that will enter that index
         * partition. Note that the leftSeparator keys do NOT have to be
         * literally present in the index - they may be (and should be) the
         * shortest prefix that creates the necessary separation between the
         * index partitions.
         * 
         * @param ndx
         *            The index to be split.
         * 
         * @return The iterator that will visit the keys at which the index will
         *         be split into partitions (that is, it will visit the keys to
         *         be used as leftSeparator keys for those index partitions).
         */
        public Iterator<byte[]/*key*/> newInstance(IIndex ndx);
        
    };

    /**
     * Simple rule divides the index into N splits of roughly M index entries.
     * <p>
     * Note: This does not account for deletion markers which can falsely
     * inflate the range count of an index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultSplitFactory implements ISplitFactory {
        
        private int m;
        
        public int getM() {
            
            return m;
            
        }
        
        public DefaultSplitFactory(int m) {

            if (m <= 0)
                throw new IllegalArgumentException();

            this.m = m;
            
        }
        
        public Iterator<byte[]/*key*/> newInstance(IIndex ndx) {
            
            return new DefaultSpliterator(ndx, m);
            
        }
        
    }

    /**
     * Given M, the goal is to divide the index into N partitions of
     * approximately M entries each.
     * 
     * <pre>
     *         
     *         n = round( rangeCount / m ) - the integer #of index partitions to be created.
     *         
     *         m' = round( rangeCount / n ) - the integer #of entries per partition.
     * </pre>
     * 
     * To a first approximation, the splits are every m' index entries.
     * 
     * However, the split rule then needs to examine the actual keys at and
     * around those split points and adjust them as necessary.
     * 
     * The running over/under run in sum(m') is maintained and the suggested
     * split points are adjusted by that over/under run.
     * 
     * Repeat until all split points have been identifed and validated by the
     * split run.
     * 
     * @todo could do special case version when the index is an
     *       {@link AbstractBTree} and {@link AbstractBTree#keyAt(int)} can be
     *       used.
     * 
     * @todo make sure that view of the index partition does not expose keyAt() -
     *       this may break a lot of code assumptions - or at least throws an
     *       exception for it.
     * 
     * FIXME when the index partition is a fused view we do not have a keyAt()
     * implementation. (It's possible that this could be achieved using keyAt()
     * to do a binary search probing on each of the components of that view and
     * computing the rangeCount(null,k) for each key pulled out of the
     * underlying indices, but this seems pretty complex.)
     * 
     * Another approach is a prefix scan. This is nice because it will only
     * touch on the possible index separator keys, but it only works when the
     * split points are constrained. If we allow any key as an index separator
     * key then there is no prefix that we can use. Also, this might not work
     * with the sparse row store since the [schema,primaryKey] are not fixed
     * length fields.
     * 
     * An incremental approach is to consume M index entries halting at the key
     * that forms an acceptable index partition separator closest to M (either
     * before or after). This suggests that we can not pre-compute the split
     * points when the index is a fused view but rather that we have to
     * sequentially process the index deciding as we go where the split points
     * are.
     * 
     * The inputs are then: (a) the #of index entries per partition, m'; (b) the
     * desired size in bytes of each index partition; (c) the split rule which
     * will decide what is an acceptable index partition separator; and (d) an
     * iterator scanning the index partition view.
     * 
     * @todo rule variants that attempt to divide the index partition into equal
     *       size (#of bytes) splits.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultSpliterator implements Iterator<byte[]> {

        private final IIndex ndx;

        private final long rangeCount;

        private final IEntryIterator src;
        
        private final int m;
        
        private final int n;
        
        /**
         * 
         * @param m
         *            The maximum #of index entries in a split.
         */
        public DefaultSpliterator(IIndex ndx, int m) {

            if (ndx == null)
                throw new IllegalArgumentException();
            
            if (m <= 0)
                throw new IllegalArgumentException();

            // the source index.
            this.ndx = ndx;

            // upper bound on #of non-deleted index entries.
            this.rangeCount = ndx.rangeCount(null, null);
            
            // iterator visiting keys for the non-deleted index entries.
            this.src = ndx.rangeIterator(null, null, 0/* capacity */,
                    IRangeQuery.KEYS, null/*filter*/);
            
            // target #of index partitions.
            this.n = Math.round(rangeCount / m);

            // adjusted value for [m]
            this.m = Math.round(rangeCount / n);

        }

        /**
         * Computes the key for the next split point and returns true iff one
         * was found.
         */
        public boolean hasNext() {
            // TODO Auto-generated method stub
            return false;
        }

        /**
         * Returns the key for the next split point, which is computed by
         * {@link #hasNext()}.
         */
        public byte[] next() {
            // TODO Auto-generated method stub
            return null;
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void remove() {

            throw new UnsupportedOperationException();
            
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

        protected final int branchingFactor;
        protected final String name;
        protected final int partId;
        protected final byte[] fromKey;
        protected final byte[] toKey;

        /**
         * Varient performs a full merge when the index partition is NOT being
         * split. The result is a single index segment containing all data in
         * the source index partition view.
         * 
         * @param partId
         *            The unique partition identifier to be assigned to the new
         *            partition.
         * @param branchingFactor
         *            The branching factor for the new {@link IndexSegment}.
         */
        public MergeTask(Journal journal, String name, int partId,
                int branchingFactor) {

            this(journal, name, partId, branchingFactor, null/* fromKey */,
                    null/* toKey */);

        }

        /**
         * Varient performs a full merge when the index partition is being split
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
        public MergeTask(Journal journal, String name, int partId,
                int branchingFactor, byte[] fromKey, byte[] toKey) {

            super(journal, name);

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
            
            // @todo from the mutable btree when the source is a view.
            final IndexMetadata metadata = ((BTree)src).getIndexMetadata();

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
            final File outFile = getSegmentFile(name, partId);

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
                    journal.tmpDir, //
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
////    /**
//
//    In order to make joins atomic, the journal on which the operation is
//    taking place obtains an exclusive write lock source index
//    partition(s).  The data are join and the metadata index is updated
//    while the lock is in place.  After join operation the source index
//    partitions are no longer valid and will be eventually be removed from
//    service.
//
//    @todo This suggests that index partitions must be co-located for a
//    join.
//
//     * @todo join of index segment with left or right sibling. unlike the
//     *       nodes of a btree we merge nodes whenever a segment goes under
//     *       capacity rather than trying to redistribute part of the key
//     *       range from one index segment to another.
//     */
//    public void join() {
//        
//    }

}
