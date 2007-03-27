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
/*
 * Created on Dec 22, 2006
 */

package com.bigdata.scaleup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.isolation.Value;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.objndx.AbstractBTree;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IFusedView;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.IndexSegmentBuilder;
import com.bigdata.objndx.IndexSegmentFileStore;
import com.bigdata.objndx.IndexSegmentMerger;
import com.bigdata.objndx.IndexSegmentMerger.MergedEntryIterator;
import com.bigdata.objndx.IndexSegmentMerger.MergedLeafIterator;
import com.bigdata.rawstore.Bytes;

/**
 * <p>
 * A store that supports {@link PartitionedIndexView}s.
 * </p>
 * <p>
 * A {@link PartitionedIndexView} is an {@link IIndex} that is dynamically
 * decomposed into key-range partitions. Each partition is defined by a
 * <i>separator key</i>. The separator key is the first key that may be
 * inserted into, or read from, that partition. A total ordering over partitions
 * and their locations is maintained in a {@link MetadataIndex}. An insert or
 * read is directed to the first partition having a separator key greater than
 * or equal to the probe key.
 * </p>
 * <p>
 * An index partition is comprised of a {@link Journal} having a {@link BTree}
 * that buffers writes for that partition and zero or more {@link IndexSegment}s
 * per partition. The relationship between keys and partitions is managed by a
 * {@link MetadataIndex}. Partitions may be multiplexed onto the same
 * {@link Journal} in order to reduce the #of disk files that are being actively
 * written and maximize the use of sequential IO on a given IO channel.
 * </p>
 * <p>
 * All writes bound for any partition of any index are absorbed on the
 * {@link Journal} to which that partition has been assigned. For each
 * {@link PartitionedIndexView}, there is a corresponding {@link BTree} that
 * absorbs writes (including deletes) on the {@link Journal}. The
 * {@link MasterJournal} actually delegates all storage services to a
 * {@link SlaveJournal}. When the {@link SlaveJournal} {@link #overflow()}s,
 * it is frozen and a new {@link SlaveJournal} is deployed to absorb further
 * writes. During this time, reads on the {@link MasterJournal} are served by an
 * {@link IFusedView} of the data on the new {@link SlaveJournal} and the data
 * on the old {@link SlaveJournal}.
 * </p>
 * <p>
 * While reads and writes proceed in the foreground on the new
 * {@link SlaveJournal}, a background thread builds an {@link IndexSegment}
 * from each modified {@link BTree} corresponding to a {@link PartitionedIndexView}.
 * Note that we would be free to delete the old {@link SlaveJournal} and old
 * {@link IndexSegment}s except that concurrent transactions may still need to
 * read from historical states which would be lost by the merge. However,
 * unisolated read and write operations and new transactions immediately begin
 * to use the new {@link IndexSegment}s.
 * </p>
 * <p>
 * If there is an existing {@link IndexSegment} for a partition then a
 * compacting merge MAY be performed. Otherwise a new {@link IndexSegment} is
 * generated, possibly resulting in more than one {@link IndexSegment} for the
 * same partition. Once all data from the {@link SlaveJournal} are stable in the
 * appropriate {@link IndexSegment}s, the old {@link SlaveJournal} is no longer
 * required for unisolated reads or writes or for new transactions (however, it
 * MAY be required to serve existing transactions reading from historical states
 * that are no longer present in the merged index segments). Each
 * {@link IndexSegment} built in this manner contains a snapshot of surviving
 * data written on {@link Journal} for a given partition. If multiple
 * {@link IndexSegment} are allowed to accumulate per partition, then the
 * {@link IndexSegment}s are combined periodically using a compacting merge
 * (responsible for retaining the most recent versions for any key and
 * processing delete markers). The inputs to a compacting merge are then no
 * longer required by unisolated reads or writes but MAY be required to support
 * concurrent transactions reading from historical states not preserved by the
 * merge.
 * </p>
 * <p>
 * An index partition overflows and is split when the total data size of an
 * index partition (estimated as the sum of the {@link IndexSegment}s and the
 * data on the {@link Journal} for that partition) exceeds a threshold.
 * Likewise, index partitions may underflow and be joined. These operations
 * require updates to the {@link MetadataIndex} so that futher requests are
 * directed based on the new partition boundaries.
 * </p>
 * <p>
 * Either once the journal is frozen or periodically during its life we would
 * evict an index to a perfect index segment on disk (write the leaves out in a
 * linear sequence, complete with prior-next references, and build up a perfect
 * index over those leaves - the branching factor here can be much higher in
 * order to reduce the tree depth and optimize IO). The perfect index would have
 * to have "delete" entries to mark deleted keys. Periodically we would merge
 * evicted index segments. The goal is to balance write absorbtion and
 * concurrency control within memory and using pure sequantial IO against 100:1
 * or better data on disk with random seeks for reading any given leaf. The
 * index nodes would be written densely after the leaves such that it would be
 * possible to fully buffer the index nodes with a single sequential IO. A key
 * range scan would likewise be a sequential IO since we would know the start
 * and end leaf for the key range directly from the index. A read would first
 * resolve against the current journal, then the prior journal iff one is in the
 * process of be paritioned out into individual perfect index segments, and then
 * against those index segments in order. A "point" read would therefore be
 * biased to be more efficient for "recently" written data, but reads otherwise
 * must present a merged view of the data in each remaining historical perfect
 * index that covers at least part of the key range. Periodically the index
 * segments could be fully merged, at which point we would no longer have any
 * delete entries in a given perfect index segment.
 * </p>
 * <p>
 * At one extreme there will be one journal per disk and the journal will use
 * the entire disk partition. In a pure write scenario the disk would perform
 * only sequential IO. However, applications will also need to read data from
 * disk. Read and write buffers need to be split. Write buffers are used to
 * defer IOs until large sequential IOs may be realized. Read buffers are used
 * for pre-fetching when the data on disk is much larger than the available RAM
 * and the expectation of reuse is low while the expectation of completing a
 * sequential scan is high. Direct buffering may be used for hot-spots but
 * requires a means to bound the locality of the buffered segment, e.g., by not
 * multiplexing an index segment that will be directly buffered and providing a
 * sufficient resource abundence for high performance.
 * <p>
 * 
 * @todo When you open an index segment the nodes are fully buffered, so that
 *       will absorb an increasing amount of RAM as we get more segments on a
 *       server. The reason to buffer the index segment nodes is that you have
 *       to do only one IO per leaf to read any data that may be in the segment
 *       relevant to a query. In the end both RAM consumption and random seeks
 *       for index segment leaves will probably prove to be the limited factors
 *       on the capacity of a server. We could evict, e.g., close, segments that
 *       are not being queried to help stave off RAM starvation and we could
 *       fully buffer the leaves for very hot index segments to help stave off
 *       IO congestion. I don’t have the data today to estimate the proportion
 *       of an index segment that is the nodes (vs the leaves). It will depend
 *       on the size of the values stored in the leaves, but generally the
 *       leaves are much larger than the nodes of the tree. Beyond that you need
 *       to get into a scale out (aka distributed) architecture.
 * 
 * @todo A scale out implementation must refactor the scale up design into a
 *       client API, protocol, a robust service for index metadata management,
 *       and a distributed protocol for access to the various index partitions.
 * 
 * @todo I've been considering the problem of a distributed index more. A
 *       mutiplexed journal holding segments for one or more clustered indices
 *       (containing anything from database rows to generic objects to terms to
 *       triples) would provide fast write absorption and good read rates
 *       without a secondary read-optimized segment. If this is combined with
 *       pipelined journals for per-index segment redundency (a given segment is
 *       replicated on N=3+ hosts) then we can have both failover and
 *       load-balancing for read-intensive segments.
 *       <p>
 *       Clustered indices support (re-)distribution and redundency since their
 *       rows are their essential data while the index simply provides efficient
 *       access. Therefore the same objects may be written onto multiple
 *       replications of an index range hosted by different journals. The object
 *       data will remain invariant, but the entailed index data will be
 *       different for each journal on which the object is written.
 *       <p>
 *       This requires a variety of metadata for each index segment. If a
 *       segment is defined by an index identifier and a separator key, then the
 *       metadata model needs to identify the master journal for a given segment
 *       (writes are directed to this journal) and the secondary journals for
 *       the segment (the master writes through to the secondary journals using
 *       a pipeline). Likewise, the journal must be able to identify the root
 *       for any given committed state of each index range written on that
 *       journal.
 *       <p>
 *       We need to track write load and read load per index range in the
 *       journal, per journal, per IO channel, per disk, per host, and per
 *       network segment. Write load can be reduced by splitting a segment, by
 *       using a host with faster resources, or by reducing other utilization of
 *       the host. The latter would include the case of preferring reads from a
 *       secondary journal for that index segment. It is an extremely cheap
 *       action to offload readers to a secondary service. Likewise, failover of
 *       the master for an index range is also inexpensive since the data are
 *       already in place on several secondaries,
 *       <p>
 *       A new replication of an index range may be built up gradually, e.g., by
 *       moving a leaf at a time and piplining only the sub-range of the index
 *       range that has been already mirrored. For simplicity, the new copy of
 *       the index range would not be visible in the index metadata until a full
 *       copy of the index range was life. Registration of the copy with the
 *       metadata index is then trivial. Until that time, the metadata index
 *       would only know that a copy was being developed. If one of the existing
 *       replicas is not heavily loaded then it can handle the creation of the
 *       new copy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MasterJournal implements IJournal {

    /**
     * The {@link SlaveJournal} currently serving requests for the master.
     */
    private SlaveJournal slave;

    /**
     * The name of the directory in which the slave files will be created.
     */
    protected final File journalDir;
    
    /**
     * The name of the directory in which the segment files will be created.
     * Segments will be organized in this directory in subdirectories whose name
     * is the index name (munged to a name that is legal for the file system)
     * followed by a subdirectory whose name is the unique partition identifier,
     * followed by a sequence identifier for the segments generated for that
     * partition.
     */
    protected final File segmentDir;
    
    /**
     * The directory that will be used for temporary files generated during bulk
     * index builds.
     */
    protected final File tmpDir;
    
    /**
     * The basename for the slave files.
     * 
     * @todo On restart, the most recent slave file will be loaded. Is that a
     *       wise policy?  Do we want to do an atomic rename after overflow?
     */
    protected final String basename;

    /**
     * The recommened extension for index segment files.
     */
    public static final String SEG = ".seg";

    /**
     * Until a btree has at least this many entries it will simply be migrated
     * to the new slave on {@link #overflow()}. After it reaches this
     * threshold it will be evicted to an {@link IndexSegment} on
     * {@link #overflow()}.
     * 
     * @todo this will probably be restated in terms of estimated bytes required
     *       to store the serialized btree.
     */
    protected final int migrationThreshold;

    public static enum MergePolicy {

        /**
         * Perform a full compacting merge every time a btree is evicted from
         * the journal on overflow.
         */
        CompactingMerge();//("compactingMerge");
        
//        final private String name;
        
        MergePolicy() {
        }
//        MergePolicy(String name) {
//            this.name = name;
//        }
        
/*        public String toString() {
            
            return name;
            
        }
*/        
//        public static MergePolicy valueOf(String name) {
//
//            if (name == null)
//                throw new IllegalArgumentException();
//
//            if (name.equals("compactingMerge"))
//                return MergePolicy.CompactingMerge;
//
//            throw new IllegalArgumentException(name);
//
//        }
        
    }

    protected final MergePolicy mergePolicy;
    
    /**
     * Options for the {@link MasterJournal}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo support the use of temporary files for the partitioned journal and
     *       their clean removal on exit.
     */
    public static class Options extends com.bigdata.journal.Options {

        /**
         * <code>basename</code> - The property whose value is the basename of
         * journal files that will be created (no default).
         */
        public static final String BASENAME = "basename";
        
        /**
         * <code>journal.dir</code> - The property whose value is the name of
         * the directory in which new slave files will be created. When not
         * specified the default is a directory named by the {@link #BASENAME}
         * property in the current directory.
         */
        public static final String JOURNAL_DIR = "journal.dir";

        /**
         * <code>segment.dir</code> - The property whose value is the name of
         * the top level directory beneath which new segment files will be
         * created. When not specified the default is a directory named "<i>segs"
         * in the directory named by the {@link #JOURNAL_DIR} property.
         */
        public static final String SEGMENT_DIR = "segment.dir";

        /**
         * <code>migrationThreshold</code> - The name of a property whose
         * value is the minimum #of entries in a btree before the btree will be
         * evicted to to an {@link IndexSegment} on
         * {@link MasterJournal#overflow()}.  A btree with fewer entries is
         * simply copied onto the new slave.
         */
        public static final String MIGRATION_THRESHOLD = "migrationThreshold";
        
        /**
         * <code>mergePolicy</code> - Only "compactingMerge" is defined at
         * this time.
         */
        public static final String MERGE_POLICY = "mergePolicy";
        
    }
    
    public SlaveJournal getSlave() {
        
        return slave;
        
    }
    
    /**
     * Create a new slave capable of supporting partitioned indices. 
     */
    public MasterJournal(Properties properties) {
        
        String val;
        int migrationThreshold = 1000; // default.
        MergePolicy mergePolicy = MergePolicy.CompactingMerge;

        // "basename"
        this.basename = properties.getProperty(Options.BASENAME);
        
        if(basename==null) {
            
            throw new IllegalArgumentException("Required property: "
                    + Options.BASENAME);
            
        }

        // "journal.dir"
        val = properties.getProperty(Options.JOURNAL_DIR);
        
        journalDir = val == null?new File(basename):new File(val); 

        if(journalDir.exists() && !journalDir.isDirectory()) {
            
            throw new RuntimeException("Regular file exists with that name: "
                    + journalDir.getAbsolutePath());
            
        }
        
        if (!journalDir.exists()) {
            
            if (!journalDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + journalDir.getAbsolutePath());
                
            }
            
        }
        
        // "segment.dir"
        val = properties.getProperty(Options.SEGMENT_DIR);
        
        segmentDir = val == null?new File(journalDir,"segs"):new File(val);

        if(segmentDir.exists() && !segmentDir.isDirectory()) {
            
            throw new RuntimeException("Regular file exists with that name: "
                    + segmentDir.getAbsolutePath());
            
        }

        if (!segmentDir.exists()) {
            
            if (!segmentDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + segmentDir.getAbsolutePath());
                
            }
            
        }
        
        /*
         * Scan the slave directory for basename*.jnl. Open the most current
         * file found in that directory. If no file is found, then create one.
         */
        
        File[] journalFiles = new NameAndExtensionFilter(new File(journalDir,
                basename).toString(), Options.JNL).getFiles();
        
        final File file;
        
        if(journalFiles.length==0) {
        
            file = new File(journalDir,basename+Options.JNL);
            
        } else if(journalFiles.length==1) {

            file = journalFiles[0];
            
        } else {
            
            // @todo Choose the most recent file.
            throw new UnsupportedOperationException();
            
        }
        
        System.err.println("file: "+file);
        properties.setProperty(Options.FILE, file.toString());

        /*
         * "migrationThreshold"
         */
        val = properties.getProperty(Options.MIGRATION_THRESHOLD);
        
        if( val != null) {

            migrationThreshold = Integer.parseInt(val);
            
            if (migrationThreshold < 0)
                throw new RuntimeException(Options.MIGRATION_THRESHOLD
                        + " is negative");
         
        }
        this.migrationThreshold = migrationThreshold;

        /*
         * "mergePolicy"
         */
        
        val = properties.getProperty(Options.MERGE_POLICY);
        
        if( val != null ) {
            
            mergePolicy = MergePolicy.valueOf(val);
            
        }
        
        this.mergePolicy = mergePolicy;
        
        /* 
         * Create the initial slave slave.
         */
        this.slave = createSlave(this,properties);

        this.tmpDir = slave.tmpDir;

        /*
         * Setup the weak value cache for index segments.
         */
        resourceCache = new WeakValueCache<String, IndexSegment>(
                new LRUCache<String, IndexSegment>(INDEX_SEGMENT_LRU_CAPACITY));
            
    }
    
    /**
     * Create a new slave journal.
     * <p>
     * Note: You MUST override this method if you subclass the
     * {@link SlaveJournal}.
     * 
     * @param master
     * @param properties
     * @return
     */
    protected SlaveJournal createSlave(MasterJournal master, Properties properties ) {
        
        return new SlaveJournal(this,properties);
        
    }
    
    /**
     * Overflow triggers an operation in which the writes absorbed for the named
     * indices are expunged onto {@link IndexSegment}s.
     * </p>
     * <p>
     * Design issues:
     * <ol>
     * <li>we need a place to store the metadata index. if this is the same
     * Journal on which we are storing normal writes, then we need to copy the
     * metadata index into the new slave or evict it to a segment just like the
     * data indices. </li>
     * <li> writes on a partitioned btree just go into the corresponding btree
     * on the slave (assuming one slave for all partitions, e.g., scale up but
     * not scale out). </li>
     * <li> reads on a partitioned btree must read a fused view over (a) the
     * partitions relevant to the query; and (b) the mutable btree on the slave
     * and any index segment(s) for that parition.</li>
     * </ol>
     * </p>
     * <p>
     * Design one:
     * <ol>
     * <li> Implement the same interfaces as Journal, but delegate to a Journal.
     * </li>
     * <li>Initialize the metadata index with a single partition for each named
     * btree that is registered on the Journal. </li>
     * <li> Detect overflow only at commit and extend the slave as necessary
     * until the next commit. </li>
     * <li> On overflow, open a Journal using the same strategy, copy the
     * metadata index over into the new slave. Synchronously, evict all
     * partitions written on for all btrees written on in the old slave onto
     * index segments. If there is an existing index segment, then use a
     * compacting merge. Finally, update the delegation logic to use a fused
     * view for reads against each partition of each btree. The inputs to that
     * fused view are the named mutable btree on new slave and the corresponding
     * index segment.</li>
     * </ol>
     * </p>
     * <p>
     * The above does not introduce new partitions. It should examine the #of
     * entries in a btree and estimate the size per entry based on the
     * historically evicted segment(s) for that btree. If the size per entry
     * times the expected number of entries per-compacting merge would exceed
     * 200M the choose a separator key from keyAt(indexOf(nentries/2)) on either
     * the mutable btree or the index segment, which ever has more data.
     * </p>
     * 
     * FIXME handle the {@link CommitRecordIndex} during overflow. This can
     * either be handled like the metadata indices (with a secondary index) or
     * by extending the {@link CommitRecordIndex} so as to carry more
     * information about {@link SlaveJournal} locations for commit records.
     * 
     * @todo implement asynchronous overflow.
     * @todo implement using compacting merges only.
     * @todo implement using non-compacting merges only.
     * @todo implement strategy that trades off the use of compacting and
     *       non-compacting merges.
     * 
     * @todo Support dynamic split/join of partitions.
     * 
     * @todo Regardless of whether one or many partitioned trees are on the
     *       slave then an export of that tree into an index segment will result
     *       in an mostly random read over the address space since the order
     *       which nodes and leaves are written onto the slave does not
     *       correspond directly to the total ordering of the btree. if the
     *       slave is in disk-only mode then it makes sense to slurp the entire
     *       disk file into a buffer during the export process to reduce
     *       multiple disk seeks with a single sequential read. the buffer can
     *       be discarded as soon as the btrees have been exported onto their
     *       various segment(s).
     * 
     * @todo (javadoc) Keys in the metadata index are updated as index
     *       partitions are split (or joined). Splits are performed when the
     *       fully compacted partition exceeds ~200M. Joins of index segments
     *       are performed when sibling partitions could be merged to produce a
     *       partition of ~100M. Splitting and joining partitions is atomic and
     *       respects transaction isolation.
     */
    public void overflow() {
               
        /* 
         * Create the new buffer.
         */
        
        Properties properties = slave.getProperties();

        if( slave.isStable() ) {
            
            /*
             * Override the filename with a unique filename based on the
             * original filename.
             */
            properties.setProperty(Options.FILE, getNextJournalFile()
                    .toString());
            
        }
        
        // The new slave.
        SlaveJournal newJournal = createSlave(this,properties);

        // The slave that overflowed.
        final SlaveJournal oldJournal = slave;
        
        synchronousOverflow(oldJournal, newJournal);
        
        /*
         * replace the old buffer with the new buffer and delete the old buffer.
         * 
         * @todo consider scheduling the old buffer for deletion instead so that
         * we have some time to verify that the new index segments are good
         * before deleting the buffered writes.  We are already doing this with
         * the old segments.
         */
        
        // atomic commit makes these changes restart safe
        newJournal.commit();

        // Change to the new journal.
        slave = newJournal;

        // immediate shutdown of the old journal.
        oldJournal.closeAndDelete();
        
    }

    /**
     * Does not queue indices for eviction, forcing the old journal to remain
     * active. The old journal is re-opened using {@link BufferMode#Disk} in
     * order to reduce the amount of RAM required to buffer the data. This
     * option is conceptually the simplest. It has the advantage of never
     * discarding historical commit states, and the corresponding disadvantage
     * of never throwing away old data that is no longer reachable from the
     * current committed state. Old journals can be distributed, just like index
     * segments, but they are not optimized for read performance and they
     * multiplex together multiple indices using smaller branching factors.
     */
    protected void noSegmentsOverflow() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Creates an asynchronous task that will evict data onto index segments.
     * For each named index for which the journal has absorbed writes, the task
     * is responsible for identifying the index partitions that were written on
     * and queuing up (in the index key order) tasks to build/merge the data on
     * the journal for each partition onto a new index segment.
     * <p>
     * While these tasks can be run in parallel either for the same index or for
     * different indices, parallism could impose a significant resource burden
     * and a single worker thread is probably sufficient to keep pace with
     * writes absorbed on the new journal.
     * <p>
     * The schedule of tasks to be executed needs to be restart safe so that all
     * tasks will run eventually. This is most easily accomplished by
     * representing the schedule on a btree that is migrated from
     * {@link SlaveJournal} to {@link SlaveJournal} on {@link #overflow()}.
     * Writes on that btree MUST be synchronized since concurrent worker threads
     * could update the schedule state concurrently and an {@link #overflow()}
     * event could be concurrent with the execution of a worker thread.
     * 
     * @todo consider making the touched (written on) partitions part of the
     *       persistent state of the {@link UnisolatedBTree} so that we do not
     *       need to explicitly search the key space of the btree and compare it
     *       with the key space of the partitions.
     */
    protected void asynchronousOverflow() {

        throw new UnsupportedOperationException();

    }
    
    /**
     * Synchronously evicts all data onto {@link IndexSegment}s.
     * 
     * @todo rewrite this in terms of {@link IPartitionTask}s that get executed
     *       synchronously.
     */
    protected void synchronousOverflow(SlaveJournal oldJournal,
            SlaveJournal newJournal) {
        
        /*
         * We need to consider each named btree registered on this store. We can
         * not consider only those btrees which have been touched since the
         * store was last opened since there may be multiple restarts before the
         * slave overflows.
         */
        Iterator itr = oldJournal.name2MetadataAddr.entryIterator();
        
        while(itr.hasNext()) {

            /*
             * The name of a PartitionedIndexView.
             */
            final String name = ((Entry) itr.next()).name;
            
            /*
             * The named PartitionedIndexView.
             */
            final PartitionedIndexView oldIndex = ((PartitionedIndexView) getIndex(name));
            
            /*
             * The mutable btree on the slave for the named PartitionedIndexView.
             */
            final BTree oldBTree = oldIndex.btree;

            /*
             * The metadata index describing the partitions for the named
             * PartitionedIndexView.
             */
            final MetadataIndex mdi = oldJournal.getMetadataIndex(name);

            /*
             * Examine the btree for buffered writes.
             */
            
            final int entryCount = oldBTree.getEntryCount();

            /*
             * Create empty btree on the new slave. This preserves the type of
             * the btree (plain vs supporting isolation).
             */
            final BTree newBTree = (oldBTree instanceof UnisolatedBTree ? new UnisolatedBTree(
                    newJournal, oldBTree.getBranchingFactor(), oldBTree
                            .getIndexUUID())
                    : new BTree(newJournal, oldBTree.getBranchingFactor(),
                            oldBTree.getIndexUUID(), oldBTree
                                    .getNodeSerializer().getValueSerializer()));

            // Register the btree under the same name on the new slave.
            newJournal.registerIndex(name, newBTree);

            if (entryCount == 0) {
                
                /*
                 * NOP: we just created a new btree by that name on the new
                 * buffer.
                 */
                
            } else if (entryCount < migrationThreshold) {
            
                /*
                 * There are only a few entries so just copy the btree onto the
                 * new buffer.
                 */
                
                newBTree.addAll(oldBTree);
                
            } else {
            
                /*
                 * Since the btree has more than a "few" entries we evict
                 * entries in the old btree onto an index segment.
                 */

                try {
                    
                    evict(name,oldIndex,mdi);
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }

            /*
             * Force all views to be discarded.
             * 
             * @todo (We should simply invalidate each resource for a partition
             * as the partition task completes so that this only happens
             * incrementally). For a compacting merge this is the right answer
             * since we will not use the old segments again. However, if this is
             * not a compacting merge then we only want to close those index
             * segments that will not participate going forward. Since the
             * partitioned index itself will be re-created for the new slave
             * journal, we will need a cache on the master journal that is used
             * to retain the still valid open index segments across an overflow.
             * This is important because opening an index segment has
             * significant latency (fix this using a weak value cache for open
             * indices).
             */
//            oldIndex.closeViews();
            
            /*
             * copy the metadata index onto the new buffer.
             * 
             * @todo eventually the metadata indices themselves will need to be
             * saved out onto index segments or the buffer will fill up with
             * just metadata. for the moment, just make sure that there is at
             * least INITIAL_EXTENT capacity before the buffer would overflow
             * again.
             */
            newJournal.getMetadataIndex(name).addAll(mdi);
            
        }
        
    }
    
    /**
     * Evicts all writes buffered on a btree onto index segments for the
     * partitions spanned by the keys in the btree.
     * <p>
     * This implementation assumes a synchronous compacting merge of all btrees
     * on the slave. All existing index segments are closed as they are
     * processed but the new index segments are NOT opened until they are
     * needed. The old index segments are immediately marked as
     * {@link ResourceState#Dead} in the {@link SegmentMetadata} and the new
     * index segments are marked as {@link ResourceState#Live}.
     * 
     * @todo this does not handle multiple partitions.
     * 
     * @param name
     *            The name of the index.
     * @param oldIndex
     *            The partitioned index that is being evicted.
     * @param mdi
     *            The metadata index defining the partitions for the btree.
     */
    protected void evict(String name, PartitionedIndexView oldIndex,
            MetadataIndex mdi) throws IOException {

        /*
         * The branching factor for the generated index segments @todo refactor
         * into a per-index name Option.
         */
        final int mseg = Bytes.kilobyte32 * 4;

        /*
         * The branching factor used by the compacting merge on its temporary
         * file.
         */
        final int mseg2 = Bytes.kilobyte32 * 4;

        if (mdi.getEntryCount() != 1) {

            // @todo handle multiple partitions.

            throw new UnsupportedOperationException();
            
        }
        
        final byte[] separatorKey = new byte[] {};
        
        final PartitionMetadata pmd = mdi.get(separatorKey);
        
        // the next segment identifier to be assigned.
        final int segId = pmd.nextSegId;
        
        if (pmd.getLiveCount()==0) {

            /*
             * Evict the btree into an index segment.
             * 
             * This control path is used when there is no pre-existing index
             * segment for a partition and the only data is in the mutable btree
             * on the slave.
             * 
             * Note: We use the entry iterator on the root of the btree so that
             * we will see IValue objects when processing an UnisolatedBTree.
             */

            File outFile = getSegmentFile(name,pmd.partId,segId);

            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
                    tmpDir, oldIndex.btree.getEntryCount(), oldIndex.btree
                            .getRoot().entryIterator(), mseg,
                    Value.Serializer.INSTANCE, true/* useChecksum */,
                    null/* new RecordCompressor() */, 0d, oldIndex.btree
                            .getIndexUUID());

            /*
             * update the metadata index for this partition.
             */
            mdi.put(separatorKey, new PartitionMetadata(0, segId + 1,
                    new SegmentMetadata[] { new SegmentMetadata("" + outFile,
                            outFile.length(), ResourceState.Live,
                            builder.segmentUUID) }));

// /*
// * open and verify the index segment against the btree data.
//             */
//            seg = new IndexSegment(new IndexSegmentFileStore(outFile01), btree
//                    .getNodeSerializer().getValueSerializer());

        } else {

            /*
             * Evict the merge of the mutable btree on the slave and the
             * existing index segment into another index segment.
             */

            System.err.println("Evicting and merging with existing segment.");

            final IFusedView view = (IFusedView)oldIndex.getView(separatorKey);
            
            // @todo assumes only one live index segment for the partition.
            final IndexSegment seg = (IndexSegment)view.getSources()[1];

            // output file for the merged segment.
            File outFile = getSegmentFile(name, pmd.partId, segId);

            // merge the data from the btree on the slave and the index
            // segment.
            MergedLeafIterator mergeItr = new IndexSegmentMerger(mseg2,
                    oldIndex.btree, seg).merge();

            // build the merged index segment.
            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
                    null, mergeItr.nentries, new MergedEntryIterator(mergeItr),
                    mseg, oldIndex.btree.getNodeSerializer()
                            .getValueSerializer(), false/* useChecksum */,
                    null/* recordCompressor */, 0d/* errorRate */,
                    oldIndex.btree.getIndexUUID());

            // close the merged leaf iterator (and release its buffer/file).
            // @todo this should be automatic when the iterator is exhausted but
            // I am not seeing that.
            mergeItr.close();

            /*
             * Update the metadata index for this partition.
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

            // #of live segments for this partition.
            final int liveCount = pmd.getLiveCount();
            
            // @todo assuming compacting merge each time.
            if(liveCount!=1) throw new UnsupportedOperationException();
            
            // new segment definitions.
            final SegmentMetadata[] newSegs = new SegmentMetadata[2];

            // assume only the last segment is live.
            final SegmentMetadata oldSeg = pmd.segs[pmd.segs.length-1];
            
            newSegs[0] = new SegmentMetadata(oldSeg.filename, oldSeg.nbytes,
                    ResourceState.Dead, oldSeg.uuid);

            newSegs[1] = new SegmentMetadata(outFile.toString(), outFile
                    .length(), ResourceState.Live, builder.segmentUUID);

            mdi.put(separatorKey, new PartitionMetadata(0, segId + 1, newSegs));

// /*
// * open the merged index segment
//             */
//            IndexSegment seg02 = new IndexSegment(new IndexSegmentFileStore(
//                    outFile02), btree.getNodeSerializer().getValueSerializer());

            /* 
             * Note: I am closing the old segments pmce the entire overflow is done
             * in synchronousOverflow() for now.
             */
//            /*
//             * Close the old index segment. We have marked it as Dead and it will
//             * be eventually deleted.
//             */
//            seg.close();

            // assuming at most one dead and one live segment.
            if(pmd.segs.length>1) {
                
                final SegmentMetadata deadSeg = pmd.segs[0];
                
                if(deadSeg.state!=ResourceState.Dead) {
                    
                    throw new AssertionError();
                    
                }
                
                File deadSegFile = new File(deadSeg.filename);
                
                if(deadSegFile.exists() && !deadSegFile.delete() ) {
                    
                    /* @todo should probably notify someone.  the most likely
                     * explanation is that we have a lock on the file which 
                     * means that we failed to close the dead index segment.
                     */
                    Journal.log.warn("Could not delete dead index segment: "
                            + deadSegFile.getAbsolutePath());
                    
                }
                
            }
//            new File(oldpart.segs[0].filename).delete();
//
//            // this is now the current index segment.
//            seg = seg02;

        }

    }

    /**
     * Return the desired filename for a segment in a partition of a named
     * index.
     * 
     * @param name
     *            The name of the index.
     * @param partId
     *            The unique within index partition identifier - see
     *            {@link PartitionMetadata#partId}.
     * @param segId
     *            The unique within partition segment identifier.
     * 
     * @todo munge the index name so that we can support unicode index names in
     *       the filesystem.
     * 
     * @todo use leading zero number format for the partitionId and the
     *       segmentId in the filenames.
     */
    protected File getSegmentFile(String name,int partId,int segId) {

        File parent = getPartitionDirectory(name,partId);
        
        if(!parent.exists() && !parent.mkdirs()) {
            
            throw new RuntimeException("Could not create directory: "+parent);
            
        }
        
        File file = new File(parent, segId + SEG);
        
        return file;
        
    }
    
    /**
     * The directory in which files for the parition should be located.
     * 
     * @param name
     *            The index name.
     * @param partId
     *            The partition identifier.
     *            
     * @return The directory in which files for the partition should be placed.
     */
    protected File getPartitionDirectory(String name,int partId) {
        
        return new File(getIndexDirectory(name),
                "part" + partId
                );

    }

    /**
     * The name of the directory in which the partitions for the named index
     * should be located.
     * 
     * @param name
     *            The index name.
     * @return
     */
    protected File getIndexDirectory(String name) {
        
        return new File(segmentDir, name);
        
    }
    
    /**
     * Return a unique name for a new slave journal file.
     */
    protected File getNextJournalFile() {
        
        final File file;
        
        try {

            file = File.createTempFile(basename,Options.JNL, journalDir);
            
            if (!file.delete()) {

                throw new AssertionError("Unable to delete new slave file");

            }
            
            return file;
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /*
     * delegation to the current slave.
     */
    
    public Properties getProperties() {
        return slave.getProperties();
    }

    /**
     * Return the file for the current {@link SlaveJournal}.
     */
    public File getFile() {
        return slave.getFile();
    }
    
    public void close() {
        slave.close();
    }

    public void closeAndDelete() {
        // @todo implement full delete on the database.
        slave.closeAndDelete();
    }
    
    public void force(boolean metadata) {
        slave.force(metadata);
    }

    public long size() {
        return slave.size();
    }
    
    public boolean isOpen() {
        return slave.isOpen();
    }

    /**
     * A partitioned journal always reports and does not allow the use
     * of non-stable backing stored for the {@link SlaveJournal}.
     */
    public boolean isStable() {
        return slave.isStable();
    }

    /**
     * true iff the {@link SlaveJournal} is fully buffered (this does not
     * consider the index segments).
     */
    public boolean isFullyBuffered() {
        return slave.isFullyBuffered();
    }
    
    public ByteBuffer read(long addr) {
        return slave.read(addr);
    }

    public long write(ByteBuffer data) {
        return slave.write(data);
    }

    public void abort() {
        slave.abort();
    }
    
    public long commit() {
        return slave.commit();
    }

    public long getRootAddr(int rootSlot) {
        return slave.getRootAddr(rootSlot);
    }

    public void setCommitter(int rootSlot, ICommitter committer) {
        slave.setCommitter(rootSlot, committer);
    }

    public IIndex getIndex(String name) {
        return slave.getIndex(name);
    }

    public IIndex registerIndex(String name) {
        return slave.registerIndex(name);
    }

    public IIndex registerIndex(String name, IIndex btree) {
        return slave.registerIndex(name, btree);
    }

    public void dropIndex(String name) {
        slave.dropIndex(name);        
    }

    public void discardCommitters() {
        slave.discardCommitters();
    }

    public void setupCommitters() {
        slave.setupCommitters();
    }

    public void abort(long ts) {
        slave.abort(ts);
    }

    public long commit(long ts) {
        return slave.commit(ts);
    }

    public IIndex getIndex(String name, long ts) {
        return slave.getIndex(name, ts);
    }

    public long newTx(IsolationEnum level) {
        return slave.newTx(level);
    }

    public ICommitRecord getCommitRecord(long commitTime) {
       return slave.getCommitRecord(commitTime);
    }

    public IRootBlockView getRootBlockView() {
        return slave.getRootBlockView();
    }

    public long nextTimestamp() {
        return slave.nextTimestamp();
    }

    /**
     * Return an index view located either on a historical slave journal or an
     * index segment.
     * 
     * @todo we need a coherent approach to managing the open index segments.
     *       there are notes on this in
     *       {@link PartitionedIndexView#openIndexSegments(PartitionMetadata)} and
     *       {@link #getIndex(String, IResourceMetadata)}. We need to explictly
     *       close index segments (or rather their backing file store) when they
     *       are no longer in use.
     * 
     * @todo instances must be stored in a weak value cache so that we do not
     *       attempt to re-open the journal (or index segment) if it is already
     *       open and so that the journal (or index segment) may be closed out
     *       once it is no longer required by any active view.
     * 
     * @todo verify that the index segment is for the named index by examining
     *       its extension metadata.
     * 
     * @todo provide for closing all open index segments for a named index in
     *       support of {@link #dropIndex(String)}.
     * 
     * @todo As a resource conservation strategy, provide for eventual close out
     *       of open {@link IndexSegment}s after a timeout, at which point they
     *       must be removed from this cache.
     * 
     * @todo this could also serve for old journals that are being held open
     *       pending completion of overflow processing.
     */
    synchronized protected AbstractBTree getIndex(String name,
            IResourceMetadata resource) {

        final String filename = resource.getFile().toString();
        
        IndexSegment seg = resourceCache.get(filename);
        
        if( seg == null ) {

            // open the file.
            IndexSegmentFileStore fileStore = new IndexSegmentFileStore(
                    resource.getFile());
            
            // load the btree.
            seg = fileStore.load();
            
            // save in the cache.
            resourceCache.put(filename, seg, false);
            
        }

        return seg;

    }

    /**
     * The maximum #of index segments that will be held open without a hard
     * reference existing for that index segment in the application.
     */
    final int INDEX_SEGMENT_LRU_CAPACITY = 5;
    
    /**
     * A cache for recently used index segments designed to prevent their being
     * swept by the VM between uses.
     */
    private final WeakValueCache<String/*filename*/, IndexSegment> resourceCache;
    
}
