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

import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.NameAddrBTree;
import com.bigdata.journal.NameAddrBTree.Entry;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.FusedView;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.IndexSegmentBuilder;
import com.bigdata.objndx.IndexSegmentMerger;
import com.bigdata.objndx.IndexSegmentMerger.MergedEntryIterator;
import com.bigdata.objndx.IndexSegmentMerger.MergedLeafIterator;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;

/**
 * <p>
 * A store that supports {@link PartitionedIndex}s.
 * </p>
 * <p>
 * An {@link PartitionedIndex} is dynamically decomposed into key-range
 * partitions. Each partition is defined by the first key that may be inserted
 * on, or read from, that parition. A total ordering over partitions and their
 * locations is maintained in a metadata index. An insert or read is directed to
 * the first partition having a minimum key greater than or equal to the probe
 * key.
 * </p>
 * <p>
 * An index partition is comprised of a {@link Journal} having a {@link BTree}
 * that buffers writes for that partition and zero or more {@link IndexSegment}s
 * per partition. The relationship between keys and partitions is managed by a
 * {@link MetadataIndex}.
 * </p>
 * <p>
 * All writes bound for any partition of any index are absorbed on this
 * {@link Journal}. For each {@link PartitionedIndex}, there is a
 * corresponding {@link BTree} that absorbs writes (including deletes) on the
 * journal. On overflow, {@link Journal}, the backing store is frozen and a new
 * buffer and backing store are deployed to absorb further writes. During this
 * time, reads on the {@link PartitionedJournal} are served by a {@link FusedView}
 * of the data on the new journal and the data on the old journal.
 * </p>
 * <p>
 * While reads and writes proceed in the forground on the new buffer and backing
 * store, a background thread builds an {@link IndexSegment} from each modified
 * {@link BTree} corresponding to a {@link PartitionedIndex}. Once all indices
 * have been processed, the old buffer and backing store are released and their
 * use is atomic replaced by the use of the new index segments.
 * </p>
 * <p>
 * If there is an existing {@link IndexSegment} for a partition then a
 * compacting merge MAY be performed. Otherwise a new {@link IndexSegment} is
 * generated, possibly resulting in more than one {@link IndexSegment} for the
 * same partition. Once all data from the journal are stable in the appropriate
 * {@link IndexSegment}s, the old {@link Journal} is closed and either deleted
 * synchronously or marked for deletion. Each {@link IndexSegment} built in this
 * manner contains a historical snapshot of data written on {@link Journal} for
 * a given partition. If multiple {@link IndexSegment} are allowed to accumulate
 * per partition, then the {@link IndexSegment}s are combined periodically
 * using a compacting merge (responsible for retaining the most recent versions
 * for any key and processing delete markers). The inputs to this process are
 * then deleted (or marked for deletion).
 * </p>
 * <p>
 * A partition overflows and is split when the total data size of an index
 * partition (estimated as the sum of the {@link IndexSegment}s and the data on
 * the {@link Journal} for that partition) exceeds a threshold. Likewise,
 * partitions may underflow and be joined. These operations require updates to
 * the metadata index so that futher requests are directed based on the new
 * parition boundaries.
 * </p>
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionedJournal implements IJournal {

    /**
     * The {@link Journal} currently serving requests for the store.
     */
    private SlaveJournal journal;

    /**
     * The name of the directory in which the journal files will be created.
     */
    private final File journalDir;
    
    /**
     * The name of the directory in which the segment files will be created.
     * Segments will be organized in this directory in subdirectories whose name
     * is the index name (munged to a name that is legal for the file system)
     * followed by a subdirectory whose name is the unique partition identifier,
     * followed by a sequence identifier for the segments generated for that
     * partition.
     */
    private final File segmentDir;
    
    /**
     * The directory that will be used for temporary files generated during bulk
     * index builds.
     */
    private final File tmpDir;
    
    /**
     * The basename for the journal files.
     * 
     * @todo On restart, the most recent journal file will be loaded. Is that a
     *       wise policy?  Do we want to do an atomic rename after overflow?
     */
    private final String basename;

    /**
     * The recommened extension for journal files.
     */
    private static final String JNL = ".jnl";
    
    /**
     * The recommened extension for index segment files.
     */
    private static final String SEG = ".seg";
    
    public static class Options extends com.bigdata.journal.Options {

        /**
         * <code>basename</code> - The property whose value is the basename of
         * journal files that will be created (no default).
         */
        public static final String BASENAME = "basename";
        
        /**
         * <code>journal.dir</code> - The property whose value is the name of
         * the directory in which new journal files will be created. When not
         * specified the default is the current directory.
         */
        public static final String JOURNAL_DIR = "journal.dir";

        /**
         * <code>segment.dir</code> - The property whose value is the name of
         * the top level directory beneath which new segment files will be
         * created. When not specified the default is the current directory.
         */
        public static final String SEGMENT_DIR = "segment.dir";

        /**
         * <code>tmp.dir</code> - The property whose value is the name of the
         * directory in which temporary files will be created.  When not
         * specified the default is governed by the value of the System
         * property named <code>java.io.tmpdir</code>
         */
        public static final String TMP_DIR = "tmp.dir";
        
    }
    
    /**
     * Create a new journal capable of supporting partitioned indices. 
     */
    public PartitionedJournal(Properties properties) {
        
        String val;

        // "journal.dir"
        val = properties.getProperty(Options.JOURNAL_DIR);
        
        journalDir = val == null?new File("."):new File(val); 

        if (!journalDir.exists()) {
            
            if (!journalDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + journalDir.getAbsolutePath());
                
            }
            
        }
        
        // "segment.dir"
        val = properties.getProperty(Options.SEGMENT_DIR);
        
        segmentDir = val == null?new File("."):new File(val);

        if (!segmentDir.exists()) {
            
            if (!segmentDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + segmentDir.getAbsolutePath());
                
            }
            
        }
        
        // "tmp.dir"
        val = properties.getProperty(Options.TMP_DIR);
        
        tmpDir = val == null ? new File(System.getProperty("java.io.tmpdir"))
                : new File(val); 

        if (!tmpDir.exists()) {
            
            if (!tmpDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + tmpDir.getAbsolutePath());
                
            }
            
        }
        
        // "basename"
        this.basename = properties.getProperty(Options.BASENAME);
        
        if(basename==null) {
            
            throw new IllegalArgumentException("Required property: "+Options.BASENAME);
            
        }

        /*
         * Scan the journal directory for basename*.jnl. Open the most current
         * file found in that directory. If no file is found, then create one.
         */
        
        File[] journalFiles = new NameAndExtensionFilter(new File(journalDir,
                basename).toString(), JNL).getFiles();
        
        final File file;
        
        if(journalFiles.length==0) {
        
            file = new File(journalDir,basename+".jnl");
            
        } else if(journalFiles.length==1) {

            file = journalFiles[0];
            
        } else {
            
            // @todo Choose the most recent file.
            throw new UnsupportedOperationException();
            
        }
        
        properties.setProperty(Options.FILE, file.toString());
        
        // Create the initial slave journal.
        this.journal = new SlaveJournal(this,properties);
        
    }
    
    /**
     * Class delegates the {@link #overflow()} event to a master
     * {@link IJournal}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SlaveJournal extends Journal {
        
        /**
         * The index of the root slot whose value is the address of the persistent
         * {@link NameAddrBTree} mapping names to {@link MetadataIndex}s registered
         * for the store.
         */
        public static transient final int ROOT_NAME_2_METADATA_ADDR = 1;
        
        /**
         * BTree mapping btree names to the last metadata record committed for the
         * {@link MetadataIndex} for the named btree. The keys are index names
         * (unicode strings). The values are the last known {@link Addr address} of
         * the {@link MetadataIndex} for the named btree.
         */
        protected NameAddrBTree name2MetadataAddr;

        private final IJournal master;
        
        public SlaveJournal(IJournal master,Properties properties) {
            
            super(properties);
            
            if (master == null)
                throw new IllegalArgumentException("master");
            
            this.master = master;

        }

        /**
         * The overflow event is delegated to the master.
         */
        public void overflow() {
        
            master.overflow();
            
        }

        public void discardCommitters() {

            super.discardCommitters();
            
            // discard.
            name2MetadataAddr = null;
            
        }
        
        public void setupCommitters() {

            super.setupCommitters();
            
            setupName2MetadataAddrBTree();

        }
        
        /**
         * Setup the btree that resolved the {@link MetadataIndex} for named
         * indices.
         */
        private void setupName2MetadataAddrBTree() {

            assert name2MetadataAddr == null;
            
            // the root address of the btree.
            long addr = getAddr(ROOT_NAME_2_METADATA_ADDR);

            if (addr == 0L) {

                /*
                 * The btree has either never been created or if it had been created
                 * then the store was never committed and the btree had since been
                 * discarded.  In any case we create a new btree now.
                 */

                // create btree mapping names to addresses.
                name2MetadataAddr= new NameAddrBTree(this);

            } else {

                /*
                 * Reload the btree from its root address.
                 */

                name2MetadataAddr = new NameAddrBTree(this, addr);

            }

            // register for commit notices.
            setCommitter(ROOT_NAME_2_METADATA_ADDR, name2MetadataAddr);

        }
        
        /**
         * Registers a {@link PartitionedIndex} under the given name and assigns
         * the supplied {@link BTree} to absorb writes for that index.
         * <p>
         * A {@link MetadataIndex} is also registered under the given name and
         * an initial partition for that index is created using the separator
         * key <code>new byte[]{}</code>. The partition will initially
         * consist of zero {@link IndexSegment}s.
         * 
         * @todo There should also be an unisolated DROP INDEX that simply
         *       removes the index and its resources, including secondary index
         *       segment files, in the metadata index, and in the various
         *       journals that were absorbing writes for that index. That
         *       operation would require a lock on the index, which could be
         *       achieved by taking the index offline in the metadata index and
         *       invalidating all of the clients. A similar "ADD INDEX" method
         *       would create a distributed index and make it available to
         *       clients.
         */
        public void registerIndex(String name, BTree btree) {

            if( getIndex(name) != null ) {
                
                throw new IllegalStateException("BTree already registered: name="+name);
                
            }

            // make sure there is no metadata index for that btree.
            if(getMetadataIndex(name) != null) {
                
                throw new AssertionError();
                
            }
            
            MetadataIndex mdi = new MetadataIndex(this,
                    BTree.DEFAULT_BRANCHING_FACTOR);
            
            // add to the persistent name map.
            name2MetadataAddr.add(name, mdi);

            // now register the mutable btree on the index.
            super.registerIndex(name, btree);
            
        }
        
        public BTree getIndex(String name) {
            
            /*
             * FIXME this needs to provide a canonicalizing mapping returning
             * the PartitionedIndex. We need to change the interface for the return
             * type to IIndex.  registerIndex needs to wrap the BTree and return 
             * the object that would be returned by getIndex, so I have to change
             * its signature.
             */ 
            throw new UnsupportedOperationException();
            
        }

        /**
         * Return the {@link MetadataIndex} for the named {@link PartitionedIndex}.
         * This object is used to maintain the definitions of the partitions for
         * that index, including where each file is located that contains live data
         * for the index.
         */
        public MetadataIndex getMetadataIndex(String name) {

            if(name==null) throw new IllegalArgumentException();
            
            return (MetadataIndex) name2MetadataAddr.get(name);

        }

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
     * metadata index into the new journal or evict it to a segment just like
     * the data indices. </li>
     * <li> writes on a partitioned btree just go into the corresponding btree
     * on the journal (assuming one journal for all partitions, e.g., scale up
     * but not scale out). </li>
     * <li> reads on a partitioned btree must read a fused view over (a) the
     * partitions relevant to the query; and (b) the mutable btree on the
     * journal and any index segment(s) for that parition.</li>
     * </ol>
     * </p>
     * <p>
     * Design one:
     * <ol>
     * <li> Implement the same interfaces as Journal, but delegate to a Journal.
     * </li>
     * <li>Initialize the metadata index with a single partition for each named
     * btree that is registered on the Journal. </li>
     * <li> Detect overflow only at commit and extend the journal as necessary
     * until the next commit. </li>
     * <li> On overflow, open a Journal using the same strategy, copy the
     * metadata index over into the new journal. Synchronously, evict all
     * partitions written on for all btrees written on in the old journal onto
     * index segments. If there is an existing index segment, then use a
     * compacting merge. Finally, update the delegation logic to use a fused
     * view for reads against each partition of each btree. The inputs to that
     * fused view are the named mutable btree on new journal and the
     * corresponding index segment.</li>
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
     * @todo implement asynchronous overflow.
     * @todo implement using compacting merges only.
     * @todo implement using non-compacting merges only.
     * @todo implement strategy that trades off the use of compacting and
     *       non-compacting merges.
     * 
     * @todo Support dynamic split/join of partitions.
     * 
     * @todo Regardless of whether one or many partitioned trees are on the
     *       journal then an export of that tree into an index segment will
     *       result in an mostly random read over the address space since the
     *       order which nodes and leaves are written onto the journal does not
     *       correspond directly to the total ordering of the btree. if the
     *       journal is in disk-only mode then it makes sense to slurp the
     *       entire disk file into a buffer during the export process to reduce
     *       multiple disk seeks with a single sequential read. the buffer can
     *       be discarded as soon as the btrees have been exported onto their
     *       various segment(s).
     */
    public void overflow() {
        
        synchronousOverflow();
        
    }

    protected void synchronousOverflow() {
        
        /* 
         * Create the new buffer.
         */
        
        Properties properties = journal.getProperties();

        if( journal.isStable() ) {
            
            /*
             * Override the filename with a unique filename based on the
             * original filename.
             */
            properties.setProperty(Options.FILE, getNextFilename(properties
                    .getProperty(Options.FILE)));
            
        }
        
        // The new journal.
        SlaveJournal newJournal = new SlaveJournal(this,properties);

        // The journal that overflowed.
        final SlaveJournal oldJournal = journal;
        
        /*
         * We need to consider each named btree registered on this store. We can
         * not consider only those btrees which have been touched since the
         * store was last opened since there may be multiple restarts before the
         * journal overflows.
         */
        Iterator itr = oldJournal.name2MetadataAddr.entryIterator();
        
        while(itr.hasNext()) {

            /*
             * The name of a PartitionedIndex.
             */
            final String name = ((Entry) itr.next()).name;
            
            /*
             * The mutable btree on the journal for the named PartitionedIndex.
             */
            final BTree oldBTree = getIndex(name);

            /*
             * The metadata index describing the partitions for the named
             * PartitionedIndex.
             */
            final MetadataIndex mdi = oldJournal.getMetadataIndex(name);

            /*
             * Examine the btree for buffered writes.
             */
            
            final int entryCount = oldBTree.getEntryCount();

            // Create empty btree on the new journal.
            final BTree newBTree = new BTree(newJournal, oldBTree
                    .getBranchingFactor(), oldBTree.getNodeSerializer()
                    .getValueSerializer());

            // Register the btree under the same name on the new journal.
            newJournal.registerIndex(name, newBTree);

            if (entryCount == 0) {
                
                /*
                 * NOP: we just created a new btree by that name on the new
                 * buffer.
                 */
                
            } else if (entryCount < 1000) {
            
                /*
                 * There are only a few entries so just copy the btree onto the
                 * new buffer.
                 */
                
                newBTree.addAll(oldBTree);
                
            } else {
            
                /*
                 * @todo Since the btree has more than a "few" entries we evict
                 * entries in the old btree onto an index segment.
                 */
                try {
                    
                    evict(name,oldBTree,mdi);
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }
            
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
        
        /*
         * replace the old buffer with the new buffer and delete the old buffer.
         * 
         * @todo consider scheduling the old buffer for deletion instead so that
         * we have some time to verify that the new index segments are good
         * before deleting the buffered writes.
         */
        
        journal = newJournal;

        // immediate shutdown.
        oldJournal.close();
        
        // delete the old backing file (if any).
        oldJournal.getBufferStrategy().deleteFile();
        
    }
    
    /**
     * Evicts all writes buffered on a btree onto index segments for the
     * partitions spanned by the keys in the btree.
     * <p>
     * This implementation assumes a synchronous compacting merge of all btrees
     * on the journal. All existing index segments are closed as they are
     * processed but the new index segments are NOT opened until they are
     * needed. The old index segments are immediately marked as
     * {@link IndexSegmentLifeCycleEnum#DEAD} in the {@link SegmentMetadata} and
     * the new index segments are marked as
     * {@link IndexSegmentLifeCycleEnum#LIVE}.
     * 
     * @todo this does not handle multiple partitions.
     * 
     * @todo we need a coherent approach to managing the open index segments.
     *       there are notes on this in
     *       {@link PartitionedIndex#openIndexSegments(PartitionMetadata)}.
     * 
     * @param name
     *            The name of the index.
     * 
     * @param btree
     *            The btree.
     * @param mdi
     *            The metadata index defining the partitions for the btree.
     */
    protected void evict(String name,BTree btree, MetadataIndex mdi) throws IOException {

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
        
        if (true) {

            /*
             * Evict the btree into an index segment.
             * 
             * This control path is used when there is no pre-existing index
             * segment for a partition and the only data is in the mutable btree
             * on the journal.
             */

            File outFile = getSegmentFile(name,pmd.partId,segId);

            new IndexSegmentBuilder(outFile, null, btree, mseg, 0d);

            /*
             * update the metadata index for this partition.
             */
            mdi.put(separatorKey,
                    new PartitionMetadata(0, segId + 1,
                            new SegmentMetadata[] { new SegmentMetadata(""
                                    + outFile, outFile.length(),
                                    IndexSegmentLifeCycleEnum.LIVE) }));

//            /*
//             * open and verify the index segment against the btree data.
//             */
//            seg = new IndexSegment(new IndexSegmentFileStore(outFile01), btree
//                    .getNodeSerializer().getValueSerializer());

        } else {

            /*
             * Evict the merge of the mutable btree on the journal and the
             * existing index segment into another index segment.
             */

            System.err.println("Evicting and merging with existing segment.");

            /*
             * FIXME get the current index segment for the partition!
             * 
             * This needs to be obtained from the PartitionedIndex. getIndex()
             * needs to be modified on the SlaveJournal to return a partitioned
             * index. The partitioned index is responsible for maintaining a
             * canonicalizing mapping and keeping hold of and expiring index
             * segments based on use.
             */
            final IndexSegment seg;

            // tmp file for the merge process.
            File tmpFile = File.createTempFile("merge", ".tmp", tmpDir);

            tmpFile.deleteOnExit();

            // output file for the merged segment.
            File outFile = getSegmentFile(name, pmd.partId, segId);

            // merge the data from the btree on the journal and the index
            // segment.
            MergedLeafIterator mergeItr = new IndexSegmentMerger(tmpFile,
                    mseg2, btree, seg).merge();

            // build the merged index segment.
            IndexSegmentBuilder builder = new IndexSegmentBuilder(outFile,
                    null, mergeItr.nentries, new MergedEntryIterator(mergeItr),
                    mseg, btree.getNodeSerializer().getValueSerializer(),
                    true/* fullyBuffer */, false/* useChecksum */,
                    null/* recordCompressor */, 0d/* errorRate */);

            // close the merged leaf iterator (and release its buffer/file).
            // @todo this should be automatic when the iterator is exhausted but
            // I am not seeing that.
            mergeItr.close();

            /*
             * Update the metadata index for this partition.
             * 
             * We mark the earlier index segment as "DEAD" for this partition
             * since it has been replaced by the merged result.
             * 
             * Note: it is a good idea to wait until you have opened the merged
             * index segment, and even until it has begun to serve data, before
             * deleting the old index segment that was an input to that merge!
             * The code currently waits until the next time a compacting merge
             * is performed for the partition and then deletes the DEAD index
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
                    IndexSegmentLifeCycleEnum.DEAD);

            newSegs[1] = new SegmentMetadata(outFile.toString(), outFile.length(),
                    IndexSegmentLifeCycleEnum.LIVE);

            mdi.put(separatorKey, new PartitionMetadata(0, segId + 1, newSegs));

// /*
// * open the merged index segment
//             */
//            IndexSegment seg02 = new IndexSegment(new IndexSegmentFileStore(
//                    outFile02), btree.getNodeSerializer().getValueSerializer());

            /*
             * Close the old index segment. We have marked it as DEAD and it will
             * be eventually deleted.
             */
            seg.close();

            // assuming at most one dead and one live segment.
            if(pmd.segs.length>1) {
                
                final SegmentMetadata deadSeg = pmd.segs[0];
                
                if(deadSeg.state!=IndexSegmentLifeCycleEnum.DEAD) {
                    
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
    private File getSegmentFile(String name,int partId,int segId) {

        File file = new File(segmentDir + File.separator + //
                basename + File.separator + //
                name + File.separator + //
                "part" + partId + File.separator + //
                segId + SEG);
        
        File parent = file.getParentFile();
        
        if(!parent.exists() && !parent.mkdirs()) {
            
            throw new RuntimeException("Could not create directory: "+parent);
            
        }
        
        return file;
        
    }
    
    /**
     * Creates the name for the new backing file when the journal
     * {@link #overflow()}s.
     * 
     * @todo this should be based on a more rigid pattern, on the original
     *       filename, or perhaps simply a better parse of the old filename.
     */
    protected String getNextFilename(String filename) {

        if (filename == null) {

            throw new IllegalArgumentException();

        }

        File file = new File(filename);

        if (!file.exists())
            throw new AssertionError();

        String prefix = file.getName();

        String suffix = "";

        if (prefix.endsWith(".jnl")) {

            prefix = prefix.substring(0, prefix.length() - 4);

            suffix = ".jnl";

        }

        try {

            file = File.createTempFile(prefix, suffix, new File(file
                    .getParent()));

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        if (!file.delete()) {

            throw new RuntimeException("Unable to delete new journal file");

        }
        
        return file.toString();
        
    }

    /*
     * delegation to the current journal.
     */
    
    public Properties getProperties() {
        return journal.getProperties();
    }

    public void close() {
        journal.close();
    }

    public void force(boolean metadata) {
        journal.force(metadata);
    }

    public boolean isOpen() {
        return journal.isOpen();
    }

    public boolean isStable() {
        return journal.isStable();
    }

    public ByteBuffer read(long addr, ByteBuffer dst) {
        return journal.read(addr, dst);
    }

    public long write(ByteBuffer data) {
        return journal.write(data);
    }

    public void commit() {
        journal.commit();
    }

    public long getAddr(int rootSlot) {
        return journal.getAddr(rootSlot);
    }

    public void setCommitter(int rootSlot, ICommitter committer) {
        journal.setCommitter(rootSlot, committer);
    }

    public BTree getIndex(String name) {
        return journal.getIndex(name);
    }

    public void registerIndex(String name, BTree btree) {
        journal.registerIndex(name, btree);
    }

    public void discardCommitters() {
        journal.discardCommitters();
    }

    public void setupCommitters() {
        journal.setupCommitters();
    }

}
