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
/*
 * Created on Dec 5, 2006
 */

package com.bigdata.btree;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.io.FileLockUtility;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Builds an {@link IndexSegment} given a source btree and a target branching
 * factor. There are two main use cases:
 * <ol>
 * 
 * <li>Evicting a key range of an index into an optimized on-disk index. In
 * this case, the input is a {@link BTree} that is ideally backed by a fully
 * buffered {@link IRawStore} so that no random reads are required.</li>
 * 
 * <li>Merging index segments. In this case, the input is typically records
 * emerging from a merge-sort. There are two distinct cases here. In one, we
 * simply have raw records that are being merged into an index. This might occur
 * when merging two key ranges or when external data are being loaded. In the
 * other case we are processing two timestamped versions of an overlapping key
 * range. In this case, the more recent version may have "delete" markers
 * indicating that a key present in an older version has been deleted in the
 * newer version. Also, key-value entries in the newer version replaced (rather
 * than are merged with) key-value entries in the older version. If an entry
 * history policy is defined, then it must be applied here to cause key-value
 * whose retention is no longer required by that policy to be dropped.</li>
 * 
 * </ol>
 * 
 * Note: In order for the nodes to be written in a contiguous block we either
 * have to buffer them in memory or have to write them onto a temporary file and
 * then copy them into place after the last leaf has been processed. The code
 * abstracts this decision using a {@link TemporaryRawStore} for this purpose.
 * This space demand was not present in West's algorithm because it did not
 * attempt to place the leaves contiguously onto the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see "Post-order B-Tree Construction" by Lawerence West, ACM 1992. Note that
 *      West's algorithm is for a b-tree (values are stored on internal stack as
 *      well as leaves), not a b+-tree (values are stored only on the leaves).
 *      Our implementation is therefore an adaptation.
 * 
 * @see "Batch-Construction of B+-Trees" by Kim and Won, ACM 2001. The approach
 *      outlined by Kim and Won is designed for B+-Trees, but it appears to be
 *      less efficient on first glance.
 * 
 * FIXME use the shortest separator key.
 * 
 * @see IndexSegment
 * @see IndexSegmentFile
 * @see IndexSegmentCheckpoint
 * @see IndexSegmentMerger
 */
public class IndexSegmentBuilder implements Callable<IndexSegmentCheckpoint> {
    
    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentBuilder.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isInfoEnabled();
    
    /**
     * The file mode used to open the file on which the {@link IndexSegment} is
     * written.
     */
    final String mode = "rw"; // also rws or rwd
    
    /**
     * The file specified by the caller on which the {@link IndexSegment} is
     * written.
     */
    public final File outFile;
    
    /**
     * The value specified to the ctor.
     */
    final public int entryCount;
    
    /**
     * The iterator specified to the ctor. This is the source for the keys and
     * values that will be written onto the generated {@link IndexSegment}.
     */
    final private ITupleIterator entryIterator;
    
    /**
     * The commit time associated with the view from which the
     * {@link IndexSegment} is being generated (from the ctor). This value is
     * written into {@link IndexSegmentCheckpoint#commitTime}.
     */
    final public long commitTime;
    
    /**
     * A copy of the metadata object provided to the ctor. This object is
     * further modified before being written on the
     * {@link IndexSegmentStore}.
     */
    final public IndexMetadata metadata;
    
    /**
     * <code>true</code> iff the source index is isolatable (supports both
     * deletion markers and version timestamps).
     */
    final boolean isolatable;
    
    /**
     * The unique identifier for the generated {@link IndexSegment} resource.
     */
    final public UUID segmentUUID;

    /**
     * Used to serialize the nodes and leaves of the output tree.
     */
    final private NodeSerializer nodeSer;

    /**
     * Note: The offset bits on the {@link IndexSegmentFileStore} does NOT
     * have to agree with the offset bits on the source store. However, it
     * must be large enough to handle the large branching factors typically
     * associated with an {@link IndexSegment} vs a {@link BTree}. Further,
     * if blobs are to be copied into the index segment then it generally
     * must be large enough for those blobs (up to 64M per record).
     * <p>
     * Note: The same #of offset bits MUST be used by the temporary stores
     * that we use to buffer nodes, leaves, and blobs as are used by the
     * generated index segment!
     */
    final int offsetBits = WormAddressManager.SCALE_OUT_OFFSET_BITS;
    
    /**
     * The {@link IAddressManager} used to form addresses for the generated
     * file. Addresses are formed from a byteCount and an <em>encoded</em>
     * offset comprised of a relative offset into a known region and the region
     * identifier.
     * 
     * @see IndexSegmentRegion
     * @see IndexSegmentAddressManager
     */
    final private WormAddressManager addressManager;

    /**
     * The bloom filter iff we build one (errorRate != 0.0).
     */
    final BloomFilter bloomFilter;
    
    /**
     * The file on which the {@link IndexSegment} is written. The file is closed
     * regardless of the outcome of the operation.
     */
    protected RandomAccessFile out = null;
    
    /**
     * The {@link IndexSegmentCheckpoint} record written on the
     * {@link IndexSegmentStore}.
     */
    private IndexSegmentCheckpoint checkpoint;
    
    /**
     * The {@link IndexSegmentCheckpoint} record written on the
     * {@link IndexSegmentStore}.
     */
    public IndexSegmentCheckpoint getCheckpoint() {
        
        return checkpoint;
        
    }
    
    /**
     * The buffer used to hold leaves so that they can be evicted en mass onto
     * a region of the {@link #outFile}.
     */
    protected TemporaryRawStore leafBuffer;
    
    /**
     * The buffer used to hold nodes so that they can be evicted en mass onto
     * a region of the {@link #outFile}.
     */
    protected TemporaryRawStore nodeBuffer;
    
    /**
     * The optional buffer used to hold records referenced by index entries. In
     * order to use this buffer the {@link IndexMetadata} MUST specify an  
     */
    protected TemporaryRawStore blobBuffer;
        
    /**
     * The encoded address of the first leaf written on the
     * {@link IndexSegmentStore} (there is always at least one, even if it is
     * the root leaf).
     * <p>
     * Note: A copy of this value is preserved by
     * {@link IndexSegmentCheckpoint#addrFirstLeaf}.
     */
    private long addrFirstLeaf = 0L;

    /**
     * The encoded address of the last leaf written on the
     * {@link IndexSegmentStore} (there is always at least one, even if it is
     * the root leaf).
     * <p>
     * Note: A copy of this value is preserved by
     * {@link IndexSegmentCheckpoint#addrLastLeaf}.
     */
    private long addrLastLeaf = 0L;

//    /**
//     * The offset in the output file of the last leaf written onto that file.
//     * Together with {@link #lastLeafSize} this is used to compute the
//     * address of the prior leaf.
//     */
//    long lastLeafOffset = -1L;
//    
//    /**
//     * The size in bytes of the last leaf written onto the output file (the size
//     * of the compressed record that is actually written onto the output file
//     * NOT the size of the serialized leaf before it is compressed). Together
//     * with {@link #lastLeafOffset} this is used to compute the address of the
//     * prior leaf.
//     */
//    int lastLeafSize = -1;

    /**
     * Tracks the maximum length of any serialized node or leaf.  This is used
     * to fill in one of the {@link IndexSegmentCheckpoint} fields.
     */
    int maxNodeOrLeafLength = 0;

    /**
     * The #of nodes written for the output tree. This will be zero if all
     * entries fit into a root leaf.
     */
    int nnodesWritten = 0;
    
    /**
     * The #of leaves written for the output tree.
     */
    int nleavesWritten = 0;
    
    /**
     * The #of nodes or leaves that have been written out in each level of the
     * tree.
     * 
     * @see IndexSegmentPlan#numInLevel
     */
    final int writtenInLevel[];

    /**
     * The stack of nodes that are currently being populated. The first N-1
     * elements in this array are always nodes while the last element is always
     * a leaf ({@link #leaf} is the same reference as the last element in this
     * array). The nodes and the leaf in this array are reused rather than being
     * reallocated.
     */
    final AbstractSimpleNodeData[] stack;
    
    /**
     * The current leaf that is being populated from the source btree. This leaf
     * is reused for each output leaf rather than being reallocated. In the
     * degenerate case when the output btree is a single root leaf then this
     * will be that leaf. This reference is always the same as the last
     * reference in {@link #stack}.
     */
    final SimpleLeafData leaf;

    /**
     * The plan for building the B+-Tree.
     */
    final public IndexSegmentPlan plan;
    
    /**
     * The time to setup the index build, including the generation of the index
     * plan and the initialization of some helper objects.
     */
    public final long elapsed_setup;
    
    /**
     * The time to write the nodes and leaves into their respective buffers, not
     * including the time to transfer those buffered onto the output file.
     */
    public long elapsed_build;
    
    /**
     * The time to write the nodes and leaves from their respective buffers
     * onto the output file and synch and close that output file.
     */
    public long elapsed_write;
    
    /**
     * The process runtime in milliseconds.
     */
    public long elapsed;
    
    /**
     * The data throughput rate in megabytes per second.
     */
    public float mbPerSec;
        
    /**
     * <p>
     * Designated constructor sets up a build of an {@link IndexSegment} for
     * some caller defined read-only view.
     * </p>
     * <p>
     * Note: The caller must determine whether or not deleted index entries are
     * present in the view. The <i>entryCount</i> MUST be the exact #of index
     * entries that are visited by the given iterator. In general, this is not
     * difficult. However, if a compacting merge is desired (that is, if you are
     * trying to generate a view containing only the non-deleted entries) then
     * you MUST explicitly count the #of entries that will be visited by the
     * iterator, e.g., it will require two passes over the iterator to setup the
     * index build operation.
     * </p>
     * <p>
     * Note: With a branching factor of 4096 a tree of height 2 (three levels)
     * could address 68,719,476,736 entries - well beyond what we want in a
     * given index segment! Well before that the index segment should be split
     * into multiple files. The split point should be determined by the size of
     * the serialized leaves and nodes, e.g., the amount of data on disk
     * required by the index segment and the amount of memory required to fully
     * buffer the index nodes. While the size of a serialized node can be
     * estimated easily, the size of a serialized leaf depends on the kinds of
     * values stored in that index. The actual sizes are recorded in the
     * {@link IndexSegmentCheckpoint} record in the header of the
     * {@link IndexSegment}.
     * </p>
     * 
     * @param outFile
     *            The file on which the index segment is written. The file MAY
     *            exist but MUST have zero length if it does exist (this permits
     *            you to use the temporary file facility to create the output
     *            file).
     * @param tmpDir
     *            The temporary directory in data are buffered during the build
     *            (optional - the default temporary directory is used if this is
     *            <code>null</code>).
     * @param entryCount
     *            The #of entries that will be visited by the iterator.
     * @param entryIterator
     *            Visits the index entries in key order that will be written
     *            onto the {@link IndexSegment}.
     * @param m
     *            The branching factor for the generated tree. This can be
     *            choosen with an eye to minimizing the height of the generated
     *            tree. (Small branching factors are permitted for testing, but
     *            generally you want something relatively large.)
     * @param metadata
     *            The metadata record for the source index. A copy will be made
     *            of this object. The branching factor in the generated tree
     *            will be overriden to <i>m</i>.
     * @param commitTime
     *            The commit time associated with the view from which the
     *            {@link IndexSegment} is being generated. This value is written
     *            into {@link IndexSegmentCheckpoint#commitTime}.
     * 
     * @throws IOException
     */
    public IndexSegmentBuilder(//
            File outFile,//
            File tmpDir,//
            final int entryCount,//
            ITupleIterator entryIterator, //
            int m,
            IndexMetadata metadata,//
            final long commitTime
            )
            throws IOException {

        assert outFile != null;
        assert tmpDir != null;
        assert entryCount > 0;
        assert entryIterator != null;
        assert commitTime > 0L;

        final long begin_setup = System.currentTimeMillis();

        // the UUID assigned to this index segment file.
        this.segmentUUID = UUID.randomUUID();

        this.entryCount = entryCount;
        
        this.entryIterator = entryIterator;
        
        /*
         * Make a copy of the caller's metadata.
         * 
         * Note: The callers's reference is replaced by a reference to the clone
         * in order to avoid accidental modifications to the caller's metadata
         * object.
         */
        this.metadata = metadata = metadata.clone();
        {
            
            LocalPartitionMetadata pmd = this.metadata.getPartitionMetadata();
            
            if (pmd != null) {
        
                /*
                 * Copy the local partition metadata, but do not include the
                 * resource metadata identifying the resources that comprise the
                 * index partition view. that information is only stored on the
                 * BTree, not on the IndexSegment.
                 */

                this.metadata.setPartitionMetadata(
                        new LocalPartitionMetadata(//
                                pmd.getPartitionId(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey(),//
                                null, // No resource metadata.
                                pmd.getHistory()+
                                "build("+pmd.getPartitionId()+") "
                        )
                        );
                
            }
            
        }
        
        // true iff the source index is isolatable.
        this.isolatable = metadata.isIsolatable();

        //
        this.commitTime = commitTime;
        
        /*
         * Override the branching factor on the index segment.
         * 
         * Note: this override is a bit dangerous since it might propagate back
         * to the mutable btree, which could hurt performance through the use of
         * a too large branching factor on the journal. However, the metadata
         * index stores the template metadata for the scale-out index and if you
         * use either that or the metadata record from an existing BTree then
         * this should never be a problem.
         */
        this.metadata.setBranchingFactor(m);
        
        /*
         * @todo The override of the BTree class name does not make much sense
         * here. Either we should strongly discourage further subclassing of
         * BTree and IndexSegment or we should allow the subclass to be named
         * for both the mutable btree and the read-only index segment.
         */
        this.metadata.setClassName(IndexSegment.class.getName());

        this.addressManager = new WormAddressManager(offsetBits);
        
        /*
         * Create the index plan and do misc setup.
         */
        {

            // Create a plan for generating the output tree.
            plan = new IndexSegmentPlan(m, entryCount);

            /*
             * Setup a stack of nodes (one per non-leaf level) and one leaf.
             * These are filled in based on the plan and the entries visited in
             * the source btree. Nodes and leaves are written out to their
             * respective channel each time they are complete as defined (by the
             * plan) by the #of children assigned to a node or values assigned
             * to a leaf.
             */

            stack = new AbstractSimpleNodeData[plan.height + 1];

            // Note: assumes defaults to all zeros.
            writtenInLevel = new int[plan.height + 1];

            for (int h = 0; h < plan.height; h++) {

                SimpleNodeData node = new SimpleNodeData(h, plan.m);

                node.max = plan.numInNode[h][0];

                stack[h] = node;

            }

            // the output leaf (reused for each leaf we populate).

            leaf = new SimpleLeafData(plan.height, plan.m, metadata);

            leaf.max = plan.numInNode[plan.height][0];

            stack[plan.height] = leaf;

            /*
             * Setup optional bloom filter.
             */
            {
             
                final double errorRate = metadata.getErrorRate();

                if (errorRate < 0.0 || errorRate > 1.0) {

                    throw new IllegalArgumentException(
                            "errorRate must be in [0:1], not " + errorRate);

                }

                if (errorRate == 0.0) {

                    bloomFilter = null;

                } else {

                    // @todo compute [d] based on the error rate.
                    bloomFilter = new BloomFilter(plan.nentries);

                }
                
            }

            /*
             * Used to serialize the nodes and leaves for the output tree.
             */
            nodeSer = new NodeSerializer(//
                    /*
                     * Note: it does not seem like there should be any
                     * interaction between various IAddressSerializer strategies
                     * and the manner in which we encode the region (BASE, NODE,
                     * or BLOB) into the offset of addresses for the index
                     * segment store. The offset is effectively left-shifted by
                     * two bits to encode the region, there by reducing the
                     * maximum possible byte offset within any region (including
                     * BASE). However, that should not pose problems for any
                     * IAddressSerializer strategy as long as the accept any
                     * legal [byteCount] and [offset] - it is just that our
                     * offsets are essentially 4x larger than they would be
                     * otherwise.
                     */
                    addressManager,//
                    NOPNodeFactory.INSTANCE,//
                    plan.m,//
                    0, // initialBufferCapacity - will be estimated.
                    metadata, //
                    false // NOT read-only (we are using it for writing).
                    );

        }
    
        this.outFile = outFile;
        
        elapsed_setup = System.currentTimeMillis() - begin_setup;
        
    }
    
    /**
     * Build the {@link IndexSegment} given the parameters specified to
     * the ctor.
     */
    public IndexSegmentCheckpoint call() throws Exception {

        /*
         * Setup for IO.
         */

        long begin_build = System.currentTimeMillis();
        
        if (outFile.exists() && outFile.length() != 0L) {
            throw new IllegalArgumentException("File exists and is not empty: "
                    + outFile.getAbsoluteFile());
        }

        final FileChannel outChannel;
        
        try {

            /*
             * Open the output channel and get an exclusive lock.
             */
            
            out = FileLockUtility.openFile(outFile, mode, true/*useFileLock*/);
//            out = new RandomAccessFile(outFile, mode);
//            
            outChannel = out.getChannel();
//            
//            if (outChannel.tryLock() == null) {
//                
//                throw new IOException("Could not lock file: "
//                        + outFile.getAbsoluteFile());
//                
//            }

            /*
             * Open the leaf buffer. We always do this, but we choose the type
             * of the buffer based on whether or not the index build operation
             * is fully buffered. Note that an index normally has much more data
             * in the leaves so the leafBuffer is where most of the IO is. By
             * writing the leaves into memory and evicting them all at once onto
             * the disk we can realize a substantial decrease in latency for the
             * index build operation.
             */
            leafBuffer = new TemporaryRawStore(offsetBits);
            
            /*
             * Open the node buffer. We only do this if there will be at least
             * one node written, i.e., the output tree will consist of more than
             * just a root leaf. The type of buffer depends on whether or not
             * the index build operation is fully buffered. When fully buffered
             * we use a memory-based buffer, otherwise the buffer is an
             * abstraction for a disk file.
             */
            nodeBuffer = plan.nnodes > 0 ? new TemporaryRawStore(offsetBits) : null;

            /*
             * Open buffer for blobs iff an overflow handler was specified.
             */
            final IOverflowHandler overflowHandler = metadata.getOverflowHandler();
            
            blobBuffer = overflowHandler == null ? null
                    : new TemporaryRawStore(offsetBits);
            
            /*
             * Scan the source btree leaves in key order writing output leaves
             * onto the index segment file with the new branching factor. We
             * also track a stack of nodes that are being written out
             * concurrently on a temporary channel.
             * 
             * The plan tells us the #of values to insert into each leaf and the
             * #of children to insert into each node. Each time a leaf becomes
             * full (according to the plan), we "close" the leaf, writing it out
             * onto the store and obtaining its "address". The "close" logic
             * also takes care of setting the address on the leaf's parent node
             * (if any). If the parent node becomes filled (according to the
             * plan) then it is also "closed".
             * 
             * Each time (except the first) that we start a new leaf we record
             * its first key as a separatorKey in the appropriate parent node.
             * 
             * Note that the root may be a leaf as a degenerate case.
             */

//            /*
//             * The #of entries consumed in the current source leaf. When this
//             * reaches the #of keys in the source leaf then we have to read the
//             * next source leaf from the source leaf iterator.
//             */
//            ILeafData sourceLeaf = leafIterator.next();
//            
//            int nconsumed = 0;
//            
//            int nsourceKeys = sourceLeaf.getKeyCount();

            // #of entries used so far.
            int nused = 0;
            
            for (int i = 0; i < plan.nleaves; i++) {

                leaf.reset(plan.numInNode[leaf.level][i]);

                MutableKeyBuffer keys = (MutableKeyBuffer)leaf.keys;
                
                /*
                 * Fill in defined keys and values for this leaf.
                 * 
                 * Note: Since the shortage (if any) is distributed from the
                 * last leaf backward a shortage will cause [leaf] to have
                 * key/val data that is not overwritten. This does not cause a
                 * problem as long as [leaf.nkeys] is set correctly since only
                 * that many key/val entries will actually be serialized.
                 */

                final int limit = leaf.max; // #of keys to fill in this leaf.
                
                for (int j = 0; j < limit; j++) {

//                    if( nconsumed == nsourceKeys ) {
//                    
//                        // get the next source leaf.
//                        sourceLeaf = leafIterator.next();
//                        
//                        nconsumed = 0;
//                        
//                        nsourceKeys = sourceLeaf.getKeyCount();
//                                                
//                    }
                    
                    // copy key from the source leaf.
//                    leaf.copyKey(keys.nkeys, sourceLeaf, nconsumed);
//                    keys.keys[keys.nkeys] = sourceLeaf.getKeys().getKey(nconsumed);
//                    
//                    leaf.vals[j] = ((Leaf)sourceLeaf).values[nconsumed];

                    final ITuple tuple;

                    try {
                        
                        tuple = entryIterator.next();
                        
                        nused++;
                        
                    } catch(NoSuchElementException ex) {
                        
                        throw new RuntimeException("Iterator exhausted after "
                                + nused + " entries, but expected "
                                + entryCount + " entries", ex);
                        
                    }

                    // make sure that the iterator is reporting the data we need. 
                    assert tuple.getKeysRequested() : "keys not reported by itr.";
                    assert tuple.getValuesRequested() : "vals not reported by itr.";
                    assert !isolatable
                            || (isolatable && ((tuple.flags() & IRangeQuery.DELETED) == 0))
                            : "version metadata not reported by itr for isolatable index"
                                ;
                    
                    /*
                     * @todo modify to copy the key using the tuple once the
                     * internal leaf data structure offers us a place into which
                     * we can copy the data - for now we need to do an
                     * allocation to obtain a new reference or just reuse the
                     * reference on the source leaf if it happens to be mutable.
                     */

                    keys.keys[keys.nkeys] = tuple.getKey();

                    final byte[] val;
                    
                    if (overflowHandler != null) {
                    
                        /*
                         * Provide the handler with the opportunity to copy the
                         * blob's data onto the buffer and re-write the value,
                         * which is presumably the blob reference.
                         */ 

                        val = overflowHandler.handle(tuple, blobBuffer);
                        
                    } else {
                        
                        val = tuple.getValue();
                        
                    }
                    
                    leaf.vals[j] = val; 

                    if( bloomFilter != null ) {
                        
//                        leaf.addKey(bloomFilter,keys.nkeys);
                        bloomFilter.add(keys.keys[keys.nkeys]);
                        
                    }
                    
                    keys.nkeys++;
                    
//                    nconsumed++;

                    if( i > 0 && j == 0 ) {

                        /*
                         * Every time (after the first) that we enter a new leaf
                         * we need to record its first key as a separatorKey in
                         * the appropriate parent.
                         */
                        addSeparatorKey(leaf);

                    }
                    
                }

                /*
                 * Close the current leaf. This will write the address of the
                 * leaf on the parent (if any). If the parent becomes full then
                 * the parent will be closed as well.
                 */
                flush(leaf);
                
            }

            // Verify that all leaves were written out.
            assert plan.nleaves == nleavesWritten;
            
            // Verify that all nodes were written out.
            assert plan.nnodes == nnodesWritten;

            elapsed_build = System.currentTimeMillis() - begin_build;
            
            final long begin_write = System.currentTimeMillis();
            
            // write everything out on the outFile.
            checkpoint = writeIndexSegment(outChannel, commitTime);
            
            /*
             * Flush this channel to disk and close the channel. This also
             * releases our lock. We are done and the index segment is ready for
             * use.
             */
            outChannel.force(true);
            FileLockUtility.closeFile(outFile, out);
//            out.close(); // also releases the lock.
////            out = null;

            elapsed_write = System.currentTimeMillis() - begin_write;
            
            /*
             * log run time.
             */
            
            elapsed = (System.currentTimeMillis() - begin_build) + elapsed_setup;

            // data rate in MB/sec.
            mbPerSec = (elapsed == 0 ? 0 : checkpoint.length / Bytes.megabyte32
                    / (elapsed / 1000f));
            
            if(INFO) {
            
                final NumberFormat cf = NumberFormat.getNumberInstance();
                
                cf.setGroupingUsed(true);
                
                final NumberFormat fpf = NumberFormat.getNumberInstance();
                
                fpf.setGroupingUsed(false);
                
                fpf.setMaximumFractionDigits(2);

                log.info("finished: total=" + elapsed + "ms := setup("
                    + elapsed_setup + "ms) + build(" + elapsed_build
                    + "ms) +  write(" + elapsed_write + "ms); nentries="
                    + plan.nentries + ", branchingFactor=" + plan.m + ", nnodes="
                    + nnodesWritten + ", nleaves=" + nleavesWritten+", length="+
                    fpf.format(((double) checkpoint.length / Bytes.megabyte32))
                    + "MB"+", rate="+fpf.format(mbPerSec)+"MB/sec");

            }
            
            return checkpoint;
            
        } catch (Exception ex) {
            
            /*
             * Note: The output file is deleted if the build fails.
             */
            deleteOutputFile();
            
            // Re-throw exception
            throw ex;
            
        } catch (Throwable ex) {

            /*
             * Note: The output file is deleted if the build fails.
             */
            deleteOutputFile();

            // Masquerade exception.
            throw new RuntimeException(ex);

        } finally {

            /*
             * make sure that the temporary file gets deleted regardless.
             */
            if (leafBuffer != null && leafBuffer.isOpen()) {
                try {
                    leafBuffer.close(); // also deletes the file if any.
                } catch (Throwable t) {
                    log.warn(t,t);
                }
            }
            
            /*
             * make sure that the temporary file gets deleted regardless.
             */
            if (nodeBuffer != null && nodeBuffer.isOpen()) {
                try {
                    nodeBuffer.close(); // also deletes the file if any.
                } catch (Throwable t) {
                    log.warn(t,t);
                }
            }
            
        }
        
    }

    /**
     * Used to make sure that the output file is deleted unless it was
     * successfully processed.
     */
    private void deleteOutputFile() {
    
        if (out != null && out.getChannel().isOpen()) {
            
            try {
            
                FileLockUtility.closeFile(outFile, out);
                // out.close();
                
            } catch (Throwable t) {
             
                log.error("Ignoring: " + t, t);
                
            }
        
        }

        if(!outFile.delete()) {
            
            log.warn("Could not delete: file=" + outFile.getAbsolutePath());
            
        }
        
    }
    
    /**
     * <p>
     * Flush a node or leaf that has been closed (no more data will be added).
     * </p>
     * <p>
     * Note: When a node or leaf is flushed we write it out to obtain its
     * address and set that address on its direct parent using
     * {@link #addChild(SimpleNodeData, long)}. This also updates the per-child
     * counters of the #of entries spanned by a node.
     * </p>
     */
    protected void flush(AbstractSimpleNodeData node) throws IOException {

        final int h = node.level;

        // the index into the level for this node or leaf.
        final int col = writtenInLevel[h];

        assert col < plan.numInLevel[h];

        if (DEBUG)
            log.debug("closing " + (node.isLeaf() ? "leaf" : "node") + "; h="
                    + h + ", col=" + col + ", max=" + node.max + ", nkeys="
                    + node.keys.getKeyCount());
        
        // Note: This uses shared buffers!
        final long addr = writeNodeOrLeaf(node);

        final SimpleNodeData parent = getParent(node);
        
        if(parent != null) {

            // #of entries spanned by this node.
            final int nentries = node.getEntryCount();
            
            addChild(parent, addr, nentries);
            
        }

//        if (col + 1 < plan.numInLevel[h]) {
//
//            int max = plan.numInNode[h][col + 1];
//
//            parent.reset(max);
//
//        }

        writtenInLevel[h]++;        

    }

    /**
     * Record the persistent address of a child on its parent and the #of
     * entries spanned by that child. If all children on the parent become
     * assigned then the parent is closed.
     * 
     * @param parent
     *            The parent.
     * @param childAddr
     *            The address of the child (node or leaf).
     * @param nentries
     *            The #of entries spanned by the child (node or leaf).
     * 
     * @throws IOException
     */
    protected void addChild(SimpleNodeData parent, long childAddr, int nentries)
            throws IOException {

        if (parent.nchildren == parent.max) {
            /*
             * If there are more nodes to be filled at this level then prepare
             * this node to receive its next values/children.
             */
            final int h = parent.level;

            /*
             * the index into the level for this node. note that we subtract one
             * since the node is full and was already "closed". what we are
             * trying to figure out here is whether the node may be reset so as
             * to allow more children into what is effectively a new node or
             * whether there are no more nodes allowed at this level of the
             * output tree.
             */
            final int col = writtenInLevel[h] - 1;

            if (col + 1 < plan.numInLevel[h]) {

                int max = plan.numInNode[h][col + 1];

                parent.reset(max);

            } else {

                /*
                 * the data is driving us to populate more nodes in this level
                 * than the plan allows for the output tree. this is either an
                 * error in the control logic or an error in the plan.
                 */
                throw new AssertionError();

            }

        }

        // assert parent.nchildren < parent.max;

        if(DEBUG)
            log.debug("setting child at index=" + parent.nchildren
                + " on node at level=" + parent.level + ", col="
                + writtenInLevel[parent.level] + ", addr="
                + addressManager.toString(childAddr));
        
        int nchildren = parent.nchildren;
        
        parent.childAddr[nchildren] = childAddr;
        
        parent.childEntryCount[nchildren] = nentries;

        parent.nentries += nentries;
        
        parent.nchildren++;

        if( parent.nchildren == parent.max ) {
            
            flush(parent);
            
        }
        
    }
    
    /**
     * Copies the first key of a new leaf as a separatorKey for the appropriate
     * parent (if any) of that leaf. This must be invoked when the first key is
     * set on that leaf. However, it must not be invoked on the first leaf.
     * 
     * @param leaf
     *            The current leaf. The first key on that leaf must be defined.
     */
    protected void addSeparatorKey(SimpleLeafData leaf) {
        
        SimpleNodeData parent = getParent(leaf);
        
        if( parent != null ) {

            addSeparatorKey(parent,leaf);
            
        }
        
    }

    /**
     * Copies the first key of a new leaf as a separatorKey for the appropriate
     * parent (if any) of that leaf.
     * 
     * @param parent
     *            A parent of that leaf (non-null).
     * @param leaf
     *            The current leaf. The first key on the leaf must be defined.
     */
    private void addSeparatorKey(SimpleNodeData parent, SimpleLeafData leaf) {

        if(parent==null) {
            
            throw new AssertionError();
            
        }
        
        /*
         * The maximum #of keys for a node is one less key than the maximum #of
         * children for that node.
         */ 
        final int maxKeys = parent.max - 1;
        
        MutableKeyBuffer parentKeys = (MutableKeyBuffer) parent.keys;
        
        if( parentKeys.nkeys < maxKeys ) {
            
            /*
             * Copy the first key from the leaf into this parent, incrementing
             * the #of keys in the parent.
             */

            if (DEBUG)
                log.debug("setting separatorKey on node at level "
                        + parent.level + ", col="
                        + writtenInLevel[parent.level]);

            /*
             * copy the first key from the leaf into the next free position on
             * the parent.
             */
            parentKeys.keys[parentKeys.nkeys++] = leaf.keys.getKey(0);
//            parent.copyKey(parentKeys.nkeys++, leaf, 0 );

        } else {

            /*
             * Delegate to the parent recursively until we find the first parent
             * into which the separatorKey can be inserted.
             */

            addSeparatorKey(getParent(parent),leaf);
            
        }
        
    }
    
    /**
     * Return the parent of a node or leaf in the {@link #stack}. 
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The parent or null iff <i>node</i> is the root node or leaf.
     */
    protected SimpleNodeData getParent(AbstractSimpleNodeData node) {
        
        if(node.level==0) {
        
            return null;
            
        }
        
        return (SimpleNodeData)stack[node.level-1];
        
    }
    
    /**
     * Serialize, compress, and write the node or leaf onto the appropriate
     * output channel.
     * 
     * @return The address that may be used to read the compressed node or leaf
     *         from the file. Note that the address of a node is relative to the
     *         start of the node channel and therefore must be adjusted before
     *         reading the node from the final index segment file.
     */
    protected long writeNodeOrLeaf(AbstractSimpleNodeData node)
            throws IOException {
        
        return node.isLeaf() ? writeLeaf((SimpleLeafData) node)
                : writeNode((SimpleNodeData) node);
        
    }

    /**
     * Serialize and write the leaf onto the {@link #leafBuffer}.
     * <p>
     * Note: For leaf addresses we know the absolute offset into the
     * {@link IndexSegmentStore} where the leaf will wind up so we encoded the
     * address of the leaf using the {@link IndexSegmentRegion#BASE} region.
     * <p>
     * Note: In order to write out the leaves using a double-linked list with
     * prior-/next-leaf addresses we have to use a "write behind" strategy.
     * Instead of writing out the leaf as soon as it is serialized, we save the
     * unencoded address and a copy of the serialized record on private member
     * fields. When we serialize the next leaf (or if we learn that we have no
     * more leaves to serialize because {@link IndexSegmentPlan#nleaves} EQ
     * {@link #nleavesWritten}) then we patch the serialized representation of
     * the prior leaf and write it on the store at the previously obtained
     * address, thereby linking the leaves together in both directions. It is
     * definately confusing.
     * 
     * @return The address that may be used to read the leaf from the file
     *         backing the {@link IndexSegmentStore}.
     */
    protected long writeLeaf(final SimpleLeafData leaf)
        throws IOException
    {

        /*
         * The encoded address of the leaf that we allocated here. The encoded
         * address will be relative to the BASE region.
         */ 
        final long addr;
        {
            
            // serialize the leaf, obtaining a view onto a internal buffer.
            final ByteBuffer buf = nodeSer.putLeaf(leaf);

            // Allocate a record for the leaf on the temporary store.
            final long addr1 = leafBuffer.allocate(buf.remaining());

            // encode the address assigned to the serialized leaf.
            addr = encodeLeafAddr(addr1);
            
            if (nleavesWritten > 0) {

                if(DEBUG)
                    log.info("Writing leaf: priorLeaf="+addrPriorLeaf+", nextLeaf="+addr);
                else if (INFO)
                    System.err.print("."); // wrote a leaf.

                // patch representation of the previous leaf
                nodeSer.updateLeaf(bufLastLeaf, addrPriorLeaf, addr/*addrNextLeaf*/);

                // write the previous leaf onto the store.
                leafBuffer.update(bufLastLeafAddr, 0/*offset*/, bufLastLeaf);
                
                // the encoded address of the leaf that we just wrote out.
                addrPriorLeaf = encodeLeafAddr(bufLastLeafAddr);
                
            }
            
            // clear the old data.
            bufLastLeaf.clear();
            
            if (buf.remaining() > bufLastLeaf.capacity()) {
                
                // reallocate buffer since too small.
                bufLastLeaf = ByteBuffer.allocate(buf.remaining() * 2);
                
            }
            
            // copy in the new data
            bufLastLeaf.put(buf); bufLastLeaf.flip();
            
            // the address allocated for the leaf in the temp store.
            bufLastLeafAddr = addr1;
            
        }

        if (nleavesWritten == 0) {
            
            // encoded addr of the 1st leaf - update only for the first leaf that we allocate.
            addrFirstLeaf = addr;
            
        }
        
        // encoded addr of the last leaf - update for each leaf that we allocate.
        addrLastLeaf = addr;
        
        // the #of leaves written so far.
        nleavesWritten++;

        if (plan.nleaves == nleavesWritten) {
            
            /*
             * Force out the last leaf.
             */
    
            if(DEBUG)
                log.debug("Writing leaf: priorLeaf="+addrPriorLeaf+", nextLeaf="+0L);
            else if (INFO)
                System.err.print("."); // wrote a leaf.
            
            // patch representation of the last leaf.
            nodeSer.updateLeaf(bufLastLeaf, addrPriorLeaf, 0L/*addrNextLeaf*/);

            // write the last leaf onto the store.
            leafBuffer.update(bufLastLeafAddr, 0/*offset*/, bufLastLeaf);
            
        }

        return addr;
        
    }

    /**
     * Encode the address of a leaf.
     * <p>
     * Note: This updates {@link #maxNodeOrLeafLength} as a side-effect.
     * 
     * @param addr1
     *            The address of a leaf as allocated by the {@link #leafBuffer}
     * 
     * @return The encoded address of the leaf relative to the
     *         {@link IndexSegmentRegion#BASE} region where it will appear once
     *         the leaves have been copied onto the output file.
     */
    private long encodeLeafAddr(long addr1) {

        final int nbytes = addressManager.getByteCount(addr1);

        if (nbytes > maxNodeOrLeafLength) {

            // track the largest node or leaf written.
            maxNodeOrLeafLength = nbytes;

        }

        /*
         * Note: The offset is adjusted by the size of the checkpoint record
         * such that the offset is correct for the generated file NOT the buffer
         * into which the leaves are being written.
         */
        final long offset = addressManager.getOffset(addr1)
                + IndexSegmentCheckpoint.SIZE;
        
        // Encode the address of the leaf.
        final long addr = addressManager.toAddr(nbytes, IndexSegmentRegion.BASE
                .encodeOffset(offset));
        
        return addr;

    }

    /*
     * Data used to chain the leaves together in a prior/next double-linked
     * list.
     */
    
    /**
     * The unencoded address of the previous leaf written on the
     * {@link #leafBuffer}.
     */
    private long addrPriorLeaf = 0L;
    
    /**
     * The address of the last leaf allocated (but not yet written) on the
     * {@link #leafBuffer} (the {@link TemporaryRawStore} on which the leaves
     * are buffered).
     * <p>
     * Note: This address is NOT encoded for the {@link IndexSegmentStore}. It
     * is used to specify which record we want to update the record on the
     * {@link #leafBuffer} using
     * {@link TemporaryRawStore#update(long, int, ByteBuffer)}.
     * <p>
     * Note: This is used to patch the representation of the last serialized
     * leaf in {@link #bufLastLeaf} with the address of the next leaf in key
     * order.
     * 
     * @see #writeLeaf(SimpleLeafData)
     * @see #writePriorLeaf(long nextLeafAddr)
     */
    private long bufLastLeafAddr = 0L;
    
    /**
     * Buffer holds a copy of the serialized representation of the last leaf.
     * This buffer is reset and written by {@link #writeLeaf(SimpleLeafData)}.
     * The contents of this buffer are used by {@link #writePriorLeaf(long)} to
     * write out the serialized representation of the previous leaf in key order
     * after it has been patched to reflect the prior and next leaf addresses.
     * 
     * @todo must re-allocate if not large enough.
     */
    private ByteBuffer bufLastLeaf = ByteBuffer.allocate(20*Bytes.kilobyte32);
    
    /**
     * Serialize and write the node onto the {@link #nodeBuffer}.
     * 
     * @return An <em>relative</em> address that must be correctly decoded
     *         before you can read the compressed node from the file. This value
     *         is also set on {@link SimpleNodeData#addr}.
     * 
     * @see SimpleNodeData
     * @see IndexSegmentRegion
     * @see IndexSegmentAddressManager
     */
    protected long writeNode(final SimpleNodeData node)
        throws IOException
    {

        final long addr2;
        {

            // serialize node.
            final ByteBuffer buf = nodeSer.putNode(node);

            // write the node on the buffer.
            addr2 = nodeBuffer.write(buf);
            
        }

        final long offset = addressManager.getOffset(addr2);
        
        final int nbytes = addressManager.getByteCount(addr2);
        
        if( nbytes > maxNodeOrLeafLength ) { 
         
            // track the largest node or leaf written.
            maxNodeOrLeafLength = nbytes;
            
        }

        // the #of nodes written so far.
        nnodesWritten++;

        if(INFO)
        System.err.print("x"); // wrote a node.

        /*
         * Encode the node address. Since we do not know the offset of the NODE
         * region in advance this address gets encoded as relative to the start
         * of the NODE region in the file.
         */
        final long addr = addressManager.toAddr(nbytes, IndexSegmentRegion.NODE
                .encodeOffset(offset));
        
        node.addr = addr;
        
        return addr;
        
    }

    /**
     * <p>
     * Writes the complete file format for the index segment. The file is
     * divided up as follows:
     * <ol>
     * 
     * <li>fixed length metadata record (required)</li>
     * <li>leaves (required)</li>
     * <li>nodes (may be empty)</li>
     * <li>the bloom filter (optional)</li>
     * <li>the extension metadata record (required, but extensible)</li>
     * </ol>
     * </p>
     * <p>
     * The index segment metadata is divided into a base
     * {@link IndexSegmentCheckpoint} record with a fixed format containing only
     * essential data and additional metadata records written at the end of the
     * file including the optional bloom filter and the required
     * {@link IndexMetadata} record. The latter is where we write variable
     * length metadata including the _name_ of the index, or additional metadata
     * defined by a specific class of index.
     * </p>
     * <p>
     * Once all nodes and leaves have been buffered we are ready to start
     * writing the data. We skip over a fixed size metadata record since
     * otherwise we are unable to pre-compute the offset to the leaves and hence
     * the addresses of the leaves. The node addresses are written in an
     * encoding that requires active translation by the receiver who must be
     * aware of the offset to the start of the node region. We can not write the
     * metadata record until we know the size and length of each of these
     * regions (leaves, nodes, and the bloom filter, or other metadata records)
     * since that information is required in order to be able to form their
     * addresses for insertion in the metadata record.
     * </p>
     * 
     * @param outChannel
     * 
     * @param commitTime
     * 
     * @throws IOException
     */
    protected IndexSegmentCheckpoint writeIndexSegment(FileChannel outChannel,
            final long commitTime) throws IOException {

        /*
         * All nodes and leaves have been written. If we wrote any nodes
         * onto the temporary channel then we also have to bulk copy them
         * into the output channel.
         */
        final long offsetLeaves = IndexSegmentCheckpoint.SIZE;
        final long extentLeaves;
        final long offsetNodes;
        final long extentNodes;
        final long offsetBlobs;
        final long extentBlobs;
        final long addrRoot;

        /*
         * Direct copy the leaves from their buffer into the output file. If the
         * buffer was backed by a file then that file will be deleted as a
         * post-condition on the index build operation.
         */
        {

            // Skip over the metadata record at the start of the file.
            outChannel.position(offsetLeaves);

            // Transfer the leaf buffer en mass onto the output channel.
            extentLeaves = leafBuffer.getBufferStrategy().transferTo(out);

            if (nodeBuffer != null) {

                // The offset to the start of the node region.
                offsetNodes = IndexSegmentCheckpoint.SIZE + extentLeaves;
                
                assert outChannel.position() == offsetNodes;
                
            } else {
                
                // zero iff there are no nodes.
                offsetNodes = 0L;
                
            }
            
            // Close the buffer.
            leafBuffer.close();
            
        }

        /*
         * Direct copy the node index from the buffer into the output file. If
         * the buffer was backed by a file then that file will be deleted as a
         * post-condition on the index build operation.
         */
        if (nodeBuffer != null) {

            // transfer the nodes en mass onto the output channel.
            extentNodes = nodeBuffer.getBufferStrategy().transferTo(out);
            
            // Close the buffer.
            nodeBuffer.close();

            // Note: already encoded relative to NODE region.
            addrRoot = (((SimpleNodeData)stack[0]).addr);

            if (INFO)
                log.info("addrRoot(Node): "+addrRoot+", "+addressManager.toString(addrRoot));
            
        } else {

            /*
             * The tree consists of just a root leaf.
             */

            // This MUST be 0L if there are no leaves.
            extentNodes = 0L;
            
            // Address of the root leaf.
            addrRoot = addrLastLeaf;
//                addressManager.toAddr(lastLeafSize,IndexSegmentRegion.BASE.encodeOffset(lastLeafOffset));

            if (INFO)
                log.info("addrRoot(Leaf): " + addrRoot + ", "
                        + addressManager.toString(addrRoot));
            
        }

        /*
         * Direct copy the optional blobBuffer onto the output file.
         */
        if (blobBuffer == null) {

            // No blobs region.
            offsetBlobs = extentBlobs = 0L;

        } else {
            
            // #of bytes written so far on the output file.
            offsetBlobs = out.length(); 

            // seek to the end of the file.
            out.seek(offsetBlobs);

            // transfer the nodes en mass onto the output channel.
            extentBlobs = blobBuffer.getBufferStrategy().transferTo(out);
            
            // Close the buffer.
            blobBuffer.close();

        }
        
        /*
         * If the bloom filter was constructed then serialize it on the end
         * of the file.
         */
        final long addrBloom;
        
        if( bloomFilter == null ) {
    
            addrBloom = 0L;
            
        } else {

            // serialize the bloom filter.
            final byte[] bloomBytes = SerializerUtil.serialize(bloomFilter);

            // #of bytes written so far on the output file.
            final long offset = out.length(); 

            // seek to the end of the file.
            out.seek(offset);
            
            // write the serialized bloom filter.
            out.write(bloomBytes, 0, bloomBytes.length);
            
            // Address of the region containing the bloom filter (one record).
            addrBloom = addressManager.toAddr(bloomBytes.length,
                    IndexSegmentRegion.BASE.encodeOffset(offset));
            
        }
        
        /*
         * Write out the metadata record.
         */
        final long addrMetadata;
        {

            /*
             * Serialize the metadata record.
             */
            final byte[] metadataBytes = SerializerUtil.serialize(metadata);

            // #of bytes written so far on the output file.
            final long offset = out.length(); 

            // seek to the end of the file.
            out.seek(offset);
            
            // write the serialized extension metadata.
            out.write(metadataBytes, 0, metadataBytes.length);

            // Address of the region containing the metadata record (one record)
            addrMetadata = addressManager.toAddr(metadataBytes.length,
                    IndexSegmentRegion.BASE.encodeOffset(offset));
            
        }
        
        /*
         * Seek to the start of the file and write out the checkpoint record.
         */
        {

//            // timestamp for the index segment.
//            final long now = System.currentTimeMillis();
            
            outChannel.position(0);
            
            final IndexSegmentCheckpoint md = new IndexSegmentCheckpoint(
                    addressManager.getOffsetBits(), plan.height, plan.nleaves,
                    nnodesWritten, plan.nentries, maxNodeOrLeafLength,
                    offsetLeaves, extentLeaves, offsetNodes, extentNodes,
                    offsetBlobs, extentBlobs, addrRoot, addrMetadata,
                    addrBloom, addrFirstLeaf, addrLastLeaf, out.length(),
                    segmentUUID, commitTime);

            md.write(out);
            
            if(INFO)
                log.info(md.toString());

            // save the index segment resource description for the caller.
            this.segmentMetadata = new SegmentMetadata(outFile, //out.length(),
                    segmentUUID, commitTime);
            
            return md;
            
        }

    }

    /**
     * The description of the constructed {@link IndexSegment} resource.
     * 
     * @throws IllegalStateException
     *             if requested before the build operation is complete.
     */
    public IResourceMetadata getSegmentMetadata() {
        
        if (segmentMetadata == null) {

            throw new IllegalStateException();
            
        }
        
        return segmentMetadata;
        
    }
    private SegmentMetadata segmentMetadata = null;
    
    /**
     * Abstract base class for classes used to construct and serialize nodes and
     * leaves written onto the index segment.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract protected static class AbstractSimpleNodeData implements IAbstractNodeData {

        /**
         * The level in the output tree for this node or leaf (origin zero). The
         * root is always at level zero (0).
         */
        final int level;
        final int m;

        // mutable.
        IKeyBuffer keys;
        
        /**
         * We precompute the #of children to be assigned to each node and the
         * #of values to be assigned to each leaf and store that value in this
         * field. While the field name is "max", this is the exact that must be
         * assigned to the node.
         */
        int max = -1;

        protected AbstractSimpleNodeData(int level,int m,byte[][] keys) {
            
            this.level = level;
            
            this.m = m;
            
            this.keys = new MutableKeyBuffer(m);
            
        }

        protected void reset(int max) {
            
            this.keys = new MutableKeyBuffer(m);
            
            this.max = max;
            
        }
        
        final public int getBranchingFactor() {
            
            return m;
            
        }

        final public int getKeyCount() {

            return keys.getKeyCount();
            
        }

        final public IKeyBuffer getKeys() {

            return keys;
            
        }

    }
    
    /**
     * A class that can be used to (de-)serialize the data for a leaf without
     * any of the logic for operations on the leaf.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class SimpleLeafData extends AbstractSimpleNodeData implements ILeafData {

        // mutable.
        
//        /**
//         * The ordinal position of this leaf in the {@link IndexSegment}.
//         */
//        int leafIndex;
        
        /**
         * The values stored in the leaf.
         */
        final byte[][] vals;
        
        public byte[][] getValues() {
            
            return vals;
            
        }
        
        /**
         * Allocated iff delete markers are maintained.
         */
        final boolean[] deleteMarkers;
        
        /**
         * Allocated iff version timestamps are maintained.
         */
        final long[] versionTimestamps;

        public SimpleLeafData(int level,int m, IndexMetadata metadata) {

            super(level,m,new byte[m][]);

            this.vals = new byte[m][];

            this.deleteMarkers = metadata.getDeleteMarkers() ? new boolean[m]
                    : null;

            this.versionTimestamps = metadata.getVersionTimestamps() ? new long[m]
                    : null;
            
        }
        
//        /**
//         * 
//         * @param max The #of values that must be assigned to this leaf.
//         */
//        public void reset(int max) {
//
//            super.reset(max);
//            
//        }
        
        final public int getValueCount() {
            
            return keys.getKeyCount();
            
        }

//        final public Object[] getValues() {
//            
//            return vals;
//            
//        }

        final public boolean isLeaf() {
            
            return true;
            
        }

        final public int getEntryCount() {
            
            return keys.getKeyCount();
            
        }

        public void copyKey(int index, OutputStream os) {
            
            try {
                
                os.write(keys.getKey(index));
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
    
        final public boolean isNull(int index) {
            
            if(vals[index]==null) {
            
                return true;
                
            }
            
            return false;
            
        }
        
        public void copyValue(int index, OutputStream os) {

            final byte[] val = vals[index];

            if (val == null)
                throw new UnsupportedOperationException();
            
            try {
                
                os.write(val);
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        public boolean getDeleteMarker(int index) {

            if (deleteMarkers == null)
                throw new UnsupportedOperationException();

            return deleteMarkers[index];

        }

        public long getVersionTimestamp(int index) {

            if (versionTimestamps == null)
                throw new UnsupportedOperationException();

            return versionTimestamps[index];

        }

        public boolean hasDeleteMarkers() {
            
            return deleteMarkers!=null;
            
        }

        public boolean hasVersionTimestamps() {
            
            return versionTimestamps!=null;
            
        }

    }

    /**
     * A class that can be used to (de-)serialize the data for a node without
     * any of the logic for operations on the node.
     * <p>
     * Note: All node addresses that are internal to a node and reference a
     * child node (vs a leaf) are correct relative to the start of the
     * {@link IndexSegmentRegion#NODE} region. This is an unavoidable
     * consequence of serializing the nodes before we have the total offset to
     * the start of the {@link IndexSegmentRegion#NODE} region.
     * 
     * @see IndexSegmentRegion
     * @see IndexSegmentAddressManager
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class SimpleNodeData extends AbstractSimpleNodeData
            implements INodeData {

        // mutable.
        
        /**
         * The relative address at which this node was written on the temporary
         * channel. This is a negative integer. If you flip the sign then it
         * encodes a relative offset to the start of the index node block and
         * the correct size for the compressed node.
         */
        long addr = 0L;
        
        /**
         * The address at which the child nodes were written. This is a negative
         * integer iff the child is a node and a positive integer iff the child
         * is a leaf. When it is a negative integer, you must flip the sign to
         * obtain a relative offset to the start of the index node block and the
         * correct size for the compressed node. The actual offset of the index
         * node block must be added to the relative offset before you can use
         * this to address into the output file.
         */
        final long[] childAddr;
        
        /**
         * This tracks the #of defined values in {@link #childAddr} separately
         * from the #of defined keys. The reason that we do this is that the
         * logic for handling a leaf eviction and recording the address of the
         * child and the separator key for the _next_ child requires an
         * awareness of the intermediate state - when we have filled in the
         * childAddr for the last leaf but not yet filled in the separatorKey
         * for the next leaf.
         */
        int nchildren = 0;
        
        /**
         * #of entries spanned by this node.
         */
        int nentries;
        
        /**
         * The #of entries spanned by each child of this node.
         */
        int[] childEntryCount;
        
        final public int getEntryCount() {
            
            return nentries;
            
        }

        final public int[] getChildEntryCounts() {
            
            return childEntryCount;
            
        }
        
        public SimpleNodeData(int level,int m) {

            super(level,m, new byte[m-1][]);
            
            this.childAddr = new long[m];
            
            this.childEntryCount = new int[m];
            
        }
        
        /**
         * Reset counters and flags so that the node may be reused.
         * 
         * @param max
         *            The new limit on the #of children to fill on this node.
         */
        public void reset(int max) {

            super.reset(max);
            
            addr = 0;
            
            nchildren = 0;

            nentries = 0;
            
        }

        final public long[] getChildAddr() {
            
            return childAddr;
            
        }

        final public int getChildCount() {

            return keys.getKeyCount() + 1;
            
        }

        final public boolean isLeaf() {
            
            return false;
            
        }

        public void copyKey(int index, OutputStream os) {
            
            try {
                
                os.write(keys.getKey(index));
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
        
    }
    
    /**
     * Factory does not support node or leaf creation.
     */
    protected static class NOPNodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new NOPNodeFactory();

        private NOPNodeFactory() {
        }

        public ILeafData allocLeaf(IIndex btree, long addr,
                int branchingFactor, IKeyBuffer keys, byte[][] values,
                long[] versionTimestamps, boolean[] deleteMarkers,
                long priorAddr, long nextAddr) {
            
            throw new UnsupportedOperationException();
            
        }

        public INodeData allocNode(IIndex btree, long addr,
                int branchingFactor, int nentries, IKeyBuffer keys,
                long[] childAddr, int[] childEntryCount) {

            throw new UnsupportedOperationException();
            
        }

    }
    
}
