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
 * Created on Dec 5, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.Journal;
import com.bigdata.objndx.DistributedIndex.PartitionMetadata;

/**
 * Builds an {@link IndexSegment} given a source btree and a target branching
 * factor. There are two main use cases:
 * <ol>
 * <li>Evicting a key range of an index into an optimized on-disk index. In
 * this case, the input is a {@link BTree} that is ideally backed by a fully
 * buffered {@link IRawStore} so that no random reads are required.</li>
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
 * </ol>
 * 
 * Note: In order for the stack to be written in a contiguous block we either
 * have to buffer them in memory or have to write them onto a temporary file and
 * then copy them into place after the last leaf has been processed. The code
 * currently uses a temporary file for this purpose. This space demand was not
 * present in West's algorithm because it did not attempt to place the leaves
 * contiguously onto the store.
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
 * @todo Support merging index segments. This will probably be a merge sort
 *       using two leaf scans in parallel followed by a scan over the merged
 *       result that builds the index segment. The latter leaf scan will be
 *       somewhat special but is required since we need to known the actual #of
 *       entries before we can build the tree. Alternatively, we could just
 *       compute the #of entries and then run two leaf scans merging as we build
 *       the index segment itself.
 * 
 * @todo Support external sorting? This is tricky since the key and value data
 *       type and comparator must have exactly the same semantics as in the
 *       {@link IBTree} which is a bit much to expect from an external sort
 *       routine. Also, since the data are pre-ordered a merging scan seems
 *       likely to be far more efficient.
 * 
 * FIXME change the way in which child addresses are serialized so that we can
 * continue to pack them and re-enable packing in the {@link NodeSerializer}.
 */
public class IndexSegmentBuilder {

    /**
     * Logger for building {@link IndexSegment}s.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentBuilder.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The file mode used to open the file on which the {@link IndexSegment} is
     * written.
     * 
     * @todo consider the file mode and buffering. We should at least buffer
     *       several pages of data per write and can experiment with writing
     *       through (vs caching writes in the OS layer). The file does not need
     *       to be "live" until it is completely written, so there is no need to
     *       update file metadata until the end of the build process.
     * 
     * @todo Consider using {@link FileOutputStream#FileOutputStream(File, boolean)} to open
     *       the temporary file in an append only mode and then get the
     *       {@link FileChannel} from {@link FileOutputStream#getChannel()}.
     *       Does this improve performance? Can we still read from the channel?
     *       Try this on the {@link Journal} as well, at least for cases where
     *       we will never read from the journal (i.e., fully buffered).
     */
    final String mode = "rw"; // also rws or rwd
    
    /**
     * The buffer used to serialize individual stack and leaves before they are
     * written onto the file.
     * 
     * @todo The capacity of this buffer is a SWAG. It is too large for most
     *       purposes but it is unlikely to be too small for most purposes. If
     *       you see a buffer overflow exception then you may have extremely
     *       long keys and/or very large values. The BTree class has the same
     *       problem with the buffer that it uses to serialize stack and leaves,
     *       but its stack and leaves are smaller since it uses a smaller
     *       branching factor.
     */
    final ByteBuffer buf = ByteBuffer.allocateDirect(1*Bytes.megabyte32);

    /**
     * Buffer for compressed records.
     * 
     * @todo The capacity of this buffer is a SWAG.
     */
    final ByteBuffer cbuf = ByteBuffer.allocateDirect(Bytes.megabyte32/2);
    
    /**
     * Compressor used for stack and leaves.
     * 
     * @todo review choice of best speed vs best compression and possibly
     *       elevate to a constuctor parameter.
     * 
     * @todo define an interface for a record compressor and write the class of
     *       the compressor used into the metadata record so that we can
     *       experiment with other compression schemes in a backward compatible
     *       manner. If the compressor has parameters then those can be capture
     *       by subclassing or instance data.
     */
    final RecordCompressor compressor = new RecordCompressor();

    /**
     * The file specified by the caller on which the {@link IndexSegment} is
     * written.
     */
    protected final File outFile;
    
    /**
     * The temporary created to hold stack unless the index segment will consist
     * of just a root leaf. When created, this file is deleted regardless of the
     * outcome of the operation.
     */
    protected final File tmpFile;
    
    /**
     * The file on which the {@link IndexSegment} is written. The file is closed
     * regardless of the outcome of the operation.
     */
    protected RandomAccessFile out = null;

    /**
     * The temporary file on which the stack destined for {@link IndexSegment}
     * will be written. The file is opened iff there are non-leaf stack in the
     * index segment. The file is closed regardless of the outcome of the
     * operation.
     */
    protected RandomAccessFile tmp = null;
    
    /**
     * The data type used for the keys in the btree.
     */
    final ArrayType keyType;

    /**
     * Used to serialize the stack and leaves of the output tree.
     */
    final NodeSerializer nodeSer;
    
    /**
     * The offset in the output file of the last leaf written onto that file.
     * Together with {@link #lastLeafSize} this is used to compute the
     * {@link Addr} of the prior leaf.
     */
    long lastLeafOffset = -1L;
    
    /**
     * The size in bytes of the last leaf written onto the output file (the size
     * of the compressed record that is actually written onto the output file
     * NOT the size of the serialized leaf before it is compressed). Together
     * with {@link #lastLeafOffset} this is used to compute the {@link Addr} of
     * the prior leaf.
     */
    int lastLeafSize = -1;

    /**
     * Tracks the maximum length of any serialized node or leaf.  This is used
     * to fill in one of the {@link PartitionMetadata} fields.
     */
    int maxNodeOrLeafLength = 0;

    /**
     * The #of stack written for the output tree. This will be zero if all
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
     * The stack of stack that are currently being populated. The first N-1
     * elements in this array are always stack while the last element is always
     * a leaf ({@link #leaf} is the same reference as the last element in this
     * array). The stack and the leaf in this array are reused rather than being
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
    final IndexSegmentPlan plan;
    
    /**
     * The process runtime in milliseconds.
     */
    final long elapsed;
    
    /**
     * <p>
     * Builds an index segment on the disk from a {@link BTree}. The index
     * segment will be written on the identified file, which must not exist. The
     * caller is responsible for updating the metadata required to locate the
     * index segment.
     * </p>
     * <p>
     * The store on which the btree exists should be read-only, e.g., a frozen
     * {@link Journal}. The typical scenario is that a {@link Journal} is
     * frozen when it overflows and a new direct buffer and backing file are
     * opened to absorb writes. In this scenario frozen journal is still fully
     * buffered, which means that the index build will perform zero IOs when
     * reading from the source {@link BTree}. Once all indices on the frozen
     * journal have been externalized as {@link IndexSegment}s the frozen
     * journal can be discarded.
     * </p>
     * <p>
     * With a branching factor of 4096 a tree of height 2 (three levels) could
     * address 68,719,476,736 entries - well beyond what we want in a given
     * index segment! Well before that the index segment should be split into
     * multiple files. The split point should be determined by the size of the
     * serialized leaves and stack, e.g., the amount of data on disk required by
     * the index segment and the amount of memory required to fully buffer the
     * index stack. While the size of a serialized node can be estimated easily,
     * the size of a serialized leaf depends on the kinds of values stored in
     * that index. The actual sizes are recorded in the
     * {@link PartitionMetadata} record in the header of the
     * {@link IndexSegment}.
     * </p>
     * 
     * @param outFile
     *            The file on which the index segment is written.
     * @param tmpDir
     *            The temporary directory in which the index stack are buffered
     *            during the build (optional - the default temporary directory
     *            is used if this is <code>null</code>).
     * @param btree
     *            The btree.
     * @param m
     *            The branching factor for the generated tree. This can be
     *            choosen with an eye to minimizing the height of the generated
     *            tree. (Small branching factors are permitted for testing, but
     *            generally you want something relatively large.)
     * 
     * @throws IOException
     */
    public IndexSegmentBuilder(File outFile, File tmpDir, AbstractBTree btree, int m)
            throws IOException {

        assert outFile != null;
        assert btree != null;
        assert m >= AbstractBTree.MIN_BRANCHING_FACTOR;
        
        final long begin = System.currentTimeMillis();
        
        // The data type used for the keys in the btree.
        keyType = btree.keyType;
        
        // Used to serialize the stack and leaves for the output tree.
        nodeSer = btree.nodeSer;

        // Create a plan for generating the output tree.
        plan = new IndexSegmentPlan(m,btree.size());

        /*
         * Setup a stack of stack (one per non-leaf level) and one leaf. These
         * are filled in based on the plan and the entries visited in the source
         * btree. Nodes and leaves are written out to their respective channel
         * each time they are complete as defined (by the plan) by the #of
         * children assigned to a node or values assigned to a leaf.
         */

        stack = new AbstractSimpleNodeData[plan.height + 1];

        // Note: assumes defaults to all zeros.
        writtenInLevel = new int[plan.height + 1];
        
        for (int h = 0; h < plan.height; h++) {

            SimpleNodeData node = new SimpleNodeData(h, m, keyType);

            node.max = plan.numInNode[h][0];
            
            stack[h] = node;
            
        }
        
        // the output leaf (reused for each leaf we populate).
        
        leaf = new SimpleLeafData(plan.height,m,keyType);

        leaf.max = plan.numInNode[plan.height][0];

        stack[plan.height] = leaf;

        /*
         * Setup for IO.
         */

        this.outFile = outFile;
        
        if (outFile.exists()) {
            throw new IllegalArgumentException("File exists: "
                    + outFile.getAbsoluteFile());
        }

        // the temporary file is used iff there are stack to write.
        tmpFile = (plan.nleaves > 1 ? File.createTempFile("index", ".seg",
                tmpDir) : null);

        final FileChannel outChannel;
        final FileChannel tmpChannel;
        
        try {

            /*
             * Open the output channel and get an exclusive lock.
             */
            
            out = new RandomAccessFile(outFile, mode);
            
            outChannel = out.getChannel();
            
            if (outChannel.tryLock() == null) {
                
                throw new IOException("Could not lock file: "
                        + outFile.getAbsoluteFile());
                
            }

            /*
             * Open the temporary channel and get an exclusive lock. We only do
             * this if there will be at least one node written, i.e., the output
             * tree will consist of more than just a root leaf.
             */

            if (tmpFile != null) {

                tmp = new RandomAccessFile(tmpFile, mode);

                tmpChannel = tmp.getChannel();

                if (tmpChannel.tryLock() == null) {

                    throw new IOException("Could not lock file: "
                            + tmpFile.getAbsoluteFile());

                }

            } else {
                
                /*
                 * Set to null iff temp file not opened because there are no
                 * stack to write.
                 */

                tmpChannel = null;
                
            }

            /*
             * Skip over the metadata record, which we have to write once we
             * know how the leaves and stack fit onto the file once they have
             * been serialized and compressed.
             * 
             * @todo we could serialize the record with zeros for values that we
             * do not know until after the index has been generated and then
             * patch them up afterwards. This would let us compute the final
             * record size while also permitting the use of Java default
             * serialization to store things like NEGINF, comparator, keySer,
             * valSer, and the record compressor.
             */
            outChannel.position(IndexSegmentMetadata.SIZE);
            
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

            // visit leaves in order in the source tree.
            Iterator sourceLeaves = btree.leafIterator();

            /*
             * The #of entries consumed in the current source leaf. When this
             * reaches the #of keys in the source leaf then we have to read the
             * next source leaf from the source leaf iterator.
             */
            Leaf sourceLeaf = (Leaf)sourceLeaves.next();
            
            int nconsumed = 0;
            
            int nsourceKeys = sourceLeaf.getKeyCount();

            for (int i = 0; i < plan.nleaves; i++) {

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

                    if( nconsumed == nsourceKeys ) {
                    
                        // Read another leaf from the source tree.
                        
                        sourceLeaf = (Leaf)sourceLeaves.next();
                        
                        nconsumed = 0;
                        
                        nsourceKeys = sourceLeaf.getKeyCount();
                                                
                    }
                    
                    leaf.copyKey(leaf.nkeys++, sourceLeaf, nconsumed);
                    
                    leaf.vals[j] = sourceLeaf.values[nconsumed];
                    
                    nconsumed++;

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
                close(leaf);
                
            }

            // Verify that all leaves were written out.
            assert plan.nleaves == nleavesWritten;
            
            // Verify that all nodes were written out.
            assert plan.nnodes == nnodesWritten;
            
            /*
             * All stack and leaves have been written. Now we prepare the final
             * metadata record. If we wrote any stack onto the temporary channel
             * then we also have to bulk copy them into the output channel.
             */
            final long offsetLeaves = IndexSegmentMetadata.SIZE;
            final long offsetNodes;
            final long addrRoot;
            
            if (tmpChannel != null) {

                /*
                 * Direct copy the node index from the temporary file into the
                 * output file and clear the reference to the temporary file.
                 * The temporary file itself will be deleted as a post-condition
                 * of the index build operation.
                 */
                
                offsetNodes = outChannel.position();
                
                if(offsetNodes + offsetLeaves > Integer.MAX_VALUE) {
                    
                    throw new IOException("Index segment exceeds int32 bytes.");
                    
                }
                
                // #of bytes to transfer.
                long nodeBytes = tmp.length();
                
                /* 
                 * Transfer data.
                 */
                
                // Set the fromPosition on source channel.
                tmpChannel.position(0);

                // Extend the output file (@todo it looks like this should be required so look into this further!)
//                out.setLength(offsetNodes+nodeBytes);
                
                // Transfer the data.
                long nxfer = outChannel.transferFrom(tmpChannel, /* toPosition */
                        offsetNodes, /* count */ nodeBytes);
                
                // Verify transfer is complete.
                if( nxfer != nodeBytes ) {
                    throw new IOException("Expected to transfer "+nodeBytes+", but transferred "+nxfer);
                }
                
                // Close the temporary file - also releases the lock.
                tmp.close();

                /*
                 * The addrRoot is computed from the offset on the tmp channel
                 * at which we wrote the root node plus the offset on the output
                 * channel to which we transferred the contents of the temporary
                 * channel. This provides a correct address for the root node in
                 * the output file.  (The addrRoot is the only _node_ address
                 * that is correct for the output file. All internal node
                 * references require some translation.)
                 */
                
                long addr = - ( ((SimpleNodeData)stack[0]).addr ); // flip sign
                
                int offset = (int)offsetNodes + Addr.getOffset(addr); // add offset.
                
                int nbytes = Addr.getByteCount(addr); // #of bytes in root node.
                
                addrRoot = Addr.toLong(nbytes, offset); // form correct addr.
                
                log.info("addrRoot(Node): "+addrRoot+", "+Addr.toString(addrRoot));
                
            } else {

                /*
                 * The tree consists of just a root leaf.
                 */
                
                offsetNodes = 0L;

                addrRoot = Addr.toLong(lastLeafSize, (int)lastLeafOffset);
                
                log.info("addrRoot(Leaf): "+addrRoot+", "+Addr.toString(addrRoot));
                
            }

            /*
             * Seek to the start of the file and write out the metadata record.
             */
            {

                // timestamp for the index segment.
                final long now = System.currentTimeMillis();
                
                // @todo name of the index segment - drop this field?
                final String name = "<no name>";
                
                outChannel.position(0);
                
                IndexSegmentMetadata md = new IndexSegmentMetadata(m,
                        plan.height, keyType, plan.nleaves, nnodesWritten,
                        plan.nentries, maxNodeOrLeafLength, offsetLeaves,
                        offsetNodes, addrRoot, out.length(), now,
                        name);
                
                md.write(out);
                
                log.info(md.toString());
                
            }
                        
            /*
             * Flush this channel to disk and close the channel. This also
             * releases our lock. We are done and the index segment is ready for
             * use.
             */
            outChannel.force(true);
            out.close(); // also releases the lock.
//            out = null;

            /*
             * log run time.
             */
            
            elapsed = System.currentTimeMillis() - begin;
            
            log.info("finished: elapsed=" + elapsed + "ms, nentries="
                    + plan.nentries + ", branchingFactor=" + m + ", nnodes="
                    + nnodesWritten + ", nleaves=" + nleavesWritten);

        } catch (Throwable ex) {

            /*
             * make sure that the output file is deleted unless it was
             * successfully processed.
             */
            if (out != null && out.getChannel().isOpen()) {
                try {
                    out.close();
                } catch (Throwable t) {
                }
            }
            outFile.delete();
            throw new RuntimeException(ex);

        }
        
        finally {

            /*
             * make sure that the temporary file gets deleted regardless.
             */
            if (tmp != null && tmp.getChannel().isOpen()) {
                try {
                    tmp.close();
                } catch (Throwable t) {
                }
            }
            if (tmpFile != null) {
                if(!tmpFile.delete()) {
                    log.warn("Could not delete temporary file: "
                            + tmpFile.getAbsoluteFile());
                }
            }
            
        }
        
    }

    /**
     * Close out a node or leaf. When a node or leaf is closed we write it out
     * to obtain its {@link Addr} and set its address on its direct parent using
     * {@link #addChild(com.bigdata.objndx.IndexSegmentBuilder.SimpleNodeData, long)}.
     * 
     */
    protected void close(AbstractSimpleNodeData node) throws IOException {

        final int h = node.level;

        // the index into the level for this node or leaf.
        final int col = writtenInLevel[h];

        assert col < plan.numInLevel[h];

        if (DEBUG)
            log.debug("closing " + (node.isLeaf() ? "leaf" : "node") + "; h="
                    + h + ", col=" + col + ", max=" + node.max + ", nkeys="
                    + node.nkeys);
        
        // Note: This uses shared buffers!
        final long addr = writeNodeOrLeaf(node);

        SimpleNodeData parent = getParent(node);
        
        if(parent != null) {
            
            addChild(parent, addr);
            
        }
        
        /*
         * If there are more nodes to be filled at this level then prepare
         * this node to receive its next values/children. 
         */
        if( col+1 < plan.numInLevel[h] ) {
            
            int max = plan.numInNode[h][col+1];
            
            node.reset( max );
            
        }

        writtenInLevel[h]++;        

    }

    /**
     * Record the persistent {@link Addr address} of a child on its parent. If
     * all children on the parent become assigned then the parent is closed.
     * 
     * @param parent
     *            The parent.
     * @param childAddr
     *            The address of the child (node or leaf).
     * 
     * @throws IOException
     */
    protected void addChild(SimpleNodeData parent,long childAddr) throws IOException {

        assert parent.nchildren < parent.max;

        if(DEBUG)
            log.debug("setting child at index=" + parent.nchildren
                + " on node at level=" + parent.level + ", col="
                + writtenInLevel[parent.level] + ", addr="
                + Addr.toString(childAddr));
        
        parent.childAddr[parent.nchildren++] = childAddr;

        if( parent.nchildren == parent.max ) {
            
            close(parent);
            
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
        
        if( parent.nkeys < maxKeys ) {
            
            /*
             * Copy the first key from the leaf into this parent, incrementing
             * the #of keys in the parent.
             */

            if (DEBUG)
                log.debug("setting separatorKey on node at level "
                        + parent.level + ", col="
                        + writtenInLevel[parent.level]);

            parent.copyKey(parent.nkeys++, leaf, 0 );

        } else {

            /*
             * Delegate to the parent recursively until we find the first parent
             * into which the separatorKey can be inserted.
             */

            addSeparatorKey(getParent(parent),leaf);
            
        }
        
    }
    
    /**
     * Return the parent of a node or leaf in the {@link #stack} stack.
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
     * Serialize and compress a node or leaf.
     * 
     * @return {@link #cbuf}, which contains the serialized and compressed
     *         representation of the node or leaf. The buffer is already flipped
     *         in preparation for writing onto a {@link FileChannel}.
     * 
     * @see #buf, which is used to serialize the node or leaf.
     * @see #cbuf, which is used to compress the node or leaf.
     */
    protected ByteBuffer serializeAndCompress(AbstractSimpleNodeData node)
        throws IOException
    {
    
        // clear the serialization buffer.
        buf.clear();

        // serialize onto buffer.
        nodeSer.putNodeOrLeaf(buf, node);
        
        // flip for reading.
        buf.flip();
        
        // clear compression buffer.
        cbuf.clear();
        
        // setup writer onto compression buffer.
        ByteBufferOutputStream bbos = new ByteBufferOutputStream(cbuf);
        
        // compress the serialized leaf.
        compressor.compress(buf, bbos);
        
        // flush the compression buffer.
        bbos.flush();
        
        bbos.close();
        
        // flip the compressed buffer to prepare for writing.
        cbuf.flip();
        
        return cbuf;

    }

    /**
     * Serialize, compress, and write the node or leaf onto the appropriate
     * output channel.
     * 
     * @return The {@link Addr} that may be used to read the compressed node or
     *         leaf from the file. Note that the address of a node is relative
     *         to the start of the node channel and therefore must be adjusted
     *         before reading the node from the final index segment file.
     */
    protected long writeNodeOrLeaf(AbstractSimpleNodeData node)
            throws IOException {

        return node.isLeaf() ? writeLeaf((SimpleLeafData) node)
                : writeNode((SimpleNodeData) node);
        
    }

    /**
     * Serialize, compress, and write the leaf onto the leaf output channel.
     * 
     * @return The {@link Addr} that may be used to read the compressed leaf
     *         from the file.
     * 
     * @todo write prior; compute next from offset+size during IndexSegment
     *       scans. Basically, we can only record one of the references in the
     *       leaf data structure since we do not know its size until it has been
     *       compressed, at which point we can no longer set the size field on
     *       the record. However, we do know that offset of the next leaf simply
     *       from the reference of the current leaf, which encodes
     *       {offset,size}. The other way is to write out the next reference on
     *       the leaf after it has been compressed. Review
     *       {@link NodeSerializer} with regard to this issue again.
     */
    protected long writeLeaf(final SimpleLeafData leaf)
        throws IOException
    {

        FileChannel outChannel = out.getChannel();
        
        // serialize and compress onto [cbuf].
        serializeAndCompress( leaf );

        /*
         * Write leaf on the channel.
         */
        
        // position on the channel before the write.
        final long offset = outChannel.position();
        
        if(offset>Integer.MAX_VALUE) {
            
            throw new IOException("Index segment exceeds int32 bytes.");
            
        }
        
        // write the compressed record on the channel.
        final int nbytes = outChannel.write(cbuf);
        
        assert nbytes == cbuf.limit();
        
        if( nbytes > maxNodeOrLeafLength ) { 
         
            // track the largest node or leaf written.
            maxNodeOrLeafLength = nbytes;
            
        }

        // the offset where we just wrote the last leaf.
        lastLeafOffset = offset;

        // the size of the compressed leaf that we just wrote out.
        lastLeafSize = nbytes;

        // the #of leaves written so far.
        nleavesWritten++;
        
        System.err.print("."); // wrote a leaf.

        return Addr.toLong(nbytes, (int)offset);
        
    }

    /**
     * Serialize, compress, and write the node onto the node output channel.
     * 
     * @return An <em>relative</em> {@link Addr} that must be correctly
     *         decoded before you can read the compressed node from the file.
     *         This value is also set on {@link SimpleNodeData#addr}.
     * 
     * @see SimpleNodeData, which describes the decoding required.
     */
    protected long writeNode(final SimpleNodeData node)
        throws IOException
    {

        FileChannel tmpChannel = tmp.getChannel();

        // serialize and compress onto [cbuf].
        serializeAndCompress( node );
        
        /*
         * Write node on the temporary channel.
         */
        
        // position on the temporary channel before the write.
        final long offset = tmpChannel.position();
        
        if(offset>Integer.MAX_VALUE) {
            
            throw new IOException("Index segment exceeds int32 bytes.");
            
        }
        
        // write the compressed record on the temporary channel.
        final int nbytes = tmpChannel.write(cbuf);
        
        assert nbytes == cbuf.limit();
        
        if( nbytes > maxNodeOrLeafLength ) { 
         
            // track the largest node or leaf written.
            maxNodeOrLeafLength = nbytes;
            
        }

//        node.written = true;

//        // the offset where we just wrote the last node.
//        lastNodeOffset = offset;
//
//        // the size of the compressed node that we just wrote out.
//        lastNodeSize = nbytes;

        // the #of stack written so far.
        nnodesWritten++;
        
        System.err.print("x"); // wrote a node.

        // flip the sign to indicate a relative reference to a node.
        long addr = - ( Addr.toLong(nbytes, (int)offset) );
        
        node.addr = addr;
        
        return addr;
        
    }

    /**
     * Abstract base class for classes used to construct and serialize stack and
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
        final ArrayType keyType;
        // mutable.
        final Object keys;
        int nkeys;
        
        /**
         * We precompute the #of children to be assigned to each node and the
         * #of values to be assigned to each leaf and store that value in this
         * field. While the field name is "max", this is the exact that must be
         * assigned to the node.
         */
        int max = -1;
        

        protected AbstractSimpleNodeData(int level,int m,ArrayType keyType,Object keys) {
            this.level = level;
            this.m = m;
            this.keyType = keyType;
            this.nkeys = 0;
            this.keys = keys;
        }

        protected void reset(int max) {
            
            nkeys = 0;
            
            this.max = max;
            
        }
        
        public int getBranchingFactor() {
            return m;
        }

        public int getKeyCount() {
            return nkeys;
        }

        public ArrayType getKeyType() {
            return keyType;
        }

        public Object getKeys() {
            return keys;
        }

        /**
         * Copy a key from the source node into this node. This method does not
         * modify the source node. This method does not update the #of keys in
         * this node. This method has the substantial advantage that primitive
         * keys are not boxed and unboxed solely to perform the cop.
         * 
         * @param dstpos
         *            The index position to which the key will be copied on this
         *            node.
         * @param src
         *            The source node from which the key will be copied.
         * @param srcpos
         *            The index position from which the key will be copied.
         */
        final protected void copyKey(int dstpos,IAbstractNodeData src,int srcpos) {
            Object srcKeys = src.getKeys();
            switch(keyType) {
            case BYTE: ((byte[])keys)[dstpos] = ((byte[])srcKeys)[srcpos]; break;
            case SHORT: ((short[])keys)[dstpos] = ((short[])srcKeys)[srcpos]; break;
            case CHAR: ((char[])keys)[dstpos] = ((char[])srcKeys)[srcpos]; break;
            case INT: ((int[])keys)[dstpos] = ((int[])srcKeys)[srcpos]; break;
            case LONG: ((long[])keys)[dstpos] = ((long[])srcKeys)[srcpos]; break;
            case FLOAT: ((float[])keys)[dstpos] = ((float[])srcKeys)[srcpos]; break;
            case DOUBLE: ((double[])keys)[dstpos] = ((double[])srcKeys)[srcpos]; break;
            case OBJECT: ((Object[])keys)[dstpos] = ((Object[])srcKeys)[srcpos]; break;
            default: throw new UnsupportedOperationException();
            }
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
        final Object[] vals;
        
        public SimpleLeafData(int level,int m,ArrayType keyType) {

            super(level,m,keyType,ArrayType.alloc(keyType, m));
            
            this.vals = new Object[m];
            
        }
        
        /**
         * 
         * @param max The #of values that must be assigned to this leaf.
         */
        public void reset(int max) {
            super.reset(max);
        }
        
        public int getValueCount() {
            return nkeys;
        }

        public Object[] getValues() {
            return vals;
        }

        public boolean isLeaf() {
            return true;
        }
    
    }

    /**
     * A class that can be used to (de-)serialize the data for a node without
     * any of the logic for operations on the node.
     * 
     * Note: All node addresses that are internal to a node and reference a
     * child node (vs a leaf) are correct relative to the start of the node
     * block. This is an unavoidable consequence of serializing the stack before
     * we have the total offset to the start of the node block. In order to flag
     * this we flip the sign on the node addresses so that the are serialized as
     * negative numbers. Flipping the sign serves to distinguish leaf addresses,
     * which are correct from node addresses, which are negative integers and
     * need some interpretation before they can be used to address into the
     * file. {@link IndexSegment} is aware of this convention and corrects the
     * sign and the offset for the internal node references whenever it
     * deserialized a node from the file.
     * 
     * @see IndexSegment#readNodeOrLead(long), which is responsible for undoing
     *      this encoding when reading from the file.
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
         * The address at which the child stack were written. This is a negative
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
        
        public SimpleNodeData(int level,int m,ArrayType keyType) {

            super(level,m,keyType,ArrayType.alloc(keyType, m-1));
            
            this.childAddr = new long[m];
            
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
            
        }

        public long[] getChildAddr() {
            return childAddr;
        }

        public int getChildCount() {
            return nkeys+1;
        }

        public boolean isLeaf() {
            return false;
        }
        
    }
    
}
