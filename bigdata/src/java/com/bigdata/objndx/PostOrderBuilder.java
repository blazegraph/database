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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.HardReferenceQueue.HardReferenceQueueEvictionListener;
import com.bigdata.journal.Bytes;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.PostOrderBuilder.IndexSegment.Metadata;

/**
 * Class supports a post-order construction of a "perfect" b+tree given sorted
 * records. There are two main use cases:
 * <ol>
 * <li>Evicting a key range of an index into an optimized on-disk index. In
 * this case, the input is a btree that is ideally backed by a fully buffered
 * {@link IRawStore} so that no random reads are required.</li>
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see "Post-order B-Tree Construction" by Lawerence West, ACM 1992.
 * @see "Batch-Construction of B+-Trees" by Kim and Won, ACM 2001.
 * 
 * FIXME West's algorithm is for a b-tree, not a b+-tree. Therefore it assumes
 * that there are key-value pairs in all nodes of the tree rather than in just
 * the leaves of the tree. This will require a pervasive adaptation in order to
 * work. The approach outlined by Kim and Won is designed for B+-Trees, but it
 * appears to be less efficient on first glance.
 * 
 * FIXME Implement the post-order builder. Support each of the use cases,
 * including external sorting. Write a package to support lookups on the
 * generated perfect indices, including buffering the nodes in memory and
 * optionally buffering the leaves as well. To what extent can we reuse the
 * btree package for these read-only indices and just swap out the
 * {@link IRawStore} implementation?
 */
public class PostOrderBuilder {

    /**
     * @todo consider the file mode and buffering. We should at least buffer
     *       several pages of data per write and can experiment with writing
     *       through (vs caching writes in the OS layer). The file does not need
     *       to be "live" until it is completely written, so there is no need to
     *       update file metadata until the end of the build process.
     */
    final String mode = "rw"; // also rws or rwd
    
    /**
     * Builds an index segment on the disk from a {@link BTree}. The store on
     * which the btree exists should be "offline" (frozen). The index segment
     * will be written on the identified file, which must not exist. The caller
     * is responsible for updating the metadata required to locate the index
     * segment.
     * 
     * With a branching factor of 4096 a tree of height 2 (three levels) could
     * address 68,719,476,736 entries - well beyond what we want in a given
     * index segment! Well before that the index segment should be split into
     * multiple files. The actual point when we would split the index segment
     * should be determined by the size of the serialized leaves and nodes,
     * e.g., the amount of data on disk required by the index segment. While the
     * size of a serialized node can be estimated easily, the size of a
     * serialized leaf depends on the kinds of values stored in that index.
     * 
     * @param outFile
     *            The file on which the index segment is written.
     * @param tmpDir
     *            The temporary directory in which the index nodes are buffered
     *            during the build (optional - the default temporary directory
     *            is used if this is <code>null</code>).
     * @param btree
     *            The btree.
     * @param m
     *            The branching factor for the generated tree. This can be
     *            choosen with an eye to minimizing the height of the generated
     *            tree.
     * 
     * @throws IOException
     * 
     * @todo There are more efficient ways to do this. This is a simple two pass
     *       algorithm. Since the leaves are not linked together, we do a
     *       post-order traversal to write all the leaves onto the file and then
     *       we do a post-order traversal to write all the nodes onto the file.
     *       Unlike a bulk index rebuild, this routine is unable to change the
     *       branching factor since it does not build new nodes from the leaves
     *       but simply serializes the leaves and nodes onto a file. The leaves
     *       will wind up at the front of the file and the nodes will wind up at
     *       the end of the file. This facilitates buffering the index nodes so
     *       that a search on the file requires only a IO.
     */
    public PostOrderBuilder(File outFile, File tmpDir, BTree btree, int m)
            throws IOException {

        /*
         * The buffer used to serialize individual nodes and leaves before they
         * are written onto the file.
         * 
         * @todo The capacity of this buffer is a SWAG. It is too large for most
         * purposes but it is unlikely to be too small for most purposes. If you
         * see a buffer overflow exception then you may have extremely long keys
         * and/or very large values. The BTree class has the same problem with
         * the buffer that it uses to serialize nodes and leaves, but its nodes
         * and leaves are smaller since it uses a smaller branching factor.
         */
        final ByteBuffer buf = ByteBuffer.allocateDirect(1*Bytes.megabyte32);
        
        /*
         * The minimum #of values that may be placed into non-root leaf (and
         * also the minimum #of children that may be placed into a non-root
         * node). (the minimum capacity).
         */
        final int m2 = (m+1)/2; 
        
        /*
         * The #of entries in the btree.
         */
        final int nentries = btree.nentries;
        
        /*
         * The #of leaves that will exist in the output tree.
         */
        final int nleaves = (int)Math.ceil((double)nentries / (double)m); 
        
        /*
         * The height of the output tree. This is used to maintain a stack of
         * the nodes that we are creating as we build the leaves under those
         * nodes. The space requirement is only height nodes and one leaf.
         * 
         * @todo In order for the nodes to be written in a contiguous block we
         * either have to write them onto a temporary file and then copy them
         * into place after the last leaf has been processed or we have to
         * buffer them in memory. I am tempted to try a temporary file first and
         * a channel-to-channel transfer post-build.
         * 
         * @todo it is possible that the index segment will consist solely of a
         * root leaf.
         */
        final int height = getMinimumHeight(m,nleaves);

        /*
         * We want to fill up every leaf, but we have to make sure that the last
         * leaf is not under capacity. To that end, we calculate the #of entries
         * that would remain if we filled up n-1 leaves completely. If the #of
         * remaining entries is greater than or equal to the minimum capacity of
         * a leaf, then we have to adjust the allocation of entries such that
         * the last leaf is full. This is done by computing the shortage and
         * then writing one fewer entries into the first shortage leaves. Once
         * we have deferred enough entries we are guarenteed that the final leaf
         * will not be under capacity.
         */
        final int shortage; 
        {
        
            // #of entries remaining for the last leaf.
            int remaining = nentries - ((nleaves-1) * m);
            
            /*
             * if the #of entries remainin would put the leaf under capacity
             * then we compute the shortage.
             */
            shortage = remaining < m2 ? m2 - remaining : 0;

        }

        /*
         * setup for IO.
         */

        if (outFile.exists()) {
            throw new IllegalArgumentException("File exists: "
                    + outFile.getAbsoluteFile());
        }

        final File tmpFile = File.createTempFile("index", ".seg", tmpDir);

        RandomAccessFile out = null;

        RandomAccessFile tmp = null;
        
        try {

            /*
             * Open the output channel and get an exclusive lock.
             */
            
            out = new RandomAccessFile(outFile, mode);
            
            final FileChannel outChannel = out.getChannel();
            
            if (outChannel.tryLock() == null) {
                
                throw new IOException("Could not lock file: "
                        + outFile.getAbsoluteFile());
                
            }

            /*
             * Open the temporary channel and get an exclusive lock.
             */
            
            tmp = new RandomAccessFile(tmpFile, mode);
            
            final FileChannel tmpChannel = tmp.getChannel();
            
            if (tmpChannel.tryLock() == null) {
                
                throw new IOException("Could not lock file: "
                        + tmpFile.getAbsoluteFile());
                
            }

            /*
             * Skip over the metadata record, which we have to write once we
             * know how the leaves and nodes fit once serialized onto the file.
             */
            outChannel.position(Metadata.SIZE);
            
            /*
             * Scan the btree leaves in key order writing out leaves onto the
             * index segment file with the new branching factor.
             * 
             * @todo we also have to track a stack of nodes that are being
             * written out concurrently on the temporary channel. Each time we
             * write out a leaf we place the separator key for that leaf into
             * its immediate parent node. If the parent becomes full then we
             * write out the node to the tmpChannel and insert the appropriate
             * separatorKey into its parent.
             * 
             * It is an error if the root becomes full.
             * 
             * The separator key for the parent is always a key in a leaf. In
             * fact it is the first key that goes into the next leaf. This means
             * that either we need to read the keys ahead or that we need to
             * fill in the separator key and evict the node behind. Note that we
             * need to evict the last node after we finish the last leaf. Note
             * that we could enter the next leaf address before we actually
             * write the leaf since it is simply the current position of the
             * outChannel. However, this does not generalize to nodes trivially
             * since we have potentially a stack of N nodes that are being
             * written out.  This implies that we defer adding the child to the
             * parent until the child has been written out.
             * 
             * Note that the root may be a leaf as a degenerate case.
             */

            // allocate keys for the leaf.
            final Object[] keys = new Object[m]; // AbstractNode.allocKeys(btree.keyType,m);

            // allocate values for the leaf.
            final Object[] vals = new Object[m];

            // @todo replace with low-level scan to avoid object creation on keys.
            KeyValueIterator itr = btree.root.entryIterator();

            long lastOffset = -1L;
            long nextOffset = outChannel.position();

            // track the maximum length of any serialized node or leaf.
            int maxNodeOrLeafLength = 0;
            
            for (int i = 0; i < nleaves; i++) {

                int nkeys = 0;

                /*
                 * @todo the shortage can require that we short early leaves by
                 * more than one key.  This shows up in our test examples when
                 * m := 8 requiring us to short the first leaf by 2 keys so that
                 * the 2nd leaf will not be under capacity.
                 */
                int limit = (shortage > 0 && i < shortage ? m - 1 : m);

                /*
                 * fill in defined keys and values for this leaf.
                 */
                for (int j = 0; j < limit; j++) {

                    Object val = itr.next();
                    Object key = itr.getKey();

                    keys[j] = key;
                    vals[j] = val;
                    nkeys++;

                }
                
//                /*
//                 * Clear out remaining elements of arrays.
//                 * 
//                 * @todo This is not required the way that things are setup
//                 * since only the first few leaves can be under capacity, and
//                 * then will always be under capacity by the same amount. Once
//                 * the shortage has been made up the rest of the leaves will be
//                 * fully populated.
//                 */
//                for (int j = limit; j < m; j++) {
//                    
//                    keys[j] = null;
//                    
//                    vals[j] = null;
//                    
////                    System.err.println("Clearing unused element: index="+j);
//                    
//                }

                // verify leaf not under capacity
                assert nkeys >= m2;

                // reset the buffer.
                buf.clear();

                /*
                 * serialize the leaf.
                 * 
                 * @todo write isLeaf, keys[], vals[], prior, next. Ideally
                 * reuse the keySer and valSer for the btree, but note that we
                 * would have to support primitive key[]s here and note that the
                 * KeyValueIterator is doing lots of object creation - we are
                 * much better off to use a native traversal mechanism!
                 */
                //btree.nodeSer.putLeaf(buf, null);
                System.err.print("."); // wrote a leaf.

                // flip buffer to prepare for writing.
                buf.flip();

                // write leaf on the channel.
                final int nwritten = outChannel.write(buf);
                assert nwritten == buf.limit();
                
                if( nwritten > maxNodeOrLeafLength ) { 
                 
                    maxNodeOrLeafLength= nwritten;
                    
                }

                // the offset where we just wrote the last leaf.
                lastOffset = nextOffset;

                // the offset where we will write the next leaf.
                nextOffset = outChannel.position();

            }

            /*
             * Direct copy the node index from the temporary file into the
             * output file and clear the reference to the temporary file. The
             * temporary file itself will be deleted as a post-condition of this
             * operation.
             */
            final long offsetLeaves = Metadata.SIZE;
            final long offsetNodes = outChannel.position();
            outChannel.transferFrom(tmpChannel, 0L, tmp.length());
            tmp.close(); // also releases the lock.
            tmp = null;

            /*
             * Seek to the start of the file and write out the metadata record.
             */
            {

                // @todo compute each of these up above.
                int nnodes = 0;
                long addrRoot = 0;
                final long now = System.currentTimeMillis();
                final String name = "<no name>"; // @todo use or drop this field.
                
                outChannel.position(0);
                
                new Metadata(m, height, btree.keyType, nleaves, nnodes,
                        nentries, maxNodeOrLeafLength, offsetLeaves,
                        offsetNodes, addrRoot, out.length(), now, name)
                        .write(out);
                
            }
                        
            /*
             * Flush this channel to disk, close the channel, and clear the
             * reference. This also releases our lock. We are done and the index
             * segment is ready for use.
             */
            outChannel.force(true);
            out.close(); // also releases the lock.
            out = null;

        } catch (Throwable ex) {

            /*
             * make sure that the output file is deleted unless it was
             * successfully processed.
             */
            if (out != null) {
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
            if (tmp != null) {
                try {tmp.close();}
                catch (Throwable t) {}
            }
            tmpFile.delete();

        }
            
        
    }

    /**
     * An index segment is read-only btree corresponding to some key range of an
     * index. The file format of the index segment includes a metadata record,
     * the leaves of the segment in key order, and the nodes of the segment in
     * post-order traversal order. It is possible to map or buffer the part of
     * the file containing the index nodes or the entire file depending on
     * application requirements.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo To what extent can I reuse the same base classes for this and for
     *       {@link BTree}?
     * 
     * @todo test suite.
     */
    public static class IndexSegment implements IBTree {
        
        /**
         * The file containing the index segment.
         */
        final File file;
        
        /**
         * The random access file used to read the index segment.
         */
        final RandomAccessFile raf;
        
        /**
         * A read-only view of the metadata record for the index segment.
         */
        final Metadata metadata;
        
        /**
         * A buffer containing the disk image of the nodes in the index segment.
         */
        private final ByteBuffer buf_nodes;
        
        /**
         * A buffer used to read a leaf or an index node.
         */
        private final ByteBuffer buf;
        
        /**
         * The root of the index segment.
         */
        final AbstractSegNode root;

        /**
         * The index segment is read only so we do not need to do IO on
         * eviction. All the listener needs to do is count queue evictions to
         * collect statistics on the index performance.
         */
        final HardReferenceQueueEvictionListener<AbstractSegNode> listener = new QueueListener();

        /**
         * The capacity is relatively low and the #of entries to scan is
         * relatively high since each entry is relatively large.
         */
        final HardReferenceQueue<AbstractSegNode> queue = new HardReferenceQueue<AbstractSegNode>(listener,100,20);

        /**
         * Text of a message used in exceptions for mutation operations on the
         * index segment.
         */
        final protected static String MSG_READ_ONLY = "Read-only index";
        
        public IndexSegment(File file) throws IOException {
        
            assert file != null;
            
            this.file = file;
            
            if( ! file.exists() ) {
                
                throw new IOException("File does not exist: "
                        + file.getAbsoluteFile());
                
            }
            
            // open the file.
            this.raf = new RandomAccessFile(file,"r"); 

            // read the metadata record from the file.
            this.metadata = new Metadata(raf);

            /*
             * read the index nodes from the file into a buffer.
             * 
             * @todo this could be an optional step. certainly if the index
             * segment is just a root leaf then we don't want to buffer it as
             * well.
             */
            this.buf_nodes = readIndexNodes(raf);

            /*
             * Allocate a shared buffer used to read a node or leaf directly
             * from the backing file.
             * 
             * @todo if there are concurrent reads on the index segment then
             * this buffer should not be shared and would have to be allocated
             * on each read against the file - no great loss.
             */
            this.buf = ByteBuffer.allocate(metadata.maxNodeOrLeafLength);

            /*
             * Read the root node.
             */
            this.root = get(metadata.addrRoot);
            
        }

        /**
         * Reads the index nodes into a buffer.
         * 
         * @return A read-only view of a buffer containing the index nodes.
         */
        protected ByteBuffer readIndexNodes(RandomAccessFile raf) throws IOException {
            
            long start = metadata.offsetNodes;
            
            long length = metadata.length - start;
            
            if (length > Integer.MAX_VALUE)
                throw new RuntimeException();
            
            final int capacity = (int)length;
            
            ByteBuffer buf = ByteBuffer.allocateDirect(capacity);
            
            raf.getChannel().read(buf, start);
            
            return buf.asReadOnlyBuffer();
            
        }

        /**
         * Touch a node or leaf on the hard reference queue.
         * 
         * @param node The node or leaf.
         */
        protected void touch(AbstractSegNode node) {
            
            assert node != null;
            
            /*
             * There is no need to touch the root since we have a hard
             * reference.
             */
            if( node == root ) return;

            // @todo track reference counts.
            queue.append( node );
            
        }
        
        /**
         * Return the node or leaf at that address in the index segment file.
         * This is used when the weak reference for a child or sibling either
         * has never been set or has been cleared.
         * 
         * @param addr
         *            The address of the node or leaf.
         * 
         * @return The node or leaf at that address.
         * 
         * @see Addr
         */
        protected AbstractSegNode get(long addr) {

            ByteBuffer buf = read(addr);
            
            return getNodeOrLeaf( buf );
            
        }

        /**
         * De-serialize a node or leaf from a buffer.
         * 
         * @param buf The buffer.
         * 
         * @return The node or leaf.
         */
        protected AbstractSegNode getNodeOrLeaf(ByteBuffer buf) {

            boolean isLeaf = buf.get() != 0;
            
            if ( isLeaf ) {

                return getLeaf(buf);
                
            } else {
                
                return getNode(buf);
                
            }

        }

        /**
         * De-serialize a node from the buffer.
         * 
         * @param buf
         *            The buffer.
         *            
         * @return The node.
         */
        protected SegNode getNode(ByteBuffer buf) {

            throw new UnsupportedOperationException();
            
        }
        
        /**
         * De-serialize a leaf from the buffer.
         * 
         * @param buf
         *            The buffer.
         *            
         * @return The leaf.
         */
        protected SegLeaf getLeaf(ByteBuffer buf) {

            throw new UnsupportedOperationException();

        }

        /**
         * Read from the index segment. If the request is in the node region and
         * the nodes have been buffered then this returns a slice on the node
         * buffer. Otherwise this reads through to the backing file.
         * 
         * @param addr
         *            The address (encoding both the offset and the length).
         * 
         * @return A read-only buffer containing the data at that address.
         */
        protected ByteBuffer read(long addr) {

            final int offset = Addr.getOffset(addr);
            
            final int length = Addr.getByteCount(addr);
            
            if (offset >= metadata.offsetNodes && buf_nodes != null ) {

                /*
                 * the data are buffered.
                 */
                
                ByteBuffer buf = buf_nodes.slice();
                
                buf.limit(offset+length);

                buf.position(offset);
                
                return buf;
                
            } else {
                
                /*
                 * the data need to be read from the file.
                 */
                
                buf.limit(length);
                
                buf.position(0);

                try {

                    raf.getChannel().read(buf, offset);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);
                    
                }
                
                return buf;
                
            }
            
        }
        
        /**
         * Listener for queue eviction events.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class QueueListener implements HardReferenceQueueEvictionListener<AbstractSegNode> {

            public void evicted(HardReferenceQueue<AbstractSegNode> cache, AbstractSegNode ref) {
                
                // @todo track reference counts.
                // @todo count evictions.
                
            }
            
        }
        
        /**
         * The metadata record for an {@link IndexSegment}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         * 
         * @todo consider recording the min/max key or just making it easy to
         *       determine that for an {@link IndexSegment}.
         */
        public static class Metadata {
            
            static final public int MAGIC = 0x87ab34f5;
            final public int branchingFactor;
            final public int height;
            final public ArrayType keyType; // serialized as int.
            /**
             * The #of leaves serialized in the file.
             */
            final public int nleaves;
            /**
             * The #of nodes serialized in the file. If zero, then
             * {@link #nleaves} MUST be ONE (1) and the index consists solely of
             * a root leaf.
             */
            final public int nnodes;
            /**
             * The #of index entries serialized in the file. This must be a
             * positive integer as an empty index is not permitted (this forces
             * the application to check the btree and NOT build the index
             * segment when it is empty).
             */
            final public int nentries;
            /**
             * The maximum #of bytes in any node or leaf stored on the index
             * segment.
             */
            final public int maxNodeOrLeafLength;
            /**
             * The offset to the start of the serialized leaves in the file.
             * 
             * Note: This should be equal to {@link #SIZE} since the leaves are
             * written immediately after the {@link Metadata} record.
             */
            final public long offsetLeaves;
            /**
             * The offset to the start of the serialized nodes in the file or
             * <code>-1L</code> iff there are no nodes in the file.
             */
            final public long offsetNodes;
            final public long addrRoot; // address of the root node/leaf.
            final public long length; // of the file in bytes.
            final public long timestamp;
            final public String name;

            /**
             * The #of bytes in the metadata record.
             * 
             * Note: This is oversized in order to allow some slop for future
             * entries and in order to permit the variable length index name to
             * be recorded in the index segment file.
             */
            public static final int SIZE = Bytes.kilobyte32 * 4;
            
            public static final int MAX_NAME_LENGTH = Bytes.kilobyte32 * 2;
            
            public Metadata(RandomAccessFile raf) throws IOException {

                raf.seek(0);
                
                final int magic = raf.readInt();

                if (magic != MAGIC) {
                    throw new IOException("Bad magic: actual=" + magic
                            + ", expected=" + MAGIC);
                }

                // @todo add assertions here parallel to those in the other ctor.
                
                branchingFactor = raf.readInt();

                height = raf.readInt();
                
                keyType = ArrayType.parseInt(raf.readInt());
                
                nleaves = raf.readInt();
                
                nnodes = raf.readInt();
                
                nentries = raf.readInt();

                maxNodeOrLeafLength = raf.readInt();
                
                offsetLeaves = raf.readLong();

                offsetNodes = raf.readLong();
                
                addrRoot = raf.readLong();

                length = raf.readLong();
                
                if (length != raf.length()) {
                    throw new IOException("Length differs: actual="
                            + raf.length() + ", expected=" + length);
                }
                
                timestamp = raf.readLong();

                name = raf.readUTF();
                
                assert name.length() <= MAX_NAME_LENGTH;
                
            }

            public Metadata(int branchingFactor, int height, ArrayType keyType,
                    int nleaves, int nnodes, int nentries,
                    int maxNodeOrLeafLength, long offsetLeaves,
                    long offsetNodes, long addrRoot, long length,
                    long timestamp, String name) {
                
                assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
                
                assert height >= 0;
                
                assert keyType != null;
                
                assert nleaves > 0; // always at least one leaf.
                
                assert nnodes >= 0; // may be just a root leaf.
                
                // index may not be empty.
                assert nentries > 0;
                // #entries must fit within the tree height.
                assert nentries <= Math.pow(branchingFactor,height+1);
                
                assert maxNodeOrLeafLength > 0;
                
                assert offsetLeaves > 0;

                if(nnodes == 0) {
                    // the root is a leaf.
                    assert offsetNodes == -1L;
                    assert offsetLeaves < length;
                    assert Addr.getOffset(addrRoot) > offsetLeaves;
                    assert Addr.getOffset(addrRoot) < length;
                } else {
                    // the root is a node.
                    assert offsetNodes > offsetLeaves;
                    assert offsetNodes < length;
                    assert Addr.getOffset(addrRoot) >= offsetNodes;
                    assert Addr.getOffset(addrRoot) < length;
                }
                
                assert timestamp != 0L;
                
                assert name != null;
                
                assert name.length() <= MAX_NAME_LENGTH;
                
                this.branchingFactor = branchingFactor;

                this.height = height;
                
                this.keyType = keyType;

                this.nleaves = nleaves;
                
                this.nnodes = nnodes;
                
                this.nentries = nentries;

                this.maxNodeOrLeafLength = maxNodeOrLeafLength;
                
                this.offsetLeaves = offsetLeaves;
                
                this.offsetNodes = offsetNodes;
                
                this.addrRoot = addrRoot;

                this.length = length;
                
                this.timestamp = timestamp;

                this.name = name;
                
            }
            
            public void write(RandomAccessFile raf) throws IOException {

                raf.seek(0);
                
                raf.write(MAGIC);
                
                raf.write(branchingFactor);
                                
                raf.write(height);
                
                raf.write(keyType.intValue());
                
                raf.write(nleaves);

                raf.write(nnodes);
                
                raf.write(nentries);

                raf.write(maxNodeOrLeafLength);
                
                raf.writeLong(offsetLeaves);
                
                raf.writeLong(offsetNodes);

                raf.writeLong(addrRoot);

                raf.writeLong(length);
                
                raf.writeLong(timestamp);
                
                raf.writeUTF(name);
                
            }

        }
        
        abstract public static class AbstractSegNode {
            
        }
        
        public static class SegNode extends AbstractSegNode {
            
        }
        
        public static class SegLeaf extends AbstractSegNode {
            
        }

        public ArrayType getKeyType() {
            
            return metadata.keyType;
            
        }
        
        public Object insert(Object key, Object entry) {
            
            throw new UnsupportedOperationException(MSG_READ_ONLY);
            
        }

        public Object remove(Object key) {
            
            throw new UnsupportedOperationException(MSG_READ_ONLY);
            
        }

        public Object lookup(Object key) {

            throw new UnsupportedOperationException();

        }

        public IRangeIterator rangeIterator(Object fromKey, Object toKey) {

            throw new UnsupportedOperationException();
            
        }
        
        public KeyValueIterator entryIterator() {
            
            throw new UnsupportedOperationException();
            
        }

    }
    
    /**
     * Chooses the minimum height for a tree having a specified branching factor
     * and a specified #of leaves.
     * 
     * @param m
     *            The branching factor.
     * @param nleaves
     *            The #of leaves that must be addressable by the tree.
     */
    public static int getMinimumHeight(int m, int nleaves) {
        
        final int maxHeight = 10;
        
        for (int h = 0; h <= maxHeight; h++) {
        
            /*
             * The maximum #of leaves addressable by a tree of height h and the
             * given branching factor.
             * 
             * Note: Java guarentees that Math.pow(int,int) produces the exact
             * result iff that result can be represented as an integer. This
             * useful feature lets us avoid having to deal with precision issues
             * or write our own integer version of pow (computing m*m h times).
             */
            final double d = (double)Math.pow(m,h);
            
            if( d >= nleaves ) {
            
                /*
                 * h is the smallest height tree of the specified branching
                 * factor m capable of addressing the specified #of leaves.
                 */
                return h;
                
            }
            
        }
        
        throw new UnsupportedOperationException(
                "Can not build a tree for that many leaves: m=" + m
                        + ", nleaves=" + nleaves + ", maxHeight=" + maxHeight);
    }
   
//    /**
//     * Choose the height and branching factor (aka order) of the generated tree.
//     * This choice is made by choosing a height and order for the tree such
//     * that:
//     * 
//     * <pre>
//     *   2(d + 1)&circ;(h-l) - 1 &lt;= N &lt;= (2d + 1)&circ;h - l
//     * </pre>
//     * 
//     * where
//     * <ul>
//     * <li>d := the minimum #of keys in a node of the generated tree (m/2).</li>
//     * <li>h := the height of the generated tree (origin one (1)).</li>
//     * <li>N := the #of entries (rows of data)</li>
//     * </ul>
//     * 
//     * This can be restated as:
//     * 
//     * <pre>
//     *  
//     *   2(m/2 + 1)&circ;h - 1 &lt;= N &lt;= (m + 1)&circ;(h+1) - l
//     * </pre>
//     * 
//     * where
//     * <ul>
//     * <li>m := the branching factor of the generated tree.</li>
//     * <li>h := the height of the generated tree (origin zero(0)).</li>
//     * <li>N := the #of entries (rows of data)</li>
//     * </ul>
//     * 
//     * @todo The #of entries to be placed into the generated perfect index must
//     *       be unchanging during this process. This suggests that it is best to
//     *       freeze the journal, opening a new journal for continued writes, and
//     *       then evict all index ranges in the frozen journal into perfect
//     *       indices.
//     * 
//     * @todo Note that this routine is limited to an index subrange with no more
//     *       entries than can be represented in an int32 signed value.
//     */
//    protected void phase1(int nentries) {
//
//        /*
//         * @todo solve for the desired height, where h is the #of non-leaf nodes
//         * and is zero (0) if the tree consists of only a root leaf.  We want to
//         * minimize the height as long as the node/leaf size is not too great.
//         */
//        int h = 0;
//        
//        /*
//         * @todo solve for the desired branching factor (#of children for a node
//         * or the #of values for a leaf. The #of keys for a node is m-1. The
//         * branching factor has to be bounded since there is some branching
//         * factor at which any btree fits into the root leaf. Therefore an
//         * allowable upper range for m should be an input, e.g., m = 4096. This
//         * can be choosen with an eye to the size of a leaf on the disk since we
//         * plan to have the index nodes in memory but to read the leaves from
//         * disk. Since the size of a leaf varies by the key and value types this
//         * can either be a SWAG, e.g., 1024 or 4096, or it can be computed based
//         * on the actual average size of the leaves as written onto the store.
//         * 
//         * Note that leaves in the journal index ranges will typically be much
//         * smaller since the journal uses smaller branching factors to minimize
//         * the cost of insert and delete operations on an index.
//         * 
//         * In order to minimize IO we probably want to write the leaves onto the
//         * output file as we go (blocking them in a page buffer of at least 32K)
//         * and generate the index nodes in a big old buffer (since we do not
//         * know their serialized size in advance) and then write them out all at
//         * once.
//         */
//        int m = 4;
//        
//        /*
//         * The minimum #of entries that will fit in a btree given (m,h).
//         */
//        int min = (int) Math.pow(m+1, h)-1;
//        
//        /*
//         * The maximum #of entries that will fit in a btree given (m,h).
//         */
//        int max = (int) Math.pow(m, h+1);
//        
//        if( min <= nentries && nentries<= max ) {
//            
//            /*
//             * A btree may be constructed for this many entries with height := h
//             * and branching factor := m. There will be many such solutions and
//             * one needs to be choosen before building the tree.
//             * 
//             * To build the tree with the fewest possible nodes, select the
//             * combination of (m,h) with the smallest value of h. This is what
//             * we are looking for since there will be no further inserted into
//             * the generated index (it is read-only for our purposes). Other
//             * applications might want to "dial-in" some sparseness to the index
//             * by choosing a larger value of h so that more inserts could be
//             * absorbed without causing nodes split.  This is not an issue for
//             * us since we never insert into the generated index file.
//             */
//            
//        }
//
//        /*
//         * compute per-level values given (h,m).
//         * 
//         * Note: our h is h-1 for West (we count the root as h == 0 and West
//         * counts it as h == 1).  While West defines the height in terms of
//         * the #of nodes in a path to a leaf from the root, West is working
//         * with a b-tree and there is no distinction between nodes and leaves.
//         */
//        int r[] = new int[h-1];
//        int n[] = new int[h-1];
//        
//    }
//
//    /**
//     *
//     */
//    protected void phase2() {
//
//    }

}
