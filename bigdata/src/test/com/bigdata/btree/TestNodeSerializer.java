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
 * Created on Oct 30, 2006
 */

package com.bigdata.btree;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.zip.Deflater;

import org.apache.log4j.Level;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.util.ChecksumError;

/**
 * Test case for {@link NodeSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNodeSerializer extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestNodeSerializer() {
    }

    /**
     * @param arg0
     */
    public TestNodeSerializer(String arg0) {
        super(arg0);
    }
    
    /**
     * Overrides to use the {@link SimpleEntry.Serializer}.
     */
    public BTree getBTree(int branchingFactor) {
        
        return getBTree(branchingFactor,false);
        
    }
    
    /**
     * Overrides to use the {@link SimpleEntry.Serializer}.
     * 
     * @param branchingFactor
     * 
     * @param useCompression
     *            when true, uses a {@link RecordCompressor}.
     * 
     * @return
     */
    public BTree getBTree(int branchingFactor,boolean useCompression) {
        
//        IRawStore store = new SimpleMemoryRawStore();
//
//        final int leafQueueCapacity = 10000;
//        
//        final int nscan = 10;
        
        /*
         * Setup the B+Tree directly so that we can override the hard reference
         * queue.
         */
        final BTree btree; 
        {
            
            IRawStore store = new SimpleMemoryRawStore();
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(branchingFactor);
            
            metadata.setIsolatable(r.nextBoolean());
            
            metadata.setDeleteMarkers(r.nextBoolean());

            metadata.setVersionTimestamps(r.nextBoolean());
            
            if(useCompression) {
                
                metadata.setRecordCompressor(new RecordCompressor(Deflater.BEST_SPEED));
                
            }
            
            metadata.write(store);
            
            Checkpoint checkpoint = metadata.firstCheckpoint();
            
            checkpoint.write(store);

            btree = new BTree(store,checkpoint,metadata) {
                
                @Override
                protected HardReferenceQueue<PO> newWriteRetentionQueue() {
                    
                    return new HardReferenceQueue<PO>(//
                            new NoEvictionListener(),//
                            10000,//
                            10//
                    );
                    
                }
                
            };
            
        }

//        BTree btree = new BTree(store,//
//                branchingFactor, //
//                UUID.randomUUID(),//
//                r.nextBoolean(),// isolatable
//                null,// conflictResolver
//                new HardReferenceQueue<PO>(new NoEvictionListener(),
//                        leafQueueCapacity, nscan),//
//                KeyBufferSerializer.INSTANCE,//
//                ByteArrayValueSerializer.INSTANCE,//
//                useCompression ? new RecordCompressor() : null);

        return btree;
        
    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization01() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree(branchingFactor,false);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);
        
//        expected.dump(System.err);
        
//        final Leaf actual = (Leaf)
        doRoundTripTest( true, ndx, expected );

//        actual.dump(System.err);
        
    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization_compressed01() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree(branchingFactor,true);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);
        
//        expected.dump(System.err);
        
//        final Leaf actual = (Leaf)
        doRoundTripTest( true, ndx, expected );

//        actual.dump(System.err);
        
    }

    /**
     * Test that an attempt to deserialize a leaf as a node will fail.
     */
    public void test_leaf_as_node_serialization_correct_rejection() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor);

        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);

        NodeSerializer nodeSer = ndx.getNodeSerializer();
        
        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = nodeSer.putLeaf(expected);
        
        /*
         * Attempt to read the buffer back as a node (vs a leaf).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getNode(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test that an attempt to deserialize a node as a leaf will fail.
     */
    public void test_node_as_leaf_serialization_correct_rejection() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree( branchingFactor);

        // Create test node.
        final Node expected = getRandomNode(ndx);

        NodeSerializer nodeSer = ndx.getNodeSerializer();
        
        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = nodeSer.putNode(expected);
        
        /*
         * Attempt to read the buffer back as a leaf (vs a node).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getLeaf(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test of node serialization.
     */
    public void test_node_serialization01() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor,false);

        // Create test node.
        final Node expected = getRandomNode(ndx);

//        expected.dump(System.err);

//        final Node actual = (Node)
        doRoundTripTest( true, ndx, expected);

//        actual.dump(System.err);

    }
    
    /**
     * Test of node serialization.
     */
    public void test_node_serialization_compressed01() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor,true);

        // Create test node.
        final Node expected = getRandomNode(ndx);

//        expected.dump(System.err);

//        final Node actual = (Node)
        doRoundTripTest( true, ndx, expected);

//        actual.dump(System.err);

    }
    
    /**
     * Round trip serialization test.
     * 
     * @param ndx
     *            The object index.
     * @param expected
     *            The node or leaf to be serialized.
     *            
     * @return The de-serialized node or leaf.
     */
    public IAbstractNode doRoundTripTest(boolean verbose, BTree ndx,
            AbstractNode expected) {

        final boolean isLeaf = expected.isLeaf();
        
        NodeSerializer nodeSer = ndx.getNodeSerializer();
        

        // final int BUF_SIZE = getSize(nodeSer,expected);

        // ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);

        final ByteBuffer buf;

        if (isLeaf) {

            buf = clone(nodeSer.putLeaf((Leaf) expected));

        } else {

            buf = clone(nodeSer.putNode((Node) expected));
            
        }

        if (verbose)
            expected.dump(Level.DEBUG,System.err);

        if (verbose)
            System.err.println("buf: " + Arrays.toString(buf.array()));
        
//        buf.flip(); // prepare for reading.
        assert buf.position() == 0;
        
        AbstractNode actual = (AbstractNode) nodeSer.getNodeOrLeaf(ndx,
                expected.getIdentity(), buf);
        
        assertEquals("position", 0,buf.position());

        if (verbose)
            actual.dump(Level.DEBUG,System.err);

        assertSameNodeOrLeaf(expected, actual);

        // write on buf2.
//        ByteBuffer buf2 = ByteBuffer.allocate(BUF_SIZE);
        final ByteBuffer buf2;

        if (isLeaf) {

            buf2 = clone(nodeSer.putLeaf((Leaf)expected));
            
        } else {
            
            buf2 = clone(nodeSer.putNode((Node)expected));
            
        }
        
        if (verbose)
            System.err.println("buf2: " + Arrays.toString(buf2.array()));
        
        // compare buffers - they should have images of the same node.
        assertEquals(buf.array(),buf2.array());

        /*
         * Overwrite the checksum to test for correct reporting of checksum
         * failure.
         * 
         * @todo the checksum mechanism works well when we are not using a
         * compression technique. depending on the compression technique, a
         * change in the compressed data may trigger a failure of the
         * decompression algorithm. in such cases we never get the decompressed
         * record and there for the checksum is not even computed.
         */

        if(nodeSer.useChecksum && nodeSer.recordCompressor==null ) {
            
        final int checksum = buf.getInt(NodeSerializer.OFFSET_CHECKSUM);
        
        int randomChecksum;
        
        do {
            
            randomChecksum = r.nextInt();
            
        } while( randomChecksum == checksum );
        
        buf.putInt(NodeSerializer.OFFSET_CHECKSUM, randomChecksum);

        assert buf.position() == 0;
//        buf.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx, expected.getIdentity(), buf);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose) 
                System.err.println("Ignoring expected exception: "+ex);
            
        }

        /*
         * Override a byte in the serialized record to test for a correctly
         * reported checksum error.
         * 
         * Note: i am not overwritting the node type byte since that causes
         * a different exception (unknown node type) before we get as far as
         * the checksum test.
         */

        final byte b = buf2.get(NodeSerializer.OFFSET_VERSION);
        
        byte randomByte;
        
        do {
            
            randomByte= (byte) r.nextInt(255);
            
        } while( randomByte == b );
        
        buf2.put(NodeSerializer.OFFSET_VERSION, randomByte );
        
//        buf2.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx,expected.getIdentity(),buf2);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose )
                System.err.println("Ignoring expected exception: "+ex);
            
        }
        }
        
        return actual;
        
    }
    
    /**
     * Creates a new buffer containing a copy of the data in the given buffer.
     * 
     * @param buf
     *            The buffer (in). The data between the position and the limit
     *            will be copied.
     * 
     * @return The buffer (out). The position will be zero and the limit will be
     *         the capacity.
     */
    private ByteBuffer clone(ByteBuffer buf) {
       
        assert buf.position() == 0;
        
        int capacity = buf.limit();
        
        ByteBuffer ret = ByteBuffer.allocate(capacity);
        
        ret.put(buf.asReadOnlyBuffer());
        
        ret.clear();
        
        return ret;
        
    }
    
    /**
     * Small (de-)serialization stress test conducted for a variety of slot
     * sizes and branching factors.
     * 
     * @see #main(String[] args) for a large stress test.
     */
    public void testStress() {
     
        int ntrials = 20;
        int nnodes = 1000;
        
        doStressTest( ntrials,nnodes);
        
    }

    /**
     * Run a stress test.
     * 
     * @param ntrials
     *            The #of trials. Each trial has a random slotSize and
     *            branchingFactor.  50% of the trials (on average) will
     *            use record compression.
     * @param nnodes
     *            The #of random nodes per trial.
     */
    public void doStressTest(int ntrials,int nnodes) {

        // Some branching factors to choose from.
        int[] branchingFactors = new int[] { 3, 4, 8, 16, 27, 32, 48, 64, 96, 99, 112, 128, 256, 512, 1024, 4096 };
//        int[] branchingFactors = new int[] {4096};
        
        for (int trial = 0; trial < ntrials; trial++) {

            // Choose the branching factor randomly.
            final int branchingFactor = branchingFactors[r.nextInt(branchingFactors.length)];

            BTree ndx = getBTree(branchingFactor,r.nextBoolean());

            System.err.println("Trial " + trial + " of " + ntrials
                    + " : testing " + nnodes
                    + " random nodes:  branchingFactor=" + branchingFactor);
            
            for( int i=0; i<nnodes; i++ ) {
                
                AbstractNode expected = getRandomNodeOrLeaf(ndx);
                
                doRoundTripTest( false, ndx, expected);
                
            }

            // Show the counters.
            System.err
                    .println(((KeyBufferSerializer) ndx.getNodeSerializer().keySerializer).counters
                            .toString());
            
        }
        
    }
    
    /**
     * Run a large stress test.
     * 
     * @param args
     *            unused.
     */
    public static void main(String[] args) {

        final int NTRIALS = 1000;
        final int NNODES = 10000;

        new TestNodeSerializer().doStressTest(NTRIALS, NNODES);
        
    }
    
    /**
     * A random {@link Addr} that is only syntactically valid (do not
     * dereference).
     */
    protected long nextAddr() {

        int offset = r.nextInt(Integer.MAX_VALUE/2);

        int nbytes = r.nextInt(1024);
        
        return Addr.toLong(nbytes,offset);
        
    }

//    /**
//     * Generates a random array of keys.
//     * 
//     * @param maxKeys
//     *            The #of key positions to allocate.  This is based on the
//     *            branching factor and whether the keys are for a node or a
//     *            leaf.
//     *            
//     * @param nkeys
//     *            The #of keys position to populate.
//     */
//    protected byte[][] getRandomKeys(int maxKeys, int nkeys) {
//
//        assert nkeys < maxKeys;
//        
//        byte[][] keys = new byte[maxKeys][];
//
//        // used to detect duplicate keys.
//        Set<byte[]> set = new TreeSet<byte[]>(
//                BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//        
//        int n = 0;
//        
//        while( n < nkeys ) {
//            
//            // length of this key in bytes.
//            int len = r.nextInt(10)+1;
//            
//            byte[] key = new byte[len];
//            
//            r.nextBytes(key);
//            
//            if( set.add(key) ) {
//                
//                keys[n++] = key;
//                     
//            }
//            
//        }
//        
//        assert set.size() == nkeys;
//        
//        /*
//         * sort those random keys.
//         */
//        Arrays.sort(keys,BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//
//        /*
//         * return the sorted keys to the caller.
//         */
//        
//        return keys;
//        
//    }
    
    /**
     * Generate a set of N random distinct byte[] keys in sorted order using an
     * unsigned byte[] comparison function.
     * 
     * @param maxKeys
     *            The capacity of the array.
     * 
     * @param nkeys
     *            The #of keys to generate.
     * 
     * @return A byte[][] with nkeys non-null byte[] entries and a capacity of
     *         maxKeys.
     */
    public byte[][] getRandomKeys(int maxKeys, int nkeys) {
        
        assert maxKeys >= nkeys;
        
        final int maxKeyLen = 20;

        /*
         * generate maxKeys distinct keys (sort requires that the keys are
         * non-null).
         */
        
        // used to ensure distinct keys.
        Set<byte[]> set = new TreeSet<byte[]>(BytesUtil.UnsignedByteArrayComparator.INSTANCE);
        
        byte[][] keys = new byte[maxKeys][];

        int n = 0;
        
        while( n < maxKeys) {

            // random key length in [1:maxKeyLen].
            byte[] key = new byte[r.nextInt(maxKeyLen)+1]; 

            // random data in the key.
            r.nextBytes(key);

            if( set.add(key)) {

                keys[n++] = key;

            }
            
        }
    
        /* 
         * place keys into sorted order.
         */
        Arrays.sort(keys,BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        /*
         * clear out keys from keys[nkeys] through keys[maxKeys-1].
         */
        for( int i=nkeys; i<maxKeys; i++ ) {
            
            keys[i] = null;
            
        }
        
        return keys;

    }

    /**
     * Generates a non-leaf node with random data.
     */
    public Node getRandomNode(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;

        final long addr = nextAddr();

        final int nchildren = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;
        
        assert nchildren>=(branchingFactor+1)/2;
        
        assert nchildren<=branchingFactor;
        
        final int nkeys = nchildren-1;

        final byte[][] keys = getRandomKeys(branchingFactor,nkeys);
        
        final long[] children = new long[branchingFactor+1];

        final int[] childEntryCounts = new int[branchingFactor+1];
        
        // node with some valid keys and corresponding child refs.

        int nentries = 0;
        
        for (int i = 0; i < nchildren; i++) {

            children[i] = nextAddr();

            childEntryCounts[i] = r.nextInt(10)+1; // some non-zero count.  
            
            nentries += childEntryCounts[i];
            
        }
                
        /*
         * create the node and set it as the root to fake out the btree.
         */
        
        Node node = new Node(btree, addr, branchingFactor, nentries,
                new ImmutableKeyBuffer(nkeys, branchingFactor, keys), children,
                childEntryCounts);
        
        btree.root = node;

        return node;
        
    }

    /**
     * Generates a leaf node with random data.
     */
    public Leaf getRandomLeaf(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;

        long addr = nextAddr();

        int nkeys = r.nextInt((branchingFactor+1)/2)+(branchingFactor+1)/2;
        assert nkeys>=(branchingFactor+1)/2;
        assert nkeys<=branchingFactor;

        final byte[][] keys = getRandomKeys(branchingFactor+1,nkeys);
        
        final byte[][] values = new byte[branchingFactor+1][];

        final boolean[] deleteMarkers = btree.getIndexMetadata()
                .getDeleteMarkers() ? new boolean[branchingFactor + 1] : null;
        
        final long[] versionTimestamps = btree.getIndexMetadata()
                .getVersionTimestamps() ? new long[branchingFactor + 1] : null;
        
        for( int i=0; i<nkeys; i++ ) {

            values[i] = new byte[r.nextInt(100)];

            r.nextBytes(values[i]);

            if (deleteMarkers != null) {

                deleteMarkers[i] = r.nextBoolean();

            }

            if (versionTimestamps != null) {

                versionTimestamps[i] = System.currentTimeMillis()
                        + r.nextInt(10000);

            }
            
        }

        /*
         * create the leaf and set it as the root to fake out the btree.
         */

        Leaf leaf = new Leaf(btree, addr, branchingFactor,
                new ImmutableKeyBuffer(nkeys, branchingFactor + 1, keys),
                values,//
                versionTimestamps,//
                deleteMarkers//
                ); // ,previous,next);
        
        btree.root = leaf;
        
        return leaf;

    }

    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public AbstractNode getRandomNodeOrLeaf(BTree ndx) {

        if( r.nextBoolean() ) {
            
            return getRandomNode(ndx);
            
        } else {
            
            return getRandomLeaf(ndx);
            
        }
        
    }

}
