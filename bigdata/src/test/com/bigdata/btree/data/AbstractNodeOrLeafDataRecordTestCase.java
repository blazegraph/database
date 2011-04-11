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
 * Created on Aug 15, 2009
 */

package com.bigdata.btree.data;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.rawstore.Bytes;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractNodeOrLeafDataRecordTestCase extends
        AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractNodeOrLeafDataRecordTestCase() {
    }

    /**
     * @param name
     */
    public AbstractNodeOrLeafDataRecordTestCase(String name) {
        super(name);
    }

//    protected IRabaCoder keysCoder = null;
//    protected IRabaCoder valuesCoder = null;

	/**
	 * Factory for the mock {@link ILeafData} object used to provide ground
	 * truth for the fixture under test.
	 * 
	 * @param keys
	 *            The keys.
	 * @param vals
	 *            The values.
	 * 
	 * @return The mock {@link ILeafData} object.
	 */
	final protected ILeafData mockLeafFactory(final IRaba keys, final IRaba vals) {

		return mockLeafFactory(keys, vals, null/* deleteMarkers */,
				null/* versionTimestamps */, null/* rawRecords */);

	}

	/**
	 * Factory for the mock {@link ILeafData} object used to provide ground
	 * truth for the fixture under test.
	 * 
	 * @param keys
	 *            The keys.
	 * @param vals
	 *            The values.
	 * @param deleteMarkers
	 *            The delete markers (optional).
	 * @param versionTimestamps
	 *            The version timestamps (optional).
	 * @param rawRecords
	 *            The bit flags indicating which tuples are have their value
	 *            stored as a raw record on the backing persistence store
	 *            (optional).
	 * 
	 * @return The mock {@link ILeafData} object.
	 */
	protected ILeafData mockLeafFactory(final IRaba keys, final IRaba vals,
			final boolean[] deleteMarkers, final long[] versionTimestamps,
			final boolean[] rawRecords) {

		return new MockLeafData(keys, vals, deleteMarkers, versionTimestamps,
				rawRecords);

    }
    
    /**
     * Set by concrete test suite classes to the coder under test.
     */
    protected IAbstractNodeDataCoder<?> coder = null;
    
    /**
     * De-serialization stress test conducted for a variety of and branching
     * factors.
     */
    public void testStress() {
     
        final int ntrials = 10;
        
        final int nnodes = 500;

        doStressTest(ntrials, nnodes);

    }

    /**
     * Run a stress test.
     * <p>
     * Note: You may run out of heap space during the test for large branching
     * factors when combined with a large #of nodes.
     * 
     * @param ntrials
     *            The #of trials. Each trial has a random slotSize and
     *            branchingFactor. 50% of the trials (on average) will use
     *            record compression.
     * @param nnodes
     *            The #of random nodes per trial.
     */
    public void doStressTest(final int ntrials, final int nnodes) {

        /*
         * Some branching factors to choose from.
         */
        final int[] branchingFactors = new int[] { 3, 4, 8, 16, 27, 32, 48, 64,
                96, 99, 112, 128, 256, 512, 1024};//, 4096};
//        int[] branchingFactors = new int[] {4096};
        
        for (int trial = 0; trial < ntrials; trial++) {

            // Choose the branching factor randomly.
            final int branchingFactor = branchingFactors[r
                    .nextInt(branchingFactors.length)];

            final boolean deleteMarkers = r.nextBoolean();

            final boolean versionTimestamps = r.nextBoolean();

            final boolean rawRecords = r.nextBoolean();

            System.err.println("Trial "
                    + trial
                    + " of "
                    + ntrials
                    + " : testing "
                    + nnodes
                    + " random nodes:  branchingFactor="
                    + branchingFactor
					+ (mayGenerateLeaves() ? ", deleteMarkers=" + deleteMarkers
							+ ", versionTimestamps=" + versionTimestamps
							+ ", rawRecords=" + rawRecords : ""));

            final DataOutputBuffer buf = new DataOutputBuffer();
            
            for (int i = 0; i < nnodes; i++) {

				final IAbstractNodeData expected = getRandomNodeOrLeaf(
						branchingFactor, deleteMarkers, versionTimestamps,
						rawRecords);

                doRoundTripTest(expected, coder, buf);
                
            }
   
        }
        
    }

    /**
     * 
     * @param expected
     * @param coder
     * @param buf
     */
    protected void doRoundTripTest(final IAbstractNodeData expected,
            final IAbstractNodeDataCoder<?> coder, final DataOutputBuffer buf) {

        // clear the buffer before encoding data on it.
        buf.reset();

        if (expected.isLeaf()) {

            /*
             * A leaf data record.
             */

            // Test the live coded path (returns coded instance for immediate use).
            final ILeafData liveCodedLeaf = ((IAbstractNodeDataCoder<ILeafData>) coder)
                    .encodeLive((ILeafData) expected, new DataOutputBuffer());

            final AbstractFixedByteArrayBuffer liveCodedData = liveCodedLeaf
                    .data();

            assertSameLeafData((ILeafData) expected, liveCodedLeaf);

            // encode
            final AbstractFixedByteArrayBuffer originalData = ((IAbstractNodeDataCoder<ILeafData>) coder)
                    .encode((ILeafData) expected, buf);

            // verify same encoding.
            assertTrue(0 == BytesUtil.compareBytesWithLenAndOffset(
                    liveCodedData.off(), liveCodedData.len(), liveCodedData
                            .array(), originalData.off(), originalData.len(),
                    originalData.array()));

            // Verify we can decode the record.
            {
                
                // decode.
                final ILeafData actual = ((IAbstractNodeDataCoder<ILeafData>) coder)
                        .decode(originalData);

                // verify the decoded data.
                assertSameLeafData((ILeafData) expected, actual);

            }
            
            // Verify encode with a non-zero offset for the DataOutputBuffer
            // returns a slice which has the same data.
            {

                // buffer w/ non-zero offset.
                final int off = 10;
                final DataOutputBuffer out = new DataOutputBuffer(off,
                        new byte[1000 + off]);

                // encode onto that buffer.
                final AbstractFixedByteArrayBuffer slice = ((IAbstractNodeDataCoder<ILeafData>) coder)
                        .encode((ILeafData) expected, out);

                // verify same encoded data for the slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());

            }

            // Verify decode when we build the decoder from a slice with a
            // non-zero offset
            {

                final int off = 10;
                final byte[] tmp = new byte[off + originalData.len()];
                System.arraycopy(originalData.array(), originalData.off(), tmp,
                        off, originalData.len());

                // create slice
                final FixedByteArrayBuffer slice = new FixedByteArrayBuffer(
                        tmp, off, originalData.len());

                // verify same slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());

                // decode the slice.
                final ILeafData actual = ((IAbstractNodeDataCoder<ILeafData>) coder)
                        .decode(slice);

                // verify the decoded slice.
                assertSameLeafData((ILeafData) expected, actual);
                
            }

        } else {

            /*
             * A node data record.
             */

            // Test the live coded path (returns coded instance for immediate use).
            final INodeData liveCodedNode = ((IAbstractNodeDataCoder<INodeData>) coder)
                    .encodeLive((INodeData) expected, new DataOutputBuffer());

            final AbstractFixedByteArrayBuffer liveCodedData = liveCodedNode
                    .data();

            assertSameNodeData((INodeData) expected, liveCodedNode);

            // encode
            final AbstractFixedByteArrayBuffer originalData = ((IAbstractNodeDataCoder<INodeData>) coder)
                    .encode((INodeData) expected, buf);

            // Verify we can decode the record.
            {

                // decode
                final INodeData actual = ((IAbstractNodeDataCoder<INodeData>) coder)
                        .decode(originalData);

                // verify the decoded data.
                assertSameNodeData((INodeData) expected, actual);

            }

            // Verify encode with a non-zero offset for the DataOutputBuffer
            // returns a slice which has the same data.
            {

                // buffer w/ non-zero offset.
                final int off = 10;
                final DataOutputBuffer out = new DataOutputBuffer(off,
                        new byte[1000 + off]);

                // encode onto that buffer.
                final AbstractFixedByteArrayBuffer slice = ((IAbstractNodeDataCoder<INodeData>) coder)
                        .encode((INodeData) expected, out);

                // verify same encoded data for the slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());

            }
            
            // Verify decode when we build the decoder from a slice with a
            // non-zero offset
            {

                final int off = 10;
                final byte[] tmp = new byte[off + originalData.len()];
                System.arraycopy(originalData.array(), originalData.off(), tmp,
                        off, originalData.len());

                // create slice
                final FixedByteArrayBuffer slice = new FixedByteArrayBuffer(
                        tmp, off, originalData.len());

                // verify same slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());

                // decode the slice.
                final INodeData actual = ((IAbstractNodeDataCoder<INodeData>) coder)
                        .decode(slice);

                // verify the decoded slice.
                assertSameNodeData((INodeData) expected, actual);
                
            }
            
        }

    }
    
    //    /**
//     * Run a large stress test.
//     * 
//     * @param args
//     *            unused.
//     */
//    public static void main(String[] args) {
//
//        final int NTRIALS = 1000;
//        final int NNODES = 10000;
//
//        new TestNodeSerializer().doStressTest(NTRIALS, NNODES);
//        
//    }
    
    /**
     * A random address that is only syntactically valid (do not dereference).
     */
    protected long nextAddr() {

        final int offset = r.nextInt(Integer.MAX_VALUE/2);

        final int nbytes = r.nextInt(1024);

        // return Addr.toLong(nbytes,offset);

        return ((long) offset) << 32 | nbytes;

    }

    /**
     * Generates a non-leaf node with random data.
     */
    public INodeData getRandomNode(final int m) {

        // #of keys per node.
        final int branchingFactor = m;

        // final long addr = nextAddr();

        final int nchildren = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;

        assert nchildren >= (branchingFactor + 1) / 2;

        assert nchildren <= branchingFactor;

        final int nkeys = nchildren - 1;

        final byte[][] keys = getRandomKeys(branchingFactor, nkeys);
        
        final long[] children = new long[branchingFactor+1];

        final int[] childEntryCounts = new int[branchingFactor+1];
        
        final boolean hasVersionTimestamp = r.nextBoolean();
        
        final long minimumVersionTimestamp = hasVersionTimestamp ? System
                .currentTimeMillis() : 0L;

        final long maximumVersionTimestamp = hasVersionTimestamp ? System
                .currentTimeMillis()
                + r.nextInt() : 0L;

        // node with some valid keys and corresponding child refs.

        int nentries = 0;
        
        for (int i = 0; i < nchildren; i++) {

            children[i] = nextAddr();

            childEntryCounts[i] = r.nextInt(10)+1; // some non-zero count.  
            
            nentries += childEntryCounts[i];
            
        }
                
        /*
         * Create the node.
         */

        return new MockNodeData(new ReadOnlyKeysRaba(nkeys, keys), nentries,
                children, childEntryCounts, hasVersionTimestamp,
                minimumVersionTimestamp, maximumVersionTimestamp);
        
    }

    /**
     * Generates a leaf node with random data.
     */
    public ILeafData getRandomLeaf(//
    		final int m,//
            final boolean isDeleteMarkers,// 
            final boolean isVersionTimestamps,//
            final boolean isRawRecords//
            ) {

        // #of keys per node.
        final int branchingFactor = m;

//        long addr = nextAddr();

        final int nkeys = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;
        assert nkeys >= (branchingFactor + 1) / 2;
        assert nkeys <= branchingFactor;

        final byte[][] keys = getRandomKeys(branchingFactor + 1, nkeys);

        final byte[][] values = new byte[branchingFactor + 1][];

        final boolean[] deleteMarkers = isDeleteMarkers ? new boolean[branchingFactor + 1]
                : null;

        final long[] versionTimestamps = isVersionTimestamps ? new long[branchingFactor + 1]
                : null;

		final boolean[] rawRecords = isRawRecords ? new boolean[branchingFactor + 1]
				: null;

        for (int i = 0; i < nkeys; i++) {

			// The % of deleted tuples in this leaf.
			final boolean deleted = deleteMarkers!=null && r.nextDouble()<.05;
        	
			if (deleted) {
				
				// Delete marker is has a [null] value.
				deleteMarkers[i] = r.nextBoolean();
				
			} else {

	        	// The % of large values in this leaf.
				final boolean isRawRecord = rawRecords != null
						&& r.nextDouble() < .3;

				// Not a deleted tuple.
				if(isRawRecord) {
					
					// Raw record on the backing store.
					values[i] = AbstractBTree.encodeRecordAddr(recordAddrBuf,
							nextAddr());

					rawRecords[i] = true;

				} else {

					// Normal value (inline w/in the leaf).
					values[i] = new byte[r.nextInt(100)];

					r.nextBytes(values[i]);

					if (rawRecords != null)
						rawRecords[i] = false;

				}
				
			}

            if (versionTimestamps != null) {

                versionTimestamps[i] = System.currentTimeMillis()
                        + r.nextInt(10000);

            }

        }

        return mockLeafFactory(//
                new ReadOnlyKeysRaba(nkeys, keys),//
                new ReadOnlyValuesRaba(nkeys, values),//
                deleteMarkers,//
                versionTimestamps,//
                rawRecords//
        );

    }

	final ByteArrayBuffer recordAddrBuf = new ByteArrayBuffer(Bytes.SIZEOF_LONG);
    
    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public IAbstractNodeData getRandomNodeOrLeaf(final int m,
            final boolean deleteMarkers, final boolean versionTimestamps,
            final boolean rawRecords) {

        assert mayGenerateLeaves() || mayGenerateNodes();

        if (!mayGenerateLeaves()) {

			return getRandomNode(m);

		} else if (!mayGenerateNodes()) {

			return getRandomLeaf(m, deleteMarkers, versionTimestamps,
					rawRecords);

		} else {

			if (r.nextBoolean()) {

				return getRandomNode(m);

			} else {

				return getRandomLeaf(m, deleteMarkers, versionTimestamps,
						rawRecords);

            }

        }

    }

    abstract protected boolean mayGenerateNodes();

    abstract protected boolean mayGenerateLeaves();

    /**
     * Verify methods that recognize a node vs a leaf based on a byte.
     */
    public void test_nodeOrLeafFlag() {

        // isLeaf()
        assertTrue(AbstractReadOnlyNodeData
                .isLeaf(AbstractReadOnlyNodeData.LEAF));
        
        assertTrue(AbstractReadOnlyNodeData
                .isLeaf(AbstractReadOnlyNodeData.LINKED_LEAF));

        assertFalse(AbstractReadOnlyNodeData
                .isLeaf(AbstractReadOnlyNodeData.NODE));
        
        // isNode()
        assertFalse(AbstractReadOnlyNodeData
                .isNode(AbstractReadOnlyNodeData.LEAF));
        
        assertFalse(AbstractReadOnlyNodeData
                .isNode(AbstractReadOnlyNodeData.LINKED_LEAF));

        assertTrue(AbstractReadOnlyNodeData
                .isNode(AbstractReadOnlyNodeData.NODE));

    }

    /* FIXME peformance tuning main() for INodeData and ILeafData so we can
     * examine things like childAddr[] and timestamp[] coding costs. 
     */
//    /**
//     * Performance stress test for B+Tree node/leaf data records.
//     * 
//     * <dl>
//     * <dt>nops</dt>
//     * <dd>
//     * The #of random operations to be performed. Large values for <i>nops</i>
//     * need to be used to get beyond the initial JVM performance tuning so you
//     * can more accurately compare the performance of the different coders. For
//     * example, a value of 1M (1000000) will run for ~ 30-40s for the
//     * front-coded coders. For shorter run times, the order in which we test the
//     * coders will dominate their performance.</dd>
//     * <dt>size</dt>
//     * <dd>The #of entries in the raba to be tested (must be LTE the capacity)</dd>
//     * </dl>
//     * 
//     * @param args
//     *            [nops [generator [size]]]
//     * 
//     *            FIXME parameterize the generator choice.
//     */
//    static public void main(final String[] args) {
//
//        final Random r = new Random();
//
//        // default nops.
//        int nops = 200000;
////        int nops = 1000000; // ~30-40s per coder @ 1M.
//        if (args.length > 0)
//            nops = Integer.valueOf(args[0]);
//        if (nops <= 0)
//            throw new IllegalArgumentException();
//        
////        // default capacity (branching factor).
////        int capacity = 256;
////        if (args.length > 1)
////            capacity = Integer.valueOf(args[1]);
////        if (capacity <= 0)
////            throw new IllegalArgumentException();
//
//        // default size (#of keys).
//        int size = 256;
//        if (args.length > 2)
//            nops = Integer.valueOf(args[2]);
//        if (size <= 0)
//            throw new IllegalArgumentException();
//        
//        // The coders to be tested.
//        final IRabaCoder[] coders = new IRabaCoder[] {
//                new MutableRabaCoder(), // provides performance baseline.
//                SimpleRabaCoder.INSTANCE, // simplest coding.
//////                new FrontCodedRabaCoder(2/* ratio */),
//                new FrontCodedRabaCoder(8/* ratio */), // front-coding.
//////                new FrontCodedRabaCoder(32/* ratio */),
//                CanonicalHuffmanRabaCoder.INSTANCE // huffman coding.
//                };
//
//        System.out.println("nops=" + nops + ", size=" + size + ", ncoders="
//                + coders.length);
//
//        /*
//         * Generate a raba.  The same data is used for each coder. 
//         */
//
//        // The raw data.
//        final byte[][] a;
//
//        // Random keys based on random variable length byte[]s.
////        a = new RandomKeysGenerator(r, size + r.nextInt(size)/* maxKeys */, 20/* maxKeyLength */)
////                .generateKeys(size);
//
//        // Random URIs in sorted order.
////        a = new RandomURIGenerator(r).generateKeys(size);
//
//        // based on a tokenized source code file.
//        a = new TokenizeKeysGenerator(
//                "bigdata/src/test/com/bigdata/btree/raba/codec/AbstractRabaCoderTestCase.java")
//                .generateKeys(size);
//        
//        /*
//         * isNull, length, get, copy, search, iterator, recode.
//         * 
//         * Note: isNull is not used for keys!
//         */
//        final Op op = new Op(0.0f, .01f, .4f, .2f, .6f, .2f, .04f);
//
//        /*
//         * Test each IRabaCoder.
//         * 
//         * @todo should also test on coded B+Tree values, which would be a
//         * different [expected] instance.
//         */
//        for(IRabaCoder rabaCoder : coders) {
//
//            // the read-only raba.
//            final ReadOnlyKeysRaba expected = new ReadOnlyKeysRaba(size, a);
//
//            final long begin = System.nanoTime();
//
//            int recordLength = -1;
//            try {
//
//                recordLength = doRabaCoderPerformanceTest(expected, rabaCoder,
//                        size, nops, r, op);
//                
//            } catch (Throwable t) {
//
//                System.err.println("coder failed: " + rabaCoder);
//                
//                t.printStackTrace(System.err);
//                
//            }
//
//            final long elapsed = System.nanoTime() - begin;
//
//            System.out.println(rabaCoder.toString() + " : elapsed="
//                    + TimeUnit.NANOSECONDS.toMillis(elapsed)
//                    + ", recordLength="
//                    + (recordLength == -1 ? "N/A" : recordLength));
//
//        }
//        
//    }
//    
//    /**
//     * Helper class generates a random sequence of operation codes obeying the
//     * probability distribution described in the constructor call.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class Op {
//        
//        static public final int ISNULL = 0;
//        static public final int LENGTH = 1;
//        static public final int GET = 2;
//        static public final int COPY = 3;
//        static public final int SEARCH = 4;
//        static public final int ITERATOR = 5;
//        static public final int RECODE = 6;
//        
//        /**
//         * The last defined operator.
//         */
//        static final int lastOp = RECODE;
//        
////        final private Random r = new Random();
//        
//        final private float[] _dist;
//
//        /*
//         * isNull, length, get, copy, search, iterator, recode.
//         */
//        public Op(float isNullRate, float lengthRate, float getRate,
//                float copyRate, float searchRate, float iteratorRate,
//                float recodeRate)
//        {
//            if (isNullRate < 0 || lengthRate < 0 || getRate < 0
//                    || copyRate < 0 || searchRate < 0 || iteratorRate < 0
//                    || recodeRate < 0) {
//                throw new IllegalArgumentException("negative rate");
//            }
//            float total = isNullRate + lengthRate + getRate + copyRate
//                    + searchRate + iteratorRate + recodeRate;
//            if( total == 0.0 ) {
//                throw new IllegalArgumentException("all rates are zero.");
//            }
//            /*
//             * Convert to normalized distribution in [0:1].
//             */
//            isNullRate /= total;
//            lengthRate /= total;
//            getRate /= total;
//            copyRate /= total;
//            searchRate /= total;
//            iteratorRate /= total;
//            recodeRate /= total;
//            /*
//             * Save distribution.
//             */
//            int i = 0;
//            _dist = new float[lastOp+1];
//            _dist[ i++ ] = isNullRate;
//            _dist[ i++ ] = lengthRate;
//            _dist[ i++ ] = getRate;
//            _dist[ i++ ] = copyRate;
//            _dist[ i++ ] = searchRate;
//            _dist[ i++ ] = iteratorRate;
//            _dist[ i++ ] = recodeRate;
//
//            /*
//             * Checksum.
//             */
//            float sum = 0f;
//            for( i = 0; i<_dist.length; i++ ) {
//                sum += _dist[ i ];
//            }
//            if( Math.abs( sum - 1f) > 0.01 ) {
//                throw new AssertionError("sum of distribution is: "+sum+", but expecting 1.0");
//            }
//            
//        }
//        
//        /**
//         * Return the name of the operator.
//         * 
//         * @param op
//         * @return
//         */
//        public String getName( final int op ) {
//            if( op < 0 || op > lastOp ) {
//                throw new IllegalArgumentException();
//            }
//            /*
//             * isNull, length, get, copy, search, iterator, recode.
//             */
//            switch( op ) {
//            case ISNULL:  return "isNull";
//            case LENGTH:  return "length";
//            case GET:     return "get   ";
//            case COPY:    return "copy  ";
//            case SEARCH:  return "search";
//            case ITERATOR:return "itr   ";
//            case RECODE:  return "recode";
//            default:
//                throw new AssertionError();
//            }
//        }
//        
//        /**
//         * An array of normalized probabilities assigned to each operator. The
//         * array may be indexed by the operator, e.g., dist[{@link #fetch}]
//         * would be the probability of a fetch operation.
//         * 
//         * @return The probability distribution over the defined operators.
//         */
//        public float[] getDistribution() {
//            return _dist;
//        }
//
//        /**
//         * Generate a random operator according to the distribution described to
//         * to the constructor.
//         * 
//         * @return A declared operator selected according to a probability
//         *         distribution.
//         */
//        public int nextOp(final Random r) {
//            final float rand = r.nextFloat(); // [0:1)
//            float cumprob = 0f;
//            for( int i=0; i<_dist.length; i++ ) {
//                cumprob += _dist[ i ];
//                if( rand <= cumprob ) {
//                    return i;
//                }
//            }
//            throw new AssertionError();
//        }
//        
//    }
//
//    /**
//     * Tests of the {@link Op} test helper class.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class TestOp extends TestCase {
//
//        private final Random r = new Random();
//        
//        public void test_Op() {
//            /*
//             * isNull, length, get, copy, search, iterator, recode.
//             */
//            Op gen = new Op(.2f, .05f, .2f, 05f, .1f, .05f, .001f);
//            doOpTest(gen);
//        }
//
//        public void test_Op2() {
//            /*
//             * isNull, length, get, copy, search, iterator, recode.
//             */
//            Op gen = new Op(0f,0f,0f,1f,0f,0f,0f);
//            doOpTest(gen);
//        }
//
//        /**
//         * Correct rejection test when all rates are zero.
//         */
//        public void test_correctRejectionAllZero() {
//            /*
//             * isNull, length, get, copy, search, iterator, recode.
//             */
//            try {
//                new Op(0f,0f,0f,0f,0f,0f,0f);
//                fail("Expecting: "+IllegalArgumentException.class);
//            }
//            catch(IllegalArgumentException ex) {
//                log.info("Ignoring expected exception: "+ex);
//            }
//        }
//
//        /**
//         * Correct rejection test when one or more rates are negative.
//         */
//        public void test_correctRejectionNegativeRate() {
//            /*
//             * isNull, length, get, copy, search, iterator, recode.
//             */
//            try {
//                new Op(0f,0f,0f,-1f,0f,1f,0f);
//                fail("Expecting: "+IllegalArgumentException.class);
//            }
//            catch(IllegalArgumentException ex) {
//                log.info("Ignoring expected exception: "+ex);
//            }
//        }
//
//        /**
//         * Verifies the {@link Op} class given an instance with some probability
//         * distribution.
//         */
//        void doOpTest(final Op gen) {
//            final int limit = 10000;
//            int[] ops = new int[limit];
//            int[] sums = new int[Op.lastOp + 1];
//            for (int i = 0; i < limit; i++) {
//                int op = gen.nextOp(r);
//                assertTrue(op >= 0);
//                assertTrue(op <= Op.lastOp);
//                ops[i] = op;
//                sums[op]++;
//            }
//            float[] expectedProbDistribution = gen.getDistribution();
//            float[] actualProbDistribution = new float[Op.lastOp + 1];
//            float sum = 0f;
//            for (int i = 0; i <= Op.lastOp; i++) {
//                sum += expectedProbDistribution[i];
//                actualProbDistribution[i] = (float) ((double) sums[i] / (double) limit);
//                float diff = Math.abs(actualProbDistribution[i]
//                        - expectedProbDistribution[i]);
//                System.err.println("expected[i=" + i + "]="
//                        + expectedProbDistribution[i] + ", actual[i=" + i
//                        + "]=" + actualProbDistribution[i] + ", diff="
//                        + ((int) (diff * 1000)) / 10f + "%");
//                assertTrue(diff < 0.02); // difference is less than 2%
//                                            // percent.
//            }
//            assertTrue(Math.abs(sum - 1f) < 0.01); // essential 1.0
//        }
//
//    }
    
}
