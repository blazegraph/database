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
 * Created on Nov 12, 2006
 */

package com.bigdata.btree;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link IKeyBuffer#search(int offset, byte[] searchKey)}.
 * 
 * @todo write performance test?  the existing code can no longer be used since
 * both linear and binary searches first test the shared prefix for the keys and
 * then search on the remainder... perhaps it could be used by elevating the
 * method to test the prefix into the performance test.  another twist for the
 * performance test would be testing for loop unrolling conditions (N<~4) and
 * testing JNI vs pure Java for the comparison functions.
 * 
 * @todo do version of test with negative and positive integer keys so that the
 *       search on the encoded keys detects whether signed bytes or unsigned
 *       bytes are being compared.
 * 
 * @todo write stress test with offset != 0.
 * 
 * @todo do tests with JNI code linked in. note that we only use
 *       {@link BytesUtil#compareBytesWithLenAndOffset(int, int, byte[], int, int, byte[])} for
 *       searching on the key buffers since that allows non-zero offsets into
 *       the search key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyBufferSearch extends TestCase2 {

    public TestKeyBufferSearch() {
    }

    public TestKeyBufferSearch(String name) {
        super(name);
    }

//    public static Test suite() {
//        
//        TestSuite suite = new TestSuite("IKeyBuffer.search");
//        
//        suite.addTestSuite(TestBinarySearch.class);
//        suite.addTestSuite(TestLinearSearch.class);
//        
//        return suite;
//
//    }

//    /*
//     * abstract search methods are implemented by subclasses for testing the
//     * linear vs binary search code.
//     */
    
    public int search(AbstractKeyBuffer kbuf,byte[]key) {
     
        return kbuf.search(key);
        
    }
    
    /**
     * Test search for keys using both a mutable and an immutable key buffer and
     * a known set of keys.
     */
    public void test_search01()
    {

        byte[][] keys = new byte[5][];

        int i = 0;
        keys[i++] = new byte[]{5};  // offset := 0, insert before := -1
        keys[i++] = new byte[]{7};  // offset := 1, insert before := -2
        keys[i++] = new byte[]{9};  // offset := 2, insert before := -3
        keys[i++] = new byte[]{11}; // offset := 3, insert before := -4
        keys[i++] = new byte[]{13}; // offset := 4, insert before := -5
                                    //              insert  after := -6
        int nkeys = 5;

        MutableKeyBuffer kbuf = new MutableKeyBuffer(nkeys,keys);
        
        doSearchTest01(kbuf);
        
        ImmutableKeyBuffer kbuf2 = new ImmutableKeyBuffer( kbuf );
        
        doSearchTest01(kbuf2);
        
    }

    /**
     * Search using the specified buffer which must be pre-initialized with a
     * known set of keys.
     * 
     * @param kbuf The buffer to be searched.
     */
    private void doSearchTest01(AbstractKeyBuffer kbuf) {
        
        // The general formula for the record offset is:
        //
        //    offset := sizeof(record) * ( index - 1 )
        //
        // The general formula for the insertion point is:
        //
        //    insert := - ( offset + 1 )
        //
        // where [offset] is the offset of the record before which the
        // new record should be inserted.

        //
        // verify offset of record found.
        //

        // Verify finds the first record in the array.
        assertEquals(0, search(kbuf, new byte[]{5}));

        // Verify finds the 2nd record in the array.
        assertEquals(1, search(kbuf, new byte[]{7}));

        // Verify finds the penultimate record in the array.
        assertEquals(3, search(kbuf, new byte[]{11}));

        // Verify finds the last record in the array.
        assertEquals(4, search(kbuf, new byte[]{13}));

        //
        // verify insertion points (key not found).
        //

        // Verify insertion point for key less than any value in the
        // array.
        assertEquals(-1, search(kbuf, new byte[]{4}));

        // Verify insertion point for key between first and 2nd
        // records.
        assertEquals(-2, search(kbuf, new byte[]{6}));

        // Verify insertion point for key between penultimate and last
        // records.
        assertEquals(-5, search(kbuf, new byte[]{12}));

        // Verify insertion point for key greater than the last record.
        assertEquals(-6, search(kbuf, new byte[]{14}));

    }

    /**
     * Tests with non-zero offset into a key buffer with a shared prefix
     * of 3 bytes.
     */
    public void test_search02() {

        // build up keys in sorted order.
        int nkeys = 3;
        int maxKeys = 3;
        byte[][] keys = new byte[nkeys][];

        int i = 0;
        keys[i++] = new byte[]{1,3,4};     // offset := 0, insert before := -1
        keys[i++] = new byte[]{1,3,4,1,0}; // offset := 1, insert before := -2
        keys[i++] = new byte[]{1,3,4,2};   // offset := 2, insert before := -3
                                           // insert after := -4

        {
            MutableKeyBuffer kbuf = new MutableKeyBuffer(nkeys, keys);

            assertEquals(3,kbuf.getPrefixLength());
            
            doSearchTest02(kbuf);

            try { // correct rejection when search key is null.
                kbuf.search(null);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }
        }

        {
            ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(nkeys, maxKeys, keys);

            assertEquals(3,kbuf.getPrefixLength());

            doSearchTest02(kbuf);
            
            try { // correct rejection when search key is null.
                kbuf.search(null);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }

        }
        
    }
    
    private void doSearchTest02(IKeyBuffer kbuf) {
        
        System.err.println("kbuf="+kbuf);
        
        assertEquals(0,kbuf.search(new byte[]{1,3,4}));
        assertEquals(1,kbuf.search(new byte[]{1,3,4,1,0}));
        assertEquals(2,kbuf.search(new byte[]{1,3,4,2}));

        // test search before/between/after keys.
        assertEquals(-1,kbuf.search(new byte[]{1,3,3}));
        assertEquals(-2,kbuf.search(new byte[]{1,3,4,0}));
        assertEquals(-3,kbuf.search(new byte[]{1,3,4,1,1}));
        assertEquals(-4,kbuf.search(new byte[]{1,3,4,3}));

    }

    /** 
    * Test with prefixLength of zero and various search keys.
    */
    public void test_search03() {

        int nkeys = 3;
        int maxKeys = 3;
        byte[][] keys = new byte[][] {
                new byte[]{2,3,5},
                new byte[]{4,5,6},
                new byte[]{5,4,9}
        };
        
        doSearchTest03( new MutableKeyBuffer(nkeys,keys));

        doSearchTest03( new ImmutableKeyBuffer(nkeys,maxKeys,keys));

    }

    private void doSearchTest03(IKeyBuffer kbuf) {
        
        assert kbuf.getPrefixLength() == 0;
        
        assertEquals(0,kbuf.search( new byte[]{2,3,5}));
        assertEquals(1,kbuf.search( new byte[]{4,5,6}));
        assertEquals(2,kbuf.search( new byte[]{5,4,9}));

        // test search before given keys.
        assertEquals(-1,kbuf.search( new byte[]{1,3,3}));
        assertEquals(-1,kbuf.search( new byte[]{2,3,3}));
        assertEquals(-1,kbuf.search( new byte[]{2,3,4}));
        assertEquals(-1,kbuf.search( new byte[]{2}));
        assertEquals(-1,kbuf.search( new byte[]{0}));
        assertEquals(-1,kbuf.search( new byte[]{}));
        // test search between given keys.
        assertEquals(-2,kbuf.search( new byte[]{2,3,5,0}));
        assertEquals(-2,kbuf.search( new byte[]{4,5,5,9}));
        assertEquals(-3,kbuf.search( new byte[]{4,5,6,0}));
        assertEquals(-3,kbuf.search( new byte[]{5,4,8,9}));
        // test search after given keys.
        assertEquals(-4,kbuf.search( new byte[]{5,4,9,0}));
        assertEquals(-4,kbuf.search( new byte[]{5,5}));
        assertEquals(-4,kbuf.search( new byte[]{6}));

    }

    /**
     * Test search on empty key buffer.
     */
    public void test_search04() {
      
        int nkeys = 0;
        int maxKeys = 4;
        byte[][] keys = new byte[][] {};
        
        doSearchTest04( new MutableKeyBuffer(nkeys,keys));

        doSearchTest04( new ImmutableKeyBuffer(nkeys,0,keys));
                
        doSearchTest04( new ImmutableKeyBuffer(nkeys,maxKeys,keys));
        
    }
    
    private void doSearchTest04(IKeyBuffer kbuf) {

        assertEquals(-1,kbuf.search(new byte[]{}));
        
        assertEquals(-1,kbuf.search(new byte[]{9,9,9,9}));
        
    }
    
    public void test_prefixMatchLength() {
        
        // build up keys in sorted order.
        int nkeys = 3;
        int maxKeys = 3;
        byte[][] keys = new byte[nkeys][];

        int i = 0;
        keys[i++] = new byte[]{1,3,4};     // offset := 0, insert before := -1
        keys[i++] = new byte[]{1,3,4,0,0}; // offset := 1, insert before := -2
        keys[i++] = new byte[]{1,3,4,1};   // offset := 2, insert before := -3
                                           // insert after := -4
        {
            
            MutableKeyBuffer kbuf = new MutableKeyBuffer(nkeys,keys);
            
            doPrefixMatchLengthTest(kbuf);
            
        }
        
        {
         
            ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(nkeys,maxKeys,keys);
            
            doPrefixMatchLengthTest(kbuf);
         
        }

    }
    
    private void doPrefixMatchLengthTest(AbstractKeyBuffer kbuf) {

        // verify the prefix length.
        final int prefixLength = 3;
        assertEquals("prefixLength", prefixLength, kbuf.getPrefixLength());
        // verify the prefix.
        assertEquals("prefix", new byte[]{1,3,4}, kbuf.getPrefix());

        System.err.println("prefix="+BytesUtil.toString(kbuf.getPrefix()));
            
        /*
         * test on some keys that are in the buffer. all keys in the buffer must
         * match the entire shared prefix.
         */
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4 }));
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4, 0, 0 }));
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4, 1 }));
        /*
         * now test on some keys that also match the entire prefix but are not
         * found in the buffer.
         */
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4, 7 }));
        assertEquals(0, kbuf
                ._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4, 0, 1, 3 }));
        /*
         * now test on some keys that order _before_ the prefix and hence before
         * all keys in the buffer. we include cases where the search key is
         * shorter than the prefix and cases when it is longer than the prefix.
         */
        /*
         * test search keys that have nothing in common with the prefix.
         */
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 0, 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 0, 0, 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 0, 0, 0, 0 }));
        /*
         * test search keys that have only their first byte in common with the
         * prefix.
         */
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 0, 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 0, 0, 0 }));
        /*
         * test search keys that have only their first two bytes in common with
         * the prefix.
         */
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 0 }));
        assertEquals(-1, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 0, 0 }));

        /*
         * test search keys that match the entire prefix (three bytes).
         */
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4 }));
        assertEquals(0, kbuf._prefixMatchLength(prefixLength, new byte[] { 1, 3, 4, 0 }));

        /*
         * now test on some keys that order _after_ the prefix and hence after
         * all keys in the buffer. we include cases where the search key is
         * shorter than the prefix and cases when it is longer than the prefix.
         */
        assertEquals(-(3) - 1, kbuf._prefixMatchLength(prefixLength, new byte[] { 9 }));
        assertEquals(-(3) - 1, kbuf._prefixMatchLength(prefixLength, new byte[] { 9, 9 }));
        assertEquals(-(3) - 1, kbuf._prefixMatchLength(prefixLength, new byte[] { 9, 9, 9 }));
        assertEquals(-(3) - 1, kbuf._prefixMatchLength(prefixLength, new byte[] { 9, 9, 9, 9, 9 }));

    }
    
// public static class TestLinearSearch extends TestKeyBufferSearch {
//        
// public int search(AbstractKeyBuffer kbuf,byte[]key) {
//            
//            return kbuf._linearSearch(offset,key);
//            
//        }
//
//    }
//    
//    public static class TestBinarySearch extends TestKeyBufferSearch {
//        
//        public int search(AbstractKeyBuffer kbuf,int offset,byte[]key) {
//            
//            return kbuf._binarySearch(offset,key);
//            
//        }
//        
//    }
    
//    /**
//     * Performance test to identify the tradeoff point for binary for linear
//     * search.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class PerformanceTest extends TestCase {
//
//        public PerformanceTest() {
//        }
//
//        public PerformanceTest(String name) {
//            super(name);
//        }
//
//        public void testPerformance() {
//            
//            int ntrials = 50000;
//            
//            doPerformanceTest(ntrials);
//            
//        }
//
//        Random r = new Random(); 
//
//        /**
//         * Generate a set of N random distinct byte[] keys in sorted order using
//         * an unsigned byte[] comparison function.
//         * 
//         * @param nkeys The #of keys to generate.
//         * 
//         * @param maxKeyLen The maximum length of a key.
//         */
//        public byte[][] getRandomKeys(int nkeys, int maxKeyLen) {
//
//            // used to ensure distinct keys.
//            Set<byte[]> set = new TreeSet<byte[]>(BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//            
//            byte[][] keys = new byte[nkeys][];
//
//            int n = 0;
//            
//            while( n < nkeys ) {
//
//                // random key length in [1:maxKeyLen].
//                byte[] key = new byte[r.nextInt(maxKeyLen)+1]; 
//
//                // random data in the key.
//                r.nextBytes(key);
//
//                if( set.add(key)) {
//
//                    keys[n++] = key;
//
//                }
//                
//            }
//        
//            // place into sorted order.
//            Arrays.sort(keys,BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//            
//            return keys;
//
//        }
//
//        /**
//         * Performance test comparing binary vs linear search.
//         * 
//         * @param ntrials
//         */
//        public void doPerformanceTest(int ntrials) {
//
//            /*
//             * Note: searching large arrays first since that warms up the code
//             * even further and the difference between the linear vs binary
//             * algorithms will only show up at small N, which we test last with
//             * the "warmest" code.
//             */ 
//            //int[] capacity = new int[]{8,16,32,48,64,96,128,256,512,768,1024};
//            int[] capacity = new int[]{1024,768,512,256,128,96,64,48,32,24,16,12,8,4};
//            
//            for( int k = 0; k < capacity.length; k++ ) {
//                
//                int nkeys = capacity[k];
//                
//                /*
//                 * @todo see how performance varies by average key length and
//                 * consider the key length distribution as well as the key value
//                 * distribution - the latter is important for interpolated
//                 * search.
//                 */
//                byte[][] keys = getRandomKeys(nkeys, 20);
//
//                {
//
//                    MutableKeyBuffer kbuf = new MutableKeyBuffer(nkeys, keys);
//
//                    final int prefixLength = kbuf.getPrefixLength();
//                    
//                    // [0:prefixLength-1].
//                    final int offset = prefixLength == 0 ? 0 : r.nextInt(prefixLength);
//                    
//                    long elapsedLinear1 = doTest(true, ntrials, keys, kbuf);
//
//                    long elapsedBinary1 = doTest(false, ntrials, keys, kbuf);
//
//                    System.err
//                            .println("  mutable[]: nkeys="
//                                    + nkeys
//                                    + ", trials="
//                                    + ntrials
//                                    + ", offset="+offset
//                                    + ", elapsedLinear="
//                                    + elapsedLinear1
//                                    + "ns"
//                                    + ", elapsedBinary="
//                                    + elapsedBinary1
//                                    + "ns"
//                                    + (elapsedLinear1 < elapsedBinary1 ? ", linear wins"
//                                            : ", binary wins") + " by "
//                                    + Math.abs(elapsedLinear1 - elapsedBinary1)
//                                    + "ns");
//                }
//
//                {
//                    
//                    ImmutableKeyBuffer kbuf = new ImmutableKeyBuffer(nkeys,keys);
//
//                    final int prefixLength = kbuf.getPrefixLength();
//                    
//                    // [0:prefixLength-1].
//                    final int offset = prefixLength == 0 ? 0 : r.nextInt(prefixLength);
//
//                    long elapsedLinear2 = doTest(true, ntrials, keys, kbuf);
//
//                    long elapsedBinary2 = doTest(false, ntrials, keys, kbuf);
//
//                    System.err
//                            .println("immutable[]: nkeys="
//                                    + nkeys
//                                    + ", trials="
//                                    + ntrials
//                                    + ", offset="+offset
//                                    + ", elapsedLinear="
//                                    + elapsedLinear2
//                                    + "ns"
//                                    + ", elapsedBinary="
//                                    + elapsedBinary2
//                                    + "ns"
//                                    + (elapsedLinear2 < elapsedBinary2 ? ", linear wins"
//                                            : ", binary wins") + " by "
//                                    + Math.abs(elapsedLinear2 - elapsedBinary2)
//                                    + "ns");
//                }
//                
//            }
//            
//        }
//        
//        /**
//         * Time a bunch of searches.
//         * 
//         * @param linear
//         *            use linear search when true
//         * @param ntrials
//         *            #of searches to perform.
//         * @param keys
//         *            search keys are randomly selected from this array of keys.
//         * @param kbuf
//         *            The key buffer on which the search will be performed.
//         * 
//         * @return the elapsed time aggregated across the searches.
//         */
//        public long doTest(boolean linear,int ntrials, byte[][] keys, AbstractKeyBuffer kbuf) {
//            
//            long elapsedNanos = 0;
//            
//            final int nkeys = kbuf.getKeyCount();
//            
//            for( int i=0; i<ntrials; i++ ) {
//
//                // index of the search key in the key buffer.
//                final int index = r.nextInt(nkeys);
//                
//                byte[] key = keys[ index ];
//                
//                final int index2;
//                
//                long beginNanos = System.nanoTime();
//                
//                if( linear ) {
//                    
//                    index2 = kbuf._linearSearch(offset,key);
//                    
//                } else {
//                    
//                    index2 = kbuf._binarySearch(offset,key);
//                    
//                }
//                
//                elapsedNanos += System.nanoTime() - beginNanos;
//                
//                // make sure the search result is correct.
//                if (index != index2) {
//                    
//                    fail("Expected to find search key at index=" + index
//                            + ", not at index=" + index2 + "; searchKey=" + key
//                            + ", key[index]="
//                            + BytesUtil.toString(kbuf.getKey(index))
//                            + ", key[index2]="
//                            + BytesUtil.toString(kbuf.getKey(index2)));
//                    
//                }
//                
//            }
//            
//            return elapsedNanos;
//
//        }
//
//    }

}
