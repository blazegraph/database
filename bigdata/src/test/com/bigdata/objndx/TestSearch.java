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

package com.bigdata.objndx;

import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * Unit tests for {@link Search}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearch extends TestCase {

    public TestSearch() {
    }

    public TestSearch(String name) {
        super(name);
    }

    public static Test suite() {
        
        TestSuite suite = new TestSuite("Search");
        
        suite.addTestSuite(TestSearch.class);
        suite.addTestSuite(TestBinarySearch.class);
        suite.addTestSuite(TestLinearSearch.class);
        
        return suite;

    }
    
    
    public int search(int key,int[]keys,int nkeys) {
    
        return Search.search(key,keys,nkeys);
        
    }

    /**
     * Test binary search for keys in a node. The binary search routine is
     * implemented just once, by {@link AbstractNode#keySearch(int)}. This test
     * sets up some keys, adjusts the #of defined keys, and then verifies both
     * correct lookup of keys that exist and the correct insertion point when
     * the key does not exist.
     * 
     * @todo expand to test when nkeys != keys.length using a new test helper?
     */
    public void test_keySearch01()
    {
    
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

        int[] keys = new int[5];

        int i = 0;
        keys[i++] = 5;  // offset := 0, insert before := -1
        keys[i++] = 7;  // offset := 1, insert before := -2
        keys[i++] = 9;  // offset := 2, insert before := -3
        keys[i++] = 11; // offset := 3, insert before := -4
        keys[i++] = 13; // offset := 4, insert before := -5
                        //              insert  after := -6
        int nkeys = 5;

        //
        // verify offset of record found.
        //

        // Verify finds the first record in the array.
        assertEquals(0, search(5,keys,nkeys));

        // Verify finds the 2nd record in the array.
        assertEquals(1, search(7,keys,nkeys));

        // Verify finds the penultimate record in the array.
        assertEquals(3, search(11,keys,nkeys));

        // Verify finds the last record in the array.
        assertEquals(4, search(13,keys,nkeys));

        //
        // verify insertion points (key not found).
        //

        // Verify insertion point for key less than any value in the
        // array.
        assertEquals(-1, search(4,keys,nkeys));

        // Verify insertion point for key between first and 2nd
        // records.
        assertEquals(-2, search(6,keys,nkeys));

        // Verify insertion point for key between penultimate and last
        // records.
        assertEquals(-5, search(12,keys,nkeys));

        // Verify insertion point for key greater than the last record.
        assertEquals(-6, search(14,keys,nkeys));

    }

    public static class TestLinearSearch extends TestSearch {
        
        public int search(int key,int[]keys,int nkeys) {
            
            return Search.linearSearch(key,keys,nkeys);
            
        }

    }
    
    public static class TestBinarySearch extends TestSearch {
        
        public int search(int key,int[]keys,int nkeys) {
            
            return Search.binarySearch(key,keys,nkeys);
            
        }
        
    }
    
    /**
     * Performance test to identify the tradeoff point for binary for linear
     * search.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PerformanceTest extends TestCase {

        public PerformanceTest() {
        }

        public PerformanceTest(String name) {
            super(name);
        }

        public void testPerformance() {
            
            doPerformanceTest(10000);
            
        }

        Random r = new Random(); 

        /**
         * Warms up the code by running a bunch of searches.
         */
        public void warmUp() {
            
            int nkeys = 100;
            
            int keys[] = new int[nkeys];
            
            /*
             * Generate keys. The keys are a monotonic progression with
             * random non-zero intervals.
             */
            
            int lastKey = 0; // origin one, so this is negative infinity.
            
            for( int i=0; i<nkeys; i++ ) {
                
                int key = lastKey + r.nextInt(100) + 1;
                
                keys[ i ] = key;
                
                lastKey = key; 
                
            }

            System.err.println("Warming code.");
            
            int ntrials = 100;

            for (int i = 0; i < 1000; i++) {

                doTest(true, ntrials, nkeys, keys);

                doTest(false, ntrials, nkeys, keys);

            }

            System.err.println("Warmed code.");
            
        }

        /**
         * Performance test comparing binary vs linear search.
         * 
         * @param ntrials
         */
        public void doPerformanceTest(int ntrials) {

            warmUp();
            
            int[] capacity = new int[]{8,16,32,48,64,96,128,256,512,768,1024};
            
            for( int k = 0; k < capacity.length; k++ ) {
                
                int nkeys = capacity[k];
                
                int[] keys = new int[nkeys];
                
                /*
                 * Generate keys. The keys are a monotonic progression with
                 * random non-zero intervals.
                 */
                
                int lastKey = 0; // origin one, so this is negative infinity.
                
                for( int i=0; i<nkeys; i++ ) {
                    
                    int key = lastKey + r.nextInt(100) + 1;
                    
                    keys[ i ] = key;
                    
                    lastKey = key; 
                    
                }

                long elapsedLinear = doTest(true,ntrials,nkeys,keys);
                
                long elapsedBinary = doTest(false,ntrials,nkeys,keys);

                System.err.println("nkeys="
                        + nkeys
                        + ", trials="
                        + ntrials
                        + ", elapsedLinear="
                        + elapsedLinear
                        + "ns"
                        + ", elapsedBinary="
                        + elapsedBinary
                        + "ns"
                        + (elapsedLinear < elapsedBinary ? ", linear wins"
                                : ", binary wins") + " by "
                        + Math.abs(elapsedLinear - elapsedBinary) + "ns");

            }
            
        }
        
        /**
         * Time a bunch of searches.
         */
        public long doTest(boolean linear,int ntrials, int nkeys, int[] keys) {
            
            long elapsedNanos = 0;
            
            for( int i=0; i<ntrials; i++ ) {
                
                int index = r.nextInt(nkeys);
                
                int key = keys[ index ];
                
                final int index2;
                
                long beginNanos = System.nanoTime();
                
                if( linear ) {
                    
                    index2 = Search.linearSearch(key, keys, nkeys);
                    
                } else {
                    
                    index2 = Search.binarySearch(key, keys, nkeys);
                    
                }
                
                elapsedNanos += System.nanoTime() - beginNanos;
                
                // make sure the search result is correct.
                assertEquals(index,index2);
                
            }
            
            return elapsedNanos;

        }

    }
    
}
