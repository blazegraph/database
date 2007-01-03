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
 * Created on Dec 30, 2006
 */

package com.bigdata.objndx;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase2;

/**
 * Test suite for bloom filter functionality.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BloomFilter
 * 
 * @todo is a bloom filter efficient when the btree is backed by the journal or
 *       should it be moved to the client in that case?
 * 
 * @todo write logic that configures a bloom filter for a target error rate
 *       given the expected number of keys. since our use case is in front of a
 *       read-only {@link IndexSegment} we always know the #of keys exactly at
 *       the time that we provision the bloom filter. this makes it easy for us
 *       to tune the filter for a desired false positive rate.
 * 
 * @todo compare the heritrix and mg4j implementations with respect to
 *       optimizations and support for non-array types, e.g., int, long, double,
 *       etc. heritrix has done a lot of optimization for int32 vs int64
 *       platforms and also for very large bloom filters. our own use is for an
 *       {@link IndexSegment} of not more than 200M of data, so the size of the
 *       filter will be significantly smaller than that required by heritrix.
 * 
 * @todo develop and validate a binary serialization of the bloom filter for
 *       inclusion in the {@link IndexSegment}.
 * 
 * @todo estimate the parameters of a bloom filter to be deployed in front of an
 *       index segment and measure the cost of constructing that bloom filter
 *       and its space requirements.
 * 
 * @todo is there a method to report the #of bits that are on? once this exceeds
 *       a threshold the bloom filter begins to loose information and the error
 *       rate goes up.
 * 
 * @todo is there a way to report the effective error rate of the bloom filter?
 */
public class TestBloomFilter extends TestCase2 {

    /**
     * 
     */
    public TestBloomFilter() {
    }

    /**
     * @param name
     */
    public TestBloomFilter(String name) {
        super(name);
    }

    /**
     * Simple constructor tests.
     * 
     * @todo correct rejection tests.
     */
    public void test_ctor() {
        
        // (nkeys)
        new BloomFilter(1000);
        
        // (nkeys,nfunctions)
        new BloomFilter(1000,5);
        
    }

    /**
     * Test that a bloom filter constructed for a given #of keys and maximum
     * error rate respects that maximum error rate in practice.
     */
    public void test_errorRate() {

        final int nkeys = 1000;

        // inputs
        System.err.println("nkeys=" + nkeys);

        Set<String> groundTruth = new HashSet<String>(nkeys);

        String[] keys = new String[nkeys];

        BloomFilter bloomFilter = new BloomFilter(nkeys);

        {
            int falsePositiveCount = 0;

            for (int i = 0; i < nkeys; i++) {

                String s = getRandomString(128, i);

                if (!groundTruth.add(s)) {

                    fail("Not expecting value to be present in the ground truth set.");

                }

                keys[i] = s;

                if (bloomFilter.contains(s)) {

                    falsePositiveCount++;

                }

                bloomFilter.add(s);

                assertTrue("key not found after insert", bloomFilter
                        .contains(s));

            }

            // outputs.
            System.err.println("#errors=" + falsePositiveCount
                    + ", actual error rate="
                    + ((double) falsePositiveCount / (double) nkeys));
        }

        /*
         * verify that all keys are correctly reported as present.
         */
        for (int i = 0; i < nkeys; i++) {

            String s = keys[i];

            assertTrue("key not found after insert", bloomFilter.contains(s));

        }

        /*
         * Test fully populated filter against new random keys that are known to
         * NOT be in the test set.
         */
        {
            int i = 0;
            int falsePositiveCount2 = 0;
            while (i < nkeys) {

                String s = getRandomString(128, i);

                if (groundTruth.contains(s)) {

                    // this is pretty unlikely, but we test it anyway.
                    continue;

                }

                i++;

                if (bloomFilter.contains(s)) {

                    falsePositiveCount2++;

                }

            }

            // outputs.
            System.err.println("#errors=" + falsePositiveCount2
                    + ", actual error rate="
                    + ((double) falsePositiveCount2 / (double) nkeys));
        }

        // @todo test deserialization for same state or same behavior given an initialized filter.

        System.err.println(bloomFilter.toString());

    }

}
