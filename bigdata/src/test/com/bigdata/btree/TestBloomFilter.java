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
 * Created on Dec 30, 2006
 */

package com.bigdata.btree;

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
