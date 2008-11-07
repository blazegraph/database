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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase2;

/**
 * Test suite for bloom filter functionality.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BloomFilter
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
     * Simple constructor tests
     */
    public void test_ctor() {

        new BloomFilter(1000/* n */, .01/* p */, 10000/* maxN */);

        new BloomFilter(5000/* n */, .02/* p */, 10000/* maxN */);

        new BloomFilter(5000/* n */, .02/* p */, 5000/* maxN == n */);

    }

    /**
     * Correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        try {
            new BloomFilter(1000/* n */, 0d/* p */, 10000/* maxN */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        try {
            new BloomFilter(1000/* n */, 1.00001/* p */, 10000/* maxN */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        try {
            new BloomFilter(0/* n */, .01/* p */, 10000/* maxN */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        try {
            new BloomFilter(1000/* n */, 0d/* p */, 100/* maxN */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

    }
    
    /**
     * Test that a bloom filter constructed for a given #of keys and maximum
     * error rate respects that maximum error rate in practice.
     */
    public void test_errorRate() {

        final double errorRate = 0.05d;

        final int nkeys = 1000;

        // inputs.
        System.err.println("targets: nkeys=" + nkeys + ", errorRate="
                + errorRate);

        final BloomFilter bloomFilter = new BloomFilter(nkeys, errorRate, nkeys/* maxKeys */);

        // displays filter state as provisioned.
        System.err.println("provisioned: " + bloomFilter.toString());

        final Set<byte[]> groundTruth = new HashSet<byte[]>(nkeys);

        final byte[][] keys = new byte[nkeys][];

        final Random r = new Random();

        final int MAX_KEY_LENGTH = 128;

        {

            /*
             * #of times the filter reported that the key was already present
             * when it was not already in the ground truth.
             */
            int falsePositiveCount = 0;

            for (int i = 0; i < nkeys; i++) {

                // loop until we get a distinct key.
                while (true) {

                    // random length key.
                    final byte[] key = new byte[r.nextInt(MAX_KEY_LENGTH)];

                    // random bytes for the key.
                    r.nextBytes(key);

                    // save key.
                    keys[i] = key;

                    // add to ground truth
                    if (groundTruth.add(key)) {

                        break;

                    }

                    System.err.print('.');

                }

                final byte[] key = keys[i];

                // test filter before.
                final boolean falsePositive = bloomFilter.contains(key);

                if (falsePositive)
                    falsePositiveCount++;

                // add to the filter.
                bloomFilter.add(key);

                // verify filter reports that the key is found.
                assertTrue(bloomFilter.contains(key));

            }

            // outputs.
            System.err.println("error rate on keys added to the filter"
                    + ": #errors=" + falsePositiveCount
                    + ", actual error rate="
                    + ((double) falsePositiveCount / (double) nkeys));

            // displays filter state after inserting keys.
            System.err.println("after inserts: " + bloomFilter.toString());

        }

        /*
         * verify that all keys are correctly reported as present.
         */
        for (int i = 0; i < nkeys; i++) {

            final byte[] key = keys[i];

            assertTrue(bloomFilter.contains(key));

        }

        /*
         * Test fully populated filter against new random keys that are known to
         * NOT be in the test set.
         */
        {
            int i = 0;
            int falsePositiveCount2 = 0;
            while (i < nkeys) {

                // random length key.
                final byte[] key = new byte[r.nextInt(MAX_KEY_LENGTH)];

                // random bytes for the key.
                r.nextBytes(key);

                if (groundTruth.contains(key)) {

                    // until we find a distinct key.

                    System.out.print(".");

                    continue;

                }

                i++;

                if (bloomFilter.contains(key)) {

                    falsePositiveCount2++;

                }

            }

            // outputs.
            System.err.println("error rate on random distinct keys"
                    + ": #errors=" + falsePositiveCount2
                    + ", actual error rate="
                    + ((double) falsePositiveCount2 / (double) nkeys));
        }

    }

    /**
     * This is not really a unit test. Instead it plots a curve of the space
     * requirements of the filter (bitLength) against the target error rate and
     * filter capacity.
     * <p>
     * Note: The table has spans within each column where the same bit length is
     * computed. This is because the #of hash functions is discrete. Each time
     * we increase the #of hash functions the table moves to a new bit length
     * value in a given column.
     */
    public void test_spaceCurve() {

        // the increments in error rate that will be plotted.
        double inc = 0.01;
        
        // the different filter capacities that will be plotted.
        final int[] capacity = new int[] { 1, 100, 1000, 10000, 100000,
                1000000, 2000000, 10000000, 20000000, 100000000 };
        
        // headings.
        {

            System.out.print("p\t");

            for (int n : capacity) {

                System.out.print(n);
                
                System.out.print('\t');

            }

            System.out.println();

        }

        // p is the target error rate.
        for (double p = inc; p < 1d; p += inc) {

            System.out.print(p);

            final int d = BloomFilter.getHashFunctionCount(p);

            for (int n : capacity) {

                final long bitLength = BloomFilter.getBitLength(d, n);

                System.out.print("\t" + bitLength);

            }

            System.out.println();

        }

    }

    /**
     * Generates a table showing the #of index entries that would produce a
     * given error rate for a filter with a specific configuration (target error
     * rate of <code>.02</code> at 1 million index entries). The code uses the
     * given filter and runs through error rates between <code>.02</code> and
     * <code>.20</code> by increments of <code>.01</code> showing the #of
     * index entries for which that would be the expected error rate.
     * <p>
     * You can verify the computation by examining certain entries in the table
     * generated by {@link #test_spaceCurve()}. The filter as configured for
     * this test has a bit length of <code>8,656,171</code>. If you look at
     * the expected error rate for a bloom filter of 2 million index entries of
     * the same bit length you will see that it lies somewhere in
     * <code>0.13 - 0.24</code> (this table has discrete regions, see the
     * javadoc for {@link #test_spaceCurve()}). If you examine the output of
     * this test, you will see that an error rate of <code>0.18</code> is
     * predicated when the #of index entries is on the order of 2 million
     * (2,008,182). The relevant cells are highlighted in the worksheet.
     * 
     * @see src/worksheet/architecture/bloomfilter.xls
     * 
     * @see BloomFilter#getEntryCountForErrorRate(double)
     */
    public void test_errorRateCurve() {

        final BloomFilter filter;
        final int k; // #of hash functions.
        final long m; // bit length of the filter.
        {

            final double p = 0.02d;

            final int n = 1000000;

            filter = new BloomFilter(n, p, n/* maxN */);

            System.out.println("Given p=" + p + ", n=" + n
                    + " the filter will use " + filter.getHashFunctionCount()
                    + " hash functions and have a bit length of "
                    + filter.getBitLength());
            
            k = filter.getHashFunctionCount();
            
            m = filter.getBitLength();
            
        }

        // headings
        System.out.println("p\tn");

        for (double p = 0.01; p < .2; p += 0.01) {

            int n = BloomFilter.getEntryCountForErrorRate(k, m, p);

            System.out.println("" + p + "\t" + n);

        }

    }

    /**
     * Test suite for (de-)serialization of the {@link BloomFilter}.
     */
    public void test_serialization() {
        
        fail("write test");
        
    }
    
}
