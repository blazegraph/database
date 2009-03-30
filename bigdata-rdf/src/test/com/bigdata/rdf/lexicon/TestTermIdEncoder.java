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
 * Created on Mar 30, 2009
 */

package com.bigdata.rdf.lexicon;

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.ICounter;

/**
 * Unit test for unified encoding / decoding for partitioned and unpartitioned
 * long counters as assigned by the {@link ICounter} for a TERM2ID index or
 * index partition. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermIdEncoder extends TestCase2 {

    public TestTermIdEncoder() {
        
    }
    
    public TestTermIdEncoder(String s) {
        super(s);
    }

    /**
     * Ctor correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        // Ok.
        new TermIdEncoder(0);
        new TermIdEncoder(1);
        new TermIdEncoder(30);
        new TermIdEncoder(31);
        
        // illegal
        try {
            new TermIdEncoder(-1);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled()) {
                log.info("Ignoring expected exception: " + ex);
            }
        }
        
        try {
            new TermIdEncoder(32);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled()) {
                log.info("Ignoring expected exception: " + ex);
            }
        }

    }

    /**
     * Unit tests for correct rejection of encoding when the long counter value
     * contains a partition identifier which is negative or a local counter
     * value which is negative or zero.
     */
    public void test_encode_correctRejection() {

        final TermIdEncoder encoder = new TermIdEncoder(1);

        // pid may not be negative.
        try {
            encoder.encode(((long) -1/* pid */) << 32 | 1/* ctr */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled()) {
                log.info("Ignoring expected exception: " + ex);
            }
        }

        // ctr may not be negative.
        try {
            encoder.encode(((long) 1/* pid */) << 32 | -1/* ctr */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled()) {
                log.info("Ignoring expected exception: " + ex);
            }
        }
        // ctr may not be zero.
        try {
            encoder.encode(((long) 1/* pid */) << 32 | 0/* ctr */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled()) {
                log.info("Ignoring expected exception: " + ex);
            }
        }

    }

    /**
     * Test of encode/decode when ZERO (0) of the low bits are reversed and
     * placed into the high bits (this corresponds closely to the historical
     * encoding of a term identifier by either an unpartitioned or a partitioned
     * TERM2ID index, however there is a difference in the encoding since the
     * new encoding steals the sign bit from both the partition identifier and
     * the local counter rather than stealing both bits from the partition
     * identifier, which is what the historical encoding scheme was doing).
     */
    public void test_encode_decode_0bits_pid1_ctr1() {

        final TermIdEncoder encoder = new TermIdEncoder(0);
        
        final int pid = 1;
        
        final int ctr = 1;
        
        doEncodeDecodeTest(encoder, pid, ctr);

    }

    /**
     * Stress test using an encoder with a random number of NO bits reversed and
     * rotated into the high bits of the long value and random values for the
     * partition identifier and the local counter.
     */
    public void test_encode_decode_0bits_stress() {

        final Random r = new Random();

        final TermIdEncoder encoder = new TermIdEncoder(0);

        for (int i = 0; i < 1000000; i++) {

            // [0:MAX_VALUE-1], but MAX_VALUE is also legal.
            final int pid = r.nextInt(Integer.MAX_VALUE);

            // [1:MAX_VALUE] since 0 is illegal.
            final int ctr = r.nextInt(Integer.MAX_VALUE) + 1;

            doEncodeDecodeTest(encoder,pid,ctr);
            
        }

    }

    public void test_encode_decode_1bits_pid1_ctr1() {

        doEncodeDecodeTest(new TermIdEncoder(1), 1/* pid */, 1/* ctr */);

    }

    /**
     * Stress test using an encoder with a random number of bits reversed and
     * rotated into the high bits of the long value and random values for the
     * partition identifier and the local counter.
     */
    public void test_encode_decode_randomBits_stress() {

        final Random r = new Random();

        for (int i = 0; i < 10000000; i++) {

            // [0:31]
            final int nbits = r.nextInt(32);
            
            // [0:MAX_VALUE-1], but MAX_VALUE is also legal.
            final int pid = r.nextInt(Integer.MAX_VALUE);

            // [1:MAX_VALUE] since 0 is illegal.
            final int ctr = r.nextInt(Integer.MAX_VALUE) + 1;

            final TermIdEncoder encoder = new TermIdEncoder(nbits);

            doEncodeDecodeTest(encoder, pid, ctr);

        }

    }

    /**
     * Encode/decode test helper.
     * 
     * @param encoder
     *            The encoder.
     * @param pid
     *            The partition identifier.
     * @param ctr
     *            The local counter.
     */
    private void doEncodeDecodeTest(final TermIdEncoder encoder, final int pid,
            final int ctr) {

        // combine to obtain a long counter value.
        final long v0 = TermIdEncoder.combine(pid, ctr);

        // sanity check extract of pid.
        assertEquals("pid", pid, TermIdEncoder.getPartitionId(v0));

        // sanity check extract of ctr.
        assertEquals("ctr", ctr, TermIdEncoder.getLocalCounter(v0));

        // encode.
        final long u = encoder.encode(v0);

        // the low bits are zero (they are left open for bit flags).
        assertTrue((u & 0x03) == 0);

        // decode.
        final long v1 = encoder.decode(u);

        // verify v0 == decode(encode(v0))
        if (v0 != v1) {
            fail(encoder + "\n" + "expected=" + v0
                    + " (0x" + Long.toHexString(v0) + ")" + ", actual=" + v1
                    + " (0x" + Long.toHexString(v0) + ")"
                    + //
                    "\n" + Long.toBinaryString(v0) + "\n"
                    + Long.toBinaryString(v1));
        }

    }

}
