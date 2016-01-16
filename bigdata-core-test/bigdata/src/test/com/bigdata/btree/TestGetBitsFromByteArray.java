/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on May 7, 2011
 */
package com.bigdata.btree;

import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;
import java.util.Formatter;
import java.util.Random;

import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link BytesUtil#getBits(byte[], int, int)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGetBitsFromByteArray extends TestCase2 {

	public TestGetBitsFromByteArray() {
	}

	public TestGetBitsFromByteArray(String name) {
		super(name);
	}

	public void test_getBitsFromByteArray_correctRejection_nullArg() {

		try {
			BytesUtil.getBits(null, 0, 0);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * offset may be zero, but not negative.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_01() {

		BytesUtil.getBits(new byte[1], 0, 0); // ok
		try {
			BytesUtil.getBits(new byte[1], -1, 0); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may be zero, but not negative.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_02() {

		BytesUtil.getBits(new byte[1], 0, 0); // ok
		try {
			BytesUtil.getBits(new byte[1], 0, -1); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may address all bits (8) in a single byte, but not the 9th bit.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_03() {

		BytesUtil.getBits(new byte[1], 0, 8); // ok
		try {
			BytesUtil.getBits(new byte[1], 0, 8 + 1); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may address all bits (32) in 4 bytes, but not 33 bits since the
	 * return value would be larger than an int32.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_04() {

		BytesUtil.getBits(new byte[4], 0, 32); // ok
		try {
			BytesUtil.getBits(new byte[4], 0, 33); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may address (32) bits in 5 bytes, but not 33 bits since the return
	 * value would be larger than an int32.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_05() {

		BytesUtil.getBits(new byte[5], 0, 32); // ok
		try {
			BytesUtil.getBits(new byte[5], 0, 33); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * You can request a zero length slice starting at bit zero of a zero length
	 * byte[].
	 */
	public void test_getBitsFromByteArray_zeroLength() {

		assertEquals(0, BytesUtil.getBits(new byte[0], 0, 0));

	}

	/** byte[4] (32-bits) with all bits zero. */
	public void test_getBitsFromByteArray_01() {

		final byte[] b = new byte[4];

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 31/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 32/* len */));

	}

	/** byte[4] (32-bits) with LSB ONE. */
	public void test_getBitsFromByteArray_02() {

		final byte[] b = new byte[4];

		BytesUtil.setBit(b, 31/* off */, true);

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 31/* len */));//x
		assertEquals(0x00000001, getBits(b, 0/* off */, 32/* len */));
		assertEquals(0x00000001, getBits(b, 31/* off */, 1/* len */));//x
		assertEquals(0x00000001, getBits(b, 30/* off */, 2/* len */));

	}

	/**
	 * byte[4] (32-bits) with bit ONE (1) set.
	 */
	public void test_getBitsFromByteArray_03() {
		
		final byte[] b = new byte[4];

		BytesUtil.setBit(b, 1/* off */, true);

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000001, getBits(b, 0/* off */, 2/* len */));
		assertEquals(0x00000002, getBits(b, 1/* off */, 2/* len */));
		assertEquals(0x00000004, getBits(b, 1/* off */, 3/* len */));

		assertEquals(0x00000001, getBits(b, 1/* off */, 1/* len */));

		assertEquals(0x40000000, getBits(b, 0/* off */, 32/* len */));
		assertEquals(0x20000000, getBits(b, 0/* off */, 31/* len */));
		assertEquals(0x10000000, getBits(b, 0/* off */, 30/* len */));
		assertEquals(0x08000000, getBits(b, 0/* off */, 29/* len */));

	}

	/**
	 * byte[4] (32-bits) with MSB ONE (this test case is the mostly likely to
	 * run a foul of a sign bit extension).
	 */
	public void test_getBitsFromByteArray_04() {
		
		final byte[] b = new byte[4];

		BytesUtil.setBit(b, 0/* off */, true);

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000001, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000002, getBits(b, 0/* off */, 2/* len */));
		assertEquals(0x00000004, getBits(b, 0/* off */, 3/* len */));
		assertEquals(0x00000008, getBits(b, 0/* off */, 4/* len */));
		
		assertEquals(0x00000000, getBits(b, 1/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 1/* off */, 1/* len */));
		assertEquals(0x00000000, getBits(b, 1/* off */, 2/* len */));
		assertEquals(0x00000000, getBits(b, 1/* off */, 3/* len */));

	}

	/**
	 * byte[4] (32-bits) with slice in the 2nd byte.
	 */
	public void test_getBitsFromByteArray_05() {
		
		final byte[] b = new byte[4];

		// four bits on starting at bit 11.
		BytesUtil.setBit(b, 11/* offset */, true);
		BytesUtil.setBit(b, 12/* offset */, true);
		BytesUtil.setBit(b, 13/* offset */, true);
		BytesUtil.setBit(b, 14/* offset */, true);

		/*
		 * Test with a window extending from bit zero with a variety of bit
		 * lengths ranging from an end bit index one less than the first ONE bit
		 * through an end bit index beyond the last ONE bit.
		 */
		assertEquals(0x00000000, getBits(b, 0/* off */, 11/* len */));
		assertEquals(0x00000001, getBits(b, 0/* off */, 12/* len */));
		assertEquals(0x00000003, getBits(b, 0/* off */, 13/* len */));
		assertEquals(0x00000007, getBits(b, 0/* off */, 14/* len */));
		assertEquals(0x0000000f, getBits(b, 0/* off */, 15/* len */));
		assertEquals(0x0000001e, getBits(b, 0/* off */, 16/* len */));
		assertEquals(0x0000003c, getBits(b, 0/* off */, 17/* len */));
		assertEquals(0x00000078, getBits(b, 0/* off */, 18/* len */));
		assertEquals(0x000000f0, getBits(b, 0/* off */, 19/* len */));

		/*
		 * Test with a 4-bit window that slides over the ONE bits. The initial
		 * window is to the left of the first ONE bit. The window slides right
		 * by one bit position for each assertion.
		 */
		assertEquals(0x00000000, getBits(b, 7/* off */, 4/* len */));
		assertEquals(0x00000001, getBits(b, 8/* off */, 4/* len */));
		assertEquals(0x00000003, getBits(b, 9/* off */, 4/* len */));
		assertEquals(0x00000007, getBits(b,10/* off */, 4/* len */));
		assertEquals(0x0000000f, getBits(b,11/* off */, 4/* len */));
		assertEquals(0x0000000e, getBits(b,12/* off */, 4/* len */));
		assertEquals(0x0000000c, getBits(b,13/* off */, 4/* len */));
		assertEquals(0x00000008, getBits(b,14/* off */, 4/* len */));
		assertEquals(0x00000000, getBits(b,15/* off */, 4/* len */));

	}

	 /** 
	 * byte[2] (16-bits) 
	 * 
	 * @todo test slices from arrays with more than 4 bytes
	 */
	public void test_getBitsFromByteArray_06() {
		
		final byte[] b = new byte[2];

		// four bits on starting at bit 11.
		BytesUtil.setBit(b, 11/* offset */, true);
		BytesUtil.setBit(b, 12/* offset */, true);
		BytesUtil.setBit(b, 13/* offset */, true);
		BytesUtil.setBit(b, 14/* offset */, true);

		/*
		 * Test with a window extending from bit zero with a variety of bit
		 * lengths ranging from an end bit index one less than the first ONE bit
		 * through an end bit index beyond the last ONE bit.
		 */
		assertEquals(0x00000000, getBits(b, 0/* off */, 11/* len */));
		assertEquals(0x00000001, getBits(b, 0/* off */, 12/* len */));
		assertEquals(0x00000003, getBits(b, 0/* off */, 13/* len */));
		assertEquals(0x00000007, getBits(b, 0/* off */, 14/* len */));
		assertEquals(0x0000000f, getBits(b, 0/* off */, 15/* len */));
		assertEquals(0x0000001e, getBits(b, 0/* off */, 16/* len */));

		/*
		 * Now that we have reached the last legal length verify that length 17
		 * is rejected.
		 */
		try {
			getBits(b, 0/* off */, 17/* len */);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		/*
		 * Now increase the offset while decreasing the length and step through
		 * a few more slices. These all see the FOUR (4) ON bits plus one
		 * trailing ZERO (0) bit.
		 */
		assertEquals(0x0000001e, getBits(b, 1/* off */, 15/* len */));
		assertEquals(0x0000001e, getBits(b, 2/* off */, 14/* len */));
		assertEquals(0x0000001e, getBits(b, 3/* off */, 13/* len */));
		assertEquals(0x0000001e, getBits(b, 4/* off */, 12/* len */));
		assertEquals(0x0000001e, getBits(b, 5/* off */, 11/* len */));
		assertEquals(0x0000001e, getBits(b, 6/* off */, 10/* len */));
		assertEquals(0x0000001e, getBits(b, 7/* off */,  9/* len */));
		assertEquals(0x0000001e, getBits(b, 8/* off */,  8/* len */));
		assertEquals(0x0000001e, getBits(b, 9/* off */,  7/* len */));
		assertEquals(0x0000001e, getBits(b,10/* off */,  6/* len */));
		assertEquals(0x0000001e, getBits(b,11/* off */,  5/* len */));

		/*
		 * Continue to increase the offset while decreasing the length, but now
		 * we will start to loose the ONE bits on both sides as the window keeps
		 * sliding and shrinking.
		 */
		assertEquals(0x0000000e, getBits(b,12/* off */,  4/* len */));
		assertEquals(0x00000006, getBits(b,13/* off */,  3/* len */));
		assertEquals(0x00000002, getBits(b,14/* off */,  2/* len */));
		assertEquals(0x00000000, getBits(b,15/* off */,  1/* len */));

		/*
		 * This is also illegal (the starting offset is too large).
		 */
		try {
			getBits(b,16/* off */,  1/* len */);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}
		
    private int getBits(final byte[] a,final int off,final int len) {
    
        final int ret = BytesUtil.getBits(a, off, len);

		if (log.isInfoEnabled()) {
			final StringBuilder sb = new StringBuilder();
			final Formatter f = new Formatter(sb);
			f.format("[%" + (a.length * 8) + "s] =(%2d:%2d)=> [%32s]",
					BytesUtil.toBitString(a), off, len, Integer.toBinaryString(ret));
			log.info(sb.toString());
		}

        return ret;

    }

    /**
     * A stress test for compatibility with {@link InputBitStream}. An array is
     * filled with random bits and the behavior of {@link InputBitStream} and
     * {@link BytesUtil#getBits(byte[], int, int)} is compared on a number of
     * randomly selected bit slices.
     * 
     * @throws IOException 
     */
    public void test_stress_InputBitStream_compatible() throws IOException {
        
        final Random r = new Random();

        // #of iterations
        final long limit = 1000000;

        // Note: length is guaranteed to be LT int32 bits so [int] index is Ok.
        // Note: + 4 since we will do [bitlen - 32] below. 4*8==32.
        final int len = r.nextInt(Bytes.kilobyte32 * 8) + 4;
        final int bitlen = len << 3;
        // Fill array with random data.
        final byte[] b = new byte[len];
        r.nextBytes(b);

        // wrap with InputBitStream.
        final InputBitStream ibs = new InputBitStream(b);

        for (int i = 0; i < limit; i++) {

            // start of the bit slice.
            final int sliceBitOff = r.nextInt(bitlen - 32);

            final int bitsremaining = bitlen - sliceBitOff;

            // allow any slice of between 1 and 32 bits length.
            final int sliceBitLen = r.nextInt(Math.min(32, bitsremaining)) + 1;
            assert sliceBitLen >= 1 && sliceBitLen <= 32;

            // position the stream.
            ibs.position(sliceBitOff);

            final int v1 = ibs.readInt(sliceBitLen);

            final int v2 = BytesUtil.getBits(b, sliceBitOff, sliceBitLen);

            if (v1 != v2) {
                fail("Expected=" + v1 + ", actual=" + v2 + ", trial=" + i
                        + ", bitSlice(off=" + sliceBitOff + ", len="
                        + sliceBitLen + ")" + ", arrayLen=" + b.length);
            }
            
        }

    }

}
