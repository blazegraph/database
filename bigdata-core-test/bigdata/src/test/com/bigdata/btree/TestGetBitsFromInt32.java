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

import java.util.Formatter;

import com.bigdata.util.BytesUtil;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link BytesUtil#getBits(int, int, int)
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGetBitsFromInt32 extends TestCase2 {

	/**
	 * 
	 */
	public TestGetBitsFromInt32() {
	}

	/**
	 * @param name
	 */
	public TestGetBitsFromInt32(String name) {
		super(name);
	}

	/**
	 * offset may be zero, but not negative.
	 */
	public void test_getBitsFromInt32_correctRejection_off_and_len_01() {

		BytesUtil.getBits(0x0, 0, 0); // ok
		try {
			BytesUtil.getBits(0x0, -1, 0); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may be zero, but not negative.
	 */
	public void test_getBitsFromInt32_correctRejection_off_and_len_02() {

		BytesUtil.getBits(0x0, 0, 0); // ok
		try {
			BytesUtil.getBits(0x0, 0, -1); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * length may address all bits (32), but not 33 bits.
	 */
	public void test_getBitsFromByteArray_correctRejection_off_and_len_04() {

		BytesUtil.getBits(0x0, 0, 32); // ok
		try {
			BytesUtil.getBits(0x0, 0, 33); // no good
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

	}

	/**
	 * You can request a zero length slice starting at bit zero.
	 */
	public void test_getBitsFromInt32_zeroLength() {

		assertEquals(0, BytesUtil.getBits(0x0, 0, 0));

	}

	/** all bits zero. */
	public void test_getBitsFromInt32_01() {

		final int b = 0x0;

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 31/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 32/* len */));

	}

	/** LSB ONE. */
	public void test_getBitsFromInt32_02() {

		final int b = 1;

		assertEquals(0x00000000, getBits(b, 0/* off */, 0/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 1/* len */));
		assertEquals(0x00000000, getBits(b, 0/* off */, 31/* len */));//x
		assertEquals(0x00000001, getBits(b, 0/* off */, 32/* len */));
		assertEquals(0x00000001, getBits(b, 31/* off */, 1/* len */));//x
		assertEquals(0x00000001, getBits(b, 30/* off */, 2/* len */));

	}

	/**
	 * Bit ONE (1) set (remember, MSB is bit ZERO (0)).
	 */
	public void test_getBitsFromByteArray_03() {
		
		final int b = 1<<30;

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
	 * MSB ONE (this test case is the mostly likely to run a foul of a sign bit
	 * extension).
	 */
	public void test_getBitsFromByteArray_04() {
		
		final int b = 1<<31;

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
	 * Slice in the 2nd byte.
	 */
	public void test_getBitsFromByteArray_05() {

		/*
		 * Four bits on starting at bit 11 (where MSB is bit zero).
		 * 
		 * Note: For normal Java bit indexing with MSB 31, this is bit indices
		 * 20,19,18,17.
		 */
		final int b = (1 << 20) | (1 << 19) | (1 << 18) | (1 << 17);

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

	private int getBits(final int a, final int off, final int len) {

		final int ret = BytesUtil.getBits(a, off, len);

		if (log.isInfoEnabled()) {
			final StringBuilder sb = new StringBuilder();
			final Formatter f = new Formatter(sb);
			f.format("[%32s] =(%2d:%2d)=> [%32s]", Integer.toBinaryString(a),
					off, len, Integer.toBinaryString(ret));
			log.info(sb.toString());
		}

        return ret;
    	
    }

}
