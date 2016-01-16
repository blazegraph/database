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
package com.bigdata.btree.raba;

import junit.framework.TestCase2;

/**
 * Test suite for {@link SubRangeRaba}.
 * 
 * @author bryan
 * @see BLZG-1537 (Schedule more IOs when loading data)
 */
public class TestSubRangeRaba extends TestCase2 {

	public TestSubRangeRaba() {
	}

	public TestSubRangeRaba(String name) {
		super(name);
	}

    public void test_subRangeRaba_constructor_correctRejection_01()
    {

        final byte[][] keys = new byte[5][];

        int i = 0;
        keys[i++] = new byte[]{5};  // offset := 0, insert before := -1
        keys[i++] = new byte[]{7};  // offset := 1, insert before := -2
        keys[i++] = new byte[]{9};  // offset := 2, insert before := -3
        keys[i++] = new byte[]{11}; // offset := 3, insert before := -4
        keys[i++] = new byte[]{13}; // offset := 4, insert before := -5
                                    //              insert  after := -6
        int nkeys = 5;

        final MutableKeyBuffer delegate = new MutableKeyBuffer(nkeys, keys);

        // null delegate
		try {
			new SubRangeRaba(null/*delegate*/, 0/* fromIndex */, 5/* toIndex */);
			fail();
		} catch (IllegalArgumentException ex) {
			// ignore expected exception.
		}		
        
		// fromIndex is negative.
		try {
			new SubRangeRaba(delegate, -1/* fromIndex */, 5/* toIndex */);
			fail();
		} catch (IllegalArgumentException ex) {
			// ignore expected exception.
		}
		
		// toIndex GT size()
		try {
			new SubRangeRaba(delegate, 0/* fromIndex */, 6/* toIndex */);
			fail();
		} catch (IllegalArgumentException ex) {
			// ignore expected exception.
		}
	
		// fromIndex == toIndex
		try {
			new SubRangeRaba(delegate, 4/* fromIndex */, 4/* toIndex */);
			fail();
		} catch (IllegalArgumentException ex) {
			// ignore expected exception.
		}
	
		// fromIndex GT toIndex
		try {
			new SubRangeRaba(delegate, 2/* fromIndex */, 1/* toIndex */);
			fail();
		} catch (IllegalArgumentException ex) {
			// ignore expected exception.
		}
		
    }

    public void test_subRangeRaba_01()
    {

        final byte[][] keys = new byte[5][];

        int i = 0;
        keys[i++] = new byte[]{5};  // offset := 0, insert before := -1
        keys[i++] = new byte[]{7};  // offset := 1, insert before := -2
        keys[i++] = new byte[]{9};  // offset := 2, insert before := -3
        keys[i++] = new byte[]{11}; // offset := 3, insert before := -4
        keys[i++] = new byte[]{13}; // offset := 4, insert before := -5
                                    //              insert  after := -6
        int nkeys = 5;

        final MutableKeyBuffer delegate = new MutableKeyBuffer(nkeys, keys);

        final int fromIndex = 2;
        final int toIndex = 4;
		assertEquals(2, toIndex - fromIndex);
        
		final IRaba fixture = new SubRangeRaba(delegate, fromIndex, toIndex);

		assertTrue(fixture.isReadOnly());
		assertEquals(delegate.isKeys(), fixture.isKeys());
		assertEquals(toIndex - fromIndex, fixture.size());
		assertEquals(toIndex - fromIndex, fixture.capacity());
		// TODO test isEmpty() on empty range (empty range is not allowed at this point so no way to test). change that?
		assertFalse(fixture.isEmpty());
		assertTrue(fixture.isFull());
		assertFalse(fixture.isNull(0));
		try {
			fixture.get(-1);
			fail();
		} catch (IndexOutOfBoundsException ex) {
			// ignore expected exception.
		}
		assertEquals(keys[fromIndex].length, fixture.length(0));
		assertEquals(keys[fromIndex], fixture.get(0));
		assertEquals(keys[fromIndex + 1], fixture.get(1));
		try {
			fixture.get(2);
			fail();
		} catch (IndexOutOfBoundsException ex) {
			// ignore expected exception.
		}
		// TODO test copy()
		assertSameIterator(new byte[][] { keys[2], keys[3] }, fixture.iterator());

		/*
		 * TODO test search on key range. test correct rejection if LT fromIndex
		 * or GTE toIndex in delegate.
		 */
		

    }

}
