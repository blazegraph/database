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
package com.bigdata.htree;

import java.io.IOException;

import com.bigdata.btree.ITupleIterator;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test {@link HTree} with duplicate keys.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/763" >
 *      Stochastic Results With Analytic Query Mode </a>
 */
public class TestDuplicates extends AbstractHTreeTestCase {

    /**
     * 
     */
    public TestDuplicates() {
   
    }

    /**
     * @param name
     */
    public TestDuplicates(String name) {
        super(name);
    }

//    private static final boolean bufferNodes = true;

	/**
	 * Tests the ability to store values against duplicate
	 * keys.
	 * 
	 * @throws IOException
	 * @throws Exception
	 */
	public void test_duplicateKeys() throws IOException, Exception {

		final IRawStore store = new SimpleMemoryRawStore();

		try {

            HTree htree = getHTree(store, 3/* addressBits */,
                    false/* rawRecords */, true/* persistent */);

			final byte[] k1 = new byte[] { 1 };
			final byte[] v2 = new byte[] { 2 };
			final byte[] v3 = new byte[] { 3 };

			assertNull(htree.lookupFirst(k1));
			assertFalse(htree.contains(k1));

			assertNull(htree.insert(k1, v2));

			assertEquals(htree.lookupFirst(k1), v2);
			assertTrue(htree.contains(k1));

			assertNull(htree.insert(k1, v3));

			final long addrCheckpoint1 = htree.writeCheckpoint();

			htree = HTree.load(store, addrCheckpoint1, true/* readOnly */);

			assertEquals(htree.lookupFirst(k1), v3);
			assertTrue(htree.contains(k1));
			
			final ITupleIterator<?> iter = htree.lookupAll(k1);
			int count = 0;
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
			
			assertEquals(2, count);

		} finally {

			store.destroy();
		
		}

	}

    public void test_duplicateKeyRangeScans_3bits_500dups() {

        doTest(3, 500);

    }

    public void test_duplicateKeyRangeScans_4bits_500dups() {

        doTest(4, 500);

    }

    public void test_duplicateKeyRangeScans_10bits_500dups() {

        doTest(10, 500);

    }

    public void test_duplicateKeyRangeScans_3bits_5000dups() {

        doTest(3, 5000);

    }

    public void test_duplicateKeyRangeScans_4bits_5000dups() {

        doTest(4, 5000);

    }

    public void test_duplicateKeyRangeScans_10bits_5000dups() {

        doTest(10, 5000);

    }

    /**
     * Test helper for a test based on duplicate keys. The test is designed to
     * verify that the keys are stored correctly and that a scan of the records
     * having that key returns all entries stored under that key.
     * 
     * @param addressBits
     *            The #of address bits.
     * @param ndups
     *            The #of duplicate entries for a given key.
     */
    private void doTest(final int addressBits, final int ndups) {

		final IRawStore store = new SimpleMemoryRawStore();
		// final IRawStore store = getRWStore();

		try {

		    HTree htree = getHTree(store, addressBits, false/* rawRecords */,
                    true/* persistent */);
			
			final byte[] k1 = new byte[] { 1,2,3,4 };
			final byte[] k2 = new byte[] { 3,6,4 };
			final byte[] k3 = new byte[] { 7,8,5,6 };
			final byte[] v1 = new byte[] { 1 };
			final byte[] v2 = new byte[] { 2 };
			final byte[] v3 = new byte[] { 3 };

			try {
				for (int i = 0; i < ndups; i++) {
					assertNull(htree.insert(k1, v1));
					assertNull(htree.insert(k2, v2));
					assertNull(htree.insert(k3, v3));
					
					// This checks boundary insert conditions on the mutable buffers
					final String failure = "After inserting: " + (i+1);
					assertTrue(failure, htree.contains(k1)); // 2000 dups fails here with 10 bit depth, 70 dups with 6 bit
					assertTrue(failure, htree.contains(k2));
					assertTrue(failure, htree.contains(k3));
				}
			} finally {
				// htree.dump(Level.DEBUG, System.out, true/* materialize */);				
			}

			
			assertTrue(htree.contains(k1)); // 2000 dups fails here with 10 bit depth, 70 dups with 6 bit
			assertTrue(htree.contains(k2));
			assertTrue(htree.contains(k3));

			// Check first with MutableBuckets
			assertTrue(getCount(htree, k1) == ndups);
			assertTrue(getCount(htree, k2) == ndups);
			assertTrue(getCount(htree, k3) == ndups);

			final long addrCheckpoint1 = htree.writeCheckpoint();
			
			htree = HTree.load(store, addrCheckpoint1, true/* readOnly */);

			final String failString1 = "addressBits: " + addressBits + ", dups: " + ndups;

			assertTrue(failString1, htree.contains(k1));
			assertTrue(failString1, htree.contains(k2));
			assertTrue(failString1, htree.contains(k3));
			
			// htree.dump(Level.DEBUG, System.out, true/* materialize */);
			
			final String failString = failString1 + " != ";

			assertTrue(failString + getCount(htree, k1), getCount(htree, k1) == ndups);
			assertTrue(failString + getCount(htree, k2), getCount(htree, k2) == ndups); // 50 dups fails with 8bit depth, 5 dups with 6 bit depth
			assertTrue(failString + getCount(htree, k3), getCount(htree, k3) == ndups); // 500 dups fails here with 10bit depth

		} finally {

			store.destroy();
		
		}

	}

//	private IRawStore getRWStore() {
//
//		final Properties properties = getProperties();
//
//		properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//
//		properties.setProperty(Options.DELETE_ON_EXIT, "true");
//
//		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
//
//		properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + true);
//
//		return new Journal(properties);//.getBufferStrategy();
//
//	}

    /**
     * Return the #of entries having the specified key.
     * 
     * @param htree
     *            The index.
     * @param k1
     *            The key.
     * @return
     */
	private int getCount(final HTree htree, final byte[] k1) {
		final ITupleIterator<?> iter = htree.lookupAll(k1);
		int count = 0;
		while (iter.hasNext()) {
			iter.next();
			count++;
		}

		return count;
	}

    /**
     * Simple test where the keys and the values are dups.
     * 
     * @throws IOException
     * @throws Exception
     */
	public void test_duplicateKeyValues() throws IOException, Exception {

		final IRawStore store = new SimpleMemoryRawStore();

		try {

            HTree htree = getHTree(store, 3/* addressBits */,
                    false/* rawRecord */, true/* persistent */);

			final byte[] k1 = new byte[] { 1 };
			final byte[] v2 = new byte[] { 2 };

			assertNull(htree.lookupFirst(k1));
			assertFalse(htree.contains(k1));

			assertNull(htree.insert(k1, v2));

			assertEquals(htree.lookupFirst(k1), v2);
			assertTrue(htree.contains(k1));

			assertNull(htree.insert(k1, v2));

			// before checkpoint.
            assertEquals(2, getCount(htree, k1));

			final long addrCheckpoint1 = htree.writeCheckpoint();

			htree = HTree.load(store, addrCheckpoint1, true/* readOnly */);

			assertEquals(htree.lookupFirst(k1), v2);
			assertTrue(htree.contains(k1));
			
			// after checkpoint.
            assertEquals(2, getCount(htree, k1));

		} finally {

			store.destroy();
		
		}

	}
}
