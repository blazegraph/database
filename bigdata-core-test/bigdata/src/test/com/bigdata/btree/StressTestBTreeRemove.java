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
package com.bigdata.btree;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

import junit.framework.TestCase2;

import org.apache.log4j.Level;

import com.bigdata.Banner;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;

public class StressTestBTreeRemove extends TestCase2 { //AbstractBTreeTestCase {

	public StressTestBTreeRemove() {
	}

	public StressTestBTreeRemove(String name) {
		super(name);
	}

    private Random r;
    private IRawStore store;
    private boolean useRawRecords;
    
    @Override
	protected void setUp() throws Exception {
		
		super.setUp();
		
		r = new Random();
		
		store = new MemStore(DirectBufferPool.INSTANCE);
		
		useRawRecords = r.nextBoolean();
		
	}
    
    @Override
	protected void tearDown() throws Exception {

		if (store != null) {

			store.close();

			store = null;

		}

		r = null;
		
		super.tearDown();
    	
    }

    private boolean useRawRecords() {
    	
    		return useRawRecords;
    	
    }
    
//    @Override
    private BTree getBTree(final int branchingFactor) {
        
        return getBTree(branchingFactor , DefaultTupleSerializer.newInstance());
        
    }

//    @Override
    private BTree getBTree(final int branchingFactor,
            final ITupleSerializer tupleSer) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

		if (useRawRecords()) {
			/*
			 * Randomly test with raw records enabled, using a small value for
			 * the maximum record length to force heavy use of raw records when
			 * they are chosen.
			 * 
			 * TODO Rather than doing this randomly, it would be better to do
			 * this systematically. This will make any problems reported by CI
			 * easier to interpret.
			 */
			metadata.setRawRecords(true);
			metadata.setMaxRecLen(8);
			
			// Set large retention queue
			metadata.setWriteRetentionQueueCapacity(6000);
		}
        
        metadata.setBranchingFactor(branchingFactor);

        metadata.setTupleSerializer(tupleSer);

        return BTree.create(store, metadata);
        
    }

    /**
	 * Stress test of insert, removal and lookup of keys in the tree (allows
	 * splitting of the root leaf).
	 * 
	 * TODO Alternative insert/remove patterns which remove many things at once
	 * and then insert many things. The insert/removes should be in a region of
	 * the index, but need not be perfectly dense (though that might make it
	 * more interesting).
	 * 
	 * TODO Multi-threaded insert/remove pattern using the Unisolated read-write
	 * index. Might be difficult to track ground truth. Just look for errors in
	 * the index.  Might need to do occasional checkpoints/commits also.
	 */
    /*
     * Note: passes with ntrials=100k, mtuples=10M @ m={3,4,5,16} against
     * memstore.  Runs for 1745s on Mac AirBook.
     */
    public void test_insertLookupRemoveKeyTreeStressTest() {

		Banner.banner();

		final int ntrials = 5;

		final int mtuples = 10000;

		doInsertLookupRemoveStressTestMGC(4, mtuples, ntrials);
		doInsertLookupRemoveStressTestMGC(5, mtuples, ntrials);
		doInsertLookupRemoveStressTestMGC(16, mtuples, ntrials);
		doInsertLookupRemoveStressTestMGC(3, mtuples, ntrials);

		doInsertLookupRemoveStressTest(3, mtuples, ntrials);
		doInsertLookupRemoveStressTest(4, mtuples, ntrials);
		doInsertLookupRemoveStressTest(5, mtuples, ntrials);
		doInsertLookupRemoveStressTest(16, mtuples, ntrials);

	}
    
    /**
     * MGC variant that populates, removes at random and then re-inserts in reverse order.
     * 
     * The idea is to try to force a problem where a deleted leaf (and its parent) have not
     * been ejected and then re-referenced for an insert,
     * 
     * @param m
     *            The branching factor
     * @param nkeys
     *            The #of distinct keys.
     * @param ntrials
     *            The #of trials.
     */
	// @Override
	private void doInsertLookupRemoveStressTestMGC(final int m, final int nkeys,
			final int ntrials) {

//        if (log.isInfoEnabled())
//            log.info
            System.out.println("m=" + m + ", nkeys=" + nkeys + ", ntrials=" + ntrials);

        // Fixture under test.
		final BTree btree = getBTree(m);

		// Ground truth.
		// final Map<Integer, SimpleEntry> expected = new TreeMap<Integer, SimpleEntry>();

		// All possible keys.
        final Integer[] keys = new Integer[nkeys];

        // All possible values.
        final SimpleEntry[] vals = new SimpleEntry[nkeys];

        long start = System.currentTimeMillis();
        for (int i = 0; i < nkeys; i++) {

			// Generate random keys.
			keys[i] = r.nextInt();

            vals[i] = new SimpleEntry();

			btree.insert(keys[i], vals[i]);
        }
        log.trace("First insert took " + (System.currentTimeMillis()-start) + "ms for " + nkeys + " inserts");
		try {

			/*
			 * Run test.
			 */
			for (int trial = 0; trial < ntrials; trial++) {
				final int mod = 3 * (trial + 1); // ratio of keys unreplaced
				log.trace("Start trial " + trial + " leaf count: " + btree.getLeafCount());
				for (int i = nkeys-1; i >= 0; i--) {
					if (i % mod != 0) // exclude 1/4
						btree.remove(keys[i]);
				}
				log.trace("After removes %" + mod + ", leaf count: " + btree.getLeafCount());
				for (int i = mod; i < nkeys; i++) {
					if (i % mod != 0) // exclude 1/4
						btree.insert(keys[i], vals[i]);
				}
			}

			assertTrue(btree.dump(System.err));

			if (log.isInfoEnabled())
				log.info(btree.getBtreeCounters().toString());

			btree.removeAll();

		} finally {

			btree.close();
    	
        }

    }
	/**
     * Stress test helper performs random inserts, removal and lookup operations
     * and compares the behavior of the {@link BTree} against ground truth as
     * tracked by a {@link TreeMap}.
     * 
     * Note: This test uses dense keys, but that is not a requirement.
     * 
     * @param m
     *            The branching factor
     * @param nkeys
     *            The #of distinct keys.
     * @param ntrials
     *            The #of trials.
     */
	// @Override
	private void doInsertLookupRemoveStressTest(final int m, final int nkeys,
			final int ntrials) {

//        if (log.isInfoEnabled())
//            log.info
            System.out.println("m=" + m + ", nkeys=" + nkeys + ", ntrials=" + ntrials);

        // Fixture under test.
		final BTree btree = getBTree(m);

		// Ground truth.
		final Map<Integer, SimpleEntry> expected = new TreeMap<Integer, SimpleEntry>();

		// All possible keys.
        final Integer[] keys = new Integer[nkeys];

        // All possible values.
        final SimpleEntry[] vals = new SimpleEntry[nkeys];

        for (int i = 0; i < nkeys; i++) {

			// Note: this produces dense keys with origin (1).
			keys[i] = i + 1;

            vals[i] = new SimpleEntry();

			if (i % 4 == 0) {

				/*
				 * Populate every Nth tuple for the starting condition.
				 */

				expected.put(keys[i], vals[i]);

				btree.insert(keys[i], vals[i]);

			}

        }
        
		try {

			/*
			 * Run test.
			 */
			for (int trial = 0; trial < ntrials; trial++) {

				final boolean insert = r.nextBoolean();

				/*
				 * Choose the starting index at random, leaving at least one
				 * index before the end of the original range.
				 */
				
				final int fromIndex = r.nextInt(nkeys - 2);

				/*
				 * Choose final index from the remaining range.
				 */
                final int toIndex = fromIndex
                        + ((nkeys - fromIndex) / (r.nextInt(1000) + 1));

				// The #of possible integer positions in that range.
				final int range = toIndex - fromIndex;

				// The #of actual tuples in that range.
				final long rangeCount = btree.rangeCount(fromIndex, toIndex);

				if(log.isTraceEnabled()) log.trace("trial=" + trial + ", "
						+ (insert ? "insert" : "remove") + ", fromIndex="
						+ fromIndex + ", toIndex=" + toIndex + ", range="
						+ range + ", rangeCount=" + rangeCount);

				for (int j = fromIndex; j < toIndex; j++) {
					
					final Integer ikey = keys[j];

					final SimpleEntry val = vals[j];

					final byte[] key = TestKeyBuilder.asSortKey(ikey);

					if (insert) {

						// System.err.println("insert("+ikey+", "+val+")");
						final SimpleEntry old = expected.put(ikey, val);

						final SimpleEntry old2 = (SimpleEntry) btree.insert(
								key, val);

						assertEquals(old, old2);

					} else {

						// System.err.println("remove("+ikey+")");
						final SimpleEntry old = expected.remove(ikey);

						final SimpleEntry old2 = (SimpleEntry) SerializerUtil
								.deserialize(btree.remove(key));

						assertEquals(old, old2);

					}
					
				}

				if (trial % 100 == 0) {

					/*
					 * Validate the keys and entries.
					 */

					assertEquals("#entries", expected.size(),
							btree.getEntryCount());

					final Iterator<Map.Entry<Integer, SimpleEntry>> itr = expected
							.entrySet().iterator();

					while (itr.hasNext()) {

						final Map.Entry<Integer, SimpleEntry> entry = itr.next();

						final byte[] tmp = TestKeyBuilder.asSortKey(entry
								.getKey());

						assertEquals("lookup(" + entry.getKey() + ")",
								entry.getValue(), btree.lookup(tmp));

					}

					assertTrue(btree.dump(Level.ERROR, System.err));

				}

			}

			assertTrue(btree.dump(System.err));

			if (log.isInfoEnabled())
				log.info(btree.getBtreeCounters().toString());

			btree.removeAll();

			/*
			 * TODO try more insert/removes after removeAll (or removeAll in
			 * some key range).  Clear the same keys out of the groundTruth
			 * map and revalidate.
			 */
			
		} finally {

			btree.close();
    	
        }

    }
    
}
