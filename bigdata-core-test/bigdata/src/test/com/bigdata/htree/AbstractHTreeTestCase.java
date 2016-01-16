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
 * Created on Jul 11, 2011
 */
package com.bigdata.htree;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.NoEvictionListener;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoderDupKeys;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

public class AbstractHTreeTestCase extends TestCase2 {

    public AbstractHTreeTestCase() {
	}

	public AbstractHTreeTestCase(String name) {
		super(name);
	}

    /*
     * TODO This might need to be modified to verify the sets of tuples in each
     * buddy bucket without reference to their ordering within the buddy bucket.
     * If so, then we will need to pass in the global depth of the bucket page
     * and clone and write new logic for the comparison of the leaf data state.
     */
    static void assertSameBucketData(ILeafData expected, ILeafData actual) {
        
        AbstractBTreeTestCase.assertSameLeafData(expected, actual);
        
    }
    
    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    static public void assertSameIteratorAnyOrder(final byte[][] expected,
            final Iterator<byte[]> actual) {

        assertSameIteratorAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
	static public void assertSameIteratorAnyOrder(final String msg,
			final byte[][] expected, final Iterator<byte[]> actual) {

		// stuff everything into a list (allows duplicates).
		final List<byte[]> range = new LinkedList<byte[]>();
		for (byte[] b : expected)
			range.add(b);

		// Do selection without replacement for the objects visited by
		// iterator.
		for (int j = 0; j < expected.length; j++) {

			if (!actual.hasNext()) {

				fail(msg + ": Index exhausted while expecting more object(s)"
						+ ": index=" + j);

			}

			final byte[] actualValue = actual.next();

			boolean found = false;

			final Iterator<byte[]> titr = range.iterator();

			while (titr.hasNext()) {

				final byte[] b = titr.next();

				if (BytesUtil.bytesEqual(b, actualValue)) {
					found = true;
					titr.remove();
					break;
				}

			}

			if (!found) {

				fail("Value not expected" + ": index=" + j + ", object="
						+ actualValue);

			}

		}

		if (actual.hasNext()) {

			final byte[] actualValue = actual.next();

			fail("Iterator will deliver too many objects object=" + actualValue);

		}

	}

	/**
	 * Return a new {@link HTree} backed by a simple transient store that will
	 * NOT evict leaves or nodes onto the store. The leaf cache will be large
	 * and cache evictions will cause exceptions if they occur. This provides an
	 * indication if cache evictions are occurring so that the tests of basic
	 * tree operations in this test suite are known to be conducted in an
	 * environment without incremental writes of leaves onto the store. This
	 * avoids copy-on-write scenarios and let's us test with the knowledge that
	 * there should always be a hard reference to a child or parent.
	 * 
	 * @param addressBits
	 *            The addressBts.
	 */
	public HTree getHTree(final IRawStore store, final int addressBits) {

		return getHTree(store, addressBits, false/* rawRecords */, false/* persistent */);

	}

	public HTree getHTree(final IRawStore store, final int addressBits,
			final boolean rawRecords, final boolean persistent) {

//		final ITupleSerializer<?,?> tupleSer = DefaultTupleSerializer.newInstance();

		final ITupleSerializer<?,?> tupleSer = new DefaultTupleSerializer(
				new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
				FrontCodedRabaCoderDupKeys.INSTANCE,// Note: reports true for isKeys()!
				// new SimpleRabaCoder(),// keys
				new SimpleRabaCoder() // vals
				);

		return getHTree(store, addressBits, rawRecords, persistent, tupleSer);

	}
	
	public HTree getHTree(final IRawStore store, final int addressBits,
			final boolean rawRecords, final boolean persistent,
			final ITupleSerializer tupleSer) {

		final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());

		if (rawRecords) {
			metadata.setRawRecords(true);
			metadata.setMaxRecLen(0);
		}
        
        metadata.setAddressBits(addressBits);

		metadata.setTupleSerializer(tupleSer);

		if (!persistent) {

			/*
			 * Does not allow incremental eviction and hence is not persistent.
			 * This is used to test the basic index maintenance operations
			 * before we test the persistence integration.
			 */
			
			// override the HTree class.
			metadata.setHTreeClassName(NoEvictionHTree.class.getName());

			return (NoEvictionHTree) HTree.create(store, metadata);

		}
		
		// Will support incremental eviction and persistence.
		return HTree.create(store, metadata);

    }
    
	/**
     * Specifies a {@link NoEvictionListener}.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class NoEvictionHTree extends HTree {

		/**
		 * @param store
		 * @param checkpoint
		 * @param metadata
		 */
		public NoEvictionHTree(IRawStore store, Checkpoint checkpoint,
				IndexMetadata metadata, boolean readOnly) {

			super(store, checkpoint, metadata, readOnly);

		}

		@Override
		protected HardReferenceQueue<PO> newWriteRetentionQueue(boolean readOnly) {

			return new HardReferenceQueue<PO>(//
					new NoEvictionListener(),//
					20000,//
					10//
			);

		}

	}

	/**
	 * FIXME Write test helper assertSameHTree(). See the code below for a
	 * starting point.
	 * 
	 * @param expected
	 * @param actual
	 */
	static protected void assertSameHTree(final AbstractHTree expected,
			final AbstractHTree actual) {
	
		assertTrue(((HTree) expected).nentries == ((HTree) actual).nentries);
		assertTrue(((HTree) expected).nleaves == ((HTree) actual).nleaves);
		assertTrue(((HTree) expected).nnodes == ((HTree) actual).nnodes);
		
		assertSameIterator(
				expected.getRoot().getTuples(), 
				actual.getRoot().getTuples());
	
	}

	protected static void assertSameIterator(final ITupleIterator<?> expected,
			final ITupleIterator<?> actual) {
		
	    @SuppressWarnings("unused")
        int index = 0;
		
	    while (expected.hasNext()) {
		
	        assertTrue(actual.hasNext());
			
	        index++;
			
            final ITuple<?> etup = expected.next();
            final ITuple<?> atup = actual.next();

            assertEquals("flags", etup.flags(), atup.flags());
            
            assertTrue("keys",
                    BytesUtil.bytesEqual(etup.getKey(), atup.getKey()));
            
            assertTrue("vals",
                    BytesUtil.bytesEqual(etup.getValue(), atup.getValue()));
        
		}

		assertFalse(actual.hasNext());
	}

	public static void assertSameOrderIterator(byte[][] keys,
			Iterator<byte[]> values) {
		for (int i = 0; i < keys.length; i++) {
			final byte[] other = values.next();
			
			if (!BytesUtil.bytesEqual(keys[i], other))
				fail("Unexpected ordered value");
		}
	}
    
//	/**
//	 * A suite of tests designed to verify that one htree correctly represents
//	 * the information present in a ground truth htree. The test verifies the
//	 * #of entries, the keys and values, and lookup by key. The address bits,
//	 * #of nodes and #of leaves may differ (the test does not presume that the
//	 * htrees were built with the same branching factor, but merely with the
//	 * same data).
//	 * 
//	 * @param expected
//	 *            The ground truth htree.
//	 * @param actual
//	 *            The htree that is being validated.
//	 */
//    static public void assertSameBTree(final AbstractHTree expected,
//            final AbstractHTree actual) {
//
//        assert expected != null;
//        
//        assert actual != null;
//        
//        // Must be the same "index".
//        assertEquals("indexUUID", expected.getIndexMetadata().getIndexUUID(),
//                actual.getIndexMetadata().getIndexUUID());
//        
////        // The #of entries must agree.
////        assertEquals("entryCount", expected.getEntryCount(), actual
////                .rangeCount(null, null));
//
//        /*
//         * Verify the forward tuple iterator.
//         * 
//         * Note: This compares the total ordering of the actual btree against
//         * the total ordering of a ground truth BTree <p> Note: This uses the
//         * {@link AbstractBTree#rangeIterator()} method. Due to the manner in
//         * which that iterator is implemented, the iterator does NOT rely on the
//         * separator keys. Therefore while this validates the total order it
//         * does NOT validate that the index may be searched by key (or by entry
//         * index).
//         */
//        {
//        
//            final long actualTupleCount = doEntryIteratorTest(expected
//                    .values(), actual.values());
//
//            // verifies based on what amounts to an exact range count.
//            assertEquals("entryCount", expected.getEntryCount(),
//                    actualTupleCount);
//            
//        }
//
//        /*
//         * Verify the reverse tuple iterator.
//         */
//        {
//            
//            final long actualTupleCount = doEntryIteratorTest(//
//                    expected.rangeIterator(null/* fromKey */, null/* toKey */,
//                            0/* capacity */, IRangeQuery.KEYS
//                                    | IRangeQuery.VALS | IRangeQuery.REVERSE,
//                            null/* filter */),
//                    //
//                    actual.rangeIterator(null/* fromKey */, null/* toKey */,
//                            0/* capacity */, IRangeQuery.KEYS
//                                    | IRangeQuery.VALS | IRangeQuery.REVERSE,
//                            null/* filter */));
//
//            // verifies based on what amounts to an exact range count.
//            assertEquals("entryCount", expected.getEntryCount(),
//                    actualTupleCount);
//
//        }
//
//        /*
//         * Extract the ground truth mapping from the input btree.
//         */
//		if (expected.getEntryCount() <= Integer.MAX_VALUE) {
//
//			final int entryCount = (int) expected.getEntryCount();
//
//			final byte[][] keys = new byte[entryCount][];
//
//			final byte[][] vals = new byte[entryCount][];
//
//			getKeysAndValues(expected, keys, vals);
//
//			/*
//			 * Verify lookup against the segment with random keys choosen from
//			 * the input btree. This vets the separatorKeys. If the separator
//			 * keys are incorrect then lookup against the index segment will
//			 * fail in various interesting ways.
//			 */
//			doRandomLookupTest("actual", actual, keys, vals);
//
//			/*
//			 * Verify lookup by entry index with random keys. This vets the
//			 * childEntryCounts[] on the nodes of the generated index segment.
//			 * If the are wrong then this test will fail in various interesting
//			 * ways.
//			 */
//			if (actual instanceof AbstractBTree) {
//
//				doRandomIndexOfTest("actual", ((AbstractBTree) actual), keys,
//						vals);
//
//			}
//
//		}
//
//        /*
//         * Examine the btree for inconsistencies (we also examine the ground
//         * truth btree for inconsistencies to be paranoid).
//         */
//
//        if(log.isInfoEnabled())
//            log.info("Examining expected tree for inconsistencies");
//        assert expected.dump(System.err);
//
//        /*
//         * Note: An IndexSegment can underflow a leaf or node if rangeCount was
//         * an overestimate so we can't run this task against an IndexSegment.
//         */
//        if(actual instanceof /*Abstract*/BTree) {
//            if(log.isInfoEnabled())
//                log.info("Examining actual tree for inconsistencies");
//            assert ((AbstractBTree)actual).dump(System.err);
//        }
//
//    }
//
//    /**
//     * Compares the total ordering of two B+Trees as revealed by their range
//     * iterators
//     * 
//     * @param expected
//     *            The ground truth iterator.
//     * 
//     * @param actual
//     *            The iterator to be tested.
//     * 
//     * @return The #of tuples that were visited in <i>actual</i>.
//     * 
//     * @see #doRandomLookupTest(String, AbstractBTree, byte[][], Object[])
//     * @see #doRandomIndexOfTest(String, AbstractBTree, byte[][], Object[])
//     */
//    static protected long doEntryIteratorTest(
//            final ITupleIterator<?> expectedItr,
//            final ITupleIterator<?> actualItr
//            ) {
//
//        int index = 0;
//
//        long actualTupleCount = 0L;
//        
//        while( expectedItr.hasNext() ) {
//            
//            if( ! actualItr.hasNext() ) {
//                
//                fail("The iterator is not willing to visit enough entries");
//                
//            }
//            
//            final ITuple<?> expectedTuple = expectedItr.next();
//            
//            final ITuple<?> actualTuple = actualItr.next();
//            
//            actualTupleCount++;
//            
//            final byte[] expectedKey = expectedTuple.getKey();
//            
//            final byte[] actualKey = actualTuple.getKey();
//
////            System.err.println("index=" + index + ": key expected="
////                    + BytesUtil.toString(expectedKey) + ", actual="
////                    + BytesUtil.toString(actualKey));
//
//            try {
//                
//                assertEquals(expectedKey, actualKey);
//                
//            } catch (AssertionFailedError ex) {
//                
//                /*
//                 * Lazily generate message.
//                 */
//                fail("Keys differ: index=" + index + ", expected="
//                        + BytesUtil.toString(expectedKey) + ", actual="
//                        + BytesUtil.toString(actualKey), ex);
//                
//            }
//
//            if (expectedTuple.isDeletedVersion()) {
//
//                assert actualTuple.isDeletedVersion();
//
//            } else {
//
//                final byte[] expectedVal = expectedTuple.getValue();
//
//                final byte[] actualVal = actualTuple.getValue();
//
//                try {
//
//                    assertSameValue(expectedVal, actualVal);
//
//                } catch (AssertionFailedError ex) {
//                    /*
//                     * Lazily generate message.
//                     */
//                    fail("Values differ: index=" + index + ", key="
//                            + BytesUtil.toString(expectedKey) + ", expected="
//                            + Arrays.toString(expectedVal) + ", actual="
//                            + Arrays.toString(actualVal), ex);
//
//                }
//
//            }
//
//            if (expectedTuple.getVersionTimestamp() != actualTuple
//                    .getVersionTimestamp()) {
//                /*
//                 * Lazily generate message.
//                 */
//                assertEquals("timestamps differ: index=" + index + ", key="
//                        + BytesUtil.toString(expectedKey), expectedTuple
//                        .getVersionTimestamp(), actualTuple
//                        .getVersionTimestamp());
//
//            }
//            
//            index++;
//            
//        }
//        
//        if( actualItr.hasNext() ) {
//            
//            fail("The iterator is willing to visit too many entries");
//            
//        }
//        
//        return actualTupleCount;
//
//    }
//
//    /**
//     * Extract all keys and values from the btree in key order.  The caller must
//     * correctly dimension the arrays before calling this method.
//     * 
//     * @param btree
//     *            The btree.
//     * @param keys
//     *            The keys in key order (out).
//     * @param vals
//     *            The values in key order (out).
//     */
//    static public void getKeysAndValues(final AbstractBTree btree, final byte[][] keys,
//            final byte[][] vals) {
//        
//        final ITupleIterator<?> itr = btree.rangeIterator();
//
//        int i = 0;
//        
//        while( itr.hasNext() ) {
//
//            final ITuple<?> tuple= itr.next();
//            
//            final byte[] val = tuple.getValue();
//            
//            final byte[] key = tuple.getKey();
//
//            assert val != null;
//            
//            assert key != null;
//            
//            keys[i] = key;
//            
//            vals[i] = val;
//            
//            i++;
//            
//        }
//        
//    }
//    
//    /**
//     * Tests the performance of random {@link IIndex#lookup(Object)}s on the
//     * btree. This vets the separator keys and the childAddr and/or childRef
//     * arrays since those are responsible for lookup.
//     * 
//     * @param label
//     *            A descriptive label for error messages.
//     * 
//     * @param btree
//     *            The btree.
//     * 
//     * @param keys
//     *            the keys in key order.
//     * 
//     * @param vals
//     *            the values in key order.
//     */
//    static public void doRandomLookupTest(final String label,
//            final IIndex btree, final byte[][] keys, final byte[][] vals) {
//
//        final int nentries = keys.length;//btree.rangeCount(null, null);
//
//        if (log.isInfoEnabled())
//            log.info("\ncondition: " + label + ", nentries=" + nentries);
//
//        final int[] order = getRandomOrder(nentries);
//
//        final long begin = System.currentTimeMillis();
//
//        final boolean randomOrder = true;
//
//        for (int i = 0; i < nentries; i++) {
//
//            final int entryIndex = randomOrder ? order[i] : i;
//            
//            final byte[] key = keys[entryIndex];
//        
//            final byte[] val = btree.lookup(key);
//
//            if (val == null && true) {
//
//                // Note: This exists only as a debug point.
//
//                btree.lookup(key);
//
//            }
//
//            final byte[] expectedVal = vals[entryIndex];
//
//            assertEquals(expectedVal, val);
//            
//        }
// 
//        if (log.isInfoEnabled()) {
//            
//            final long elapsed = System.currentTimeMillis() - begin;
//
//            log.info(label + " : tested " + nentries
//                + " keys order in " + elapsed + "ms");
//        
////        log.info(label + " : " + btree.getCounters().asXML(null/*filter*/));
//            
//        }
//        
//    }
//
//    /**
//     * Tests the performance of random lookups of keys and values by entry
//     * index. This vets the separator keys and childRef/childAddr arrays, which
//     * are used to lookup the entry index for a key, and also vets the
//     * childEntryCount[] array, since that is responsible for lookup by entry
//     * index.
//     * 
//     * @param label
//     *            A descriptive label for error messages.
//     * @param btree
//     *            The btree.
//     * @param keys
//     *            the keys in key order.
//     * @param vals
//     *            the values in key order.
//     */
//    static public void doRandomIndexOfTest(final String label,
//            final AbstractBTree btree, 
//            final byte[][] keys, final byte[][] vals) {
//
//        final int nentries = keys.length;//btree.getEntryCount();
//
//        if (log.isInfoEnabled())
//            log.info("\ncondition: " + label + ", nentries=" + nentries);
//
//        final int[] order = getRandomOrder(nentries);
//
//        final long begin = System.currentTimeMillis();
//
//        final boolean randomOrder = true;
//
//        for (int i = 0; i < nentries; i++) {
//
//            final int entryIndex = randomOrder ? order[i] : i;
//
//            final byte[] key = keys[entryIndex];
//
//            assertEquals("indexOf", entryIndex, btree.indexOf(key));
//
//            final byte[] expectedVal = vals[entryIndex];
//
//            assertEquals("keyAt", key, btree.keyAt(entryIndex));
//
//            assertEquals("valueAt", expectedVal, btree.valueAt(entryIndex));
//
//        }
//
//        if (log.isInfoEnabled()) {
//
//            final long elapsed = System.currentTimeMillis() - begin;
//
//            log.info(label + " : tested " + nentries + " keys in " + elapsed
//                    + "ms");
//
//// log.info(label + " : " + btree.getBtreeCounters());
//        }
//
//    }
//    
//    /**
//     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces the
//     * expected values in the expected order. Errors are reported if too few or
//     * too many values are produced, etc.
//     */
//    static public void assertSameIterator(byte[][] expected, ITupleIterator actual) {
//
//        assertSameIterator("", expected, actual);
//
//    }
//
//    /**
//     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces
//     * the expected values in the expected order. Errors are reported if too few
//     * or too many values are produced, etc.
//     */
//    static public void assertSameIterator(String msg, final byte[][] expected,
//            final ITupleIterator actual) {
//
//        int i = 0;
//
//        while (actual.hasNext()) {
//
//            if (i >= expected.length) {
//
//                fail(msg + ": The iterator is willing to visit more than "
//                        + expected.length + " values.");
//
//            }
//
//            ITuple tuple = actual.next();
//
//            final byte[] val = tuple.getValue();
//
//            if (expected[i] == null) {
//
//                if (val != null) {
//
//                    /*
//                     * Only do message construction if we know that the assert
//                     * will fail.
//                     */
//                    fail(msg + ": Different values at index=" + i
//                            + ": expected=null" + ", actual="
//                            + Arrays.toString(val));
//
//                }
//
//            } else {
//
//                if (val == null) {
//
//                    /*
//                     * Only do message construction if we know that the assert
//                     * will fail.
//                     */
//                    fail(msg + ": Different values at index=" + i
//                            + ": expected=" + Arrays.toString(expected[i])
//                            + ", actual=null");
//
//                }
//                
//                if (BytesUtil.compareBytes(expected[i], val) != 0) {
//                    
//                    /*
//                     * Only do message construction if we know that the assert
//                     * will fail.
//                     */
//                    fail(msg + ": Different values at index=" + i
//                            + ": expected=" + Arrays.toString(expected[i])
//                            + ", actual=" + Arrays.toString(val));
//                    
//                }
//
//            }
//            
//            i++;
//
//        }
//
//        if (i < expected.length) {
//
//            fail(msg + ": The iterator SHOULD have visited " + expected.length
//                    + " values, but only visited " + i + " values.");
//
//        }
//
//    }
//
//    /**
//     * Verifies the data in the two indices using a batch-oriented key range
//     * scans (this can be used to verify a key-range partitioned scale-out index
//     * against a ground truth index) - only the keys and values of non-deleted
//     * index entries in the <i>expected</i> index are inspected.  Deleted index
//     * entries in the <i>actual</i> index are ignored.
//     * 
//     * @param expected
//     * @param actual
//     */
//    public static void assertSameEntryIterator(IIndex expected, IIndex actual) {
//
//        final ITupleIterator expectedItr = expected.rangeIterator(null, null);
//
//        final ITupleIterator actualItr = actual.rangeIterator(null, null);
//
//        assertSameEntryIterator(expectedItr, actualItr);
//
//    }
//    
//    /**
//     * Verifies that the iterators visit tuples having the same data in the same
//     * order.
//     * 
//     * @param expectedItr
//     * @param actualItr
//     */
//    public static void assertSameEntryIterator(
//            final ITupleIterator expectedItr, final ITupleIterator actualItr) { 
//        
//        long nvisited = 0L;
//        
//        while (expectedItr.hasNext()) {
//
//            assertTrue("Expecting another index entry: nvisited=" + nvisited,
//                    actualItr.hasNext());
//
//            final ITuple expectedTuple = expectedItr.next();
//
//            final ITuple actualTuple = actualItr.next();
//
////            if(true) {
////                System.err.println("expected: " + expectedTuple);
////                System.err.println("  actual: " + actualTuple);
////            }
//            
//            nvisited++;
//
//            if (!BytesUtil.bytesEqual(expectedTuple.getKey(), actualTuple
//                    .getKey())) {
//
//                fail("Wrong key: nvisited=" + nvisited + ", expected="
//                        + expectedTuple + ", actual=" + actualTuple);
//
//            }
//
//            if (!BytesUtil.bytesEqual(expectedTuple.getValue(), actualTuple
//                    .getValue())) {
//
//                fail("Wrong value: nvisited=" + nvisited + ", expected="
//                        + expectedTuple + ", actual=" + actualTuple);
//                        
//            }
//            
//        }
//        
//        assertFalse("Not expecting more tuples", actualItr.hasNext());
//        
//    }

//   /**
//    * Moved to test suite - requires scans.
//    * 
//    * Note: This is only used for an informational message by a stress test. It
//    * could easily be replaced by dumpPages() which is part of the standard API
//    * and which is more efficient (a single scan versus two scans).
//    */
//   public String getPageInfo(final HTree htree) {
//      return "Created Pages for " + htree.getAddressBits() + " addressBits"
//            + ",  directory pages: " + activeDirectoryPages(htree) + " of "
//            + DirectoryPage.createdPages + ", bucket pages: "
//            + activeBucketPages(htree) + " of " + BucketPage.createdPages;
//   }
//
//   /**
//    * Moved to test suite - requires scans.
//    */
//   private final int activeBucketPages(final HTree htree) {
//      return activeBucketPages(htree.getRoot());
//   }
//
//   /**
//    * Moved to test suite - requires scans.
//    */
//   private final int activeDirectoryPages(final HTree htree) {
//      return activeDirectoryPages(htree.getRoot());
//   }
//
//   /**
//    * Moved to test suite - requires scans.
//    */
//   private int activeBucketPages(final DirectoryPage directoryPage) {
//      int ret = 0;
//      final Iterator<AbstractPage> children = directoryPage.childIterator();
//      while (children.hasNext()) {
//         final AbstractPage childPage = children.next();
//         if (childPage.isLeaf()) {
//            // A bucket page.
//            ret += 1;
//         } else {
//            // Recursion.
//            activeBucketPages((DirectoryPage) childPage);
//         }
//      }
//      return ret;
//   }
//
//   /**
//    * Moved to test suite - requires scans.
//    */
//   private int activeDirectoryPages(final DirectoryPage directoryPage) {
//      int ret = 1;
//      final Iterator<AbstractPage> children = directoryPage.childIterator();
//      while (children.hasNext()) {
//         final AbstractPage childPage = children.next();
//         if (childPage.isLeaf()) {
//            // Ignore leaves.
//            continue;
//         }
//         // recursion.
//         ret += activeDirectoryPages((DirectoryPage) childPage);
//      }
//      return ret;
//   }

}
