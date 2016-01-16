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
 * Created on Jul 16, 2011
 */
package com.bigdata.htree;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoderDupKeys;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.AbstractHTree.HTreePageStateException;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * Integration test with a persistence store.
 * 
 * @author bryan
 */
public class TestHTreeWithMemStore extends TestCase {

    private final static Logger log = Logger.getLogger(TestHTreeWithMemStore.class);

    public TestHTreeWithMemStore() {
    }

    public TestHTreeWithMemStore(String name) {
        super(name);
    }
    
    public void test_stressInsert_addressBits1() {

        /*
         * Note: If the retention queue is too small here then the test will
         * fail once the maximum depth of the tree exceeds the capacity of the
         * retention queue and a parent is evicted while a child is being
         * mutated.
         */
        doStressTest(1/* addressBits */, 50);
        
    }

   public void test_stressInsert_addressBits2() {

        doStressTest(2/* addressBits */, s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits3() {

        doStressTest(3/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits4() {

        doStressTest(4/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits5() {

        doStressTest(5/* addressBits */,s_retentionQueueCapacity);

    }

    public void test_stressInsert_addressBits6() {

        doStressTest(6/* addressBits */,s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits8() {

        doStressTest(8/* addressBits */,s_retentionQueueCapacity);
        
    }

    public void test_stressInsert_addressBits10() {

        doStressTest(10/* addressBits */,s_retentionQueueCapacity);

    }
    
    /**
     * Stress test for handling of overflow pages.
     */
    public void test_overflowPage_addressBits3() {

        final int addressBits = 3; 

        final int writeRetentionQueueCapacity = 20; // FIXME was 20
        
        final int numOverflowPages = 5000; // FIXME was 5000

        doOverflowStressTest(addressBits, writeRetentionQueueCapacity, numOverflowPages);
        
    }

    public void test_overflowPage_addressBits10() {
    
        final int addressBits = 10; 

        final int writeRetentionQueueCapacity = 30; // FIXME was 20
        
        final int numOverflowPages = 1000; // FIXME was 5000

        doOverflowStressTest(addressBits, writeRetentionQueueCapacity, numOverflowPages);
        
    }

    /**
     * 
     * @param addressBits
     * @param writeRetentionQueueCapacity
     * @param numOverflowPages
     *            The #of overflow bucket pages for the largest overflow bucket
     *            in the tree.
     */
    private void doOverflowStressTest(final int addressBits,
            final int writeRetentionQueueCapacity, final int numOverflowPages) {
        
        final long start = System.currentTimeMillis();

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

        try {

            final HTree htree = getHTree(store, addressBits,
                    false/* rawRecords */, writeRetentionQueueCapacity);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final IKeyBuilder keyBuilder = new KeyBuilder();

            // Overflow keys
            final byte[][] keys = new byte[2000][];
            for (int i = 0; i < 2000; i++) {
                keys[i] = keyBuilder.reset().append(new Integer(i).hashCode()).getKey();
            }
            
            final byte[] val = new byte[]{2};
            
            final int inserts = (1 << addressBits) * numOverflowPages;
            
//            long altInserts = 0;
            long altInserts1 = 0;
            long altInserts2 = 0;
            
            // insert enough tuples to fill the page twice over.
            for (int i = 0; i < inserts; i++) {

                htree.insert(keys[0], val);
                
                try {
                	// This can slow down the stress tests with large numbers, disable for standard CI run
                	// htree.checkConsistency(false);
                } catch (HTreePageStateException spe) {
                	System.err.println("Problem on " + i + "th insert, with key: " + BytesUtil.toString(keys[0]));
                	System.out.println(htree.PP());
                	
                	throw spe;
                }
                
                if (i % 5 == 0) {
                    htree.insert(keys[1], val);
                    altInserts1++;
                }
                if (i % 7 == 0) {
                    htree.insert(keys[2], val);
                    altInserts2++;
                }
                
            }
            
            long randInserts = 0;
            for (int i = 3; i < keys.length; i++) {
                htree.insert(keys[i], val);
                randInserts++;              
            }
                       
            final long load = System.currentTimeMillis();
            
            // now iterate over all the values
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(keys[0]);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    assertTrue(BytesUtil.bytesEqual(keys[0], tup.getKey()));
                    visits++;
                }
                assertEquals(inserts,visits);
            }
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(keys[1]);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    assertTrue(BytesUtil.bytesEqual(keys[1], tup.getKey()));
                    visits++;
                }
                assertEquals(altInserts1,visits);
            }
            {
                //  first using lookupAll
                final ITupleIterator tups = htree.lookupAll(keys[2]);
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    assertTrue(BytesUtil.bytesEqual(keys[2], tup.getKey()));
                    visits++;
                }
                assertEquals(altInserts2,visits);
            }
            {
                // then using the rangeIterator
                final ITupleIterator tups = htree.rangeIterator();
                long visits = 0;
                while (tups.hasNext()) {
                    final ITuple tup = tups.next();
                    visits++;
                }
                final long total =(inserts + altInserts1 + altInserts2 + randInserts);
                assertEquals(total,visits);
            }
            
            for (int i = 0; i < keys.length; i++) {
             	assertTrue(htree.contains(keys[i]));
            }
            
            final long end = System.currentTimeMillis();
            
            final BTreeCounters counters = htree.getBtreeCounters();
            
            if (log.isInfoEnabled()) {
            	log.info("Htree Leaves: " + htree.nleaves + ", Evicted: " + counters.leavesWritten 
                		+ ", Nodes: " + htree.nnodes + ", Evicted: " + counters.nodesWritten
						+ ", allocation count="
						+ ((MemStore) store).getMemoryManager()
						.getAllocationCount());

                log.info("Load took " + (load - start) + "ms, loops for "
                        + (inserts + altInserts1 + altInserts2) + " "
                        + (end - load) + "ms");
            }

            htree.flush();
            
        } finally {

            store.destroy();

        }

    }


//    public void test_stressInsert_addressBitsMAX() {
//
//        doStressTest(16/* addressBits */);
//
//    }

    /**
     * 
     * Note: If the retention queue is less than the maximum depth of the HTree
     * then we can encounter a copy-on-write problem where the parent directory
     * becomes immutable during a mutation on the child.
     * 
     * @param store
     * @param addressBits
     * @param rawRecords
     * @param writeRetentionQueueCapacity
     * 
     * @return
     */
    private HTree getHTree(final IRawStore store, final int addressBits,
            final boolean rawRecords, final int writeRetentionQueueCapacity) {

        final ITupleSerializer<?,?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                FrontCodedRabaCoderDupKeys.INSTANCE,// keys
                new SimpleRabaCoder() // vals
                );
        
        final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());

        if (rawRecords) {
            metadata.setRawRecords(true);
            metadata.setMaxRecLen(0);
        }

        metadata.setAddressBits(addressBits);

        metadata.setTupleSerializer(tupleSer);

        /*
         * Note: A low retention queue capacity will drive evictions, which is
         * good from the perspective of stressing the persistence store
         * integration.
         */
        metadata.setWriteRetentionQueueCapacity(writeRetentionQueueCapacity);
        metadata.setWriteRetentionQueueScan(10); // Must be LTE capacity.

        return HTree.create(store, metadata);

    }
    
    private static final int s_limit = 10000;
    private static final int s_retentionQueueCapacity = 20;

    /**
     * Note: If the retention queue is less than the maximum depth of the HTree
     * then we can encounter a copy-on-write problem where the parent directory
     * becomes immutable during a mutation on the child.
     * 
     * @param addressBits
     * @param writeRetentionQueueCapacity
     */
    private void doStressTest(final int addressBits,
            final int writeRetentionQueueCapacity) {

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

        try {

            final HTree htree = getHTree(store, addressBits,
                    false/* rawRecords */, writeRetentionQueueCapacity);

            try {

                // Verify initial conditions.
                assertTrue("store", store == htree.getStore());
                assertEquals("addressBits", addressBits, htree.getAddressBits());

                final IKeyBuilder keyBuilder = new KeyBuilder();

                final byte[][] keys = new byte[s_limit][];
                for (int i = 0; i < s_limit; i++) {
                    keys[i] = keyBuilder.reset().append(new Integer(i).hashCode()).getKey();
                }
                final byte[] badkey = keyBuilder.reset().append(new Integer(s_limit * 32).hashCode()).getKey();

                final long begin = System.currentTimeMillis();
                for (int i = 0; i < s_limit; i++) {
                    final byte[] key = keys[i];
                    htree.insert(key, key);
                    try {
                    	// This can slow down the stress tests with large numbers, disable for standard CI run
                    	// htree.checkConsistency(false);
                    } catch (HTreePageStateException spe) {
                    	System.err.println("Problem on " + i + "th insert, with key: " + BytesUtil.toString(keys[0]));
                    	System.out.println(htree.PP());
                    	
                    	throw spe;
                    }
                    if (log.isTraceEnabled())
                        log.trace("after key=" + i + "\n" + htree.PP());

                }

                final long elapsedInsertMillis = System.currentTimeMillis() - begin;
                
                assertEquals(s_limit, htree.getEntryCount());
                
                final long beginLookupFirst = System.currentTimeMillis();
                // Verify all tuples are found.
                for (int i = 0; i < s_limit; i++) {

                    final byte[] key = keys[i];

                    final byte[] firstVal = htree.lookupFirst(key);

                    if (!BytesUtil.bytesEqual(key, firstVal))
                        fail("Expected: " + BytesUtil.toString(key)
                                + ", actual="
                                + Arrays.toString(htree.lookupFirst(key)));

                }
                
                final long elapsedLookupFirstTime = System.currentTimeMillis()
                        - beginLookupFirst;

                final long beginValueIterator = System.currentTimeMillis();
                
                // Verify the iterator visits all of the tuples.
                AbstractHTreeTestCase.assertSameIteratorAnyOrder(keys,
                        htree.values());

                final long elapsedValueIteratorTime = System
                        .currentTimeMillis() - beginValueIterator;
                
                if (log.isInfoEnabled()) {
					log.info("Inserted: "
							+ s_limit
							+ " tuples in "
							+ elapsedInsertMillis
							+ "ms, lookupFirst(all)="
							+ elapsedLookupFirstTime
							+ ", valueScan(all)="
							+ elapsedValueIteratorTime
							+ ", addressBits="
							+ htree.getAddressBits()
							+ ", nnodes="
							+ htree.getNodeCount()
							+ ", nleaves="
							+ htree.getLeafCount()
							+ ", allocation count="
							+ ((MemStore) store).getMemoryManager()
									.getAllocationCount());
				}
                
                // Attempts to access absent keys should not lazily create bucketPage
                assertTrue(htree.lookupFirst(badkey) == null); 
                assertFalse(htree.lookupAll(badkey).hasNext()); // should not lazily create bucketPage if value not present!

                if (true) {

                    /*
                     * Note: This code verifies that the dumpPages() code is
                     * working. If you comment this out, then write an explicit
                     * unit test for dumpPages().
                     */
                    
                    // Checkpoint the index before computing the stats.
                    htree.writeCheckpoint();

                    // Verify that we can compute the page stats.
                    final BaseIndexStats stats = htree.dumpPages(true/* recursive */, true/* visitLeaves */);

                    if (log.isInfoEnabled())
                        log.info(stats.toString());
                
                    System.err.println(stats);

                }
                
            } catch (Throwable t) {

                log.error(t);

//              try {
//                  log.error("Pretty Print of error state:\n" + htree.PP(), t);
//              } catch (Throwable t2) {
//                  log.error("Problem in pretty print: t2", t2);
//              }

                // rethrow the original exception.
                throw new RuntimeException(t);

            }

            // log.error("Pretty Print of final state:\n" + htree.PP());

        } finally {
        	
            store.destroy();

        }

    }

    public void test_orderedInsert_addressBits2() {

    	doOrderedTest(2/* addressBits */, 40/*s_retentionQueueCapacity*/);
        
    }

    public void test_orderedInsert_addressBits4() {

    	doOrderedTest(4/* addressBits */, s_retentionQueueCapacity);
        
    }

    public void test_orderedInsert_addressBits8() {

    	doOrderedTest(8/* addressBits */, s_retentionQueueCapacity);
        
    }

    public void test_orderedInsert_addressBits10() {

    	doOrderedTest(10/* addressBits */, s_retentionQueueCapacity);
        
    }

    private void doOrderedTest(final int addressBits,
            final int writeRetentionQueueCapacity) {

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

        try {

            final HTree htree = getHTree(store, addressBits,
                    false/* rawRecords */, writeRetentionQueueCapacity);

            try {

                // Verify initial conditions.
                assertTrue("store", store == htree.getStore());
                assertEquals("addressBits", addressBits, htree.getAddressBits());

                final IKeyBuilder keyBuilder = new KeyBuilder();

                final byte[][] keys = new byte[s_limit][];
                for (int i = 0; i < s_limit; i++) {
                    keys[i] = keyBuilder.reset().append(new Integer(i).hashCode()).getKey();
                }
                final long begin = System.currentTimeMillis();
                // insert in overlapping sequences of 0,2,4,6,8 - 1,3,5,7,9
                for (int i = 0; i < s_limit; i+=10) {
                	
                    htree.insert(keys[i], keys[i]);
                    htree.insert(keys[i+2], keys[i+2]);
                    htree.insert(keys[i+4], keys[i+4]);
                    htree.insert(keys[i+6], keys[i+6]);
                    htree.insert(keys[i+8], keys[i+8]);
                    htree.insert(keys[i+1], keys[i+1]);
                    htree.insert(keys[i+3], keys[i+3]);
                    htree.insert(keys[i+5], keys[i+5]);
                    htree.insert(keys[i+7], keys[i+7]);
                    htree.insert(keys[i+9], keys[i+9]);
                   
                    if (log.isTraceEnabled())
                        log.trace("after key=" + i + "\n" + htree.PP());

                }

                final long elapsedInsertMillis = System.currentTimeMillis() - begin;
                
                assertEquals(s_limit, htree.getEntryCount());
                
                final long beginLookupFirst = System.currentTimeMillis();
                // Verify all tuples are found.
                for (int i = 0; i < s_limit; i++) {

                    final byte[] key = keys[i];

                    final byte[] firstVal = htree.lookupFirst(key);

                    if (!BytesUtil.bytesEqual(key, firstVal))
                        fail("Expected: " + BytesUtil.toString(key)
                                + ", actual="
                                + Arrays.toString(htree.lookupFirst(key)));

                }

                final long elapsedLookupFirstTime = System.currentTimeMillis()
                        - beginLookupFirst;

                final long beginValueIterator = System.currentTimeMillis();
                
                // Verify the iterator visits all of the tuples.
                AbstractHTreeTestCase.assertSameOrderIterator(keys,
                        htree.values());

                final long elapsedValueIteratorTime = System
                        .currentTimeMillis() - beginValueIterator;
                
                if (log.isInfoEnabled()) {
                    log.info("Inserted: " + s_limit + " tuples in "
                            + elapsedInsertMillis + "ms, lookupFirst(all)="
                            + elapsedLookupFirstTime+ ", valueScan(all)="
                            + elapsedValueIteratorTime + ", addressBits="
                            + htree.getAddressBits() + ", nnodes="
                            + htree.getNodeCount() + ", nleaves="
                            + htree.getLeafCount()
							+ ", allocation count="
							+ ((MemStore) store).getMemoryManager()
							.getAllocationCount());
                }
                
                for (int i = 0; i < s_limit; i++) {
                	
                    assertTrue(htree.contains(keys[i]));
                }
                
//                if (log.isInfoEnabled()) {
//                    log.info("HTree: " + htree.PP());
//                }
                
                htree.removeAll();
                
                if (log.isInfoEnabled()) {
                    log.info("After removeAll: nnodes="
                            + htree.getNodeCount() + ", nleaves="
                            + htree.getLeafCount()
							+ ", allocation count="
							+ ((MemStore) store).getMemoryManager()
							.getAllocationCount());
                }


            } catch (Throwable t) {

                log.error(t);

//              try {
//                  log.error("Pretty Print of error state:\n" + htree.PP(), t);
//              } catch (Throwable t2) {
//                  log.error("Problem in pretty print: t2", t2);
//              }

                // rethrow the original exception.
                throw new RuntimeException(t);

            }

//          log.error("Pretty Print of final state:\n" + htree.PP());

        } finally {

            store.destroy();

        }

    }


}
