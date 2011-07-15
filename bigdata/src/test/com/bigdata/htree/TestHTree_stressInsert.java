package com.bigdata.htree;

import org.apache.log4j.Logger;

import com.bigdata.btree.DefaultEvictionListener;
import com.bigdata.btree.NoEvictionListener;
import com.bigdata.btree.PO;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.IHardReferenceQueue;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Stress test for insert.
 * 
 * @author thompsonbry@users.sourceforge.net
 * @author martyncutcher@users.sourceforge.net
 */
public class TestHTree_stressInsert extends AbstractHTreeTestCase {

	private final static Logger log = Logger.getLogger(TestHTree_stressInsert.class);
	
	public TestHTree_stressInsert() {
	}

	public TestHTree_stressInsert(String name) {
		super(name);
	}

    /**
     * FIXME Write a stress test which inserts a series of integer keys and then
     * verifies the state of the index. Any consistency problems should be
     * worked out in depth with an extension of the unit test for the insert
     * sequence (1,2,3,4,5).
     * 
     * TODO Test with int32 keys (versus single byte keys).
     * 
     * TODO This could be parameterized for the #of address bits, the #of keys
     * to insert, etc. [Test w/ 1 address bit (fan-out := 2). Test w/ 1024
     * address bits.  Use the stress test with the nominal fan out to look at
     * hot spots in the code.  Instrument counters for structural and other
     * modifications.]
     * 
     * TODO Stress test with random integers.
     * 
     * TODO Stress test with sequential negative numbers and a sequence of
     * alternating negative and positive integers.
     */
    public void test_stressInsert_noEviction_addressBits1() {

		doStressTest(10000/* limit */, 1/* addressBits */);
        
    }

   public void test_stressInsert_noEviction_addressBits2() {

		doStressTest(17/* limit */, 2/* addressBits */);
        
    }

   public void test_stressInsert_noEviction_addressBits3() {

		doStressTest(10000/* limit */, 3/* addressBits */);
       
   }

    public void test_stressInsert_noEviction_addressBits4() {

		doStressTest(10000/* limit */, 4/* addressBits */);
        
    }

    public void test_stressInsert_noEviction_addressBits6() {

		doStressTest(10000/* limit */, 6/* addressBits */);
        
    }

    public void test_stressInsert_noEviction_addressBits8() {

		doStressTest(10000/* limit */, 8/* addressBits */);
        
    }

    public void test_stressInsert_noEviction_addressBits10() {

		doStressTest(10000/* limit */, 10/* addressBits */);
        
    }

    private void doStressTest(final int limit, final int addressBits) {
        
        final IRawStore store = new SimpleMemoryRawStore();

		final int validateInterval = limit < 100 ? 1 : limit / 100;
        
        try {

            final HTree htree = new HTree(store, addressBits, false/* rawRecords */) {
                IHardReferenceQueue<PO> newWriteRetentionQueue(final boolean readOnly) {
                	return new HardReferenceQueue<PO>(//
                            new NoEvictionListener(),//
                            20000,//writeRetentionQueueCapacity,//
                            10//writeRetentionQueueScan//
                    );
                }
            };

            try {
            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final IKeyBuilder keyBuilder = new KeyBuilder();
            
			final byte[][] keys = new byte[limit][];

            for (int i = 0; i < limit; i++) {

                // final byte[] key = keyBuilder.reset().append((byte)i).getKey();
                final byte[] key = false ? 
                		keyBuilder.reset().append((byte) i).getKey()
                		: keyBuilder.reset().append(i).getKey();


                keys[i] = key;
                htree.insert(key, key);
				if (log.isInfoEnabled())
					log.info("after key=" + i + "\n" + htree.PP());

                if ((i % validateInterval) == 0) {

                    for (int j = 0; j <= i; j++) {

                        final byte[] b = keys[j];
                        if (log.isDebugEnabled()) {
                        	log.debug("verifying: " + j);
                        }
                        //assertEquals(b, htree.lookupFirst(b));
                        
//                        // TODO Verify the iterator.
//                        final byte[][] tmp = new byte[i][];
//                        System.arraycopy(keys/*src*/, 0/*srcOff*/, tmp/*src*/, 0/*dstOff*/, tmp.length/*length*/);
//                        assertSameIteratorAnyOrder(tmp/*expected*/, htree.values());

                    }
                    
                }
                
            }

            // Verify all tuples are found.
            for (int i = 0; i < limit; i++) {

                final byte[] key = keys[i];

                assertEquals(key, htree.lookupFirst(key));

            }
            
            assertEquals(limit, htree.getEntryCount());

            // Verify the iterator visits all of the tuples.
            assertSameIteratorAnyOrder(keys, htree.values());
            
            } catch (Throwable t) {

            	log.error("Pretty Print of error state:\n" + htree.PP(), t);
            	
            	throw new RuntimeException(t);
            
            }
            
        } finally {

            store.destroy();

        }
        
    }
    
}
