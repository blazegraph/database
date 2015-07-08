package com.bigdata.htree;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.util.BytesUtil;

/**
 * A stress test which inserts a series of integer keys and then verifies the
 * state of the index.
 * 
 * TODO Test with int32 keys (versus single byte keys).
 * 
 * TODO Stress test with random integers.
 * 
 * TODO Stress test with sequential negative numbers and a sequence of
 * alternating negative and positive integers.
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

    public void test_stressInsert_noEviction_addressBits1() {

		doStressTest(50/* limit */, 1/* addressBits */);
        
    }

   public void test_stressInsert_noEviction_addressBits2() {

		doStressTest(100/* limit */, 2/* addressBits */);
        
    }

   public void test_stressInsert_noEviction_addressBits3() {

		doStressTest(500/* limit */, 3/* addressBits */);
       
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
    public void test_stressInsert_noEviction_addressBitsMAX() {

		doStressTest(10000/* limit */, 16/* addressBits */);
        
    }

    private void doStressTest(final int limit, final int addressBits) {
//        BucketPage.createdPages = 0;
//        DirectoryPage.createdPages = 0;
        
        final IRawStore store = new SimpleMemoryRawStore();

//		final int validateInterval = limit < 100 ? 1 : limit / 100;
        
        try {

			final HTree htree = getHTree(store, addressBits,
					false/* rawRecords */, false/* persistent */);

            try {
            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final IKeyBuilder keyBuilder = new KeyBuilder();
            
			final byte[][] keys = new byte[limit][];

            for (int i = 0; i < limit; i++) {

                final byte[] key = keyBuilder.reset().append(i).getKey();

                keys[i] = key;
                htree.insert(key, key);
				if (log.isInfoEnabled())
					log.info("after key=" + i + "\n" + htree.PP());

//                if ((i % validateInterval) == 0) {
//
//                    for (int j = 0; j <= i; j++) {
//
//                        final byte[] b = keys[j];
//                        if (log.isDebugEnabled()) {
//                        	log.debug("verifying: " + j);
//                        }
//                        assertEquals(b, htree.lookupFirst(b));
//                        
////                        // TODO Verify the iterator.
////                        final byte[][] tmp = new byte[i][];
////                        System.arraycopy(keys/*src*/, 0/*srcOff*/, tmp/*src*/, 0/*dstOff*/, tmp.length/*length*/);
////                        assertSameIteratorAnyOrder(tmp/*expected*/, htree.values());
//
//                    }
//                    
//                }
                
            }

            assertEquals(limit, htree.getEntryCount());

            // Verify all tuples are found.
            for (int i = 0; i < limit; i++) {

                final byte[] key = keys[i];

				final byte[] firstVal = htree.lookupFirst(key);

				if (!BytesUtil.bytesEqual(key, firstVal))
					fail("Expected: " + BytesUtil.toString(key)
							+ ", actual="
							+ Arrays.toString(htree.lookupFirst(key)));

            }
            
            // Verify the iterator visits all of the tuples.
            assertSameIteratorAnyOrder(keys, htree.values());
            
            if (log.isInfoEnabled())
               log.info(htree
                     .dumpPages(true/* recursive */, true/* visitLeaves */));

            } catch (Throwable t) {
            	try {
                	log.error("Pretty Print of error state:\n" + htree.PP());
                  log.error(htree
                        .dumpPages(true/* recursive */, true/* visitLeaves */));
            	} catch (Throwable et) {
            		log.error("Problem with PP", et);
            	}
            	
            	throw new RuntimeException(t);
            
            }
            
        } finally {

            store.destroy();

        }
        
    }
    
}
