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
 * Created on Jul 16, 2011
 */
package com.bigdata.htree;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;

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

    public void test_stressInsert_noEviction_addressBits1() {

		doStressTest(10000/* limit */, 1/* addressBits */);
        
    }

   public void test_stressInsert_noEviction_addressBits2() {

		doStressTest(10000/* limit */, 2/* addressBits */);
        
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

	private HTree getHTree(final IRawStore store, final int addressBits,
			final boolean rawRecords) {

		/*
		 * TODO This sets up a tuple serializer for a presumed case of 4 byte
		 * keys (the buffer will be resized if necessary) and explicitly chooses
		 * the SimpleRabaCoder as a workaround since the keys IRaba for the
		 * HTree does not report true for isKeys(). Once we work through an
		 * optimized bucket page design we can revisit this as the
		 * FrontCodedRabaCoder should be a good choice, but it currently
		 * requires isKeys() to return true.
		 */
		final ITupleSerializer<?,?> tupleSer = new DefaultTupleSerializer(
				new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
				//new FrontCodedRabaCoder(),// Note: reports true for isKeys()!
				new SimpleRabaCoder(),// keys
				new SimpleRabaCoder() // vals
				);
		
		final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

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
		metadata.setWriteRetentionQueueCapacity(10);
		metadata.setWriteRetentionQueueScan(10); // Must be LTE capacity.

		return HTree.create(store, metadata);

	}
    
    private void doStressTest(final int limit, final int addressBits) {

		/*
		 * FIXME We can not yet specify an unbounded pool size for the MemStore,
		 * so this is just specifying a relatively big value but then it is
		 * possible for the unit test to overflow the available memory.
		 */
		final IRawStore store = new MemStore(DirectBufferPool.INSTANCE, 100);

		try {

			final HTree htree = getHTree(store, addressBits, false/* rawRecords */);

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
				AbstractHTreeTestCase.assertSameIteratorAnyOrder(keys,
						htree.values());

			} catch (Throwable t) {

				log.error(t, t);

				log.error("Pretty Print of error state:\n" + htree.PP());

				throw new RuntimeException(t);

			}

//			log.error("Pretty Print of final state:\n" + htree.PP());

		} finally {

			store.destroy();

		}

	}

}
