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
 * Created on Apr 17, 2007
 */

package com.bigdata.btree;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.log4j.Level;

import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.TestRestartSafe;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link BTree#removeAll()}.
 * 
 * @see TestRestartSafe#test_restartSafe01()
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRemoveAll extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestRemoveAll() {
    }

    /**
     * @param name
     */
    public TestRemoveAll(String name) {
        super(name);
    }

    /**
     *
     */
    public void test_removeAll() {

        final int m = 3;

        BTree btree = getBTree( m );
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        {
            
            byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                    new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                    new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };
            
//            btree.insert(new BatchInsert(values.length, keys, values));
            
            for (int i = 0; i < values.length; i++) {
                
                btree.insert(keys[i], values[i]);
                
            }

            assertTrue(btree.dump(Level.DEBUG,System.err));
    
            assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                    btree.rangeIterator());

            btree.removeAll();

            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            assertSameIterator(new Object[] {}, btree.rangeIterator());
            
        }

    }
    
    /**
     * Stress test of {@link BTree#removeAll()} where the btree is flushed
     * {@link BTree#writeCheckpoint()}. This will cause problems unless the cache is also
     * cleared since nodes that have been detached from their parents will be in
     * the cache.
     */
    public void test_removeAll_02() {

        final BTree btree;
        {

            final IRawStore store = new SimpleMemoryRawStore();

            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

            metadata.setBranchingFactor(3);

            btree = BTree.create(store, metadata);

        }

        final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);

        final int NTRIALS = 100;

        final int NINSERTS = 1000;

        final double removeAllRate = 0.02;

        for (int i = 0; i < NTRIALS; i++) {

            for (int j = 0; j < NINSERTS; j++) {

                if (r.nextDouble() < removeAllRate) {

                    if (log.isInfoEnabled())
                        log.info("removeAll with " + btree.getEntryCount()
                                + " entries");

                    btree.removeAll();

                }
                
                final int tmp = r.nextInt(10000);
                
                final byte[] key = keyBuilder.reset().append(tmp).getKey();
                
                btree.insert(key, new SimpleEntry(tmp));
                
            }

            // flush to the backing store.
            btree.writeCheckpoint();
            
        }
        
    }

	/**
	 * Unit test for {@link BTree#removeAll()} which verifies that the tuples
	 * are actually deleted one-by-one and the backing raw records released if
	 * the index supports raw records.
	 */
    public void test_removeAll_rawRecords() {
    	
        final MyRawStore store = new MyRawStore(new SimpleMemoryRawStore());

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBranchingFactor(10);
        metadata.setRawRecords(true);
        metadata.setMaxRecLen(64);

		final BTree btree = BTree.create(store, metadata);

		assertEquals(64, btree.getMaxRecLen());

		assertTrue(((ILeafData) btree.getRoot()).hasRawRecords());

		btree.insert(new byte[] { 1 }, new byte[] { 1 });
		btree.insert(new byte[] { 2 }, new byte[btree.getMaxRecLen() + 1]);
		btree.insert(new byte[] { 3 }, new byte[] { 3 });

		final Leaf root = (Leaf)btree.getRoot();
		
		assertTrue(root.getRawRecord(0) == IRawStore.NULL);
		
		assertTrue(root.getRawRecord(1) != IRawStore.NULL);
		
		assertTrue(root.getRawRecord(2) == IRawStore.NULL);
		
		store.expectDelete = root.getRawRecord(1);

		btree.removeAll();
		
		// verify that the raw record was deleted.
		assertEquals(IRawStore.NULL,store.expectDelete);
    	
    }
    
    /**
	 * Helper class is used to watch for deletes of raw records from the backing
	 * store.
	 * 
	 * @author thompsonbry
	 */
	private static class MyRawStore implements IRawStore {
		
		final IRawStore delegate;
		
		long expectDelete = IRawStore.NULL;

		public MyRawStore(final IRawStore delegate) {
			this.delegate = delegate;
		}

		public void close() {
			delegate.close();
		}

		public void delete(long addr) {
			if (expectDelete != IRawStore.NULL) {
				assertEquals(expectDelete, addr);
				expectDelete = IRawStore.NULL;
			}
			delegate.delete(addr);
		}

		public void deleteResources() {
			delegate.deleteResources();
		}

		public void destroy() {
			delegate.destroy();
		}

		public void force(boolean metadata) {
			delegate.force(metadata);
		}

		public int getByteCount(long addr) {
			return delegate.getByteCount(addr);
		}

		public CounterSet getCounters() {
			return delegate.getCounters();
		}

		public File getFile() {
			return delegate.getFile();
		}

		public long getOffset(long addr) {
			return delegate.getOffset(addr);
		}

		public IResourceMetadata getResourceMetadata() {
			return delegate.getResourceMetadata();
		}

		public UUID getUUID() {
			return delegate.getUUID();
		}

		public boolean isFullyBuffered() {
			return delegate.isFullyBuffered();
		}

		public boolean isOpen() {
			return delegate.isOpen();
		}

		public boolean isReadOnly() {
			return delegate.isReadOnly();
		}

		public boolean isStable() {
			return delegate.isStable();
		}

		public ByteBuffer read(long addr) {
			return delegate.read(addr);
		}

		public long size() {
			return delegate.size();
		}

		public long toAddr(int nbytes, long offset) {
			return delegate.toAddr(nbytes, offset);
		}

		public String toString(long addr) {
			return delegate.toString(addr);
		}

		public long write(ByteBuffer data, long oldAddr) {
			return delegate.write(data, oldAddr);
		}

		public long write(ByteBuffer data) {
			return delegate.write(data);
		}

	}

}
