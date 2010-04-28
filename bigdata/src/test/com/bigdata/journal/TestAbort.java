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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.LRUNexus;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.util.ChecksumUtility;

/**
 * Test the ability to abort (discard an uncommitted write set). This is a test
 * of the unisolated {@link IAtomicStore#abort()} semantics, NOT a test of
 * transactional abort semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAbort extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestAbort() {
    }

    /**
     * @param name
     */
    public TestAbort(String name) {
     
        super(name);
        
    }

    public void test_rollback() {

        final Journal journal = getStore();//new Journal(getProperties());

        try {

            final ChecksumUtility checker = new ChecksumUtility();
            
            final ByteBuffer rootBlock0 = journal.getBufferStrategy()
                    .readRootBlock(true/* rootBlock0 */);
            
            final IRootBlockView rootBlockView0 = new RootBlockView(
                    true/* rootBlock0 */, rootBlock0, checker);

            if (log.isInfoEnabled())
                log.info("rootBlock0=" + rootBlockView0);

            final ByteBuffer rootBlock1 = journal.getBufferStrategy()
                    .readRootBlock(false/* rootBlock0 */);

            final IRootBlockView rootBlockView1 = new RootBlockView(
                    false/* rootBlock0 */, rootBlock1, checker);

            if (log.isInfoEnabled())
                log.info("rootBlock1=" + rootBlockView1);

            // true iff rootBlock0 is the current root block.
            final boolean isRootBlock0 = journal.getRootBlockView()
                    .isRootBlock0();

            if (log.isInfoEnabled())
                log.info("Using rootBlock=" + (isRootBlock0 ? 0 : 1));

            // write a record, inserting it into the cache.
            
            final ByteBuffer a = getRandomData();
            
            final long addr_a = journal.write(a);
            
            final ILRUCache<Long, Object> cache = LRUNexus.getCache(journal);

            if (cache != null) {

                assertNull(cache.putIfAbsent(addr_a, a));

                assertTrue(a == cache.get(addr_a));

            }

            // abort the journal (discard the write set, reload the current root
            // block)
            journal.abort();

            if(cache != null) {

                // the record is gone from the cache.
                assertNull(cache.get(addr_a));
                
            }

            if (log.isInfoEnabled())
                log.info("After commit   =" + journal.getRootBlockView());

            // verify the root blocks have not been changed.
            assertEquals(rootBlock0, journal.getBufferStrategy().readRootBlock(
                    true/* rootBlock0 */));
            
            assertEquals(rootBlock1, journal.getBufferStrategy().readRootBlock(
                    false/* rootBlock0 */));
            
        } finally {
            
            journal.destroy();
            
        }
        
    }
        
    /**
     * Test of abort semantics when registering named {@link BTree}s (this tests
     * the integration of {@link Name2Addr} with abort).
     */
    public void test_abort() {

        Journal store = getStore();

        try {

            final String name = "test";

            final int nrecs = 500;
            
            /*
             * The initial state of the root blocks (they will be the same
             * initially).
             */
            final IRootBlockView rootBlock0 = store.getRootBlockView();
            
            // The first offset which can be assigned to a user record.
            final long firstOffset = rootBlock0.getNextOffset();

            // Abort.  Should discard the initial commit record index, etc.
            store.abort();
            
            // make sure the offset has not been changed.
            assertEquals(firstOffset, store.getRootBlockView()
                    .getNextOffset());

            // write some data onto the store.
            for (int i = 0; i < nrecs; i++) {

                store.write(getRandomData());
                
            }
            
            // Abort again.
            store.abort();

            // make sure the offset has not been changed.
            assertEquals(firstOffset, store.getRootBlockView()
                    .getNextOffset());

            // register a named index.
            {

                final IndexMetadata md = new IndexMetadata(name, UUID
                        .randomUUID());
                
                store.registerIndex(name, md);
                
            }
            
            // Abort again.  
            store.abort();

            // make sure the offset has not been changed.
            assertEquals(firstOffset, store.getRootBlockView()
                    .getNextOffset());

            /*
             * Write a record larger than the write cache capacity.
             */
            final byte[] a;
            {

                a = new byte[DirectBufferPool.INSTANCE
                        .getBufferCapacity() + 1];
                
                r.nextBytes(a);
                
                final long addr = store.write(ByteBuffer.wrap(a));

                /*
                 * Verify read back
                 * 
                 * Note: This assumes the buffer is backed by an array; change
                 * the test if this assumption is violated to copy the data out
                 * of the buffer before comparing.
                 */
                final ByteBuffer b = store.read(addr);
                
                assertEquals(a, b/*.array()*/);
                
            }

            /*
             * After writing that big record, now register a named index.
             * 
             * Note: This ordering (big record, then index registered) is based
             * on a bug pattern. The problem arises (for the WORM) when the
             * large record is written out to disk immediately leaving some
             * smaller records in a partially filled WriteCache. The WriteCache
             * is not flushed until we add some more records and do the commit.
             * At that point we have now partly overwritten our larger record
             * with the WriteCache's data. Basically, the WORM requires that all
             * writes onto the backing file are serialized. When it sees a
             * record which is too large for the write cache it needs to evict
             * the current write cache, then write the large record, and then
             * continue with an empty cache.
             */
            {
            
                final IndexMetadata md = new IndexMetadata(name, UUID
                        .randomUUID());

                final BTree ndx = store.registerIndex(name, md);

                ndx.insert(new byte[] { 1, 3 }, new byte[] { 2, 4 });

            }

            // commit.
            store.commit();

            if (store.isStable()) {

                // reopen.
                store = reopenStore(store);

            }
            
            // look up the index again.
            final BTree ndx = store.getIndex(name);

            // the index should exist.
            assertNotNull(ndx);

            // verify the write.
            assertEquals(new byte[] { 2, 4 }, ndx
                    .lookup(new byte[] { 1, 3 }));

        } finally {

            store.destroy();

        }

    }

}
