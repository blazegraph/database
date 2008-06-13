/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 12, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.btree.AbstractTupleFilterator.Removerator;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link AbstractChunkedRangeIterator} and its concrete
 * {@link ChunkedLocalRangeIterator} implementation.
 * <p>
 * Note: There are other implementations derived from the same abstract base
 * class so they have a dependency on this test suite to get it right for the
 * base class.
 * 
 * @deprecated This class existed to test {@link ChunkedLocalRangeIterator} but
 *             that class is no longer used since we are able to gain REMOVEALL
 *             with less confusion by simply copying a tuple at a time into an
 *             internal Tuple buffer. This is now done by the
 *             {@link Removerator} and that is applied by
 *             {@link AbstractBTree#rangeIterator(byte[], byte[], int, int, ITupleFilter)}.
 *             <p>
 *             Note: The {@link AbstractChunkedRangeIterator} class is still
 *             used for its original purpose - progressing scans through an
 *             index partition by a remote client as part of a scale-out index
 *             scan.
 * 
 * @todo Test {@link IBlock} read through semantics.
 * 
 * @todo Test when version timestamps are supported.
 * 
 * @todo write a test suite for concurrent modification under traversal and
 *       implement support for that feature in the various iterators.
 * 
 * @todo test with to/from keys.
 * 
 * @todo test with filter.
 * 
 * @todo Test delete behind semantics more throughly.
 * 
 * @todo write tests when some entries are deleted (when deletion markers are
 *       and are not supported and when DELETED are and are not requested). make
 *       sure that the concept of delete marker vs version timestamp is
 *       differentiated in the {@link ITuple} interface and the
 *       {@link ITupleIterator} implementations.
 * 
 * @todo check the last visited key when changing chunks and its interaction
 *       with the delete behind semantics.
 * 
 * @todo test delete behind of the last key of the last chunk (non batch, but
 *       everything up to that should be batched on the chunked boundary).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestChunkedIterators extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestChunkedIterators() {
    }

    /**
     * @param name
     */
    public TestChunkedIterators(String name) {
        super(name);
    }

    /**
     * Test correct traversal when all the data fits into a single chunk.
     */
    public void test_oneChunk() {

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int capacity = 10;
        final int nentries = capacity;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }

        ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(btree,
                null/* fromKey */, null/* toKey */, capacity,
                IRangeQuery.DEFAULT, null/*filter*/);
        
        assertEquals("capacity", capacity, itr.capacity);

        assertFalse("exhausted", itr.exhausted);

        assertEquals("nqueries", 0, itr.nqueries);

        assertEquals("nvisited", 0L, itr.nvisited);

        assertNull("rset",itr.rset);

        /*
         * hasNext() should cause the first query to be issued.
         */
        assertTrue(itr.hasNext());
        
        assertFalse("exhausted", itr.exhausted);

        assertEquals("nqueries", 1, itr.nqueries);

        assertEquals("nvisited", 0L, itr.nvisited);
        
        assertNotNull("rset",itr.rset);

        assertEquals("resultSet.ntuples",capacity,itr.rset.getNumTuples());

        assertTrue("resultSet.exhausted", itr.rset.isExhausted());

        /*
         * visit the entries in the result set.
         */
        for(int i=0; i<capacity; i++) {

            ITuple tuple = itr.next();

            assertEquals(keys[i],tuple.getKey());

            assertEquals(vals[i],tuple.getValue());

            // @todo allowed iff the metadata was requested and available.
            
//            assertFalse(tuple.isDeletedVersion());
//
//            assertEquals(0L,tuple.getVersionTimestamp());
            
        }

        /*
         * Verify nothing remaining.
         */
        
        assertFalse(itr.hasNext());

        assertTrue("exhausted", itr.exhausted);

        assertEquals("nqueries", 1, itr.nqueries);

        assertEquals("nvisited", (long) capacity, itr.nvisited);
        
        // not cleared.
        assertNotNull("rset",itr.rset);
        
    }

    /**
     * Test where the iterator has to fetch a second {@link ResultSet}.
     */
    public void test_twoChunks() {

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int capacity = 5;
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }

        ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(btree,
                null/* fromKey */, null/* toKey */, capacity,
                IRangeQuery.DEFAULT, null/*filter*/);
        
        assertEquals("capacity", capacity, itr.capacity);

        assertFalse("exhausted", itr.exhausted);

        assertEquals("nqueries", 0, itr.nqueries);

        assertEquals("nvisited", 0L, itr.nvisited);

        assertNull("rset",itr.rset);

        /*
         * hasNext() should cause the first query to be issued.
         */
        assertTrue(itr.hasNext());
        
        assertFalse("exhausted", itr.exhausted);

        assertEquals("nqueries", 1, itr.nqueries);

        assertEquals("nvisited", 0L, itr.nvisited);
        
        assertNotNull("rset",itr.rset);

        assertEquals("resultSet.ntuples", capacity, itr.rset.getNumTuples());

        assertFalse("resultSet.exhausted", itr.rset.isExhausted());

        /*
         * visit the entries in the 1st result set.
         */
        for(int i=0; i<capacity; i++) {

            ITuple tuple = itr.next();

            assertEquals(keys[i],tuple.getKey());

            assertEquals(vals[i],tuple.getValue());

            // @todo allowed iff the metadata was requested and available.
            
//            assertFalse(tuple.isDeletedVersion());
//
//            assertEquals(0L,tuple.getVersionTimestamp());
            
        }

        /*
         * verify iterator is willing to keep going.
         */
        
        assertFalse("exhausted", itr.exhausted);

        assertEquals("nqueries", 1, itr.nqueries);
        
        assertTrue(itr.hasNext());
        
        assertEquals("nqueries", 2, itr.nqueries);
        
        assertNotNull("rset",itr.rset);

        assertEquals("resultSet.ntuples", capacity, itr.rset.getNumTuples());

        assertTrue("resultSet.exhausted", itr.rset.isExhausted());

        /*
         * visit the entries in the 2nd result set.
         */
        for(int i=0; i<capacity; i++) {

            ITuple tuple = itr.next();

            assertEquals(keys[i+capacity],tuple.getKey());

            assertEquals(vals[i+capacity],tuple.getValue());

            // @todo allowed iff the metadata was requested and available.
            
//            assertFalse(tuple.isDeletedVersion());
//
//            assertEquals(0L,tuple.getVersionTimestamp());
            
        }

        /*
         * Verify nothing remaining.
         */
        
        assertFalse(itr.hasNext());

        assertTrue("exhausted", itr.exhausted);

        assertEquals("nqueries", 2, itr.nqueries);

        assertEquals("nvisited", (long) nentries, itr.nvisited);
        
        // not cleared.
        assertNotNull("rset",itr.rset);
        
    }


    /**
     * Test of {@link IRangeQuery#REMOVEALL} using a limit (capacity := 1). This
     * form of the iterator is used to support queue constructs since the delete
     * is performed on the unisolated index. The state of the index is verified
     * afterwards.
     */
    public void test_removeAll_limit1() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }

        assertEquals(nentries, btree.getEntryCount());

        /*
         * Range delete the keys w/ limit of ONE (1).
         */
        {
            ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
                    btree,//
                    null,// fromKey,
                    null,// toKey
                    1, // capacity (aka limit)
                    IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.REMOVEALL,
                    null// filter
            );

            /*
             * This should delete the first indedx entry but NOT buffer the next
             * entry.
             */
            itr.next();
            
        }

        /*
         * Now verify the state of the index.
         */
        {

            assertEquals(nentries - 1, btree.getEntryCount());
            
            int nremaining = 0;
            
            ITupleIterator itr = btree.rangeIterator();
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();

                byte[] key = tuple.getKey();

                int i = KeyBuilder.decodeInt(key, 0);

                byte[] val = tuple.getValue();

                assertEquals(keys[i], key);

                assertEquals(vals[i], val);

                nremaining++;

            }
            
            assertEquals("#remaining", nentries - 1, nremaining);
            
        }
        
    }

    /**
     * Test of {@link IRangeQuery#REMOVEALL} using a filter. Only the even keys
     * are deleted. The state of the index is verified afterwards.
     */
    public void test_removeAll() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int capacity = 5;
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }

        /*
         * Filter selects only the even keys.
         */
        ITupleFilter filter = new TupleFilter() {

            private static final long serialVersionUID = 1L;

            public boolean isValid(ITuple tuple) {
                
                byte[] key = tuple.getKey();
                
                int i = KeyBuilder.decodeInt(key, 0);
                
                // delete only the even keys.
                if(i % 2 == 0) return true;
                
                return false;
                
            }
            
        };
        
        /*
         * Range delete the keys matching the filter.
         */
        {
            ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
                    btree,
                    null/* fromKey */,
                    null/* toKey */,
                    capacity,
                    IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.REMOVEALL,
                    filter);

            int ndeleted = 0;

            while (itr.hasNext()) {

                ITuple tuple = itr.next();

                byte[] key = tuple.getKey();

                int i = KeyBuilder.decodeInt(key, 0);

                // delete only the even keys.
                assertEquals(0, (i % 2));

                byte[] val = tuple.getValue();

                assertEquals(keys[i], key);

                assertEquals(vals[i], val);

                ndeleted++;

            }

            assertEquals("#deleted", 5, ndeleted);
        }

        /*
         * Now verify the state of the index.
         */
        {

            int nremaining = 0;
            
            ITupleIterator itr = btree.rangeIterator();
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();

                byte[] key = tuple.getKey();

                int i = KeyBuilder.decodeInt(key, 0);

                // deleted only the even keys.
                assertNotSame(0, (i % 2));

                byte[] val = tuple.getValue();

                assertEquals(keys[i], key);

                assertEquals(vals[i], val);

                nremaining++;

            }
            
            assertEquals("#remaining",5,nremaining);
        }
        
    }
    
}
