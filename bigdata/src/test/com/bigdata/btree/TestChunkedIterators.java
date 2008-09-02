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

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link AbstractChunkedTupleIterator} and its concrete
 * {@link ChunkedLocalRangeIterator} implementation.
 * <p>
 * Note: There are other implementations derived from the same abstract base
 * class so they have a dependency on this test suite to get it right for the
 * base class.
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
        
        metadata.setTupleSerializer(NOPTupleSerializer.INSTANCE);
        
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
        final IFilterConstructor filter = new FilterConstructor()
                .addFilter(new TupleFilter() {

                    private static final long serialVersionUID = 1L;

                    protected boolean isValid(ITuple tuple) {

                        final byte[] key = tuple.getKey();

                        final int i = KeyBuilder.decodeInt(key, 0);

                        // delete only the even keys.
                        if (i % 2 == 0)
                            return true;

                        return false;

                    }

                });
              
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

                final ITuple tuple = itr.next();

                final byte[] key = tuple.getKey();

                final int i = KeyBuilder.decodeInt(key, 0);

                // delete only the even keys.
                assertEquals(0, (i % 2));

                final byte[] val = tuple.getValue();

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
            
            final ITupleIterator itr = btree.rangeIterator();
            
            int n = 0;
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();

                final byte[] key = tuple.getKey();

                final int i = KeyBuilder.decodeInt(key, 0);

                // verify deleted only the even keys.
                if (0 == (i % 2)) {
                    /*
                     * Found a key that decodes as an even integer.
                     */
                    fail("n=" + n + ", tuple=" + tuple + ", i=" + i);
                }

                final byte[] val = tuple.getValue();

                assertEquals(keys[i], key);

                assertEquals(vals[i], val);

                nremaining++;

            }
            
            assertEquals("#remaining",5,nremaining);
        }
        
    }
    
    /**
     * Test progression of a chunked iterator scan in reverse order. The test
     * verifies that the tuples within the index partitions are also visited in
     * reverse order when more than one chunk must be fetched.
     */
    public void test_reverseScan() {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        final BTree ndx = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        ndx.insert(new byte[] { 1 }, new byte[] { 1 });
        ndx.insert(new byte[] { 2 }, new byte[] { 2 });
        ndx.insert(new byte[] { 3 }, new byte[] { 3 });
        ndx.insert(new byte[] { 4 }, new byte[] { 4 });

        ndx.insert(new byte[] { 5 }, new byte[] { 5 });
        ndx.insert(new byte[] { 6 }, new byte[] { 6 });
        ndx.insert(new byte[] { 7 }, new byte[] { 7 });
        ndx.insert(new byte[] { 8 }, new byte[] { 8 });

        /*
         * Query the entire key range (reverse scan).
         * 
         * Note: This tests with a capacity of (2) in order to force the
         * iterator to read in chunks of two tuples at a time. This helps verify
         * that the base iterator is in reverse order, that the chunked iterator
         * is moving backwards through the index partition, and that the total
         * iterator is moving backwards through the index partitions.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    1/* capacity */, IRangeQuery.DEFAULT
                            | IRangeQuery.REVERSE, null/* filter */);

            ITuple tuple;

            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 8 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 8 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 7 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 7 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 6 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 6 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 5 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 5 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 4 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 4 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 3 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 3 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 2 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 2 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 1 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 1 }, tuple.getValue());
            
        }

    }

    /**
     * Unit test for (de-)serialization of {@link ResultSet}s used by the
     * chunked iterators.
     */
    public void test_deserialization() {

        doDeserializationTest(1000/* N */, true/* deleteMarkers */);

        doDeserializationTest(1000/* N */, false/* deleteMarkers */);
        
    }
    
    protected void doDeserializationTest(int N, boolean deleteMarkers) {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());
        
        // optionally enable delete markers.
        metadata.setDeleteMarkers(deleteMarkers);

        // the default serializer will work fine for this.
        final ITupleSerializer<Long, String> tupleSer = new DefaultTupleSerializer<Long, String>(
                new DefaultKeyBuilderFactory(new Properties()));
        
        metadata.setTupleSerializer(tupleSer);
        
        final BTree ndx = BTree.create(new SimpleMemoryRawStore(), metadata);

        {
            /*
             * Test with an empty index.
             */
            doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                    0/* capacity */, IRangeQuery.DEFAULT/* flags */, null/* filter */);

        }
        
        final TupleData<Long, String>[] data = new TupleData[N];
        
        for (int i = 0; i < N; i++) {
            
            /*
             * note: avoids possibility of duplicate keys and generates the data
             * in a known order
             */ 
            data[i] = new TupleData<Long, String>(i * 2L, getRandomString(
                    100/* len */, i/* id */), tupleSer); 
        
            // add to the tree as we go.
            ndx.insert(data[i].k, data[i].v);
            
        }
        
        /*
         * Verify a full index scan.
         */
        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                0/* capacity */, IRangeQuery.DEFAULT/* flags */, null/* filter */);

        /*
         * Verify key-range scans.
         */

        doDeserializationTest(ndx, tupleSer.serializeKey(2L)/* fromKey */,
                null/* toKey */, 0/* capacity */,
                IRangeQuery.DEFAULT/* flags */, null/* filter */);
        
        doDeserializationTest(ndx, null/* fromKey */, tupleSer
                .serializeKey(20L)/* toKey */, 0/* capacity */,
                IRangeQuery.DEFAULT/* flags */, null/* filter */);
        
        doDeserializationTest(ndx, tupleSer.serializeKey(2L)/* fromKey */,
                tupleSer.serializeKey(10L)/* toKey */, 0/* capacity */,
                IRangeQuery.DEFAULT/* flags */, null/* filter */);
        
        /*
         * Verify with overriden capacity.
         */

        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                100/* capacity */, IRangeQuery.DEFAULT/* flags */, null/* filter */);
        
        // and key-range constraint.
        doDeserializationTest(ndx, tupleSer.serializeKey(2L)/* fromKey */,
                tupleSer.serializeKey(10L)/* toKey */, 1/* capacity */,
                IRangeQuery.DEFAULT/* flags */, null/* filter */);

        /*
         * Verify with different flags.
         */

        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                0/* capacity */, IRangeQuery.KEYS/* flags */, null/* filter */);
        
        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                0/* capacity */, IRangeQuery.VALS/* flags */, null/* filter */);

        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                0/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.REVERSE/* flags */, null/* filter */);

        doDeserializationTest(ndx, null/* fromKey */, null/* toKey */,
                0/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.CURSOR/* flags */, null/* filter */);

        /*
         * Force all tuples to be removed.
         */
        final int n1;
        {

            final ITupleIterator itr = ndx
                    .rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.DEFAULT
                                    | IRangeQuery.REMOVEALL/* flags */, null/* filter */);

            int i = 0;
            while (itr.hasNext()) {

                itr.next();

                i++;

            }

            n1 = i;

        }

        // visited all elements
        assertNotSame(N, n1);

        /*
         * Visit again - both iterators should be empty now.
         */
        final int n0 = doDeserializationTest(ndx, null/* fromKey */,
                null/* toKey */, 0/* capacity */,
                IRangeQuery.DEFAULT/* flags */, null/* filter */);

        // should be an empty iterator.
        assertEquals(0, n0);

        /*
         * Visit again, but specify the flag for visiting the deleted tuples.
         */
        final int n2 = doDeserializationTest(ndx, null/* fromKey */,
                null/* toKey */, N/* capacity */, IRangeQuery.DEFAULT
                        | IRangeQuery.DELETED/* flags */, null/* filter */);

        if (ndx.getIndexMetadata().getDeleteMarkers()) {
            /*
             * If delete markers are enabled, then should visit the same #of
             * tuples as the iterator before we deleted those tuples.
             */
            assertEquals(n1, n2);
        } else {
            /*
             * Otherwise should visit NO tuples.
             */
            assertEquals(0, n2);
        }
        
    }

    /**
     * Test helper requests an {@link ITupleIterator} using the specified
     * parameters directly on the {@link BTree} (ground truth for the purposes
     * of this test) and indirectly via an {@link AbstractChunkedTupleIterator}.
     * The {@link AbstractChunkedTupleIterator} is layered in order to force
     * (de-)serialization of each {@link ResultSet}. The iterators are then
     * compared and should visit the same {@link ITuple}s in the same order.
     * 
     * @param ndx
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filterCtor
     * 
     * @return The #of tuples visited.
     * 
     * @throws AssertionError
     *             if the tuples visited differ in any manner.
     */
    protected int doDeserializationTest(final BTree ndx, final byte[] fromKey,
            final byte[] toKey, final int capacity, final int flags,
            final IFilterConstructor filterCtor) {

        // the ground truth iterator.
        final ITupleIterator<String> itre = ndx.rangeIterator(fromKey, toKey, capacity,
                flags, filterCtor);

        final ITupleIterator<String> itre2 = ndx.rangeIterator(fromKey, toKey, capacity,
                flags, filterCtor);

//        if(true) return assertSameIterator(itre, itre2);
        
        final ITupleIterator<String> itra = new ChunkedLocalRangeIterator<String>(ndx,
                fromKey, toKey, capacity, flags, filterCtor) {
            
            protected ResultSet getResultSet(long timestamp, byte[] fromKey,
                    byte[] toKey, int capacity, int flags, IFilterConstructor filter) {

                final ResultSet rset = super.getResultSet(timestamp, fromKey, toKey,
                        capacity, flags, filter);
                
//                if(true) return rset;
                
                final byte[] data = SerializerUtil.serialize(rset);
                
                final ResultSet rset2 = (ResultSet)SerializerUtil.deserialize(data); 

                final byte[] data2 = SerializerUtil.serialize(rset2);

                if (!BytesUtil.bytesEqual(data, data2)) {

                    throw new AssertionError("Re-serialization differs");
                    
                }
                
                // return the de-serialized version.
                return rset2;
                
            }
            
        };

        return assertSameIterator(itre, itra);
        
    }

    /**
     * Compares the data in the {@link ITuple}s visited by two iterators.
     * 
     * @param itre
     *            The 'expected' iterator.
     * @param itra
     *            The 'actual' iterator.
     * 
     * @return The #of tuples visited.
     * 
     * @throws AssertionError
     *             if the tuples visited differ in any manner.
     * 
     * @todo refactor into base class for B+Tree unit tests.
     */
    protected int assertSameIterator(final ITupleIterator itre,
            final ITupleIterator itra) {
        
        int i = 0;

        while (itre.hasNext()) {

            assertTrue(itra.hasNext());

            final ITuple te = itre.next();

            final ITuple ta = itra.next();

            assertEquals("flags", te.flags(), ta.flags());

            if ((te.flags() & IRangeQuery.KEYS) != 0) {

                assertEquals("key[]", te.getKey(), ta.getKey());

            }

            if ((te.flags() & IRangeQuery.VALS) != 0) {

                assertEquals("value[]", te.getValue(), ta.getValue());

            }

            assertEquals("visitCount", te.getVisitCount(), ta.getVisitCount());
            
            assertEquals("versionTimestamp", te.getVersionTimestamp(), ta
                    .getVersionTimestamp());

            if (te.isDeletedVersion() != ta.isDeletedVersion()) {
                
                log.error("expected[" + i + "]=" + te);
                
                log.error("  actual[" + i + "]=" + ta);
                
                fail("deleteMarker: expecting=" + te.isDeletedVersion()
                        + ", actual=" + ta.isDeletedVersion());
                
            }
            
//            assertEquals("deleteMarker", te.isDeletedVersion(), ta
//                    .isDeletedVersion());

            /*
             * note: the source index will always be zero unless you are reading
             * from a fused view.
             */
            assertEquals("sourceIndex", te.getSourceIndex(), ta
                    .getSourceIndex());

            {

                /*
                 * Compare objects de-serialized from the tuple. If the
                 * operation is not supported for the expected iterator, then it
                 * should also be not supported for the actual iterator.
                 */
                boolean unsupported = false;
                Object oe = null;
                try {
                    oe = te.getObject();
                } catch (UnsupportedOperationException ex) {
                    oe = null;
                    unsupported = true;
                }

                Object oa = null;
                try {
                    oa = ta.getObject();
                    if (unsupported)
                        fail("Should not be able to de-serialize an object here");
                } catch (UnsupportedOperationException ex) {
                    if (log.isInfoEnabled()) {
                        log.info("Ignoring expected exception: " + ex);
                    }
                }

                if (oe == null) {

                    assertNull(oa);

                } else {

                    assertTrue(oe.equals(oa));

                }

            }

            i++;

        }

        assertFalse(itra.hasNext());

        return i;

    }
    
    /**
     * Helper class pairs an application key and value with the generated sort
     * key for that application key.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     *            The generic type of the application key.
     * @param <V>
     *            The generic type of the application value.
     */
    static class TupleData<K, V> implements Comparable<TupleData<K,V>> {
        
        /** The application key. */
        final public K k;
        
        /** The application value. */
        final public V v;
        
        /** The generated sort key (unsigned byte[]). */
        final public byte[] sortKey;

        /**
         * 
         * @param k
         *            The application key.
         * @param v
         *            The application value.
         * @param tupleSer
         *            Used to generate the sort key and (de-)serialize the
         *            application value.
         */
        public TupleData(final K k, final V v, final ITupleSerializer<K, V> tupleSer) {

            this.k = k;

            this.v = v;

            this.sortKey = tupleSer.serializeKey(k);

        }

        /**
         * Places into order by the {@link #sortKey}. This is the same order
         * that the data will be in when they are inserted into the B+Tree,
         * except that the B+Tree does not permit duplicate keys.
         */
        public int compareTo(TupleData<K, V> o) {

            return BytesUtil.compareBytes(sortKey, o.sortKey);
            
        }

    }

}
