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
 * Created on Aug 5, 2008
 */

package com.bigdata.btree.filter;

import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.NOPTupleSerializer;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * @todo Add tests at that layer for cursor, reverse, remove, and other flag
 *       based iterator semantics. Potentially aggregate the stacked iterators
 *       into a package and their own test suite.
 *       <p>
 *       It would be best if that test suite could be reused against the
 *       federation as well, but testing with the federation generally requires
 *       additional effort in order to look for fence posts around chunk and
 *       partition boundaries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTupleFilters extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestTupleFilters() {

    }

    /**
     * @param name
     */
    public TestTupleFilters(String name) {

        super(name);
        
    }

    /**
     * Test of {@link IRangeQuery#REMOVEALL}. The state of the index is
     * verified afterwards.
     */
    public void test_removeAll() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setTupleSerializer(NOPTupleSerializer.INSTANCE);
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = TestKeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }
        
        /*
         * Visit all tuples, removing each one as we go.
         */
        {
            
            int n = 0;
            
            final ITupleIterator itr = btree.rangeIterator(null/* fromKey */,
                    null/* toKey */, 0/* capacity */, IRangeQuery.DEFAULT
                            | IRangeQuery.REMOVEALL, null/* filter */);
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();
                
                log.info(tuple.toString());
                
                n++;
                
            }
            
            assertEquals(nentries, n);
            
        }
        
        /*
         * Verify everything was removed.
         */
        {
            
            int n = 0;
            
            final ITupleIterator itr = btree
                    .rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.DEFAULT, null/*filter*/);
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();
                
                log.error(tuple.toString());
                
                n++;
                
            }
            
            assertEquals("Not expecting any entries",0,n);
            
        }
    
    }
    
    /**
     * Test of {@link IRangeQuery#REMOVEALL} using a {@link TupleFilter}. Only
     * the even keys are deleted. The state of the index is verified afterwards.
     */
    public void test_removeAll_with_TupleFilter() {
        
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setTupleSerializer(NOPTupleSerializer.INSTANCE);
        
        BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        final int capacity = 5;
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = TestKeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);

            assertNull(btree.insert(keys[i], vals[i]));
            
        }

        /*
         * Filter selects only the even keys.
         */
        final TupleFilter filter = new TupleFilter() {

                    private static final long serialVersionUID = 1L;

                    protected boolean isValid(ITuple tuple) {

                        final byte[] key = tuple.getKey();

                        final int i = KeyBuilder.decodeInt(key, 0);

                        // delete only the even keys.
                        if (i % 2 == 0)
                            return true;

                        return false;

                    }

                };
        
        /*
         * Range delete the keys matching the filter.
         */
        {

            final ITupleIterator itr = btree.rangeIterator(null/* fromKey */,
                    null/* toKey */, capacity, IRangeQuery.KEYS
                            | IRangeQuery.VALS | IRangeQuery.REMOVEALL, filter);
            
            int ndeleted = 0;

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                if (log.isInfoEnabled())
                    log.info("visiting: "+tuple);
                
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
            
            assertEquals("#remaining", 5, nremaining);
            
        }
        
    }

//    /**
//     * Test explores the behavior of a filtering {@link Striterator} when
//     * removing elements during traversal. The {@link Filter} uses a one step
//     * lookahead, and this test is designed to see whether the {@link Filter}
//     * causes the wrong element to be removed when {@link Iterator#remove()} is
//     * invoked on the top level of the iterator stack.
//     * <p>
//     * Note: This test fails because remove() is broken for {@link Filter}.
//     */
//    public void test_striterator_remove() {
//        
//        List<Integer> l = new LinkedList<Integer>();
//        
//        l.add(0);
//        l.add(1);
//        l.add(2);
//        l.add(3);
//        
//        IStriterator itr = new Striterator(l.iterator())
//                .addFilter(new Filter() {
//                    private static final long serialVersionUID = 1L;
//                    /**
//                     * Accept only the even values.
//                     */
//                    @Override
//                    protected boolean isValid(Object obj) {
//                        final int i = ((Integer) obj).intValue();
//                        boolean ok = i == 0 || i == 2;
//                        log.info("obj="+i+", ok="+ok);
//                        return ok;
//                    }
//                }); 
//        
//        int expected = 0;
//        
//        while(itr.hasNext()) {
//            
//            int actual = ((Integer)itr.next()).intValue();
//            
//            log.info("expected="+expected+", actual="+actual);
//            
//            assertEquals(expected,actual);
//
//            assertTrue(l.contains(actual));
//            
//            itr.remove();
//            
//            assertFalse("list=" + l + ", should have removed " + actual, l
//                    .contains(actual));
//            
//            expected += 2;
//            
//        }
//        
//    }
    
}
