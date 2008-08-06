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
 * Created on Apr 23, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.NOPTupleSerializer;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.PartitionLocator;

/**
 * Test suite for the {@link IRangeQuery} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRangeQuery extends AbstractEmbeddedFederationTestCase {

    public TestRangeQuery() {

    }

    public TestRangeQuery(String name) {
   
        super(name);
        
    }
    
    /*
     * Range query tests with static partitions.
     */
    
    /**
     * Range count tests with two (2) static partitions where the successor of a
     * key is found in the next partition (tests the fence post for the mapping
     * of the rangeCount operation over the different partitions).
     * 
     * @throws IOException 
     */
    public void test_rangeCount_staticPartitions_01() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        /*
         * Register and statically partition an index.
         */
        fed.registerIndex( metadata, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        /*
         * Request a view of that partitioned index.
         */
        ClientIndexView ndx = (ClientIndexView) fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Range count the view to verify that it is empty.
         */
        assertEquals("rangeCount",0,ndx.rangeCount(null, null));

        /*
         * Get metadata for the index partitions that we will need to verify
         * the splits.
         */
        final PartitionLocator pmd0 = ndx.getMetadataIndex().get(new byte[]{});
        final PartitionLocator pmd1 = ndx.getMetadataIndex().get(new byte[]{5});
        assertNotNull("partition#0",pmd0);
        assertNotNull("partition#1",pmd1);

        /*
         * Insert keys into each partition, but not on the partition
         * separator.
         */
        ndx.insert(new byte[]{3}, new byte[]{3});
        ndx.insert(new byte[]{4}, new byte[]{4});
        ndx.insert(new byte[]{6}, new byte[]{6});

        /*
         * Verify range counts.
         */
        assertEquals("rangeCount",2,ndx.rangeCount(null, new byte[]{5}));
        assertEquals("rangeCount",1,ndx.rangeCount(new byte[]{5},null));
        assertEquals("rangeCount",3,ndx.rangeCount(null, null));
        
        /*
         * Insert another key right on the partition separator.
         */
        ndx.insert(new byte[]{5}, new byte[]{5});

        /*
         * Verify range counts.
         */
        assertEquals("rangeCount",2,ndx.rangeCount(null, new byte[]{5}));
        assertEquals("rangeCount",2,ndx.rangeCount(new byte[]{5},null));
        assertEquals("rangeCount",4,ndx.rangeCount(null, null));
        
    }

    /**
     * Test unbounded range query with an empty index and two partitions.
     * 
     * @throws IOException 
     */
    public void test_rangeQuery_staticPartitions_unbounded_emptyIndex_2partitions() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        final IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Query entire key range.
         */
        {
            
            final PartitionedRangeQueryIterator itr = (PartitionedRangeQueryIterator) ndx
                    .rangeIterator(null, null);

            // nothing visited yet.
            assertEquals("nvisited", 0, itr.getVisitedCount());

            // no partitions queried yet.
            assertEquals("npartitions", 0, itr.getPartitionCount());

            // look for the first matching index entry (there are none).
            assertFalse("hasNext", itr.hasNext());

            // nothing was visisted.
            assertEquals("nvisited", 0, itr.getVisitedCount());

            // we queried two index partitions.
            assertEquals("npartitions", 2, itr.getPartitionCount());
            
        }
        
    }

    /**
     * Test unbounded range query with one entry in the index and two index
     * partitions. The entry is in the first partition.
     * 
     * @throws IOException 
     */
    public void test_rangeQuery_staticPartitions_unbounded_1entry_2partitions_01() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex( metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        final IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Insert an entry into the first partition.
         */
        ndx.insert(new byte[] { 1 }, new byte[] { 1 });

        /*
         * Query the entire key range.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null);

            assertTrue("hasNext", itr.hasNext());

            final ITuple tuple = itr.next();
            assertEquals("getKey()", new byte[] { 1 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 1 }, tuple.getValue());

            assertFalse("hasNext", itr.hasNext());

        }
       
    }
    
    /**
     * Test unbounded range query with one entry in the index and two index
     * partitions. The entry is in the 2nd partition.
     * 
     * @throws IOException 
     */
    public void test_rangeQuery_staticPartitions_unbounded_1entry_2partitions_02() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex( metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Insert an entry into the 2nd partition.
         */
        ndx.insert(new byte[] { 5 }, new byte[] { 5 });

        /*
         * Query the entire key range.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null);

            assertTrue("hasNext", itr.hasNext());

            final ITuple tuple = itr.next();
            assertEquals("getKey()", new byte[] { 5 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 5 }, tuple.getValue());

            assertFalse("hasNext", itr.hasNext());

        }
       
    }
    
    /**
     * Test unbounded range query with two entries in the index and two index
     * partitions. There is one entry in each partition.
     * 
     * @throws IOException
     */
    public void test_rangeQuery_staticPartitions_unbounded_2entries_2partitions_01() throws IOException {
                
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        final IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Insert an entry into the first partition.
         */
        ndx.insert(new byte[] { 1 }, new byte[] { 1 });

        /*
         * Insert an entry into the 2nd partition.
         */
        ndx.insert(new byte[] { 5 }, new byte[] { 5 });

        /*
         * Query the entire key range.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null);

            assertTrue("hasNext", itr.hasNext());

            ITuple tuple = itr.next();
            assertEquals("getKey()",new byte[]{1},tuple.getKey());
            assertEquals("getValue()",new byte[]{1},tuple.getValue());

            assertTrue("hasNext", itr.hasNext());

            tuple = itr.next();
            assertEquals("getKey()",new byte[]{5},tuple.getKey());
            assertEquals("getValue()",new byte[]{5},tuple.getValue());

            assertFalse("hasNext", itr.hasNext());

        }
       
    }
    
    /**
     * Test unbounded range query with two entries in the index and two index
     * partitions. Both entries are in the 1st index partition and we limit the
     * data service query to one result per query.
     * 
     */
    public void test_rangeQuery_staticPartitions_unbounded_2entries_2partitions_02() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);

        /*
         * Insert the entries into the first partition.
         */
        ndx.insert(new byte[] { 1 }, new byte[] { 1 });
        ndx.insert(new byte[] { 2 }, new byte[] { 2 });

        /*
         * Query the entire key range.
         */
        {

            // Limit to one entry per data service query.
            final int capacity = 1;

            final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;

            final ITupleIterator itr = ndx.rangeIterator(null, null, capacity,
                    flags, null/* filter */);

            assertTrue("hasNext", itr.hasNext());

            ITuple tuple = itr.next();
            assertEquals("getKey()", new byte[] { 1 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 1 }, tuple.getValue());

            assertTrue("hasNext", itr.hasNext());

            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 2 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 2 }, tuple.getValue());

            assertFalse("hasNext", itr.hasNext());

        }
       
    }
    
    /**
     * Test of {@link IRangeQuery#REMOVEALL} using a limit (capacity := 1). This
     * form of the iterator is used to support queue constructs since the delete
     * is performed on the unisolated index. The state of the index is verified
     * afterwards.
     * 
     * @throws IOException 
     */
    public void test_removeAll_limit1_twoPartitions() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                KeyBuilder.asSortKey(5) // the half-way point.
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        final IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);
        
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);
            
        }
        
        ndx.submit(0/*fromIndex*/,nentries/*toIndex*/, keys, vals,
                BatchInsertConstructor.RETURN_NO_VALUES, null/*handler*/);

        assertEquals(nentries, ndx.rangeCount(null,null));

        /*
         * Range delete the keys w/ limit of ONE (1).
         */
        {
            ITupleIterator itr = ndx.rangeIterator(
                    null,// fromKey,
                    null,// toKey
                    1, // capacity (aka limit)
//                    IRangeQuery.KEYS | IRangeQuery.VALS | 
                    IRangeQuery.REMOVEALL,
                    null// filter
            );

            /*
             * This should delete the first index entry but NOT buffer the next
             * entry.
             */
            itr.next();
            
        }

        /*
         * Now verify the state of the index.
         */
        {

            // Note: range count is unchanged since delete markers are in use.
            assertEquals(nentries, ndx.rangeCount(null,null));
            
            int ndeleted = 0;
            
            int nremaining = 0;

            // iterator visits deleted entries too.
            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    0/* capacity */, IRangeQuery.ALL, null/* filter */);

            int index = 0;
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();

                final byte[] key = tuple.getKey();

                final int i = KeyBuilder.decodeInt(key, 0);

                assertEquals(index, i);

                assertEquals(keys[i], key);

                if (index == 0) {

                    assertTrue(tuple.isDeletedVersion());
                    
                }
                
                if(tuple.isDeletedVersion()) {

                    ndeleted++;
                    
                } else {

                    final byte[] val = tuple.getValue();

                    assertEquals(vals[i], val);

                    nremaining++;
                    
                }
                
                index++;

            }
            
            assertEquals("#remaining", nentries - 1, nremaining);
            
        }
        
    }

    /**
     * Test of {@link IRangeQuery#REMOVEALL} using a filter. Only the even keys
     * are deleted. The state of the index is verified afterwards.
     * 
     * @throws IOException 
     */
    public void test_removeAll() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
       
        metadata.setTupleSerializer(NOPTupleSerializer.INSTANCE);

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                KeyBuilder.asSortKey(5) // the half-way point.
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        final int capacity = 5;
        final int nentries = 10;
        
        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];
        
        for(int i=0; i<nentries; i++) {
            
            keys[i] = KeyBuilder.asSortKey(i);
            
            vals[i] = new byte[4];
            
            r.nextBytes(vals[i]);
            
        }

        ndx.submit(0/* fromIndex */, nentries/* toIndex */, keys, vals,
                BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);

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
            ITupleIterator itr = ndx.rangeIterator(
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
            
            ITupleIterator itr = ndx.rangeIterator(null,null);
            
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

    /**
     * Test the ability to scan a partitioned index in forward and reverse
     * order. The test verifies that index partitions are visited in the correct
     * order, that the chunks within each index partition are visited in the
     * correct order, and that the tuples within each chunk are visited in the
     * correct order. One of the index partitions is deliberately left empty in
     * order to verify that the iterator will correctly cross over an index
     * partition in which the chunked iterator does not visit anything.
     * 
     * @throws IOException
     */
    public void test_reverseScan() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        /*
         * Note: Run with the {7} partition defined but empty to verify that the
         * iterator is robust to empty partitions!
         */
        
        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{4},
                new byte[]{7},
                new byte[]{10},
        }, null/* dataServiceUUIDs */
        );
        
        final IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Insert entries into the first partition.
         */
        ndx.insert(new byte[] { 1 }, new byte[] { 1 });
        ndx.insert(new byte[] { 2 }, new byte[] { 2 });
        ndx.insert(new byte[] { 3 }, new byte[] { 3 });
        
        /*
         * Insert entries into the 2nd partition.
         */
        ndx.insert(new byte[] { 4 }, new byte[] { 4 });
        ndx.insert(new byte[] { 5 }, new byte[] { 5 });
        ndx.insert(new byte[] { 6 }, new byte[] { 6 });

        /*
         * The 3rd partition is left empty to check for fence posts.
         */
        
        /*
         * Insert entries into the 4th partition.
         */
        ndx.insert(new byte[] { 10 }, new byte[] { 10 });
        ndx.insert(new byte[] { 11 }, new byte[] { 11 });
        ndx.insert(new byte[] { 12 }, new byte[] { 12 });

        /*
         * Query the entire key range (forward scan).
         * 
         * Note: This tests with a capacity of (2) in order to force the
         * iterator to read from each partition in chunks of no more than (2)
         * tuples at a time. This helps verify that the base iterator is in
         * forward order, that the chunked iterator is moving forwards through
         * the index partition, and that the total iterator is moving forwards
         * through the index partitions.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    1/* capacity */, IRangeQuery.DEFAULT, null/* filter */);

            ITuple tuple;

            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 1 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 1 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 2 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 2 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 3 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 3 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 4 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 4 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 5 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 5 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 6 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 6 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 10 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 10 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 11 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 11 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 12 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 12 }, tuple.getValue());

            assertFalse("hasNext", itr.hasNext());

        }

        /*
         * Query the entire key range (reverse scan).
         * 
         * Note: This tests with a capacity of (2) in order to force the
         * iterator to read from each partition in chunks of no more than (2)
         * tuples at a time. This helps verify that the base iterator is in
         * reverse order, that the chunked iterator is moving backwards through
         * the index partition, and that the total iterator is moving backwards
         * through the index partitions.
         */
        {

            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    1/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.REVERSE, null/* filter */);

            ITuple tuple;

            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 12 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 12 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 11 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 11 }, tuple.getValue());
            
            assertTrue("hasNext", itr.hasNext());
            tuple = itr.next();
            assertEquals("getKey()", new byte[] { 10 }, tuple.getKey());
            assertEquals("getValue()", new byte[] { 10 }, tuple.getValue());
            
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
            
            assertFalse("hasNext", itr.hasNext());

        }

    }

}
