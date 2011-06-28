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
 * Created on May 7, 2009
 */

package com.bigdata.service.ndx;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.Split;

/**
 * Unit tests for {@link ISplitter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo review the costs of the validation logic used by
 *       {@link AbstractSplitter}.
 */
public class TestSplitter extends TestCase2 {

    /**
     * 
     */
    public TestSplitter() {
    }

    /**
     * @param name
     */
    public TestSplitter(String name) {
        super(name);
    }

    /**
     * API error checking for args.
     */
    public void test_splitter_correctRejection() {

        final ISplitter splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(long ts) {

                throw new UnsupportedOperationException();
                
            }
            
        };
        
        final long timestampOk = 0L;

        // fromIndex is negative.
        try {
        
            splitter.splitKeys(timestampOk, -1/* fromIndex */, 1/* toIndex */,
                    new byte[][] {}/* keys */);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

        // fromIndex GTE length (also toIndex GT length)
        try {
        
            final byte[][] keys = new byte[][] {
            
                    new byte[] {},
                    new byte[] {}
                    
            };
            
            splitter.splitKeys(timestampOk, 2/* fromIndex */, 3/* toIndex */,
                    keys);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

        // fromIndex GTE toIndex
        try {
        
            final byte[][] keys = new byte[][] {
            
                    new byte[] {},
                    new byte[] {}
                    
            };
            
            splitter.splitKeys(timestampOk, 1/* fromIndex */, 1/* toIndex */,
                    keys);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

        // keys is null
        try {
        
            final byte[][] keys = null;

            splitter.splitKeys(timestampOk, 0/* fromIndex */, 1/* toIndex */,
                    keys);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

        // keys has null element.
        try {

            final byte[][] keys = new byte[][] {

                    null

            };

            splitter.splitKeys(timestampOk, 0/* fromIndex */, 1/* toIndex */,
                    keys);

            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        }

    }

    /**
     * Test computing the correct splits with a single key and a single index
     * partition.
     * 
     * @todo unit test when the MDI does not have any entries (this is an error
     *       which is checked by {@link MetadataIndex#find(byte[])}).
     */
    public void test_splitter_correctSplits_01() {

        // use one data service (one UUID).
        final UUID dataServiceUUID = UUID.randomUUID();
        
        final MetadataIndex mdi = MetadataIndex.create(
                new SimpleMemoryRawStore(), UUID.randomUUID(),
                new IndexMetadata("test-ndx", UUID.randomUUID()));

        final PartitionLocator loc1 = new PartitionLocator(mdi
                .incrementAndGetNextPartitionId(), dataServiceUUID,//
                new byte[] {}, // leftSeparatorKey
                null// rightSeparatorKey
        );

        mdi.insert(loc1.getLeftSeparatorKey(), loc1);
        
        final ISplitter splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(long ts) {

                return mdi;
                
            }
            
        };
        
        final long ts = 0L;

        final byte[][] keys = new byte[][] {
          
                new byte[]{1}
                
        };
        
        assertSplits(new Split[] {//
                new Split(loc1, 0, 1)//
                },//
                splitter.splitKeys(ts, 0/* fromIndex */, 1/* toIndex */, keys)
                        .iterator());

    }

    /**
     * Test with more than one key in the same index partition.
     */
    public void test_splitter_correctSplits_02() {

        // use one data service (one UUID).
        final UUID dataServiceUUID = UUID.randomUUID();
        
        final MetadataIndex mdi = MetadataIndex.create(
                new SimpleMemoryRawStore(), UUID.randomUUID(),
                new IndexMetadata("test-ndx", UUID.randomUUID()));

        final PartitionLocator loc1 = new PartitionLocator(mdi
                .incrementAndGetNextPartitionId(), dataServiceUUID,//
                new byte[] {}, // leftSeparatorKey
                null// rightSeparatorKey
        );

        mdi.insert(loc1.getLeftSeparatorKey(), loc1);
        
        final ISplitter splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(long ts) {

                return mdi;
                
            }
            
        };
        
        final long ts = 0L;

        final byte[][] keys = new byte[][] {
          
                new byte[]{1},
                new byte[]{3}
                
        };
        
        assertSplits(new Split[] {//
                new Split(loc1, 0, 1)//
                },//
                splitter.splitKeys(ts, 0/* fromIndex */, 1/* toIndex */, keys)
                        .iterator());

    }

    /**
     * Test with one key into each of two index partitions. The separator key
     * between the index partitions is also used as the key that goes into the
     * 2nd index partition so this tests right on the fence post.
     */
    public void test_splitter_correctSplits_03() {

        // use one data service (one UUID).
        final UUID dataServiceUUID = UUID.randomUUID();
        
        final MetadataIndex mdi = MetadataIndex.create(
                new SimpleMemoryRawStore(), UUID.randomUUID(),
                new IndexMetadata("test-ndx", UUID.randomUUID()));

        final PartitionLocator loc1 = new PartitionLocator(mdi
                .incrementAndGetNextPartitionId(), dataServiceUUID,//
                new byte[] {}, // leftSeparatorKey
                new byte[] {2} // rightSeparator
        );

        final PartitionLocator loc2 = new PartitionLocator(mdi
                .incrementAndGetNextPartitionId(), dataServiceUUID,//
                new byte[] {2}, // leftSeparatorKey
                null// rightSeparatorKey
        );

        mdi.insert(loc1.getLeftSeparatorKey(), loc1);

        mdi.insert(loc2.getLeftSeparatorKey(), loc2);
        
        final ISplitter splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(long ts) {

                return mdi;
                
            }
            
        };
        
        final long ts = 0L;

        final byte[][] keys = new byte[][] {
          
                new byte[]{1},
                new byte[]{2}
                
        };

        // verify with just the 1st key.
        assertSplits(new Split[] {//
                new Split(loc1, 0/*fromIndex*/, 1/*toIndex*/),//
//                new Split(loc2, 1/*fromIndex*/, 2/*toIndex*/)//
                },//
                splitter.splitKeys(ts, 0/* fromIndex */, 1/* toIndex */, keys)
                        .iterator());


        // verify with just the 2nd key.
        assertSplits(new Split[] {//
//                new Split(loc1, 0/*fromIndex*/, 1/*toIndex*/),//
                new Split(loc2, 1/*fromIndex*/, 2/*toIndex*/)//
                },//
                splitter.splitKeys(ts, 1/* fromIndex */, 2/* toIndex */, keys)
                        .iterator());

        // verify with both keys.
        assertSplits(new Split[] {//
                new Split(loc1, 0/*fromIndex*/, 1/*toIndex*/),//
                new Split(loc2, 1/*fromIndex*/, 2/*toIndex*/)//
                },//
                splitter.splitKeys(ts, 0/* fromIndex */, 2/* toIndex */, keys)
                        .iterator());

    }

    /**
     * Correct detection when the keys are not fully ordered.
     * 
     * @todo we need to test two code paths. One where the rightSeparator is
     *       bound and one where it is null (done). This means that we have to
     *       test with more than one index partition.
     */
    public void test_splitter_keysOutOfOrder() {

        // use one data service (one UUID).
        final UUID dataServiceUUID = UUID.randomUUID();
        
        final MetadataIndex mdi = MetadataIndex.create(
                new SimpleMemoryRawStore(), UUID.randomUUID(),
                new IndexMetadata("test-ndx", UUID.randomUUID()));

        final PartitionLocator loc1 = new PartitionLocator(mdi
                .incrementAndGetNextPartitionId(), dataServiceUUID,//
                new byte[] {}, // leftSeparatorKey
                null// rightSeparatorKey
        );

        mdi.insert(loc1.getLeftSeparatorKey(), loc1);
        
        final ISplitter splitter = new AbstractSplitter() {

            @Override
            protected IMetadataIndex getMetadataIndex(long ts) {

                return mdi;
                
            }
            
        };
        
        final long ts = 0L;

        // keys are out of order.
        final byte[][] keys = new byte[][] {
          
                new byte[] { 1 },//
                new byte[] { 3 }, //
                new byte[] { 2 },

        };

        try {
            splitter.splitKeys(ts, 0/* fromIndex */, keys.length/* toIndex */,
                    keys);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Recognizing duplicate keys, which are OK as long as the do not violate
     * the key ordering. This situation arises with asynchronous writes when
     * duplicates are not filtered out.
     * 
     * @todo write test.
     */
    public void test_splitter_duplicateKeys() {

//        fail("write tests");

    }

    /**
     * Verifies the {@link PartitionLocator}, fromIndex, and toIndex on each
     * {@link Split} and the order of the {@link Split}s.
     * 
     * @param expected
     * @param actual
     */
    protected void assertSplits(final Split[] expected,
            final Iterator<Split> actual) {

        for (Split eSplit : expected) {

            assertTrue("Actual did not produce enough splits.", actual
                    .hasNext());

            final Split aSplit = actual.next();

            assertEquals(eSplit.pmd.getPartitionId(), aSplit.pmd
                    .getPartitionId());

            assertEquals(eSplit.pmd.getLeftSeparatorKey(), aSplit.pmd
                    .getLeftSeparatorKey());

            assertEquals(eSplit.pmd.getRightSeparatorKey(), aSplit.pmd
                    .getRightSeparatorKey());
            
            assertEquals(eSplit.fromIndex, aSplit.fromIndex);

            assertEquals(eSplit.toIndex, aSplit.toIndex);

        }
        
        // should be exhausted.
        assertFalse(actual.hasNext());

    }

}
