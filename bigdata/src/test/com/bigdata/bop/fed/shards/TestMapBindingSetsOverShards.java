/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.fed.shards;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.AbstractEmbeddedFederationTestCase;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Unit tests for {@link MapBindingSetsOverShardsBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestMapBindingSetsOverShards.java 3448 2010-08-18 20:55:58Z
 *          thompsonbry $
 */
public class TestMapBindingSetsOverShards extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestMapBindingSetsOverShards() {
        super();
    }

    /**
     * @param name
     */
    public TestMapBindingSetsOverShards(final String name) {
        super(name);
    }

    // Namespace for the relation.
    static private final String namespace = "ns";
    
    // The separator key between the index partitions.
    private byte[] separatorKey;

    public void setUp() throws Exception {
        
        super.setUp();
        
        loadData();
        
    }
    
    public void tearDown() throws Exception {
        
        // clear reference.
        separatorKey = null;
        
        super.tearDown();
        
    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     * 
     * @throws IOException 
     */
    private void loadData() throws IOException {

        /*
         * The data to insert (in sorted order this time).
         */
        final E[] a = {//
                // partition0
                new E("John", "Mary"),// 
                new E("Leon", "Paul"),// 
                // partition1
                new E("Mary", "John"),// 
                new E("Mary", "Paul"),// 
                new E("Paul", "Leon"),// 
        };

        // The separator key between the two index partitions.
        separatorKey = KeyBuilder.newUnicodeInstance().append("Mary").getKey();

        final byte[][] separatorKeys = new byte[][] {//
                new byte[] {}, //
                separatorKey //
        };

        final UUID[] dataServices = new UUID[] {//
                dataService0.getServiceUUID(),//
                dataService1.getServiceUUID() //
        };

        /*
         * Create the relation with the primary index key-range partitioned
         * using the given separator keys and data services.
         */
        
        final R rel = new R(fed, namespace, ITx.UNISOLATED, new Properties());

        rel.create(separatorKeys, dataServices);

        /*
         * Insert data into the appropriate index partitions.
         */
        rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

    }

    /**
     * Verify the expected {@link PartitionLocator}s.
     * 
     * @throws IOException
     */
    public void test_locatorScan() throws IOException {

        // The name of the primary index for the relation (hardcoded).
        final String primaryIndexName = namespace + ".primary";

        // Setup locator scan for that index.
        final Iterator<PartitionLocator> itr = ((AbstractScaleOutFederation<?>) fed)
                .locatorScan(primaryIndexName, ITx.READ_COMMITTED,
                        null/* fromKey */, null/* toKey */, false/* reverse */);

        // The expected locators.
        final PartitionLocator[] expected = new PartitionLocator[] {
                
                new PartitionLocator(0/* partitionId */, dataService0
                        .getServiceUUID(), new byte[]{}/* leftSeparatorKey */,
                        separatorKey/* rightSeparatorKey */),
                
                new PartitionLocator(1/* partitionId */, dataService1
                        .getServiceUUID(), separatorKey/* leftSeparatorKey */,
                        null/* rightSeparatorKey */), 
                        
        };

        // Verify the test setup.
        assertTrue(itr.hasNext());
        {

            final PartitionLocator locator = itr.next();
            
            assertEquals(expected[0], locator);
            
        }
        
        assertTrue(itr.hasNext());
        {

            final PartitionLocator locator = itr.next();
            
            assertEquals(expected[1], locator);
            
        }
        
        assertFalse(itr.hasNext());

    }

    /**
     * Verify the data are in the expected shards.
     * 
     * @throws IOException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_data() throws InterruptedException, ExecutionException, IOException {

        // scale-out view of the relation.
        final R rel = (R) fed.getResourceLocator().locate(namespace,
                ITx.UNISOLATED);

        // the fully qualified name of that scale-out index.
        final String name = rel.getFQN(rel.getPrimaryKeyOrder());

        // scale-out view of the primary index for the relation.
        final IIndex ndx = rel.getIndex(rel.getPrimaryKeyOrder());

        {

            /*
             * @todo Due to a conflict between DefaultTupleSerializer and
             * R.KeyOrder we can not pass the element objects directly into the
             * s/o index API. That is why this is getting the IKeyBuidler and
             * then building the keys directly.
             */
            final IKeyBuilder keyBuilder = ndx.getIndexMetadata()
                    .getKeyBuilder();

            // verify index reports value exists for the key.
            assertTrue(ndx.contains(keyBuilder.reset().append("Mary").append(
                    "John").getKey()));

            // but this is not found in the index.
            assertFalse(ndx.contains(keyBuilder.reset().append("Mary").append(
                    "Fred").getKey()));

        }

        // partition0
        assertEquals(2L, ((Long) dataService0.submit(
                ITx.UNISOLATED,//
                DataService.getIndexPartitionName(name, 0/* partitionId */),//
                new RangeCountProcedure(true/* exact */, false/* deleted */,
                        null/* fromKey */, null/* toKey */)).get()).longValue());
        
        // partition1
        assertEquals(3L, ((Long) dataService1.submit(
                ITx.UNISOLATED,//
                DataService.getIndexPartitionName(name, 1/* partitionId */),//
                new RangeCountProcedure(true/* exact */, false/* deleted */,
                        null/* fromKey */, null/* toKey */)).get()).longValue());

//        {
//        // the metadata for that index.
//        final IndexMetadata metadata = ndx.getIndexMetadata();
//
//    // verify correct value in the index on the correct data service.
//    assertEquals(new byte[] { 1 }, ((ResultBuffer) dataService0.submit(
//            ITx.UNISOLATED,//
//            DataService.getIndexPartitionName(name, 0/*partitionId*/),//
//            BatchLookupConstructor.INSTANCE.newInstance(//
//                    metadata, //
//                    0,// fromIndex
//                    1,// toIndex
//                    new byte[][] { new byte[] { 1 } },// keys
//                    null // vals
//                    )).get()).getValues().get(0));
//    //
//    assertEquals(new byte[] { 5 }, ((ResultBuffer) dataService1.submit(
//            ITx.UNISOLATED,//
//            DataService.getIndexPartitionName(name, 1/*partitionId*/),//
//            BatchLookupConstructor.INSTANCE.newInstance(//
//                    metadata,//
//                    0,// fromIndex
//                    1,// toIndex
//                    new byte[][] { separatorKey },// keys
//                    null// vals
//                    )).get()).getValues().get(0));
//    }

    }

    /**
     * Unit test verifies that binding sets are correctly mapped over shards
     * when the target access path will be fully bound.
     * 
     * @throws IOException
     */
    public void test_mapShards_fullyBound() throws IOException {

        // scale-out view of the relation.
        final R rel = (R) fed.getResourceLocator().locate(namespace,
                ITx.UNISOLATED);

        /*
         * Setup the binding sets to be mapped across the shards.
         */
        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        final List<IBindingSet> expectedPartition0 = new LinkedList<IBindingSet>();
        final List<IBindingSet> expectedPartition1 = new LinkedList<IBindingSet>();
        {
            IBindingSet bset = null;
            { 
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("John"));
                bset.set(y, new Constant<String>("Mary"));
                data.add(bset);
                expectedPartition0.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Jane"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("John"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
            { // partition0
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Leon"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
                expectedPartition0.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("Leon"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
        
        }

        final Predicate<E> pred = new Predicate<E>(new BOp[] { x, y }, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { namespace }) //
                }));

        final long tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

        try {
            
            final MockMapBindingSetsOverShardsBuffer<E> fixture = new MockMapBindingSetsOverShardsBuffer<E>(
                    fed, pred, rel.getPrimaryKeyOrder(), tx, 100/* capacity */);

            // write the binding sets on the fixture.
            for (IBindingSet bindingSet : data) {

                fixture.add(bindingSet);

            }

            // flush (verify #of binding sets reported by flush).
            assertEquals((long) data.size(), fixture.flush());

            /*
             * Examine the output sinks, verifying that each binding set was
             * mapped onto the correct index partition.
             */
            {

                final List<Bundle> flushedChunks = fixture.flushedChunks;
                final List<IBindingSet[]> actualPartition0 = new LinkedList<IBindingSet[]>();
                final List<IBindingSet[]> actualPartition1 = new LinkedList<IBindingSet[]>();
                for (Bundle b : flushedChunks) {
                    if (b.locator.getPartitionId() == 0) {
                        actualPartition0.add(b.bindingSets);
                    } else if (b.locator.getPartitionId() == 1) {
                        actualPartition1.add(b.bindingSets);
                    } else {
                        fail("Not expecting: " + b.locator);
                    }
                }

                final int nflushed = flushedChunks.size();
                
//                assertEquals("#of sinks", 2, nflushed);

                // partition0
                {

//                    assertEquals("#of binding sets", partition0.size(),
//                            bundle0.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(
                            expectedPartition0.toArray(new IBindingSet[0]),
                            new Dechunkerator<IBindingSet>(actualPartition0
                                    .iterator()));

                }
                
                // partition1
                {
//                    final Bundle bundle1 = flushedChunks.get(1);
//
//                    assertEquals("partitionId", 1/* partitionId */,
//                            bundle1.locator.getPartitionId());

//                    assertEquals("#of binding sets", partition1.size(),
//                            bundle1.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(
                            expectedPartition1.toArray(new IBindingSet[0]),
                            new Dechunkerator<IBindingSet>(actualPartition1
                                    .iterator()));
                }

            }
        
        } finally {

            fed.getTransactionService().abort(tx);
            
        }

    }

    /**
     * Unit test verifies that binding sets are correctly mapped over shards
     * when only one component of the key is bound (the key has two components,
     * this unit test only binds the first component in the key).
     * 
     * @throws IOException
     */
    public void test_mapShards_oneBound() throws IOException {

        // scale-out view of the relation.
        final R rel = (R) fed.getResourceLocator().locate(namespace,
                ITx.UNISOLATED);

        /*
         * Setup the binding sets to be mapped across the shards.
         */
        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        final List<IBindingSet> expectedPartition0 = new LinkedList<IBindingSet>();
        final List<IBindingSet> expectedPartition1 = new LinkedList<IBindingSet>();
        {
            IBindingSet bset = null;
            { // partition0
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("John"));
//                bset.set(y, new Constant<String>("Mary"));
                data.add(bset);
                expectedPartition0.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
//                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
            { // partition1
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
//                bset.set(y, new Constant<String>("John"));
                data.add(bset);
                expectedPartition1.add(bset);
            }
            { // partition0
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Leon"));
//                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
                expectedPartition0.add(bset);
            }
        
//            // partition0
//            new E("John", "Mary"),// 
//            new E("Leon", "Paul"),// 
//            // partition1
//            new E("Mary", "John"),// 
//            new E("Mary", "Paul"),// 
//            new E("Paul", "Leon"),// 

        }

        final Predicate<E> pred = new Predicate<E>(new BOp[] { x, y }, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { namespace }) //
                }));

        final long tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

        try {
            
            final MockMapBindingSetsOverShardsBuffer<E> fixture = new MockMapBindingSetsOverShardsBuffer<E>(
                    fed, pred, rel.getPrimaryKeyOrder(), tx, 100/* capacity */);

            // write the binding sets on the fixture.
            for (IBindingSet bindingSet : data) {

                fixture.add(bindingSet);

            }

            // flush (verify #of binding sets reported by flush).
            assertEquals((long) data.size(), fixture.flush());

            /*
             * Examine the output sinks, verifying that each binding set was
             * mapped onto the correct index partition.
             */
            {

                final List<Bundle> flushedChunks = fixture.flushedChunks;
                final List<IBindingSet[]> actualPartition0 = new LinkedList<IBindingSet[]>();
                final List<IBindingSet[]> actualPartition1 = new LinkedList<IBindingSet[]>();
                for (Bundle b : flushedChunks) {
                    if (b.locator.getPartitionId() == 0) {
                        actualPartition0.add(b.bindingSets);
                    } else if (b.locator.getPartitionId() == 1) {
                        actualPartition1.add(b.bindingSets);
                    } else {
                        fail("Not expecting: " + b.locator);
                    }
                }

                final int nflushed = flushedChunks.size();
                
//                assertEquals("#of sinks", 2, nflushed);

                // partition0
                {

//                    assertEquals("#of binding sets", partition0.size(),
//                            bundle0.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(
                            expectedPartition0.toArray(new IBindingSet[0]),
                            new Dechunkerator<IBindingSet>(actualPartition0
                                    .iterator()));

                }
                
                // partition1
                {
//                    final Bundle bundle1 = flushedChunks.get(1);
//
//                    assertEquals("partitionId", 1/* partitionId */,
//                            bundle1.locator.getPartitionId());

//                    assertEquals("#of binding sets", partition1.size(),
//                            bundle1.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(
                            expectedPartition1.toArray(new IBindingSet[0]),
                            new Dechunkerator<IBindingSet>(actualPartition1
                                    .iterator()));
                }

            }
        
        } finally {

            fed.getTransactionService().abort(tx);
            
        }

    }

    /**
     * A unit test where no variables are bound. This should cause the binding
     * sets to be mapped across all shards.
     * 
     * @throws IOException
     */
    public void test_mapShards_nothingBound() throws IOException {

        // scale-out view of the relation.
        final R rel = (R) fed.getResourceLocator().locate(namespace,
                ITx.UNISOLATED);

        /*
         * Setup the binding sets to be mapped across the shards.
         */
        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        final List<IBindingSet> partition0 = new LinkedList<IBindingSet>();
        final List<IBindingSet> partition1 = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new HashBindingSet();
            data.add(bset);
            partition0.add(bset);
            partition1.add(bset);
        }

        final Predicate<E> pred = new Predicate<E>(new BOp[] { x, y }, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { namespace }) //
                }));

        final long tx = fed.getTransactionService().newTx(ITx.READ_COMMITTED);

        try {
            
            final MockMapBindingSetsOverShardsBuffer<E> fixture = new MockMapBindingSetsOverShardsBuffer<E>(
                    fed, pred, rel.getPrimaryKeyOrder(), tx, 100/* capacity */);

            // write the binding sets on the fixture.
            for (IBindingSet bindingSet : data) {

                fixture.add(bindingSet);

            }

            // flush (verify #of binding sets reported by flush).
            assertEquals((long) data.size(), fixture.flush());

            /*
             * Examine the output sinks, verifying that each binding set was
             * mapped onto the correct index partition.
             * 
             * Note: This depends on the output buffers being large enough to
             * hold all of the binding sets. It also depends on the assumption
             * that the outputs were generated in ascending key order such that
             * the first output chunk will be partition0 and the second will be
             * partition1.
             */
            {

                final List<Bundle> flushedChunks = fixture.flushedChunks;

                assertEquals("#of sinks", 2, flushedChunks.size());

                // partition0
                {
                    final Bundle bundle0 = flushedChunks.get(0);

                    assertEquals("partitionId", 0/* partitionId */,
                            bundle0.locator.getPartitionId());

                    assertEquals("#of binding sets", partition0.size(),
                            bundle0.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(partition0
                            .toArray(new IBindingSet[0]), Arrays.asList(
                            bundle0.bindingSets).iterator());

                }
                
                // partition1
                {
                    final Bundle bundle1 = flushedChunks.get(1);

                    assertEquals("partitionId", 1/* partitionId */,
                            bundle1.locator.getPartitionId());

                    assertEquals("#of binding sets", partition1.size(),
                            bundle1.bindingSets.length);

                    TestQueryEngine.assertSameSolutionsAnyOrder(partition1
                            .toArray(new IBindingSet[0]), Arrays.asList(
                            bundle1.bindingSets).iterator());
                }

            }

        } finally {

            fed.getTransactionService().abort(tx);
            
        }

    }

    /**
     * Helper class associates a {@link PartitionLocator} with a chunk of binding sets.
     */
    static private class Bundle {

        final PartitionLocator locator;

        final IBindingSet[] bindingSets;

        public Bundle(final PartitionLocator locator,
                final IBindingSet[] bindingSets) {

            this.locator = locator;

            this.bindingSets = bindingSets;

        }

    }

    /**
     * Mock class under test
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @param <F>
     *            The generic type of the elements in the relation for the
     *            target predicate.
     */
    static private class MockMapBindingSetsOverShardsBuffer<F> extends
            MapBindingSetsOverShardsBuffer<IBindingSet, F> {

        /**
         * The capacity of the output buffer.
         */
        private final int outputBufferCapacity;

        /**
         * @param fed
         * @param pred
         * @param keyOrder
         * @param timestamp
         * @param capacity
         *            The capacity of this buffer
         */
        public MockMapBindingSetsOverShardsBuffer(
                final IBigdataFederation<?> fed, final IPredicate<F> pred,
                final IKeyOrder<F> keyOrder, final long timestamp,
                final int capacity) {

            super(fed, pred, keyOrder, timestamp, capacity);

            /*
             * Output capacity of each sink is the input capacity for the
             * purposes of this test.
             */
            this.outputBufferCapacity = capacity;

        }

        @Override
        protected IBuffer<IBindingSet[]> newBuffer(final PartitionLocator locator) {

            return new AbstractArrayBuffer<IBindingSet[]>(outputBufferCapacity,
                    IBindingSet[].class, null/* filter */) {

                /**
                 * Puts a copy of the locator and the binding set chunk onto a
                 * list for examination by the test harness.
                 */
                @Override
                protected long flush(final int n, final IBindingSet[][] a) {

                    for (int i = 0; i < n; i++) {

                        flushedChunks.add(new Bundle(locator, a[i]));
//                        flushedChunks.add(new Bundle(locator, Arrays.copyOf(
//                                a[i], n)));

                    }

                    return n;

                }
            };

        }

        /**
         * A list of the binding set chunks which were flushed out. Each chunk
         * is paired with the {@link PartitionLocator} onto which the binding
         * sets in that chunk were mapped.
         */
        final LinkedList<Bundle> flushedChunks = new LinkedList<Bundle>();

    } // class MockDistributedOutputBuffer

}
