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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.fed;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.PipelineDelayOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOp;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.service.AbstractEmbeddedFederationTestCase;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.util.config.NicUtil;

/**
 * Unit tests for {@link FederatedQueryEngine} running against an
 * {@link EmbeddedFederation} having a single {@link DataService}. The data may
 * be partitioned, but the partitions will live on the same {@link DataService}.
 * <p>
 * Note: You can not use multiple {@link DataService}s in this test suite with
 * the {@link EmbeddedFederation} because the services are not sufficiently
 * distinct based on how they are created. Therefore unit tests against more
 * than one {@link DataService} are located in the <code>bigdata-jini</code>
 * module.
 * <p>
 * Note: Distributed query processing generally means that the order in which
 * the chunks arrive is non-deterministic. Therefore the order of the solutions
 * can not be checked unless a total order is placed on the solutions using a
 * {@link SortOp}.
 * 
 * <pre>
 * -Dlog4j.configuration=bigdata/src/resources/logging/log4j.properties
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestFederatedQueryEngine.java 3508 2010-09-05 17:02:34Z
 *          thompsonbry $
 */
public class TestFederatedQueryEngine extends AbstractEmbeddedFederationTestCase {

    public TestFederatedQueryEngine() {
        
    }
    
    public TestFederatedQueryEngine(String name) {
        
        super(name);
        
    }

    // Namespace for the relation.
    static private final String namespace = TestFederatedQueryEngine.class.getName();
    
    // The separator key between the index partitions.
    private byte[] separatorKey;

    /** The local persistence store for the {@link #queryEngine}. */
    private Journal queryEngineStore;
    
    /** The local {@link ResourceService} for the {@link #queryEngine}. */
    private ManagedResourceService queryEngineResourceService;
    
    /** The query controller. */
    private FederatedQueryEngine queryEngine;
    
    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());
        
        /*
         * Restrict to a single data service.
         */
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

        return properties;
        
    }

    public void setUp() throws Exception {

        super.setUp();

        assertNotNull(dataService0);
        assertNull(dataService1);
        
//        final IBigdataFederation<?> fed = client.connect();

        // create index manager for the query controller.
        {
            final Properties p = new Properties();
            p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                    .toString());
            queryEngineStore = new Journal(p);
        }
        
        // create resource service for the query controller.
        {
            queryEngineResourceService = new ManagedResourceService(
                    new InetSocketAddress(InetAddress
                            .getByName(NicUtil.getIpAddress("default.nic",
                                    "default", true/* loopbackOk */)), 0/* port */
                    ), 0/* requestServicePoolSize */) {

                @Override
                protected File getResource(UUID uuid) throws Exception {
                    // Will not serve up files.
                    return null;
                }
            };
        }

        {

            // create the query controller.
            queryEngine = new FederatedQueryEngine(UUID.randomUUID(), fed,
                    queryEngineStore, queryEngineResourceService);

            queryEngine.init();

            System.err.println("controller: " + queryEngine);

        }
        
//        dataService0 = fed.getDataService(dataServices[0]); 
//        dataService1 = fed.getDataService(dataServices[1]); 
        {

            // @todo need to wait for the dataService to be running.
            assertTrue(((DataService) dataService0).getResourceManager()
                    .awaitRunning());

            // resolve the query engine on one of the data services.
            while (dataService0.getQueryEngine() == null) {

                if (log.isInfoEnabled())
                    log.info("Waiting for query engine on dataService0");

                Thread.sleep(250);

            }
            
            System.err.println("queryPeer : " + dataService0.getQueryEngine());
            
        }

//        // resolve the query engine on the other data services.
//        {
//
//            IQueryPeer other = null;
//            
////            assertTrue(((DataService) dataServer.getProxy())
////                    .getResourceManager().awaitRunning());
//            
//            while ((other = dataService1.getQueryEngine()) == null) {
//
//                if (log.isInfoEnabled())
//                    log.info("Waiting for query engine on dataService1");
//
//                Thread.sleep(250);
//
//            }
//
//            System.err.println("other     : " + other);
//            
//        }

        loadData();
        
    }

    public void tearDown() throws Exception {
        
        // clear reference.
        separatorKey = null;
        
        if (queryEngineResourceService != null) {
            queryEngineResourceService.shutdownNow();
            queryEngineResourceService = null;
        }
        if (queryEngineStore != null) {
            queryEngineStore.destroy();
            queryEngineStore = null;
        }
        if (queryEngine != null) {
            queryEngine.shutdownNow();
            queryEngine = null;
        }

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

        // two partitions on the same data service.
        final UUID[] dataServices = new UUID[] {//
                dataService0.getServiceUUID(),//
                dataService0.getServiceUUID(),//
        };

        /*
         * Create the relation with the primary index key-range partitioned
         * using the given separator keys and data services.
         */

        final R rel = new R(client.getFederation(), namespace, ITx.UNISOLATED,
                new Properties());

        if (client.getFederation().getResourceLocator().locate(namespace,
                ITx.UNISOLATED) == null) {

            rel.create(separatorKeys, dataServices);

            /*
             * Insert data into the appropriate index partitions.
             */
            rel
                    .insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        }

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Starts and stops the {@link QueryEngine}, but does not validate the
     * semantics of shutdown() versus shutdownNow() since we need to be
     * evaluating query mixes in order to verify the semantics of those
     * operations.
     * 
     * @throws Exception
     */
    public void test_startStop() throws Exception {

        // NOP
        
    }

    /**
     * Test the ability to run a query which does nothing and produces no
     * solutions.
     * 
     * @throws Exception
     */
    public void test_query_startRun() throws Exception {

        final int startId = 1;
        final BindingSetPipelineOp query = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                }));

        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1 /* partitionId */,
                        newBindingSetIterator(new HashBindingSet())));

        // Wait until the query is done.
        final Map<Integer, BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(1, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

        // validate the query solution stats.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info(stats.toString());

            // query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // Verify results.
        {
            // the expected solution (just one empty binding set).
            final IBindingSet[] expected = new IBindingSet[] {//
            new HashBindingSet() //
            };

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        }
        
    }

    /**
     * Test the ability run a simple join. There are three operators. One feeds
     * an empty binding set[] into the join, another is the predicate for the
     * access path on which the join will read (it probes the index once for
     * "Mary" and bindings "Paul" when it does so), and the third is the join
     * itself (there is one solution, which is "value=Paul").
     * 
     * @throws Exception
     */
    public void test_query_join1() throws Exception {

        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final int sliceId = 4;
        final BindingSetPipelineOp query = 
            new SliceOp(new BOp[]{new PipelineJoin<E>(
        // left
                new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(new IVariableOrConstant[] {
                        new Constant<String>("Mary"), Var.var("value") }, NV
                        .asMap(new NV[] {//
                                new NV(Predicate.Annotations.RELATION_NAME,
                                        new String[] { namespace }),//
                                new NV(Predicate.Annotations.KEY_ORDER,
                                        R.primaryKeyOrder),//
                                new NV(Predicate.Annotations.PARTITION_ID,
                                        Integer.valueOf(-1)),//
                                new NV(Predicate.Annotations.OPTIONAL,
                                        Boolean.FALSE),//
                                new NV(Predicate.Annotations.CONSTRAINT, null),//
                                new NV(Predicate.Annotations.EXPANDER, null),//
                                new NV(Predicate.Annotations.BOP_ID, predId),//
                                new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        })),
                // join annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, joinId),//
                        })//
        )},
        // slice annotations
        NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, sliceId),//
                })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
            new ArrayBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("Paul") }//
                ), //
            new ArrayBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("John") }//
        ) };

        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId,//
                        -1, /* partitionId */
                        newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        // Wait until the query is done.
        final Map<Integer,BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(2, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

        // validate the stats for the start operator.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("start: "+stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the join operator.
        {
            final BOpStats stats = statsMap.get(joinId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join : "+stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get()); // @todo this depends on which index partitions we read on.
        }

    }

    /**
     * @todo Test the ability close the iterator draining a result set before
     *       the query has finished executing and verify that the query is
     *       correctly terminated [this is difficult to test without having
     *       significant data scale since there is an implicit race between the
     *       consumer and the producer to close out the query evaluation, but
     *       the {@link PipelineDelayOp} can be used to impose sufficient
     *       latency on the pipeline that the test can close the query buffer
     *       iterator first].
     *       <p>
     *       This must also be tested in scale-out to make sure that the data
     *       backing the solutions is not discarded before the caller can use
     *       those data. [This could be handled by materializing binding set
     *       objects out of a {@link ByteBuffer} rather than using a live decode
     *       of the data in that {@link ByteBuffer}.]
     */
    public void test_query_closeIterator() {

        fail("write test");

    }

    /**
     * @todo Test ability to impose a limit/offset slice on a query.
     *       <p>
     *       Note: While the logic for visiting only the solutions selected by
     *       the slice can be tested against a mock object, the integration by
     *       which a slice halts a query when it is satisfied has to be tested
     *       against a {@link QueryEngine}.
     *       <p>
     *       This must also be tested in scale-out to make sure that the data
     *       backing the solutions is not discarded before the caller can use
     *       those data. [This could be handled by materializing binding set
     *       objects out of a {@link ByteBuffer} rather than using a live decode
     *       of the data in that {@link ByteBuffer}.]
     */
    public void test_query_slice() {

        fail("write test");

    }

    /**
     * @todo Test the ability run a query reading on an access path using a
     *       element filter (other than DISTINCT).
     */
    public void test_query_join1_filter() {
        
        fail("write test");

    }

    /**
     * @todo Test the ability run a query reading on an access path using a
     *       DISTINCT filter for selected variables on that access path (the
     *       DISTINCT filter is a different from most other access path filters
     *       since it stateful and is applied across all chunks on all shards).
     */
    public void test_query_join1_distinctAccessPath() {
        
        fail("write test");
        
    }

    /**
     * Test the ability run a query requiring two joins.
     * 
     * @todo Verify join constraints (e.g., x == y or x != y).
     * 
     * @todo run with different initial bindings (x=Mary, x is unbound, etc).
     */
    public void test_query_join2() throws Exception {

        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3;
        final int joinId2 = 4;
        final int predId2 = 5;
        final int sliceId = 6;
        
        final BindingSetPipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("x"), Var.var("y") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID,
                                Integer.valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL,
                                Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("y"), Var.var("z") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID,
                                Integer.valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL,
                                Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final BindingSetPipelineOp join1Op = new PipelineJoin<E>(//
                startOp, pred1Op,//
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, joinId1),//
                        }));

        final BindingSetPipelineOp join2Op = new PipelineJoin<E>(//
                join1Op, pred2Op,//
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, joinId2),//
                        }));

        final BindingSetPipelineOp query = new SliceOp(new BOp[] { join2Op },
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, sliceId),//
                        }));

        // start the query.
        final UUID queryId = UUID.randomUUID();
        final IChunkMessage<IBindingSet> initialChunkMessage;
        {

            final IBindingSet initialBindings = new HashBindingSet();

            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            initialChunkMessage = new LocalChunkMessage<IBindingSet>(
                    queryEngine, queryId, startId,//
                    -1, // partitionId
                    newBindingSetIterator(initialBindings));

        }
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialChunkMessage);

        // verify solutions.
        {

            // the expected solution (just one).
            final IBindingSet[] expected = new IBindingSet[] {//
            new ArrayBindingSet(//
                    new IVariable[] { Var.var("x"), Var.var("y"), Var.var("z") },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Paul"),
                            new Constant<String>("Leon") }//
            ),//
            new ArrayBindingSet(//
                    new IVariable[] { Var.var("x"), Var.var("y"), Var.var("z") },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("John"),
                            new Constant<String>("Mary") }//
            )};

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    new Dechunkerator<IBindingSet>(runningQuery.iterator()));

        }

        // Wait until the query is done.
        final Map<Integer, BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(3, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

        // validate the stats for the start operator.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("start: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the 1st join operator.
        {
            final BOpStats stats = statsMap.get(joinId1);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join1: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get()); // @todo depends on where the shards are.
        }

        // validate the stats for the 2nd join operator.
        {
            final BOpStats stats = statsMap.get(joinId2);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join2: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get()); // @todo depends on where the shards are.
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get()); // @todo depends on where the shards are.
        }

        // validate stats for the sliceOp (on the query controller)
        {
            final BOpStats stats = statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get()); // @todo?
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get()); // @todo?
        }
        
    }

    /**
     * @todo Write unit tests for optional joins, including where an alternative
     *       sink is specified in the {@link BOpContext} and is used when the
     *       join fails.
     * */
    public void test_query_join2_optionals() {

        fail("write test");

    }
    
}
