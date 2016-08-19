/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Aug 23, 2010
 */

package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.EQ;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.fed.TestFederatedQueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SliceOp.SliceStats;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Test suite for the {@link QueryEngine} against a local database instance.
 * <p>
 * Note: The {@link BOp}s are unit tested separately. This test suite is focused
 * on interactions when {@link BOp}s are chained together in a query, such as a
 * sequence of pipeline joins, a slice applied to a query, etc.
 * 
 * <pre>
 * -Dlog4j.configuration=bigdata/src/resources/logging/log4j.properties
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see TestFederatedQueryEngine
 */
public class TestQueryEngine extends AbstractQueryEngineTestCase {

    /**
     * 
     */
    public TestQueryEngine() {
    }

    /**
     * @param name
     */
    public TestQueryEngine(final String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;
        
    }

    static private final String namespace = "ns";
    private Journal jnl;
    private QueryEngine queryEngine;

    @Override
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);
        
        queryEngine = new QueryEngine(jnl);
        
        queryEngine.init();

    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     */
    private void loadData(final Journal store) {

        // create the relation.
        final R rel = new R(store, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert (in key order for convenience).
        final E[] a = {//
                new E("John", "Mary"),// [0]
                new E("Leon", "Paul"),// [1]
                new E("Mary", "Paul"),// [2]
                new E("Paul", "Leon"),// [3]
        };

        // insert data (the records are not pre-sorted).
        rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        store.commit();

    }

    @Override
    public void tearDown() throws Exception {

        if (queryEngine != null) {
            queryEngine.shutdownNow();
            queryEngine = null;
        }

        if (jnl != null) {
            jnl.destroy();
            jnl = null;
        }

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
        final PipelineOp query = new StartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                        StandaloneChunkHandler.TEST_INSTANCE),//
                }));

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                new ListBindingSet());
//                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
//                        startId, -1/* partitionId */,
//                        AbstractQueryEngineTestCase.newBindingSetIterator(new HashBindingSet())));

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
            // the expected solution.
            final IBindingSet[] expected = new IBindingSet[] {//
            new ListBindingSet() //
            };

            AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);
        }
        
    }

    public void test_slice_threadSafe() throws Exception {

    	// @todo also stress with parallel trials.
        final int ntrials = 10000;

        for(int i=0; i<ntrials; i++) {

        	test_query_join1_without_StartOp();
        	
        }
        
    }
    
    /**
     * Test the ability run a simple join without a {@link StartOp}. An empty
     * binding set[] is fed into the join. The join probes the index once for
     * "Mary" and binds "Paul" when it does so. There there is one solution,
     * which is "value=Paul".
     * 
     * @throws Exception
     */
    public void test_query_join1_without_StartOp() throws Exception {

//        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;

//        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
//                        new NV(Predicate.Annotations.BOP_ID, startId),//
//                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
//                                BOpEvaluationContext.CONTROLLER),//
//                }));

        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Mary"), Var.var("value") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = new PipelineJoin<E>(new BOp[] { /*startOp*/ },//
                new NV(Predicate.Annotations.BOP_ID, joinId),//
                new NV(PipelineJoin.Annotations.PREDICATE, pred),
                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                        StandaloneChunkHandler.TEST_INSTANCE)//
                );

        // the expected solution.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("Paul") }//
        ) };

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                new ListBindingSet());
//                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
//                        joinId, -1 /* partitionId */,
//                        AbstractQueryEngineTestCase.newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(1, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
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
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * Test the ability run a simple join. There are three operators. One feeds
     * an empty binding set[] into the join, another is the predicate for the
     * access path on which the join will read (it probes the index once for
     * "Mary" and binds "Paul" when it does so), and the third is the join
     * itself (there is one solution, which is "value=Paul").
     * 
     * @throws Exception
     */
    public void test_query_join1() throws Exception {

        final int startId = 1;
		final int joinId = 2;
		final int predId = 3;

		final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
						new NV(Predicate.Annotations.BOP_ID, startId),//
						new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
								BOpEvaluationContext.CONTROLLER),//
				}));

		final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
				new Constant<String>("Mary"), Var.var("value") }, NV
				.asMap(new NV[] {//
						new NV(Predicate.Annotations.RELATION_NAME,
								new String[] { namespace }),//
						new NV(Predicate.Annotations.BOP_ID, predId),//
						new NV(Annotations.TIMESTAMP,
								ITx.READ_COMMITTED),//
				}));

		final PipelineOp query = new PipelineJoin<E>(new BOp[] { startOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred),
                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                        StandaloneChunkHandler.TEST_INSTANCE)
                );

        // the expected solution.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("Paul") }//
        ) };

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                new ListBindingSet());
//                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
//                        startId, -1 /* partitionId */,
//                        AbstractQueryEngineTestCase.newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * Test the ability run a simple join when multiple binding sets are
     * submitted as the initial input. The access path associated with the join
     * does not have any constants but the join picks up bindings from the input
     * binding sets and uses them to constrain the access path.
     * 
     * @todo Support for this test is no longer present. It was lost when the
     *       {@link StandaloneChunkHandler} was written (ONE_MESSAGE_PER_CHUNK).
     */
    public void test_query_join1_multipleChunksIn() throws Exception {

//        fail("reenable this test");
        
        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final int sliceId = 4;

        /*
         * Enforce a constraint on the source such that it hands 3 each source
         * chunk to the join operator as a separate chunk
         */
        final int nsources = 3;
        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),//
                new NV(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY, nsources),//
                new NV(PipelineOp.Annotations.MAX_MESSAGES_PER_TASK, 1),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                x, y }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));
        
		final PipelineJoin<E> joinOp = new PipelineJoin<E>(
				new BOp[] { startOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId),//
				new NV(PipelineJoin.Annotations.PREDICATE, predOp));
        
        final SliceOp sliceOp = new SliceOp(new BOp[] { joinOp },
                // slice annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, sliceId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                        new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                StandaloneChunkHandler.TEST_INSTANCE),//
//                        new NV(
//                                QueryEngineTestAnnotations.COMBINE_RECEIVED_CHUNKS,
//                                false),//
                        })//
        );
        
        final PipelineOp query = sliceOp;

        /*
         * Source binding sets.
         * 
         * Note: We can't bind y in advance for the primary index!
         */
        final IBindingSet[] source;
		{
			final IBindingSet bset1 = new ListBindingSet();
			bset1.set(x, new Constant<String>("Paul"));
			final IBindingSet bset2 = new ListBindingSet();
			bset2.set(x, new Constant<String>("Leon"));
			final IBindingSet bset3 = new ListBindingSet();
			bset3.set(x, new Constant<String>("Mary"));

			source = new IBindingSet[] {//
        		bset1,bset2,bset3
//                new HashBindingSet(new ListBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Paul") }//
//                )),//
//                new HashBindingSet(new ListBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Leon") }//
//                )),
//                new HashBindingSet(new ListBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Mary") }//
//                )),
        };
        }
        // Put each source binding set into a chunk by itself.
        final IBindingSet[][] sources = new IBindingSet[source.length][];
        for (int i = 0; i < sources.length; i++) {
            sources[i] = new IBindingSet[] { source[i] };
        }
        assertEquals(nsources, source.length);
        assertEquals(nsources, sources.length);

//        new E("John", "Mary"),// [0]
//        new E("Leon", "Paul"),// [1]
//        new E("Mary", "Paul"),// [2]
//        new E("Paul", "Leon"),// [3]
        
        // the expected solution.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Paul"), new Constant<String>("Leon") }//
        ),//
        new ListBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Leon"), new Constant<String>("Paul") }//
        ),
        new ListBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Mary"), new Constant<String>("Paul") }//
        ),
        };

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                null/* queryAttributes */, sources);
        
        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
                log.info("start: "+stats.toString());

            assertEquals(3L, stats.chunksIn.get());
            assertEquals(3L, stats.unitsIn.get());
            assertEquals(3L, stats.unitsOut.get());
            assertEquals(3L, stats.chunksOut.get());
        }

        // validate the stats for the join operator.
        {
            final BOpStats stats = statsMap.get(joinId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join : "+stats.toString());

//            assertEquals(3L, stats.chunksIn.get());
            assertEquals(3L, stats.unitsIn.get());
            assertEquals(3L, stats.unitsOut.get());
//            assertEquals(3L, stats.chunksOut.get());
        }

        // validate the stats for the slice operator.
        {
            final SliceStats stats = (SliceStats) statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: "+stats.toString());

            assertEquals(3L, stats.nseen.get());
            assertEquals(3L, stats.naccepted.get());
//            assertEquals(3L, stats.chunksIn.get());
            assertEquals(3L, stats.unitsIn.get());
            assertEquals(3L, stats.unitsOut.get());
//            assertEquals(3L, stats.chunksOut.get());
        }

    }

    /**
     * Test verifies the ability close the iterator draining a result set before
     * the query has finished executing and also verify that the query is
     * correctly terminated.
     * <p>
     * Note: This is difficult to test without having significant data scale
     * since there is an implicit race between the consumer and the producer to
     * close out the query evaluation, but the {@link PipelineDelayOp} can be
     * used to impose sufficient latency on the pipeline that the test can close
     * the query buffer iterator first.
     * <p>
     * Note: This must also be tested in scale-out to make sure that the data
     * backing the solutions is not discarded before the caller can use those
     * data.
     * <p>
     * This could be handled by: (a) materializing binding set objects out of a
     * {@link ByteBuffer} rather than using a live decode of the data in that
     * {@link ByteBuffer}; or by (b) using an special {@link IAllocationContext}
     * which is scoped to the query results such that they are not released
     * until the iterator draining the buffer is closed.
     * 
     * @throws Exception
     */
    public void test_query_closeIterator() throws Exception {

        final int startId = 1;
        final int delayId = 2;

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        /*
         * Enforce a constraint on the source such that it hands 3 each source
         * chunk to the join operator as a separate chunk
         */
        final int nsources = 4;
        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),//
                new NV(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY, nsources),//
                new NV(PipelineOp.Annotations.MAX_MESSAGES_PER_TASK, 1),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final PipelineDelayOp delayOp = new PipelineDelayOp(new BOp[]{startOp},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, delayId),//
                        new NV(PipelineDelayOp.Annotations.DELAY, 2000L/*ms*/),//
                        new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                StandaloneChunkHandler.TEST_INSTANCE),//
                        }));
        
        // the source data.
        final IBindingSet[] source = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("John"),
                                new Constant<String>("Mary") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Paul"),
                                new Constant<String>("Mary") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Paul"),
                                new Constant<String>("Mark") }//
                )};
        // Put each source binding set into a chunk by itself.
        final IBindingSet[][] sources = new IBindingSet[source.length][];
        for (int i = 0; i < sources.length; i++) {
            sources[i] = new IBindingSet[] { source[i] };
        }
        assertEquals(nsources, source.length);
        assertEquals(nsources, sources.length);
        
        final PipelineOp query = delayOp; 
        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                null/* queryAttributes */, sources);

        assertFalse(runningQuery.isDone());
        
        final ICloseableIterator<IBindingSet[]> itr = runningQuery.iterator();
        assertFalse(runningQuery.isDone());
        
        // eagerly terminate the query.
        itr.close();
        assertTrue(runningQuery.isCancelled());
        try {
            runningQuery.get();
            fail("Expecting: " + CancellationException.class);
        } catch (CancellationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

//    /**
//     * Test the ability of the query engine to defer the evaluation of a one
//     * shot operator until all inputs are available for that operator.
//     * 
//     * @todo We could do this using a mock operator and feeding a bunch of
//     *       chunks into the query by controlling the chunk size, as we do in
//     *       {@link #test_query_join1_multipleChunksIn()}. Make sure that the
//     *       mock operator is not evaluated until all inputs are available for
//     *       that operator.
//     */
//    public void test_oneShot_operator() {
//
//        fail("write test");
//
//    }

    /**
     * Unit test runs chunks into a slice without a limit. This verifies that
     * the query terminates properly even though the slice is willing to accept
     * more data.
     * <p>
     * When the {@link IRunningQuery} implementation supports it, the source
     * data are presented in multiple chunks in order to verify the behavior of
     * the {@link SliceOp} across multiple (and potentially concurrent)
     * invocations of that operator.
     * 
     * @throws Exception
     */
    public void test_query_slice_noLimit() throws Exception {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final int startId = 1;
        final int sliceId = 2;

        /*
         * Enforce a constraint on the source such that it hands each source
         * chunk to the join operator as a separate chunk
         */
        final int nsources = 4;
        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),//
                new NV(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY, nsources),//
                new NV(PipelineOp.Annotations.MAX_MESSAGES_PER_TASK, 1),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final SliceOp sliceOp = new SliceOp(new BOp[] { startOp },
                // slice annotations
                        NV.asMap(new NV[] { //
                                new NV(BOp.Annotations.BOP_ID, sliceId),//
                                        new NV(SliceOp.Annotations.OFFSET, 0L),//
                                        new NV(SliceOp.Annotations.LIMIT, Long.MAX_VALUE),//
                                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                                BOpEvaluationContext.CONTROLLER),//
                                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                                        new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                                        new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                                StandaloneChunkHandler.TEST_INSTANCE),//
//                                        // Require the chunked running query impl.
//                                        new NV(QueryEngine.Annotations.RUNNING_QUERY_CLASS,
//                                                ChunkedRunningQuery.class.getName()),//
                                })//
                );
        
        // the source data.
        final IBindingSet[] source = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("John"),
                                new Constant<String>("Mary") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Paul"),
                                new Constant<String>("Mary") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Paul"),
                                new Constant<String>("Mark") }//
                )};
        // Put each source binding set into a chunk by itself.
        final IBindingSet[][] sources = new IBindingSet[source.length][];
        for (int i = 0; i < sources.length; i++) {
            sources[i] = new IBindingSet[] { source[i] };
        }
        assertEquals(nsources, source.length);
        assertEquals(nsources, sources.length);
        
        final PipelineOp query = sliceOp; 
        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                null/* queryAttributes */, sources);
        
        //
        //
        //
        
        // the expected solutions.
        final IBindingSet[] expected = source;

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
                log.info("start: " + stats.toString());

            // verify query solution stats details.
            assertEquals((long)nsources, stats.chunksIn.get());
            assertEquals((long)nsources, stats.unitsIn.get());
            assertEquals((long)nsources, stats.unitsOut.get());
            assertEquals((long)nsources, stats.chunksOut.get());
        }

        // validate the stats for the slice operator.
        {
            final BOpStats stats = statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: " + stats.toString());

            // verify query solution stats details.
            assertEquals((long) nsources, stats.unitsIn.get());
            assertEquals((long) nsources, stats.unitsOut.get());
            if (runningQuery instanceof ChunkedRunningQuery) {
                /*
                 * The unit test setup will result in four distinct chunks
                 * being presented to the SliceOp and four distinct invocations
                 * of the ChunkTask for that SliceOp.
                 */
                assertEquals((long) nsources, stats.chunksIn.get());
                assertEquals((long) nsources, stats.chunksOut.get());
//            } else if (runningQuery instanceof StandaloneChainedRunningQuery) {
//                /*
//                 * The chunks will have been combined and the SliceOp will only
//                 * run once.
//                 */
//                assertEquals(1L, stats.chunksIn.get());
//                assertEquals(1L, stats.chunksOut.get());
            } else {
                fail("Unknown implementation class: "
                        + runningQuery.getClass().getName());
            }
        }
        
    }
    
    /**
     * Run a join with a slice. The slice is always evaluated on the query
     * controller so adding it to the query plan touches a slightly different
     * code path from adding another join (joins are evaluated shardwise, at
     * least in scale-out).
     * <p>
     * Note: While the logic for visiting only the solutions selected by the
     * slice can be tested against a mock object, the integration by which a
     * slice halts a query when it is satisfied has to be tested against a
     * {@link QueryEngine}.
     * <p>
     * This must also be tested in scale-out to make sure that the data backing
     * the solutions is not discarded before the caller can use those data.
     * [This could be handled by materializing binding set objects out of a
     * {@link ByteBuffer} rather than using a live decode of the data in that
     * {@link ByteBuffer}.]
     */
    public void test_query_slice() throws Exception {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final int sliceId = 4;

        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                x, y }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));

		final PipelineJoin<E> joinOp = new PipelineJoin<E>(
				new BOp[] { startOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId),//
				new NV(PipelineJoin.Annotations.PREDICATE, predOp));

        final PipelineOp query = new SliceOp(new BOp[] { joinOp },
        // slice annotations
                NV.asMap(new NV[] { //
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                                new NV(SliceOp.Annotations.OFFSET, 0L),//
                                new NV(SliceOp.Annotations.LIMIT, 2L),//
                                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER),//
                                new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                                new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                        StandaloneChunkHandler.TEST_INSTANCE),//
                        })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("John"),
                                new Constant<String>("Mary") }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ) };

        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                new ListBindingSet());
//                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
//                        startId, -1 /* partitionId */,
//                        AbstractQueryEngineTestCase.newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals("statsMap.size()", 3, statsMap.size());
            if (log.isInfoEnabled())
                log.info(statsMap.toString());
        }

        /*
         * Note: SliceOp can cause the Start operator to be interrupted. If this
         * occurs, the BOpStats for the join are not reported and aggregated
         * reliably (there is a race between the completion of the Start and its
         * interrupt by the slice). Since this unit test has a slice which will
         * interrupt the running query, we can not test the stats on the Start
         * reliably for this unit test.
         */
//        // validate the stats for the start operator.
//        {
//            final BOpStats stats = statsMap.get(startId);
//            assertNotNull(stats);
//            if (log.isInfoEnabled())
//                log.info("start: " + stats.toString());
//
//            // verify query solution stats details.
//            assertEquals(1L, stats.chunksIn.get());
//            assertEquals(1L, stats.unitsIn.get());
//            assertEquals(1L, stats.unitsOut.get());
//            assertEquals(1L, stats.chunksOut.get());
//        }

        /*
         * Note: SliceOp can cause the Join operator to be interrupted. If this
         * occurs, the BOpStats for the join are not reported and aggregated
         * reliably (there is a race between the completion of the join and its
         * interrupt by the slice). Since this unit test has a slice which will
         * interrupt the running query, we can not test the stats on the join
         * reliably for this unit test.
         */
//        // validate the stats for the join operator.
//        {
//            final BOpStats stats = statsMap.get(joinId);
//            assertNotNull(stats);
//            if (log.isInfoEnabled())
//                log.info("join : " + stats.toString());
//
//            // verify query solution stats details.
//            assertEquals(1L, stats.chunksIn.get());
//            assertEquals(1L, stats.unitsIn.get());
//            assertEquals(4L, stats.unitsOut.get());
//            assertEquals(1L, stats.chunksOut.get());
//        }

        // validate the stats for the slice operator.
        {
            final BOpStats stats = statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get()); // @todo use non-zero offset?
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * A join with an {@link IConstraint}.
     */
    public void test_query_join_withConstraint() throws Exception {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final int sliceId = 4;

        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                }));

        /*
         * 
         * Note: Since the index on which this reads is formed as (column1 +
         * column2) the probe key will be [null] if it does not bind the first
         * column. Therefore, in order to have the 2nd column constraint we have
         * to model it as an IElementFilter on the predicate.
         */
        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                x, y}, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
//                        new NV(Predicate.Annotations.KEY_ORDER,
//                                R.primaryKeyOrder),//
                }));

		final PipelineJoin<E> joinOp = new PipelineJoin<E>(
				new BOp[] { startOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId),//
				new NV(PipelineJoin.Annotations.PREDICATE, predOp),//
				// impose constraint on the join.
				new NV(PipelineJoin.Annotations.CONSTRAINTS,
						new IConstraint[] { Constraint.wrap(new EQConstant(y,
								new Constant<String>("Paul"))) })//
		);

        final PipelineOp query = new SliceOp(new BOp[] { joinOp },
        // slice annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, sliceId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                        new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                StandaloneChunkHandler.TEST_INSTANCE),//
                        })//
        );

        // the expected solutions (order is not reliable due to concurrency).
        final IBindingSet[] expected = new IBindingSet[] {//
//                new ListBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("John"),
//                                new Constant<String>("Mary") }//
//                ), //
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ), // 
//                new ListBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("Mary"),
//                                new Constant<String>("John") }//
//                ), //
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Mary"),
                                new Constant<String>("Paul") }//
                ), //
//                new ListBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("Paul"),
//                                new Constant<String>("Leon") }//
//                ), //
        };
//        new E("John", "Mary"),// [0]
//        new E("Leon", "Paul"),// [1]
//        new E("Mary", "Paul"),// [2]
//        new E("Paul", "Leon"),// [3]

        final IRunningQuery runningQuery;
        {
//            final IBindingSet initialBindingSet = new HashBindingSet();

//            initialBindingSet.set(y, new Constant<String>("Paul"));
            
            final UUID queryId = UUID.randomUUID();

            runningQuery = queryEngine.eval(queryId, query,new ListBindingSet());
//                    new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
//                            startId,//
//                            -1, /* partitionId */
//                            AbstractQueryEngineTestCase.newBindingSetIterator(initialBindingSet)));
        }

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the slice operator.
        {
            final BOpStats stats = statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: "+stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

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
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("x"), Var.var("y") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("y"), Var.var("z") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
		final PipelineOp join1Op = new PipelineJoin<E>(//
				new BOp[] { startOp },//
				new NV(Predicate.Annotations.BOP_ID, joinId1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred1Op));

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { join1Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op),
                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                        StandaloneChunkHandler.TEST_INSTANCE)//
				);

		final PipelineOp query = join2Op;

        // start the query.
        final UUID queryId = UUID.randomUUID();
//        final IChunkMessage<IBindingSet> initialChunkMessage;
        final IBindingSet initialBindings = new ListBindingSet();
        {

//            final IBindingSet initialBindings = new HashBindingSet();

            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

//            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
//                    queryId, startId,//
//                    -1, // partitionId
//                    AbstractQueryEngineTestCase.newBindingSetIterator(initialBindings));
        }
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialBindings);
//                initialChunkMessage);

        // verify solutions.
        {

            // the expected solution.
            final IBindingSet[] expected = new IBindingSet[] {//
            new ListBindingSet(//
                    new IVariable[] { Var.var("x"), Var.var("y"), Var.var("z") },//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Paul"),
                            new Constant<String>("Leon") }//
            ) };

            AbstractQueryEngineTestCase.assertSameSolutions(expected,
                    runningQuery);
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
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
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the 2nd join operator.
        {
            final BOpStats stats = statsMap.get(joinId2);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join2: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * A stress test of {@link #test_query_join2()} which runs a fixed number of
     * presentations in a single thread.
     * 
     * @throws Exception
     */
    public void test_queryJoin2_stressTest() throws Exception {
        
        final long timeout = Long.MAX_VALUE;//TimeUnit.MINUTES.toMillis(1);

        final int ntrials = 100;

        final int poolSize = 1; // no concurrency.

        final int nsuccess = doStressTest(timeout, ntrials, poolSize);

        if (nsuccess < ntrials) {

            /*
             * Note: This test should run to completion using a single thread in
             * order to detect problems with improper termination of a query.
             */
            
            fail("Only completed " + nsuccess + " out of " + ntrials
                    + " trials");

        }

    }

    /**
     * Concurrent stress test of {@link #test_queryJoin2()} which runs a fixed
     * number of trials on a pool of N=10 threads.
     * 
     * @throws Exception
     */
    public void test_queryJoin2_concurrentStressTest() throws Exception {
        
        final long timeout = Long.MAX_VALUE; // ms

        final int ntrials = 1000;

        final int poolSize = 10;

        doStressTest(timeout, ntrials, poolSize);

    }

    /**
     * 
     * @param timeout
     * @param ntrials
     * @param poolSize
     * @return The #of successful trials.
     * @throws Exception
     */
    protected int doStressTest(final long timeout, final int ntrials,
            final int poolSize) throws Exception {

        // start time in nanos.
        final long begin = System.nanoTime();

        // timeout in nanos.
        final long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);
        
        final Executor service = new LatchedExecutor(jnl.getExecutorService(),
                poolSize);
        
        final List<FutureTask<Void>> futures = new LinkedList<FutureTask<Void>>();
        
        for (int i = 0; i < ntrials; i++) {

            final int trial = i;
            final FutureTask<Void> ft = new FutureTask<Void>(new Runnable() {
                public void run() {
                    try {
                        if (log.isInfoEnabled())
                            log.info("trial=" + trial);
                        test_query_join2();
                    } catch (Throwable t) {
                        // log error.
                        log.error("trial=" + trial + " : " + t, t);
                        // wrap exception.
                        throw new RuntimeException(t);
                    }
                }
            }, (Void) null);

            futures.add(ft);
            
            service.execute(ft);

        }

        int nerror = 0;
        int ncancel = 0;
        int ntimeout = 0;
        int nsuccess = 0;
        int ninterrupt = 0;
        final LinkedList<ExecutionException> errors = new LinkedList<ExecutionException>();
        for (FutureTask<Void> ft : futures) {
            // remaining nanoseconds.
            final long remaining = nanos - (System.nanoTime() - begin);
            if (remaining <= 0)
                ft.cancel(true/* mayInterruptIfRunning */);
            try {
                ft.get(remaining, TimeUnit.NANOSECONDS);
                nsuccess++;
            } catch (CancellationException ex) {
                ncancel++;
            } catch (TimeoutException ex) {
                ntimeout++;
            } catch (ExecutionException ex) {
                if(InnerCause.isInnerCause(ex, InterruptedException.class)) {
                    ninterrupt++;
                } else if(InnerCause.isInnerCause(ex, CancellationException.class)) {
                    ncancel++;
                } else {
                    nerror++;
                    errors.add(ex);
                }
            }
        }

        final String msg = "nerror=" + nerror + ", ncancel=" + ncancel
                + ", ntimeout=" + ntimeout + ", ninterrupt=" + ninterrupt
                + ", nsuccess=" + nsuccess;

        if (nerror > 0) {
            // write out the stack traces for the errors.
            for (ExecutionException ex : errors) {
                log.error("STACK TRACE FOR ERROR: " + ex, ex);
            }
            // write out the summary.
            System.err.println("\n" + getClass().getName() + "." + getName()
                    + " : " + msg);
            // fail the test.
            fail(msg);
        }

        // write out the summary.
        System.err
                .println(getClass().getName() + "." + getName() + " : " + msg);

        return nsuccess;
        
    }

    /**
     * Unit test for optional join. Two joins are used and target a
     * {@link SliceOp}. The 2nd join is marked as optional. Intermediate results
     * which do not succeed on the optional join are forwarded to the
     * {@link SliceOp} which is the target specified by the
     * {@link PipelineOp.Annotations#ALT_SINK_REF}.
     */
    public void test_query_join2_optionals() throws Exception {

        final int startId = 1;
        final int joinId1 = 2;
        final int predId1 = 3;
        final int joinId2 = 4;
        final int predId2 = 5;
        final int condId = 6;
        final int sliceId = 7;
        
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(
                new IVariableOrConstant[] { x, y }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(
                new IVariableOrConstant[] { y, z }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Predicate.Annotations.OPTIONAL, true),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final PipelineOp join1Op = new PipelineJoin<E>(//
                new BOp[]{startOp},// 
                        new NV(Predicate.Annotations.BOP_ID, joinId1),//
                        new NV(PipelineJoin.Annotations.PREDICATE,pred1Op));

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { join1Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op),//
//				// constraint x == z
//				new NV(PipelineJoin.Annotations.CONSTRAINTS,
//						new IConstraint[] { Constraint.wrap(new EQ(x, z)) }),
				// join is optional.
				// optional target is the same as the default target.
				new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId));

		final PipelineOp condOp = new ConditionalRoutingOp(
				new BOp[] { join2Op }, NV.asMap(
				new NV(ConditionalRoutingOp.Annotations.BOP_ID, condId),
				new NV(ConditionalRoutingOp.Annotations.CONDITION,
						Constraint.wrap(new EQ(x, z)))
				));
		
        final PipelineOp sliceOp = new SliceOp(//
                new BOp[]{condOp},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                        new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                StandaloneChunkHandler.TEST_INSTANCE),//
                        }));

        final PipelineOp query = sliceOp;

        // start the query.
        final UUID queryId = UUID.randomUUID();
//        final IChunkMessage<IBindingSet> initialChunkMessage;
        final IBindingSet initialBindings = new ListBindingSet();
        {

//            final IBindingSet initialBindings = new HashBindingSet();

//            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

//            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
//                    queryId, startId,//
//                    -1, // partitionId
//                    AbstractQueryEngineTestCase.newBindingSetIterator(initialBindings));
        }
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialBindings);
//                initialChunkMessage);

        // verify solutions.
        {

            // the expected solutions.
            final IBindingSet[] expected = new IBindingSet[] {//
            // two solutions where the 2nd join succeeds.
            new ListBindingSet(//
                    new IVariable[] { x, y, z },//
                    new IConstant[] { new Constant<String>("Leon"),
                            new Constant<String>("Paul"),
                            new Constant<String>("Leon") }//
            ),
            new ListBindingSet(//
                    new IVariable[] { x, y, z },//
                    new IConstant[] { new Constant<String>("Paul"),
                            new Constant<String>("Leon"),
                            new Constant<String>("Paul") }//
            ),
            /*
             * No. The CONSTRAINT on the 2nd join [x == y] filters all
             * solutions. For solutions where the optional join fails, [y] is
             * not bound. Since [y] is part of the constraint on that join we DO
             * NOT observe those solutions which only join on the first access
             * path.
             */
//            // plus anything we read from the first access path which did not join.
//            new ListBindingSet(//
//                    new IVariable[] { Var.var("x"), Var.var("y") },//
//                    new IConstant[] { new Constant<String>("John"),
//                            new Constant<String>("Mary") }//
//            ),
//            new ListBindingSet(//
//                    new IVariable[] { Var.var("x"), Var.var("y") },//
//                    new IConstant[] { new Constant<String>("Mary"),
//                            new Constant<String>("Paul") }//
//            )
            };

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                    runningQuery);
        
//            new E("John", "Mary"),// [0]
//            new E("Leon", "Paul"),// [1]
//            new E("Mary", "Paul"),// [2]
//            new E("Paul", "Leon"),// [3]
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(5, statsMap.size());
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
            assertEquals(4L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the 2nd join operator.
        {
            final BOpStats stats = statsMap.get(joinId2);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join2: " + stats.toString());

            // verify query solution stats details.
//            assertEquals(1L, stats.chunksIn.get());
            assertEquals(4L, stats.unitsIn.get());
            assertEquals(4L, stats.unitsOut.get());
//            assertEquals(1L, stats.chunksOut.get());
        }
        
        // Validate stats for the sliceOp.
        {
            final BOpStats stats = statsMap.get(sliceId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("slice: " + stats.toString());

            // verify query solution stats details.
//            assertEquals(2L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
//            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * Unit test for {@link ConditionalRoutingOp}. This test case tests when the
     * condition is true (the joins are not skipped) in which case the test
     * (and results) are essentially identical to test_query_join2(). 
     */
    public void test_query_join2_conditionalRoutingTrue() throws Exception {
        int startId = 1;
        int joinId1 = 2;
        int joinId2 = 3;
        
        IConstraint condition = Constraint.wrap(new EQConstant(Var.var("x"), new Constant<String>("Mary")));
        IRunningQuery runningQuery = initQueryWithConditionalRoutingOp(condition, startId, joinId1, joinId2);

        // verify solutions.
        {
            // the expected solution.
            final IBindingSet[] expected = new IBindingSet[] {//
            new ListBindingSet(//
                    new IVariable[] { Var.var("x"), Var.var("y"), Var.var("z")},//
                    new IConstant[] { new Constant<String>("Mary"),
                            new Constant<String>("Paul"),
                            new Constant<String>("Leon")}//
            ) };

            AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            log.info(statsMap.toString());
            assertEquals(5, statsMap.size());
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
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // validate the stats for the 2nd join operator.
        {
            final BOpStats stats = statsMap.get(joinId2);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join2: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }
    
    /**
     * Unit test for {@link ConditionalRoutingOp}. This test case tests when the
     * condition is false (the joins are skipped).
     */
    public void test_query_join2_conditionalRoutingFalse() throws Exception {
        int startId = 1;
        int joinId1 = 2;
        int joinId2 = 3;
        
        // 'x' is actually bound to "Mary" so this condition will be false.
        IConstraint condition = Constraint.wrap(new EQConstant(Var.var("x"), new Constant<String>("Fred")));
        
        IRunningQuery runningQuery = initQueryWithConditionalRoutingOp(condition, startId, joinId1, joinId2);

        // verify solutions.
        {
            // the expected solution.
            final IBindingSet[] expected = new IBindingSet[] {//
            new ListBindingSet(//
                    new IVariable[] { Var.var("x")},//
                    new IConstant[] { new Constant<String>("Mary")}//
            ) };

            AbstractQueryEngineTestCase.assertSameSolutions(expected, runningQuery);
        
        }

        // Wait until the query is done.
        runningQuery.get();
        final Map<Integer, BOpStats> statsMap = runningQuery.getStats();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            log.info(statsMap.toString());
            assertEquals(5, statsMap.size());
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
        
        // validate the stats for the 1st join operator. This will have been skipped
        // and so the counts will be 0.
        {
            final BOpStats stats = statsMap.get(joinId1);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join1: " + stats.toString());

            // verify query solution stats details.
            assertEquals(0L, stats.chunksIn.get());
            assertEquals(0L, stats.unitsIn.get());
            assertEquals(0L, stats.unitsOut.get());
            assertEquals(0L, stats.chunksOut.get());
        }

        // validate the stats for the 2nd join operator.  This will have been skipped
        // and so the counts will be 0.
        {
            final BOpStats stats = statsMap.get(joinId2);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join2: " + stats.toString());

            // verify query solution stats details.
            assertEquals(0L, stats.chunksIn.get());
            assertEquals(0L, stats.unitsIn.get());
            assertEquals(0L, stats.unitsOut.get());
            assertEquals(0L, stats.chunksOut.get());
        }
    }
    
    /**
     * Helper method to initialize a BOp tree that includes a ConditionalRoutinOp
     * 
     * @param condition the condition to be tested in the ConditionalalRoutingOp
     * @param startId   the bopId of the startOp
     * @param joinId1   the bopId of the first join
     * @param joinId2   the bopId of the second join
     * 
     * @return          the RunningQuery created
     * 
     * @throws Exception
     */
    private IRunningQuery initQueryWithConditionalRoutingOp(IConstraint condition, int startId, int joinId1, int joinId2) throws Exception {
        final int predId1 = 10;
        final int predId2 = 11;
        final int condId = 12;
        final int sliceId = 13;

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        final Predicate<?> pred1Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("x"), Var.var("y") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId1),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final Predicate<?> pred2Op = new Predicate<E>(new IVariableOrConstant[] {
                Var.var("y"), Var.var("z") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId2),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));
        
        final ConditionalRoutingOp cond = new ConditionalRoutingOp(new BOp[]{startOp},
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID,condId),
                            new NV(PipelineOp.Annotations.SINK_REF, joinId1),
                            new NV(PipelineOp.Annotations.ALT_SINK_REF, sliceId),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, condition),
                        }));
        
		final PipelineOp join1Op = new PipelineJoin<E>(//
				new BOp[] { cond }, //
				new NV(Predicate.Annotations.BOP_ID, joinId1),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred1Op));

		final PipelineOp join2Op = new PipelineJoin<E>(//
				new BOp[] { join1Op },//
				new NV(Predicate.Annotations.BOP_ID, joinId2),//
				new NV(PipelineJoin.Annotations.PREDICATE, pred2Op));
        
        final PipelineOp sliceOp = new SliceOp(//
                        new BOp[]{join2Op},
                        NV.asMap(new NV[] {//
                                new NV(BOp.Annotations.BOP_ID, sliceId),//
                                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER),//
                                new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                                new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,false),//
                                new NV(QueryEngine.Annotations.CHUNK_HANDLER,
                                        StandaloneChunkHandler.TEST_INSTANCE),//
                                }));

        final PipelineOp query = sliceOp;

        // start the query.
        final UUID queryId = UUID.randomUUID();
//        final IChunkMessage<IBindingSet> initialChunkMessage;
        final IBindingSet initialBindings = new ListBindingSet();
        {            
//            final IBindingSet initialBindings = new HashBindingSet();
            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

//            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
//                    queryId, startId,//
//                    -1, // partitionId
//                    AbstractQueryEngineTestCase.newBindingSetIterator(initialBindings));
        }
        
        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                initialBindings);
//                initialChunkMessage);
        
        return runningQuery;
    }

    /**
     * Test the LatchExecutor to verify reliable progression with multiple threads contending
     * to add tasks.
     * 
     * @throws InterruptedException
     */
	public void testLatchExecutorProgression() throws InterruptedException {

	    final LatchedExecutor latched = new LatchedExecutor(
				Executors.newCachedThreadPool(), 1);

		final Semaphore sem = new Semaphore(1);

		final Runnable task = new Runnable() {

			@Override
			public void run() {

				sem.release();

			}

		};

		// Without the fix to the LatchedExecutor.scheduleNext this deadlocks in less than
		//	10,000 iterations
		for (int n = 0; n < (1 * 1024 * 1024); n++) { // try 1 million iterations
			// System.err.println("Iteration: " + n);
			
			sem.acquire();

			latched.execute(task);

		}

	}
}
