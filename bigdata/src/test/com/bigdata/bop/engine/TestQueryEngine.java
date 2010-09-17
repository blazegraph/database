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
 * Created on Aug 23, 2010
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.fed.TestFederatedQueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SliceOp.SliceStats;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.ibm.icu.impl.ByteBuffer;

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
 * @version $Id$
 * 
 * @see TestFederatedQueryEngine
 * 
 * @todo write a unit and stress tests for deadlines.
 */
public class TestQueryEngine extends TestCase2 {

    /**
     * 
     */
    public TestQueryEngine() {
    }

    /**
     * @param name
     */
    public TestQueryEngine(String name) {
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
    Journal jnl;
    QueryEngine queryEngine;
    
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
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSets
     *            the binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[] bindingSets) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { bindingSets });

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);

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
                        startId, -1/* partitionId */,
                        newBindingSetIterator(new HashBindingSet())));

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
            // the expected solution (just one empty binding set).
            final IBindingSet[] expected = new IBindingSet[] {//
            new HashBindingSet() //
            };

            assertSameSolutions(expected, runningQuery.iterator());
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
        final BindingSetPipelineOp query = new PipelineJoin<E>(
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
                                new NV(Predicate.Annotations.PARTITION_ID,
                                        Integer.valueOf(-1)),//
                                new NV(Predicate.Annotations.OPTIONAL,
                                        Boolean.FALSE),//
                                new NV(Predicate.Annotations.CONSTRAINT, null),//
                                new NV(Predicate.Annotations.EXPANDER, null),//
                                new NV(Predicate.Annotations.BOP_ID, predId),//
                                new NV(Predicate.Annotations.TIMESTAMP,ITx.READ_COMMITTED),//
                        })),
                // join annotations
                NV.asMap(new NV[] { //
                        new NV(Predicate.Annotations.BOP_ID, joinId),//
                        })//
        );

        // the expected solution (just one).
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("Paul") }//
        ) };

        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1 /* partitionId */,
                        newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        assertSameSolutions(expected, runningQuery.iterator());

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
     */
    public void test_query_join1_multipleChunksIn() throws Exception {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;

        /*
         * Enforce a constraint on the source such that it hands 3 each source
         * chunk to the join operator as a separate chunk
         * 
         * @todo This is not enough to force the query engine to run the join
         * operator once per source chunk. Instead, it takes the output of the
         * source operator, which is N chunks, and sends them all to a single
         * invocation of the join task. To do better than that we have to send
         * multiple chunk messages rather than just one.
         */
        final int nsources = 3;
        final StartOp startOp = new StartOp(new BOp[] {}, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),//
                new NV(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY, nsources),//
                }));

        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                x, y }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID, Integer
                                .valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL, Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Predicate.Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));
        
        final PipelineJoin<E> joinOp = new PipelineJoin<E>(
                startOp/* left */, predOp/* right */,
                // join annotations
                NV.asMap(new NV[] { //
                        new NV(Predicate.Annotations.BOP_ID, joinId),//
//                        new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),//
//                        new NV(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY, 1),//
                        })//
        );
        
        final int sliceId = 4;
        final SliceOp sliceOp = new SliceOp(new BOp[] { joinOp },
                // slice annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, sliceId),//
                        })//
        );
        
        final BindingSetPipelineOp query = sliceOp;

        /*
         * Source binding sets.
         * 
         * Note: We can't bind y in advance for the primary index!
         */
        final IBindingSet[] source = new IBindingSet[] {//
                new HashBindingSet(new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Paul") }//
                )),//
                new HashBindingSet(new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Leon") }//
                )),
                new HashBindingSet(new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                )),
        };
        // Put each source binding set into a chunk by itself.
        final IBindingSet[][] sources = new IBindingSet[source.length][];
        for (int i = 0; i < sources.length; i++) {
            sources[i] = new IBindingSet[] { source[i] };
        }
        assertEquals(nsources,source.length);
        assertEquals(nsources,sources.length);

//        new E("John", "Mary"),// [0]
//        new E("Leon", "Paul"),// [1]
//        new E("Mary", "Paul"),// [2]
//        new E("Paul", "Leon"),// [3]
        
        // the expected solution (just one).
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Paul"), new Constant<String>("Leon") }//
        ),//
        new ArrayBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Leon"), new Constant<String>("Paul") }//
        ),
        new ArrayBindingSet(//
                new IVariable[] { x, y },//
                new IConstant[] { //
                new Constant<String>("Mary"), new Constant<String>("Paul") }//
        ),
        };

        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1 /* partitionId */,
                        newBindingSetIterator(sources)));

        // verify solutions.
        assertSameSolutionsAnyOrder(expected, new Dechunkerator<IBindingSet>(
                runningQuery.iterator()));

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
                }));

        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                x, y }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.PARTITION_ID, Integer
                                .valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL, Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT, null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Predicate.Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));

        final PipelineJoin<E> joinOp = new PipelineJoin<E>(startOp/* left */,
                predOp/* right */,
                // join annotations
                NV.asMap(new NV[] { //
                        new NV(Predicate.Annotations.BOP_ID, joinId),//
                        })//
        );

        final BindingSetPipelineOp query = new SliceOp(new BOp[] { joinOp },
        // slice annotations
                NV.asMap(new NV[] { //
                        new NV(BOp.Annotations.BOP_ID, sliceId),//
                                new NV(SliceOp.Annotations.OFFSET, 0L),//
                                new NV(SliceOp.Annotations.LIMIT, 2L),//
                        })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("John"),
                                new Constant<String>("Mary") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ) };

        final UUID queryId = UUID.randomUUID();
        final RunningQuery runningQuery = queryEngine.eval(queryId, query,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                        startId, -1 /* partitionId */,
                        newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        assertSameSolutions(expected, runningQuery.iterator());

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

        // validate the stats for the join operator.
        {
            final BOpStats stats = statsMap.get(joinId);
            assertNotNull(stats);
            if (log.isInfoEnabled())
                log.info("join : " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(4L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

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
                        new NV(Predicate.Annotations.PARTITION_ID, Integer
                                .valueOf(-1)),//
                        new NV(Predicate.Annotations.OPTIONAL, Boolean.FALSE),//
                        new NV(Predicate.Annotations.CONSTRAINT,null),//
                        new NV(Predicate.Annotations.EXPANDER, null),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Predicate.Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                        new NV(Predicate.Annotations.KEY_ORDER,
                                R.primaryKeyOrder),//
                }));

        final PipelineJoin<E> joinOp = new PipelineJoin<E>(startOp/* left */,
                predOp/* right */,
                // join annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, joinId),//
                        // impose constraint on the join.
                        new NV(PipelineJoin.Annotations.CONSTRAINTS,
                                new IConstraint[] { new EQConstant(y,
                                        new Constant<String>("Paul")) }),//
                        })//
        );
        
        final BindingSetPipelineOp query = new SliceOp(new BOp[] { joinOp },
        // slice annotations
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, sliceId),//
                        })//
        );

        // the expected solutions (order is not reliable due to concurrency).
        final IBindingSet[] expected = new IBindingSet[] {//
//                new ArrayBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("John"),
//                                new Constant<String>("Mary") }//
//                ), //
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Leon"),
                                new Constant<String>("Paul") }//
                ), // 
//                new ArrayBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("Mary"),
//                                new Constant<String>("John") }//
//                ), //
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Mary"),
                                new Constant<String>("Paul") }//
                ), //
//                new ArrayBindingSet(//
//                        new IVariable[] { x, y },//
//                        new IConstant[] { new Constant<String>("Paul"),
//                                new Constant<String>("Leon") }//
//                ), //
        };
//        new E("John", "Mary"),// [0]
//        new E("Leon", "Paul"),// [1]
//        new E("Mary", "Paul"),// [2]
//        new E("Paul", "Leon"),// [3]

        final RunningQuery runningQuery;
        {
            final IBindingSet initialBindingSet = new HashBindingSet();

//            initialBindingSet.set(y, new Constant<String>("Paul"));
            
            final UUID queryId = UUID.randomUUID();

            runningQuery = queryEngine.eval(queryId, query,
                    new LocalChunkMessage<IBindingSet>(queryEngine, queryId,
                            startId,//
                            -1, /* partitionId */
                            newBindingSetIterator(initialBindingSet)));
        }

        // verify solutions.
        TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                new Dechunkerator<IBindingSet>(runningQuery.iterator()));

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
        
        final BindingSetPipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        }));
        
        // @todo the KEY_ORDER should be bound before evaluation.
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

        final BindingSetPipelineOp query = join2Op;

        // start the query.
        final UUID queryId = UUID.randomUUID();
        final IChunkMessage<IBindingSet> initialChunkMessage;
        {

            final IBindingSet initialBindings = new HashBindingSet();

            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            initialChunkMessage = new LocalChunkMessage<IBindingSet>(queryEngine,
                    queryId, startId,//
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
            ) };

            assertSameSolutions(expected, runningQuery.iterator());
        
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
     * A stress test to look for an occasional non-termination in
     * {@link #test_query_join2()}.
     * 
     * @throws Exception 
     */
    public void test_queryJoin2_stressTest() throws Exception {
        
        final long timeout = TimeUnit.MINUTES.toMillis(1);

        final int ntrials = 1000;

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
     * Concurrent stress test.
     * 
     * @throws Exception
     */
    public void test_queryJoin2_concurrentStressTest() throws Exception {
        
        final long timeout = 5000; // ms

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
                    } catch (Exception e) {
                        // wrap exception.
                        throw new RuntimeException(e);
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
                nerror++;
            }
        }

        final String msg = "nerror=" + nerror + ", ncancel=" + ncancel
                + ", ntimeout=" + ntimeout + ", nsuccess=" + nsuccess;

        System.err
                .println(getClass().getName() + "." + getName() + " : " + msg);

        if (nerror > 0)
            fail(msg);

        return nsuccess;
        
    }
    
    /**
     * @todo Write unit tests for optional joins, including where an alternative
     *       sink is specified in the {@link BOpContext} and is used when the
     *       join fails.
     */
    public void test_query_join2_optionals() {

        fail("write test");

    }

    /**
     * @todo Write unit tests for the {@link ConditionalRoutingOp}?
     */
    public void test_query_join2_conditionalRouting() {

        fail("write test");

    }

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     * @param itr
     */
    static public void assertSameSolutions(final IBindingSet[] expected,
            final IAsynchronousIterator<IBindingSet[]> itr) {
        try {
            int n = 0;
            while (itr.hasNext()) {
                final IBindingSet[] e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : chunkSize=" + e.length);
                for (int i = 0; i < e.length; i++) {
                    if (log.isInfoEnabled())
                        log.info(n + " : " + e[i]);
                    if (n >= expected.length) {
                        fail("Willing to deliver too many solutions: n=" + n
                                + " : " + e[i]);
                    }
                    if (!expected[n].equals(e[i])) {
                        fail("n=" + n + ", expected=" + expected[n]
                                + ", actual=" + e[i]);
                    }
                    n++;
                }
            }
            assertEquals("Wrong number of solutions", expected.length, n);
        } finally {
            itr.close();
        }
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final T[] expected,
            final Iterator<T> actual) {

        assertSameSolutionsAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final String msg,
            final T[] expected, final Iterator<T> actual) {

        try {

            // Populate a map that we will use to realize the match and
            // selection without replacement logic.

            final int nrange = expected.length;

            final java.util.Map<T, T> range = new java.util.HashMap<T, T>();

            for (int j = 0; j < nrange; j++) {

                range.put(expected[j], expected[j]);

            }

            // Do selection without replacement for the objects visited by
            // iterator.

            for (int j = 0; j < nrange; j++) {

                if (!actual.hasNext()) {

                    fail(msg
                            + ": Iterator exhausted while expecting more object(s)"
                            + ": index=" + j);

                }

                final T actualObject = actual.next();

                if (range.remove(actualObject) == null) {

                    fail("Object not expected" + ": index=" + j + ", object="
                            + actualObject);

                }

            }

            if (actual.hasNext()) {

                fail("Iterator will deliver too many objects.");

            }

        } finally {

            if (actual instanceof ICloseableIterator<?>) {

                ((ICloseableIterator<T>) actual).close();

            }

        }

    }

}
