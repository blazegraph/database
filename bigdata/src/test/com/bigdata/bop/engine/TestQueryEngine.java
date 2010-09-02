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

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineStartOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Test suite for the {@link QueryEngine} against a local database instance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test core implementations of each {@link BOp} separately, but have a
 *       test suite here for distributed execution of potentially distributed
 *       operations including: JOIN (selective and unselective), DISTINCT access
 *       path, DISTINCT solutions, GROUP BY, ORDER BY.
 * 
 * @todo test suite for join evaluation against a {@link Journal}.
 * 
 * @todo test suite for join evaluation against an {@link EmbeddedFederation}
 *       with 1DS.
 * 
 * @todo test suite for join evaluation against an {@link EmbeddedFederation}
 *       with 2DS.
 * 
 * @todo test suite for join evaluation against an {@link JiniFederation} with
 *       2DS.
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
    ManagedBufferService bufferService;
    QueryEngine queryEngine;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);
        
        bufferService = new ManagedBufferService();

        queryEngine = new QueryEngine(null/* fed */, jnl, bufferService);
        
        queryEngine.init();

    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     */
    private void loadData(final Journal store) {

        // create the relation.
        final R rel = new R(store, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert.
        final E[] a = {//
                new E("John", "Mary"),// 
                new E("Mary", "Paul"),// 
                new E("Paul", "Leon"),// 
                new E("Leon", "Paul"),// 
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

        if (bufferService != null) {
            bufferService.shutdownNow();
            bufferService = null;
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
        final BindingSetPipelineOp query = new PipelineStartOp(new BOp[] {}, NV
                .asMap(new NV[] {//
                new NV(Predicate.Annotations.BOP_ID, startId),//
                }));

        final long queryId = 1L;
        final long readTimestamp = ITx.READ_COMMITTED;
        final long writeTimestamp = ITx.UNISOLATED;
        final RunningQuery runningQuery = queryEngine.eval(queryId,
                readTimestamp, writeTimestamp, query);

        runningQuery.startQuery(new BindingSetChunk(
                        queryId,
                        startId,//
                        -1, //partitionId
                        new ThickAsynchronousIterator<IBindingSet[]>(
                                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet()} })));

        // Wait until the query is done.
        final Map<Integer, BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(1, statsMap.size());
            System.err.println(statsMap.toString());
        }

        // validate the query solution stats.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            System.err.println(stats.toString());

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

            assertSolutions(expected, runningQuery.iterator());
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
        final BindingSetPipelineOp query = new PipelineJoin(
        // left
                new PipelineStartOp(new BOp[] {}, NV.asMap(new NV[] {//
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
                        })),
                // join annotations
                NV
                        .asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                                joinId),//
                        })//
        );

        // the expected solution (just one).
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
                new IVariable[] { Var.var("value") },//
                new IConstant[] { new Constant<String>("Paul") }//
        ) };

        final long queryId = 1L;
        final long readTimestamp = ITx.READ_COMMITTED;
        final long writeTimestamp = ITx.UNISOLATED;
        final RunningQuery runningQuery = queryEngine.eval(queryId,
                readTimestamp, writeTimestamp, query);

        runningQuery.startQuery(new BindingSetChunk(queryId, startId,//
                -1, // partitionId
                newBindingSetIterator(new HashBindingSet())));

        // verify solutions.
        assertSolutions(expected, runningQuery.iterator());

        // Wait until the query is done.
        final Map<Integer,BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(2, statsMap.size());
            System.err.println(statsMap.toString());
        }

        // validate the stats for the start operator.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            System.err.println("start: "+stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

//        // validate the stats for the access path.
//        {
//            final BOpStats stats = statsMap.get(predId);
//            assertNotNull(stats);
//            System.err.println("pred : "+stats.toString());
//
//            // verify query solution stats details.
//            assertEquals(1L, stats.chunksIn.get());
//            assertEquals(1L, stats.unitsIn.get());
//            assertEquals(1L, stats.unitsOut.get());
//            assertEquals(1L, stats.chunksOut.get());
//        }

        // validate the stats for the join operator.
        {
            final BOpStats stats = statsMap.get(joinId);
            assertNotNull(stats);
            System.err.println("join : "+stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

    }

    /**
     * @todo Test the ability close the iterator draining a result set before
     *       the query has finished executing and verify that the query is
     *       correctly terminated [this is difficult to test without having
     *       significant data scale since there is an implicit race between the
     *       consumer and the producer to close out the query evaluation].
     */
    public void test_query_closeIterator() {

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
        
        final BindingSetPipelineOp startOp = new PipelineStartOp(new BOp[] {},
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
                }));
        
        final BindingSetPipelineOp join1Op = new PipelineJoin(startOp, pred1Op,
                NV.asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                        joinId1),//
                        }));

        final BindingSetPipelineOp join2Op = new PipelineJoin(join1Op, pred2Op,
                NV.asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                        joinId2),//
                        }));
        
        final BindingSetPipelineOp query = join2Op;

        final long queryId = 1L;
        final long readTimestamp = ITx.READ_COMMITTED;
        final long writeTimestamp = ITx.UNISOLATED;
        final RunningQuery runningQuery = queryEngine.eval(queryId,
                readTimestamp, writeTimestamp, query);

        // start the query.
        {
         
            final IBindingSet initialBindings = new HashBindingSet();
            
            initialBindings.set(Var.var("x"), new Constant<String>("Mary"));

            runningQuery.startQuery(new BindingSetChunk(queryId, startId,//
                    -1, // partitionId
                    newBindingSetIterator(initialBindings)));
        }

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

            assertSolutions(expected, runningQuery.iterator());
        
        }

        // Wait until the query is done.
        final Map<Integer, BOpStats> statsMap = runningQuery.get();
        {
            // validate the stats map.
            assertNotNull(statsMap);
            assertEquals(3, statsMap.size());
            System.err.println(statsMap.toString());
        }

        // validate the stats for the start operator.
        {
            final BOpStats stats = statsMap.get(startId);
            assertNotNull(stats);
            System.err.println("start: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
        }

        // // validate the stats for the access path.
        // {
        // final BOpStats stats = statsMap.get(predId);
        // assertNotNull(stats);
        // System.err.println("pred : "+stats.toString());
        //
        // // verify query solution stats details.
        // assertEquals(1L, stats.chunksIn.get());
        // assertEquals(1L, stats.unitsIn.get());
        // assertEquals(1L, stats.unitsOut.get());
        // assertEquals(1L, stats.chunksOut.get());
        // }

        // validate the stats for the 1st join operator.
        {
            final BOpStats stats = statsMap.get(joinId1);
            assertNotNull(stats);
            System.err.println("join1: " + stats.toString());

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
            System.err.println("join2: " + stats.toString());

            // verify query solution stats details.
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(1L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
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

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     * @param itr
     */
    static public void assertSolutions(final IBindingSet[] expected,
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

}
