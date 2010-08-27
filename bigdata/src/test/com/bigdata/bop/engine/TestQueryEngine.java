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

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.EmptyBindingSet;
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
import com.bigdata.bop.ap.TestPredicateAccessPath;
import com.bigdata.bop.engine.QueryEngine.BindingSetChunk;
import com.bigdata.bop.engine.QueryEngine.RunningQuery;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedIterator;

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
 * @todo test suite for join evaluation against a {@link Journal} [One of the
 *       big open questions for me is how we will handle purely local evaluation
 *       versus evaluation against a distributed cluster. I can see two
 *       approaches. One is to do a IQueryEngine interface and an implementation
 *       of that interface for a Journal which is purely local and either uses
 *       BlockingBuffers (or BlockingQueue) to transfer data (effectively just
 *       buffering the data in on the Java heap as normal objects). I think that
 *       it will be better in the long run if we can have the same evaluation
 *       mechanisms in place for standalone and distributed query processing as
 *       long as we do not have to marshall binding sets when we are not going
 *       across a network boundary since we will otherwise benefit from having
 *       identical APIs for evaluation of local and distributed query
 *       operations.
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

    static private final int nThreads = 10;

    static private final String namespace = "ns";
    Journal jnl;
    ManagedBufferService bufferService;
    QueryEngine queryEngine;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);
        
        bufferService = new ManagedBufferService();

        queryEngine = new QueryEngine(null/* fed */, jnl, bufferService,
                nThreads);
        
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

        if (queryEngine != null)
            queryEngine.shutdownNow();

        if (bufferService != null)
            bufferService.shutdownNow();

        if (jnl != null)
            jnl.destroy();

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
        final RunningQuery<?> runningQuery = queryEngine.eval(queryId,
                readTimestamp, writeTimestamp, query);

        runningQuery
                .add(new BindingSetChunk(
                        startId,//
                        -1, //partitionId
                        new ThickAsynchronousIterator<IBindingSet[]>(
                                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet()} })));

        // Wait until the query is done.
        final BOpStats stats = runningQuery.get();

        assertEquals(1L, stats.chunksIn.get());
        assertEquals(0L, stats.unitsIn.get());
        assertEquals(0L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        
        // Verify no results.
        final IChunkedIterator<IBindingSet> itr = runningQuery.iterator();
        try {
            if (itr.hasNext())
                fail("Not expecting any solutions");
        } finally {
            itr.close();
        }
        
    }

    /**
     * Test the ability run a simple join. There are three operators. One feeds
     * an empty binding set[] into the join, another is the predicate for the
     * access path on which the join will read, and the third is the join
     * itself.
     * 
     * @throws Exception
     * 
     * @todo setup and run bops against it w/ filters, sorts, joins (we can join
     *       with an incoming binding set easily enough using only a single
     *       primary index), distincts, selecting only certain columns, etc.
     * 
     * @todo Add asserts for the statistics reported for the query.
     * 
     * @see TestPredicateAccessPath
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
        final RunningQuery<?> runningQuery = queryEngine.eval(queryId,
                readTimestamp, writeTimestamp, query);

        runningQuery
                .add(new BindingSetChunk(
                        startId,//
                        -1, //partitionId
                        newBindingSetIterator(EmptyBindingSet.INSTANCE)));

        final IChunkedIterator<IBindingSet> itr = runningQuery.iterator();
        try {
            int n = 0;
            while (itr.hasNext()) {
                final IBindingSet e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : " + e);
                assertEquals(expected[n], e);
                n++;
            }
        } finally {
            itr.close();
        }

        // Wait until the query is done.
        final BOpStats stats = runningQuery.get();

        assertEquals(1L, stats.chunksIn.get());
        assertEquals(4L, stats.unitsIn.get());
        assertEquals(1L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());

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
    public void test_query_join1_distinct() {
        
        fail("write test");
        
    }

    /**
     * @todo Test the ability run a query requiring two joins.
     */
    public void test_query_join2() {
        
        fail("write test");

    }

    /**
     * @todo Test optional joins (left joins in which the join succeeds even if
     *       the constraint fails). Optional joins designate an "optional"
     *       target (also known as an optional "goto"), which is the operator to
     *       which the source binding set is relayed if the join does not
     *       succeed.
     * 
     * @todo The target for the optional may be another join or a non-join
     *       ancestor of the current join, such as a DISTINCT, SORT, GROUP_BY
     *       operator. When there is no such operator, should the binding set be
     *       relayed to the client running the query or should we create a
     *       operator which stands in for the total query? [The former I think
     *       since it is more direct and we can assign a reserved id for the
     *       client.]
     * 
     * @todo The use optional gotos are in use introduces a race condition for
     *       per join chunk task processing. This race condition is probably
     *       present in standalone query execution, but it is most clearly a
     *       problem in scale-out. If the #of active tasks for a given join is
     *       not correctly coordinated with the client there is the danger that
     *       a query may terminate too early (because the optional goto target
     *       is done with the binding sets routed directly to it before it
     *       receives binding sets which route through the intermediate joins).
     *       There is also a danger that the query may not terminate cleanly
     *       (because the optional goto target may receive a chunk of binding
     *       sets after the query was terminated). These issues will be
     *       difficult to detect without carefully crafted stress tests.
     *       Luckily, the BSBM query mixes are reasonably well suited to bring
     *       out such problems.
     */
    public void test_query_join2_optionals() {

        fail("write test");

    }

}
