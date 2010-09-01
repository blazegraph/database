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

package com.bigdata.bop.join;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineStartOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Unit tests for the {@link PipelineJoin} operator.
 * <p>
 * Note: The operators to map binding sets over shards are tested independently.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Write unit tests where the sink is closed by the consumer while the
 *       join is still running. For this test we need more data, whether in the
 *       source binding sets or in the access path. Joins are pretty quick so it
 *       is really difficult to test this outside of a stress test. BSBM is a
 *       good way to validate this (run the qualification trials).
 * 
 * @todo Write unit tests for star-joins (in their own test suite and ideally
 *       factor them out from the standard {@link PipelineJoin} operator).
 */
public class TestPipelineJoin extends TestCase2 {

    /**
     * 
     */
    public TestPipelineJoin() {
    }

    /**
     * @param name
     */
    public TestPipelineJoin(String name) {
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
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

        loadData(jnl);

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

        if (jnl != null) {

            jnl.destroy();
         
            jnl = null;
            
        }

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single
     * {@link IBindingSet}.
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
     * Explore how we could setup a unit test without using an access path for
     * this query, or better yet, for a single join operator from this query.
     * That would probably have to happen at the ChunkTask level since the
     * AccessPathTask is going to apply an {@link IBindingSet} to the
     * {@link IPredicate} to read on an {@link IAccessPath}.
     * 
     * <pre>
     * :- ..., POS(A loves B), SPO(B loves C).
     * 
     *      and the following intermediate results from the POS shard:
     * 
     *      B0:[A=John, B=Mary, ...]
     *      B1:[A=Mary, B=Paul, ...]
     *      B2:[A=Paul, B=Leon, ...]
     *      B3:[A=Leon, B=Paul, ...]
     * 
     *      and the following tuples read from the SPO shard:
     * 
     *      T0:(John loves Mary)
     *      T1:(Mary loves Paul)
     *      T2:(Paul loves Leon)
     *      T3:(Leon loves Paul)
     * 
     *      then we have the following joins:
     * 
     *      (T2, B3) // T2:(Paul loves Leon) with B3:[A=Leon, B=Paul, ...].
     *      (T3, B2) // T3:(Leon loves Leon) with T2:[A=Paul, B=Leon, ...].
     * </pre>
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_pipelineJoin() throws InterruptedException, ExecutionException {

        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final PipelineJoin query = new PipelineJoin(
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

        final PipelineJoinStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet()} });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                null/* fed */, jnl/* indexManager */,
                ITx.READ_COMMITTED/* readTimestamp */,
                ITx.UNISOLATED/* writeTimestamp */, -1/* partitionId */, stats,
                source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSolutions(expected, sink.iterator());
//        final IAsynchronousIterator<IBindingSet[]> itr = sink.iterator();
//        try {
//            int n = 0;
//            while (itr.hasNext()) {
//                final IBindingSet[] chunk = itr.next();
//                if (log.isInfoEnabled())
//                    log.info(n + " : chunkSize=" + chunk.length);
//                for (int i = 0; i < chunk.length; i++) {
//                    assertTrue(expected[n++].equals(chunk[i]));
//                }
//            }
//            assertEquals(n, expected.length);
//        } finally {
//            itr.close();
//        }

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(1L, stats.unitsIn.get());
        assertEquals(1L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(1L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(1L, stats.elementCount.get());
        
        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

    }

    /**
     * Unit tests for optional joins. For an optional join, an alternative sink
     * may be specified in the {@link BOpContext}. When specified, it is used if
     * the join fails (if not specified, the binding sets which do not join are
     * forwarded to the primary sink). Binding sets which join go to the primary
     * sink regardless.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     * @todo write a unit test for an {@link IConstraint} first.
     * 
     * @todo test w/ and w/o the alternative sink.
     */
    public void test_optionalJoin() throws InterruptedException, ExecutionException {
        
//        final int startId = 1;
//        final int joinId = 2;
//        final int predId = 3;
//        final PipelineJoin query = new PipelineJoin(
//        // left
//                new PipelineStartOp(new BOp[] {}, NV.asMap(new NV[] {//
//                        new NV(Predicate.Annotations.BOP_ID, startId),//
//                        })),
//                // right
//                new Predicate<E>(new IVariableOrConstant[] {
//                        new Constant<String>("Mary"), Var.var("value") }, NV
//                        .asMap(new NV[] {//
//                                new NV(Predicate.Annotations.RELATION_NAME,
//                                        new String[] { namespace }),//
//                                new NV(Predicate.Annotations.PARTITION_ID,
//                                        Integer.valueOf(-1)),//
//                                new NV(Predicate.Annotations.OPTIONAL,
//                                        Boolean.FALSE),//
//                                new NV(Predicate.Annotations.CONSTRAINT, null),//
//                                new NV(Predicate.Annotations.EXPANDER, null),//
//                                new NV(Predicate.Annotations.BOP_ID, predId),//
//                        })),
//                // join annotations
//                NV.asMap(new NV[] {//
//                        new NV(PipelineJoin.Annotations.BOP_ID, joinId),//
//                        new NV(PipelineJoin.Annotations.OPTIONAL, Boolean.TRUE),//
//                        })//
//        );
//
//        // the expected solution (just one).
//        final IBindingSet[] expected = new IBindingSet[] {//
//        new ArrayBindingSet(//
//                new IVariable[] { Var.var("value") },//
//                new IConstant[] { new Constant<String>("Paul") }//
//        ) };
//
//        final PipelineJoinStats stats = query.newStats();
//
//        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
//                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet()} });
//
//        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();
//
//        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
//                null/* fed */, jnl/* indexManager */,
//                ITx.READ_COMMITTED/* readTimestamp */,
//                ITx.UNISOLATED/* writeTimestamp */, -1/* partitionId */, stats,
//                source, sink, null/* sink2 */);
//
//        // get task.
//        final FutureTask<Void> ft = query.eval(context);
//        
//        // execute task.
//        jnl.getExecutorService().execute(ft);
//
//        final IAsynchronousIterator<IBindingSet[]> itr = sink.iterator();
//        try {
//            int n = 0;
//            while (itr.hasNext()) {
//                final IBindingSet[] chunk = itr.next();
//                if (log.isInfoEnabled())
//                    log.info(n + " : chunkSize=" + chunk.length);
//                for (int i = 0; i < chunk.length; i++) {
//                    assertTrue(expected[n++].equals(chunk[i]));
//                }
//            }
//        assertEquals(n, expected.length);
//        } finally {
//            itr.close();
//        }
//
//        // join task
//        assertEquals(1L, stats.chunksIn.get());
//        assertEquals(1L, stats.unitsIn.get());
//        assertEquals(1L, stats.unitsOut.get());
//        assertEquals(1L, stats.chunksOut.get());
//        // access path
//        assertEquals(0L, stats.accessPathDups.get());
//        assertEquals(1L, stats.accessPathCount.get());
//        assertEquals(1L, stats.chunkCount.get());
//        assertEquals(1L, stats.elementCount.get());
//        
//        assertTrue(ft.isDone());
//        assertFalse(ft.isCancelled());
//        ft.get(); // verify nothing thrown.

        fail("write test");
        
    }
    
    public void test_joinConstraint() {
        fail("write test");
    }
    
    public void test_joinDistinctFilter() {
        fail("write test");
    }
    
    public void test_joinOtherFilter() {
        fail("write test");
    }
    
}
