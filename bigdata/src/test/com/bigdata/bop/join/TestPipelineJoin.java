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
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bset.CopyBindingSetOp;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
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
                new E("Mary", "John"),// 
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
     * Unit test for a pipeline join.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_join() throws InterruptedException, ExecutionException {

        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final PipelineJoin<E> query = new PipelineJoin<E>(
        // left
                new CopyBindingSetOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(new IVariableOrConstant[] {
                        new Constant<String>("Mary"), Var.var("x") }, NV
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
                                new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        })),
                // join annotations
                NV
                        .asMap(new NV[] { new NV(Predicate.Annotations.BOP_ID,
                                joinId),//
                        })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("x") },//
                        new IConstant[] { new Constant<String>("John") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("x") },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ),//
        };

        final PipelineJoinStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet()} });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */
                ), -1/* partitionId */, stats,
                source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(1L, stats.unitsIn.get());
        assertEquals(2L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(1L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(2L, stats.elementCount.get());
        
        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

    }

    /**
     * Unit test for a join with an {@link IConstraint}. The constraint is used
     * to filter out one of the solutions where "Mary" is the present in the
     * first column of the relation.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_join_constraint() throws InterruptedException, ExecutionException {

//        final Var<String> x = Var.var("x");
        final Var<String> y = Var.var("y");
        final IConstant<String>[] set = new IConstant[]{
                new Constant<String>("John"),//
        };
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final PipelineJoin<E> query = new PipelineJoin<E>(
        // left
                new CopyBindingSetOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(BOpBase.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(
                        new IVariableOrConstant[] { new Constant<String>("Mary"), y },//
                        NV.asMap(new NV[] {//
                                        new NV(
                                                Predicate.Annotations.RELATION_NAME,
                                                new String[] { namespace }),//
                                        new NV(
                                                Predicate.Annotations.PARTITION_ID,
                                                Integer.valueOf(-1)),//
                                        new NV(Predicate.Annotations.OPTIONAL,
                                                Boolean.FALSE),//
                                        new NV(
                                                Predicate.Annotations.CONSTRAINT,
                                                null),//
                                        new NV(Predicate.Annotations.EXPANDER,
                                                null),//
                                        new NV(Predicate.Annotations.BOP_ID,
                                                predId),//
                                        new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                                })),
                // join annotations
                NV.asMap(new NV[] {//
                                new NV(BOpBase.Annotations.BOP_ID, joinId),//
                                new NV( PipelineJoin.Annotations.CONSTRAINTS,
                                        new IConstraint[] { new INBinarySearch<String>(
                                                y, set) }) })//
        );

        // the expected solution (just one).
        final IBindingSet[] expected = new IBindingSet[] {//
        new ArrayBindingSet(//
                new IVariable[] { Var.var("y") },//
                new IConstant[] { new Constant<String>("John") }//
        ) };

        final PipelineJoinStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet() } });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */),
                -1/* partitionId */, stats, source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);

        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(1L, stats.unitsIn.get());
        assertEquals(1L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(1L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(2L, stats.elementCount.get());

        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

    }

    /**
     * Unit test for a join which selects a subset of the variables to pass
     * along.
     * <p>
     * Note: The order of the expected solutions for this test depends on the
     * order of the keys associated with the tuples in the relation. Since the
     * key is [name,value], the result order is based on this tuple order:
     * 
     * <pre>
     * E("John", "Mary") 
     * E("Leon", "Paul") 
     * E("Mary", "John") 
     * E("Mary", "Paul") 
     * E("Paul", "Leon")
     * </pre>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_join_selectVariables() throws InterruptedException, ExecutionException {

        final Var<String> x = Var.var("x");
        final Var<String> y = Var.var("y");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;
        final PipelineJoin<E> query = new PipelineJoin<E>(
        // left
                new CopyBindingSetOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(BOpBase.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(
                        new IVariableOrConstant[] { x, y },//
                        NV.asMap(new NV[] {//
                                        new NV(
                                                Predicate.Annotations.RELATION_NAME,
                                                new String[] { namespace }),//
                                        new NV(
                                                Predicate.Annotations.PARTITION_ID,
                                                Integer.valueOf(-1)),//
                                        new NV(Predicate.Annotations.OPTIONAL,
                                                Boolean.FALSE),//
                                        new NV(
                                                Predicate.Annotations.CONSTRAINT,
                                                null),//
                                        new NV(Predicate.Annotations.EXPANDER,
                                                null),//
                                        new NV(Predicate.Annotations.BOP_ID,
                                                predId),//
                                        new NV(Predicate.Annotations.TIMESTAMP,
                                                ITx.READ_COMMITTED),//
                                })),
                // join annotations
                NV.asMap(new NV[] {//
                        new NV(BOpBase.Annotations.BOP_ID, joinId),//
                        new NV(PipelineJoin.Annotations.SELECT,new IVariable[]{y})//
                        })//
        );

        /*
         * The expected solutions.
         */
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("y") },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("y") },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("y") },//
                        new IConstant[] { new Constant<String>("John") }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("y") },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ),
                new ArrayBindingSet(//
                        new IVariable[] { Var.var("y") },//
                        new IConstant[] { new Constant<String>("Leon") }//
                ),
                };

        final PipelineJoinStats stats = query.newStats();

        final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { new HashBindingSet() } });

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */),
                -1/* partitionId */, stats, source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);

        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(1L, stats.unitsIn.get());
        assertEquals(5L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(1L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(5L, stats.elementCount.get());

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
     */
    public void test_optionalJoin() throws InterruptedException, ExecutionException {

        final Var<?> x = Var.var("x");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;

        final PipelineJoin<E> query = new PipelineJoin<E>(
        // left
                new CopyBindingSetOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(new IVariableOrConstant[] {
                        new Constant<String>("Mary"), x }, NV
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
                                new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        })),
                // join annotations
                NV
                        .asMap(new NV[] { //
                                new NV(BOpBase.Annotations.BOP_ID,
                                joinId),
                                new NV(PipelineJoin.Annotations.OPTIONAL,
                                        Boolean.TRUE),//
//
                        })//
        );

        /*
         * Setup the source with two initial binding sets. One has nothing bound
         * and will join with (Mary,x:=John) and (Mary,x:=Paul). The other has
         * x:=Luke which does not join. However, this is an optional join so
         * x:=Luke should output anyway.
         */
        final IAsynchronousIterator<IBindingSet[]> source;
        {
            final IBindingSet bset1 = new HashBindingSet();
            final IBindingSet bset2 = new HashBindingSet();
            {
             
                bset2.set(x, new Constant<String>("Luke"));
                
            }
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2 } });
        }

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("John") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Luke") }//
                ),//
        };

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final PipelineJoinStats stats = query.newStats();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */),
                -1/* partitionId */, stats, source, sink, null/* sink2 */);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(2L, stats.unitsIn.get());
        assertEquals(3L, stats.unitsOut.get());
        assertEquals(1L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(2L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(2L, stats.elementCount.get());
        
        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.

    }

    /**
     * Unit test for an optional {@link PipelineJoin} when the
     * {@link BOpContext#getSink2() alternative sink} is specified.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_optionalJoin_withAltSink() throws InterruptedException,
            ExecutionException {
        
        final Var<?> x = Var.var("x");
        
        final int startId = 1;
        final int joinId = 2;
        final int predId = 3;

        final PipelineJoin<E> query = new PipelineJoin<E>(
        // left
                new CopyBindingSetOp(new BOp[] {}, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, startId),//
                        })),
                // right
                new Predicate<E>(new IVariableOrConstant[] {
                        new Constant<String>("Mary"), x }, NV
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
                                new NV(Predicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                        })),
                // join annotations
                NV
                        .asMap(new NV[] { //
                                new NV(BOpBase.Annotations.BOP_ID,
                                joinId),
                                new NV(PipelineJoin.Annotations.OPTIONAL,
                                        Boolean.TRUE),//
//
                        })//
        );

        /*
         * Setup the source with two initial binding sets. One has nothing bound
         * and will join with (Mary,x:=John) and (Mary,x:=Paul). The other has
         * x:=Luke which does not join. However, this is an optional join so
         * x:=Luke should output anyway.
         */
        final IAsynchronousIterator<IBindingSet[]> source;
        {
            final IBindingSet bset1 = new HashBindingSet();
            final IBindingSet bset2 = new HashBindingSet();
            {
             
                bset2.set(x, new Constant<String>("Luke"));
                
            }
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2 } });
        }

        // the expected solutions for the default sink.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("John") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Paul") }//
                ),//
        };

        // the expected solutions for the alternative sink.
        final IBindingSet[] expected2 = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Luke") }//
                ),//
        };

        final IBlockingBuffer<IBindingSet[]> sink = query.newBuffer();

        final IBlockingBuffer<IBindingSet[]> sink2 = query.newBuffer();

        final PipelineJoinStats stats = query.newStats();

        final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                new MockRunningQuery(null/* fed */, jnl/* indexManager */),
                -1/* partitionId */, stats, source, sink, sink2);

        // get task.
        final FutureTask<Void> ft = query.eval(context);
        
        // execute task.
        jnl.getExecutorService().execute(ft);

        TestQueryEngine.assertSameSolutions(expected, sink.iterator());
        TestQueryEngine.assertSameSolutions(expected2, sink2.iterator());

        // join task
        assertEquals(1L, stats.chunksIn.get());
        assertEquals(2L, stats.unitsIn.get());
        assertEquals(3L, stats.unitsOut.get());
        assertEquals(2L, stats.chunksOut.get());
        // access path
        assertEquals(0L, stats.accessPathDups.get());
        assertEquals(2L, stats.accessPathCount.get());
        assertEquals(1L, stats.chunkCount.get());
        assertEquals(2L, stats.elementCount.get());
        
        assertTrue(ft.isDone());
        assertFalse(ft.isCancelled());
        ft.get(); // verify nothing thrown.
        
    }
    
}
