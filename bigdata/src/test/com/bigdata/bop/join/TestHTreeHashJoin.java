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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.controller.SubqueryHashJoinOp;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.TestQueryEngine;
import com.bigdata.bop.solutions.MockQueryContext;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Unit tests for the {@link HTreeHashJoinOp} operator.
 * <p>
 * Note: The logic to map binding sets over shards is tested independently.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeHashJoin extends TestCase2 {

    /**
     * 
     */
    public TestHTreeHashJoin() {
    }

    /**
     * @param name
     */
    public TestHTreeHashJoin(String name) {
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

        // data to insert (in key order for convenience).
        final E[] a = {//
                new E("Paul", "Mary"),// [0]
                new E("Paul", "Brad"),// [1]
                
                new E("John", "Mary"),// [2]
                new E("John", "Brad"),// [3]
                
                new E("Mary", "Brad"),// [4]
                
                new E("Brad", "Fred"),// [5]
                new E("Brad", "Leon"),// [6]
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

    protected PipelineOp newJoin(final BOp[] args, final int joinId,
            final IVariable<E>[] joinVars,
            final Predicate<E> predOp,
            final NV... annotations) {

        final Map<String,Object> tmp = NV.asMap(
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER), //
                new NV(Predicate.Annotations.BOP_ID, joinId),//
                new NV(PipelineJoin.Annotations.PREDICATE, predOp),//
                new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.MAX_MEMORY, Bytes.megabyte)//
                );

        if (annotations != null) {
         
            for (NV nv : annotations) {
            
                tmp.put(nv.getName(), nv.getValue());
                
            }
            
        }
        
        final PipelineOp joinOp = new HTreeHashJoinOp<E>(args, tmp);

        return joinOp;
        
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_correctRejection() {
        
        final BOp[] emptyArgs = new BOp[]{};
        final int joinId = 1;
        final int predId = 2;
        @SuppressWarnings("unchecked")
        final IVariable<E> x = Var.var("x");

        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Mary"), Var.var("x") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));
        
        // w/o variables.
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
//                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // bad evaluation context.
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.ANY),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // missing predicate.
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
//                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // shared state not specified.
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
//                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // maxParallel not set to ONE (1).
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 2),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // maxMemory not overridden.
        try {
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                            new NV(PipelineOp.Annotations.MAX_MEMORY,
//                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // maxMemory != MAX_LONG and predicate is OPTIONAL
        try {
            @SuppressWarnings("unchecked")
            final Predicate<E> pred2 = (Predicate<E>) pred.setProperty(
                    IPredicate.Annotations.OPTIONAL, true);
            new HTreeHashJoinOp<E>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred2),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
    }
    
    /**
     * Unit test for a simple join. There are two source solutions. Each binds
     * the join variable (there is only one join variable, which is [x]). The
     * access path is run once and visits two elements, yielding as-bound
     * solutions. The hash map containing the buffered source solutions is
     * probed and the as-bound solutions which join are written out.
     */
    public void test_join_simple()
            throws InterruptedException, ExecutionException {

        final int joinId = 2;
        final int predId = 3;
        @SuppressWarnings("unchecked")
        final IVariable<E> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<E> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<E>[] joinVars = new IVariable[] { x };
        
		final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
				new Constant<String>("John"), x }, NV
				.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars, predOp);

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Brad"),
                                          new Constant<String>("Fred"),
                                }//
                ),//
        };

        /*
         * Setup the input binding sets. Each input binding set MUST provide
         * binding for the join variable(s).
         */
        final IBindingSet[] initialBindingSets;
        {
            final List<IBindingSet> list = new LinkedList<IBindingSet>();
            
            IBindingSet tmp;

            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Brad"));
            tmp.set(y, new Constant<String>("Fred"));
            list.add(tmp);
            
            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, jnl/* indexManager */,
                            queryContext), -1/* partitionId */, stats, source,
                    sink, null/* sink2 */);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */
            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            jnl.getExecutorService().execute(ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            // join task
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
            // access path
            assertEquals(0L, stats.accessPathDups.get());
            assertEquals(1L, stats.accessPathCount.get());
            assertEquals(1L, stats.accessPathChunksIn.get());
            assertEquals(2L, stats.accessPathUnitsIn.get());

        } finally {

            queryContext.close();
            
        }

    }

    public void test_join_simple_withConstraint()
            throws InterruptedException, ExecutionException {

        final int joinId = 2;
        final int predId = 3;
        @SuppressWarnings("unchecked")
        final IVariable<E> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<E> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<E>[] joinVars = new IVariable[] { x };
        
        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("John"), x }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars, predOp, 
                new NV(SubqueryHashJoinOp.Annotations.CONSTRAINTS,
                        new IConstraint[] { Constraint
                                .wrap(new EQConstant(x, new Constant<String>("Brad"))),//
                        }));

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
//                new ArrayBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Mary") }//
//                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<String>("Brad"),
                                          new Constant<String>("Fred"),
                                }//
                ),//
        };

        /*
         * Setup the input binding sets. Each input binding set MUST provide
         * binding for the join variable(s).
         */
        final IBindingSet[] initialBindingSets;
        {
            final List<IBindingSet> list = new LinkedList<IBindingSet>();
            
            IBindingSet tmp;

            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Brad"));
            tmp.set(y, new Constant<String>("Fred"));
            list.add(tmp);
            
            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, jnl/* indexManager */,
                            queryContext), -1/* partitionId */, stats, source,
                    sink, null/* sink2 */);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */
            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            jnl.getExecutorService().execute(ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            // join task
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(1L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
            // access path
            assertEquals(0L, stats.accessPathDups.get());
            assertEquals(1L, stats.accessPathCount.get());
            assertEquals(1L, stats.accessPathChunksIn.get());
            assertEquals(2L, stats.accessPathUnitsIn.get());

        } finally {

            queryContext.close();
            
        }

    }

    public void test_join_simple_selectOnly_x()
            throws InterruptedException, ExecutionException {

        final int joinId = 2;
        final int predId = 3;
        @SuppressWarnings("unchecked")
        final IVariable<E> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<E> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<E>[] joinVars = new IVariable[] { x };
        
        final Predicate<E> predOp = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("John"), x }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { namespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars,
                predOp,//
                new NV(SubqueryHashJoinOp.Annotations.SELECT,
                        new IVariable[] { x })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ),//
                new ArrayBindingSet(//
                        new IVariable[] { x/*, y*/ },//
                        new IConstant[] { new Constant<String>("Brad"),
//                                          new Constant<String>("Fred"),
                                }//
                ),//
        };

        /*
         * Setup the input binding sets. Each input binding set MUST provide
         * binding for the join variable(s).
         */
        final IBindingSet[] initialBindingSets;
        {
            final List<IBindingSet> list = new LinkedList<IBindingSet>();
            
            IBindingSet tmp;

            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Brad"));
            tmp.set(y, new Constant<String>("Fred"));
            list.add(tmp);
            
            tmp = new HashBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, jnl/* indexManager */,
                            queryContext), -1/* partitionId */, stats, source,
                    sink, null/* sink2 */);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */
            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            jnl.getExecutorService().execute(ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            // join task
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(2L, stats.unitsIn.get());
            assertEquals(2L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
            // access path
            assertEquals(0L, stats.accessPathDups.get());
            assertEquals(1L, stats.accessPathCount.get());
            assertEquals(1L, stats.accessPathChunksIn.get());
            assertEquals(2L, stats.accessPathUnitsIn.get());

        } finally {

            queryContext.close();
            
        }

    }

    /**
     * Unit tests for optional joins, including a constraint on solutions which
     * join.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_optionalJoin_and_constraint() throws InterruptedException, ExecutionException {

        final int joinId = 2;
        final int predId = 3;

        @SuppressWarnings("unchecked")
        final Var<E> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<E>[] joinVars = new IVariable[]{x};
        
        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Paul"), x }, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { namespace }),//
                new NV(Predicate.Annotations.BOP_ID, predId),//
                new NV(Predicate.Annotations.OPTIONAL, Boolean.TRUE),//
                // constraint x != Luke
                new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        new IConstraint[] { Constraint.wrap(new NEConstant(x,
                                new Constant<String>("Luke"))) }),
                new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
        }));
        
        final PipelineOp query = newJoin(
                new BOp[] { }, // args
                joinId,
                joinVars,
                pred,
                // Note: memory can not be constrained since predicate is optional.
                new NV(PipelineOp.Annotations.MAX_MEMORY,Long.MAX_VALUE)
                );

        /**
         * Setup the source.
         * 
         * bset1: This has nothing bound and can not join since the join
         * variable is not bound. However, it is passed along any as an
         * "optional" solution.
         * 
         * bset2: This has x:=Luke, which does not join. However, this is an
         * optional join so x:=Luke should be output anyway. There is a
         * constraint that x!= Luke, but that constraint does not fail the
         * solution because it is an optional solution.
         * 
         * bset3: This has x:=Mary, which joins and is output as a solution.
         *  
         * <pre>
         *                 new E("Paul", "Mary"),// [0]
         *                 new E("Paul", "Brad"),// [1]
         *                 
         *                 new E("John", "Mary"),// [2]
         *                 new E("John", "Brad"),// [3]
         *                 
         *                 new E("Mary", "Brad"),// [4]
         *                 
         *                 new E("Brad", "Fred"),// [5]
         *                 new E("Brad", "Leon"),// [6]
         * </pre>
         */
        final IAsynchronousIterator<IBindingSet[]> source;
        {
         
            final IBindingSet bset1 = new HashBindingSet();
            
            final IBindingSet bset2 = new HashBindingSet();
            {
             
                bset2.set(x, new Constant<String>("Luke"));
                
            }
            
            final IBindingSet bset3 = new HashBindingSet();
            {
             
                bset3.set(x, new Constant<String>("Mary"));
                
            }
            
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2,
                            bset3 } });
        }

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                // bset1: optional solution.
                new ArrayBindingSet(//
                        new IVariable[] { },//
                        new IConstant[] {}//
                ),//
                // bset2: optional solution.
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Luke") }//
                ),//
                  // bset3: joins.
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ),//
        };

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {

            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, jnl/* indexManager */,
                            queryContext), -1/* partitionId */, stats, source,
                    sink, null/* sink2 */);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */
            context.setLastInvocation();
            
            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            jnl.getExecutorService().execute(ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            // join task
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(3L, stats.unitsIn.get());
            assertEquals(3L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());
            // access path
            assertEquals(0L, stats.accessPathDups.get());
            assertEquals(1L, stats.accessPathCount.get());
            assertEquals(1L, stats.accessPathChunksIn.get());
            assertEquals(2L, stats.accessPathUnitsIn.get());

        } finally {

            queryContext.close();
            
        }
        
    }

    /**
     * Unit test for an optional {@link PipelineJoin} when the
     * {@link BOpContext#getSink2() alternative sink} is specified (simple
     * variant of the unit test above).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_optionalJoin_withAltSink() throws InterruptedException,
            ExecutionException {

        final int joinId = 2;
        final int predId = 3;

        @SuppressWarnings("unchecked")
        final Var<E> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<E>[] joinVars = new IVariable[]{x};
        
        final Predicate<E> pred = new Predicate<E>(new IVariableOrConstant[] {
                new Constant<String>("Paul"), x }, NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { namespace }),//
                new NV(Predicate.Annotations.BOP_ID, predId),//
                new NV(Predicate.Annotations.OPTIONAL, Boolean.TRUE),//
                // constraint x != Luke
                new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        new IConstraint[] { Constraint.wrap(new NEConstant(x,
                                new Constant<String>("Luke"))) }),
                new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
        }));
        
        final PipelineOp query = newJoin(
                new BOp[] { }, // args
                joinId,
                joinVars,
                pred,
                // Note: memory can not be constrained since predicate is optional.
                new NV(PipelineOp.Annotations.MAX_MEMORY,Long.MAX_VALUE)
                );

        /**
         * Setup the source.
         * 
         * bset1: This has nothing bound and can not join since the join
         * variable is not bound. However, it is passed along any as an
         * "optional" solution.
         * 
         * bset2: This has x:=Luke, which does not join. However, this is an
         * optional join so x:=Luke should be output anyway. There is a
         * constraint that x!= Luke, but that constraint does not fail the
         * solution because it is an optional solution.
         * 
         * bset3: This has x:=Mary, which joins and is output as a solution.
         *  
         * <pre>
         *                 new E("Paul", "Mary"),// [0]
         *                 new E("Paul", "Brad"),// [1]
         *                 
         *                 new E("John", "Mary"),// [2]
         *                 new E("John", "Brad"),// [3]
         *                 
         *                 new E("Mary", "Brad"),// [4]
         *                 
         *                 new E("Brad", "Fred"),// [5]
         *                 new E("Brad", "Leon"),// [6]
         * </pre>
         */
        final IAsynchronousIterator<IBindingSet[]> source;
        {
         
            final IBindingSet bset1 = new HashBindingSet();
            
            final IBindingSet bset2 = new HashBindingSet();
            {
             
                bset2.set(x, new Constant<String>("Luke"));
                
            }
            
            final IBindingSet bset3 = new HashBindingSet();
            {
             
                bset3.set(x, new Constant<String>("Mary"));
                
            }
            
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2,
                            bset3 } });
        }

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
//                // bset1: optional solution.
//                new ArrayBindingSet(//
//                        new IVariable[] { },//
//                        new IConstant[] {}//
//                ),//
//                // bset2: optional solution.
//                new ArrayBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Luke") }//
//                ),//
                  // bset3: joins.
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Mary") }//
                ),//
        };

        // the expected solutions for the alternative sink (the optional solutions).
        final IBindingSet[] expected2 = new IBindingSet[] {//
                // bset1: optional solution.
                new ArrayBindingSet(//
                        new IVariable[] { },//
                        new IConstant[] {}//
                ),//
                // bset2: optional solution.
                new ArrayBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<String>("Luke") }//
                ),//
        };

        final UUID queryId = UUID.randomUUID();
        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {

            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final IBlockingBuffer<IBindingSet[]> sink2 = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, jnl/* indexManager */,
                            queryContext), -1/* partitionId */, stats, source,
                    sink, sink2);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */
            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            jnl.getExecutorService().execute(ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            TestQueryEngine.assertSameSolutionsAnyOrder(expected2,
                    sink2.iterator(), ft);

            // join task
            assertEquals(1L, stats.chunksIn.get());
            assertEquals(3L, stats.unitsIn.get());
            assertEquals(3L, stats.unitsOut.get());
            assertEquals(2L, stats.chunksOut.get());
            // access path
            assertEquals(0L, stats.accessPathDups.get());
            assertEquals(1L, stats.accessPathCount.get());
            assertEquals(1L, stats.accessPathChunksIn.get());
            assertEquals(2L, stats.accessPathUnitsIn.get());

        } finally {

            queryContext.close();
            
        }
        
    }

}
