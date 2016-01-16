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
 * Created on Oct 27, 2011
 */

package com.bigdata.bop.join;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
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
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.solutions.MockQueryContext;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Common base class for hash join with access path unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: AbstractHashJoinOpTestCase.java 5499 2011-11-03 19:49:10Z
 *          thompsonbry $
 */
@SuppressWarnings("rawtypes")
abstract public class AbstractHashJoinOpTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractHashJoinOpTestCase() {
    }

    /**
     * @param name
     */
    public AbstractHashJoinOpTestCase(String name) {
        super(name);
    }

    /**
     * Setup for a problem used by many of the join test suites.
     */
    static public class JoinSetup {

        protected final String spoNamespace;

        protected final IV<?, ?> knows, brad, john, fred, mary, paul, leon, luke;
        
        private Journal jnl;
        
        public JoinSetup(final String kbNamespace) {

            if (kbNamespace == null)
                throw new IllegalArgumentException();
            
            final Properties properties = new Properties();

            properties.setProperty(Journal.Options.BUFFER_MODE,
                    BufferMode.Transient.toString());
            
            jnl = new Journal(properties);

            // create the kb.
            final AbstractTripleStore kb = new LocalTripleStore(jnl,
                    kbNamespace, ITx.UNISOLATED, properties);

            kb.create();

            this.spoNamespace = kb.getSPORelation().getNamespace();

            // Setup the vocabulary.
            {
                final BigdataValueFactory vf = kb.getValueFactory();
                final String uriString = "http://bigdata.com/";
                final BigdataURI _knows = vf.asValue(FOAFVocabularyDecl.knows);
                final BigdataURI _brad = vf.createURI(uriString+"brad");
                final BigdataURI _john = vf.createURI(uriString+"john");
                final BigdataURI _fred = vf.createURI(uriString+"fred");
                final BigdataURI _mary = vf.createURI(uriString+"mary");
                final BigdataURI _paul = vf.createURI(uriString+"paul");
                final BigdataURI _leon = vf.createURI(uriString+"leon");
                final BigdataURI _luke = vf.createURI(uriString+"luke");

                final BigdataValue[] a = new BigdataValue[] {
                      _knows,//
                      _brad,
                      _john,
                      _fred,
                      _mary,
                      _paul,
                      _leon,
                      _luke
                };

                kb.getLexiconRelation()
                        .addTerms(a, a.length, false/* readOnly */);

                knows = _knows.getIV();
                brad = _brad.getIV();
                john = _john.getIV();
                fred = _fred.getIV();
                mary = _mary.getIV();
                paul = _paul.getIV();
                leon = _leon.getIV();
                luke = _luke.getIV();

            }

            // data to insert (in key order for convenience).
            final SPO[] a = {//
                    new SPO(paul, knows, mary, StatementEnum.Explicit),// [0]
                    new SPO(paul, knows, brad, StatementEnum.Explicit),// [1]
                    
                    new SPO(john, knows, mary, StatementEnum.Explicit),// [2]
                    new SPO(john, knows, brad, StatementEnum.Explicit),// [3]
                    
                    new SPO(mary, knows, brad, StatementEnum.Explicit),// [4]
                    
                    new SPO(brad, knows, fred, StatementEnum.Explicit),// [5]
                    new SPO(brad, knows, leon, StatementEnum.Explicit),// [6]
            };

            // insert data (the records are not pre-sorted).
            kb.addStatements(a, a.length);

            // Do commit since not scale-out.
            jnl.commit();

        }

        protected void destroy() {

            if (jnl != null) {
                jnl.destroy();
                jnl = null;
            }

        }
        
    }

    protected JoinSetup setup = null;
    
    public void setUp() throws Exception {
        
        setup = new JoinSetup(getName());
        
    }
    
    public void tearDown() throws Exception {

        if (setup != null) {
            setup.destroy();
            setup = null;
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
     * Return a new join operator instance for the test.
     * 
     * @param args
     * @param joinId
     * @param joinVars
     * @param predOp
     * @param queryId
     * @param annotations
     * @return
     */
    abstract protected PipelineOp newJoin(final BOp[] args, final int joinId,
            final IVariable<IV>[] joinVars,
            final Predicate<IV> predOp,
            final UUID queryId,
            final NV... annotations);
    
    /*
     * Tests
     */
    
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
        final IVariable<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[] { x };
        final UUID queryId = UUID.randomUUID();
        
        final Predicate<IV> predOp = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.john),
                        new Constant<IV>(setup.knows), x }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { setup.spoNamespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars,
                predOp, queryId);

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
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

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.brad));
            tmp.set(y, new Constant<IV>(setup.fred));
            list.add(tmp);
            
            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, null/* sink2 */);

//            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
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
     * Unit test for a simple join. There are two source solutions. Each binds
     * the join variable (there is only one join variable, which is [x]). The
     * access path is run once and visits two elements, yielding as-bound
     * solutions. The hash map containing the buffered source solutions is
     * probed and the as-bound solutions which join are written out.
     * <p>
     * For this variant, there are no join variables. We should get exactly the
     * same solutions but the join will do more work.
     */
    public void test_join_simple_noJoinVars()
            throws InterruptedException, ExecutionException {

        final int joinId = 2;
        final int predId = 3;
        @SuppressWarnings("unchecked")
        final IVariable<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[] { /* x */};
        final UUID queryId = UUID.randomUUID();
        
        final Predicate<IV> predOp = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.john),
                        new Constant<IV>(setup.knows), x }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { setup.spoNamespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars,
                predOp, queryId);

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
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

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.brad));
            tmp.set(y, new Constant<IV>(setup.fred));
            list.add(tmp);
            
            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, null/* sink2 */);

//            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
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
        final IVariable<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[] { x };
        final UUID queryId = UUID.randomUUID();

        final Predicate<IV> predOp = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.john),
                        new Constant<IV>(setup.knows), x }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { setup.spoNamespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars, predOp,
                queryId,//
                new NV(JoinAnnotations.CONSTRAINTS,
                        new IConstraint[] { Constraint
                                .wrap(new EQConstant(x, new Constant<IV>(setup.brad))),//
                        }));

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
//                new ListBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Mary") }//
//                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
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

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.brad));
            tmp.set(y, new Constant<IV>(setup.fred));
            list.add(tmp);
            
            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, null/* sink2 */);

//            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
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
        final IVariable<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV> y = Var.var("y");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[] { x };
        final UUID queryId = UUID.randomUUID();

        final Predicate<IV> predOp = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.john),
                        new Constant<IV>(setup.knows), x }, NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { setup.spoNamespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
                }));

        final PipelineOp query = newJoin(new BOp[] {}, joinId, joinVars,
                predOp, queryId, //
                new NV(JoinAnnotations.SELECT,
                        new IVariable[] { x })//
        );

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x/*, y*/ },//
                        new IConstant[] { new Constant<IV>(setup.brad),
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

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.brad));
            tmp.set(y, new Constant<IV>(setup.fred));
            list.add(tmp);
            
            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(setup.mary));
            list.add(tmp);
            
            initialBindingSets = list.toArray(new IBindingSet[0]);
            
        }

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { initialBindingSets });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, null/* sink2 */);

//            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
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
        final Var<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[]{x};
        final UUID queryId = UUID.randomUUID();

        // AP("Paul" ?x)
        final Predicate<IV> pred = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.paul),
                        new Constant<IV>(setup.knows), x },
                NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { setup.spoNamespace }),//
                new NV(Predicate.Annotations.BOP_ID, predId),//
                new NV(Predicate.Annotations.OPTIONAL, Boolean.TRUE),//
                // constraint x != Luke
                new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        new IConstraint[] { Constraint.wrap(new NEConstant(x,
                                new Constant<IV>(setup.luke))) }),
                new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
        }));
        
        final PipelineOp query = newJoin(
                new BOp[] { }, // args
                joinId,
                joinVars,
                pred,
                queryId
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
         */
        final IAsynchronousIterator<IBindingSet[]> source;
        {
         
            final IBindingSet bset1 = new ListBindingSet();
            
            final IBindingSet bset2 = new ListBindingSet();
            {
             
                bset2.set(x, new Constant<IV>(setup.luke));
                
            }
            
            final IBindingSet bset3 = new ListBindingSet();
            {
             
                bset3.set(x, new Constant<IV>(setup.mary));
                
            }
            
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2,
                            bset3 } });
        }

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
                // bset1: optional solution.
                new ListBindingSet(//
                        new IVariable[] { },//
                        new IConstant[] {}//
                ),//
                // bset2: optional solution.
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.luke) }//
                ),//
                  // bset3: joins.
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
        };

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {

            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, null/* sink2 */);

//            context.setLastInvocation();
            
            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
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
        final Var<IV> x = Var.var("x");
        @SuppressWarnings("unchecked")
        final IVariable<IV>[] joinVars = new IVariable[]{x};
        final UUID queryId = UUID.randomUUID();

        final Predicate<IV> pred = new Predicate<IV>(
                new IVariableOrConstant[] { new Constant<IV>(setup.paul),
                        new Constant<IV>(setup.knows), x },
                NV.asMap(new NV[] {//
                new NV(Predicate.Annotations.RELATION_NAME,
                        new String[] { setup.spoNamespace }),//
                new NV(Predicate.Annotations.BOP_ID, predId),//
                new NV(Predicate.Annotations.OPTIONAL, Boolean.TRUE),//
                // constraint x != Luke
                new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        new IConstraint[] { Constraint.wrap(new NEConstant(x,
                                new Constant<IV>(setup.luke))) }),
                new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED),//
        }));
        
        final PipelineOp query = newJoin(
                new BOp[] { }, // args
                joinId,
                joinVars,
                pred,
                queryId
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
         
            final IBindingSet bset1 = new ListBindingSet();
            
            final IBindingSet bset2 = new ListBindingSet();
            {
             
                bset2.set(x, new Constant<IV>(setup.luke));
                
            }
            
            final IBindingSet bset3 = new ListBindingSet();
            {
             
                bset3.set(x, new Constant<IV>(setup.mary));
                
            }
            
            source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { new IBindingSet[] { bset1, bset2,
                            bset3 } });
        }

        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
//                // bset1: optional solution.
//                new ListBindingSet(//
//                        new IVariable[] { },//
//                        new IConstant[] {}//
//                ),//
//                // bset2: optional solution.
//                new ListBindingSet(//
//                        new IVariable[] { x },//
//                        new IConstant[] { new Constant<String>("Luke") }//
//                ),//
                  // bset3: joins.
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
        };

        // the expected solutions for the alternative sink (the optional solutions).
        final IBindingSet[] expected2 = new IBindingSet[] {//
                // bset1: optional solution.
                new ListBindingSet(//
                        new IVariable[] { },//
                        new IConstant[] {}//
                ),//
                // bset2: optional solution.
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.luke) }//
                ),//
        };

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {

            final BaseJoinStats stats = (BaseJoinStats) query.newStats();

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final IBlockingBuffer<IBindingSet[]> sink2 = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            /*
             * Note: Since the operator relies on the isLastInvocation() test to
             * run the hash join, this is signal is required in order to have
             * the operator produce output. Otherwise it will just buffer the
             * source solutions until it exceeds its memory budget.
             */

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */,
                            setup.jnl/* indexManager */, queryContext),
                    -1/* partitionId */, stats, query/* op */,
                    true/* lastInvocation */, source, sink, sink2);

//            context.setLastInvocation();

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            setup.jnl.getExecutorService().execute(ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                    sink.iterator(), ft);

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected2,
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
