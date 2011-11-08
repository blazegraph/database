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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.BlockingBufferWithStats;
import com.bigdata.bop.engine.MockRunningQuery;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

/**
 * Unit tests for {@link HTreeDistinctBindingSetsOp}.
 * 
 * FIXME This test suite need to be refactored since this operator is now RDF
 * specific.
 * 
 * FIXME Write a unit test in which some variables are unbound; verify that only
 * the variables which are being made DISTINCT are projected.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestDistinctBindingSets.java 4259 2011-02-28 16:24:53Z
 *          thompsonbry $
 */
public class TestDistinctBindingSetsWithHTree extends TestCase2 {

    /**
     * 
     */
    public TestDistinctBindingSetsWithHTree() {
    }

    /**
     * @param name
     */
    public TestDistinctBindingSetsWithHTree(String name) {
        super(name);
    }
    
    /**
     * Setup for a problem used by many of the join test suites.
     */
    static protected class Setup {

        protected final String namespace;

        protected final IV<?, ?> brad, john, fred, jane, mary, paul, leon;

        protected final List<IBindingSet> data;

        public Setup(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();
            
            this.namespace = namespace;

            brad = makeIV(new LiteralImpl("Brad"));
            
            john = makeIV(new LiteralImpl("John"));

            fred = makeIV(new LiteralImpl("Fred"));

            jane = makeIV(new LiteralImpl("Jane "));

            mary = makeIV(new LiteralImpl("Mary"));

            paul = makeIV(new LiteralImpl("Paul"));

            leon = makeIV(new LiteralImpl("Leon"));
            
            final Var<?> x = Var.var("x");
            final Var<?> y = Var.var("y");

            data = new LinkedList<IBindingSet>();
            IBindingSet bset = null;
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(john));
                bset.set(y, new Constant<IV>(mary));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(mary));
                bset.set(y, new Constant<IV>(paul));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(mary));
                bset.set(y, new Constant<IV>(jane));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(paul));
                bset.set(y, new Constant<IV>(leon));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(paul));
                bset.set(y, new Constant<IV>(john));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<IV>(leon));
                bset.set(y, new Constant<IV>(paul));
                data.add(bset);
            }

        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

    }

//    @Override
//    public Properties getProperties() {
//
//        final Properties p = new Properties(super.getProperties());
//
//        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//                .toString());
//
//        return p;
//        
//    }

//    Journal jnl = null;

    protected Setup setup;
    
    public void setUp() throws Exception {

//        jnl = new Journal(getProperties());
        
        setup = new Setup(getName());

    }
    
    public void tearDown() throws Exception {

//        if (jnl != null) {
//            jnl.destroy();
//            jnl = null;
//        }
//        
        // clear reference.
        setup = null;

    }
    
    public void test_ctor_correctRejection() {
    	
        final Var<?> x = Var.var("x");

        final int distinctId = 1;

        final UUID queryId = UUID.randomUUID();
        
        final IVariable<?>[] vars = new IVariable[] { x };

        // w/o variables.
        try {
        new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                            new NamedSolutionSetRef(queryId, getName(), vars)),//
//                    new NV(DistinctBindingSetOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/ illegal evaluation context.
        try {
        new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                            new NamedSolutionSetRef(queryId, getName(), vars)),//
                    new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.ANY),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // w/ illegal evaluation context.
        try {
        new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                            new NamedSolutionSetRef(queryId, getName(), vars)),//
                    new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.SHARDED),//
                    new NV(PipelineOp.Annotations.SHARED_STATE,
                            true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/o named solution set reference.
        try {
        new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
//                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.NAMED_SET_REF,
//                            new NamedSolutionSetRef(queryId, getName(), vars)),//
                    new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
                            1),//
                }));
        fail("Expecting: "+IllegalStateException.class);
        } catch(IllegalStateException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

        // w/o maxParallel := 1.
        try {
        new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                            new NamedSolutionSetRef(queryId, getName(), vars)),//
                    new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
//                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
//                            1),//
                }));
        fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

    }

    /**
     * Unit test for distinct.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_distinctBindingSets() throws InterruptedException,
            ExecutionException {

        final UUID queryId = UUID.randomUUID();

        final Var<?> x = Var.var("x");
//        final Var<?> y = Var.var("y");
        
        final IVariable<?>[] vars = new IVariable[]{x};
        
        final int distinctId = 1;
        
        final HTreeDistinctBindingSetsOp query = new HTreeDistinctBindingSetsOp(new BOp[]{},
                NV.asMap(new NV[]{//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.BOP_ID,distinctId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.VARIABLES,vars),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                            new NamedSolutionSetRef(queryId, getName(), vars)),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
					new NV(PipelineOp.Annotations.SHARED_STATE, true),//
					new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                }));
        
        // the expected solutions
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
                new IVariable[] { x },//
                new IConstant[] { new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ), new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.paul) }//
                ), new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.leon) }//
                ), };

        final MockQueryContext queryContext = new MockQueryContext(queryId);
        try {
            
            final BOpStats stats = query.newStats();

            final IAsynchronousIterator<IBindingSet[]> source = new ThickAsynchronousIterator<IBindingSet[]>(
                    new IBindingSet[][] { setup.data.toArray(new IBindingSet[0]) });

            final IBlockingBuffer<IBindingSet[]> sink = new BlockingBufferWithStats<IBindingSet[]>(
                    query, stats);

            final BOpContext<IBindingSet> context = new BOpContext<IBindingSet>(
                    new MockRunningQuery(null/* fed */, null/* indexManager */,
                            queryContext),
                    -1/* partitionId */, stats, source, sink, null/* sink2 */);

            // get task.
            final FutureTask<Void> ft = query.eval(context);

            // execute task.
            // jnl.getExecutorService().execute(ft);
            ft.run();

            AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder("", expected,
                    sink.iterator(), ft);

            // assertTrue(ft.isDone());
            // assertFalse(ft.isCancelled());
            // ft.get(); // verify nothing thrown.

            assertEquals(1L, stats.chunksIn.get());
            assertEquals(6L, stats.unitsIn.get());
            assertEquals(4L, stats.unitsOut.get());
            assertEquals(1L, stats.chunksOut.get());

        } finally {
        
            queryContext.close();
            
        }

    }

}
