/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 11, 2011
 */

package com.bigdata.bop.join;

import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.R;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Test suite for {@link HashIndexOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHashIndexOp extends TestCase2 {

    /**
     * 
     */
    public TestHashIndexOp() {
    }

    /**
     * @param name
     */
    public TestHashIndexOp(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;

    }

    private Journal jnl;

    private QueryEngine queryEngine;

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

        final String namespace = "ns";

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
        rel
                .insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

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

    static private ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSets) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSets);

    }
    
    /**
     * Correct rejection tests for the constructor (must run on the controller,
     * parallelism is not allowed since the solutions must be written onto an
     * HTree and that is not thread safe for mutation, join variables may be
     * empty but not null, selected may be null, solution set name must be
     * specified; last pass semantics must be requested, etc).
     */
    public void test_hashIndexOp() {

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[]{Var.var("x")};

        @SuppressWarnings("rawtypes")
        final IVariable[] selected = new IVariable[]{Var.var("y")};
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                queryId, solutionSetName, joinVars);

        new HashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selected),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Must run on the query controller.
        try {
            new HashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
//                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HashIndexOp.Annotations.SELECT, selected),//
                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Parallel evaluation is not permitted since operator writes on HTree.
        try {
            new HashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
//                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HashIndexOp.Annotations.SELECT, selected),//
                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Last pass evaluation must be requested since operator defers outputs
        // until all inputs have been consumed.
        try {
            new HashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HashIndexOp.Annotations.SELECT, selected),//
                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // OPTIONAL semantics are supported.
        new HashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.OPTIONAL, true),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selected),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Join vars must be specified.
        try {
            new HashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HashIndexOp.Annotations.OPTIONAL, false),//
//                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HashIndexOp.Annotations.SELECT, selected),//
                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalStateException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Join vars may be an empty [].
        new HashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, new IVariable[] {}),//
                new NV(HashIndexOp.Annotations.SELECT, selected),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        
        // The selected variables annotation is optional.
        new HashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, new IVariable[] {}),//
                new NV(HashIndexOp.Annotations.SELECT, null),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        
        // The solution set name must be specified.
        try {
            new HashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HashIndexOp.Annotations.SELECT, selected)//
//                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalStateException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
    }

    /**
     * A simple test of a {@link HashIndexOp} followed by a
     * {@link SolutionSetHashJoinOp}. In practice we should never follow the
     * {@link HashIndexOp} immediately with a {@link SolutionSetHashJoinOp} as
     * this is basically a complex NOP. However, this does provide a simple test
     * of the most basic mechanisms for those two operators.
     * 
     * TODO Variant with non-empty join vars.
     * 
     * TODO Variant with SELECT projects only the selected variables.
     */
    public void test_hashIndexOp_01() throws Exception {

        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[]{};
        
        @SuppressWarnings("rawtypes")
        final IVariable[] selectVars = null;
        
        final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                queryId, solutionSetName, joinVars);

        final HashIndexOp op = new HashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HashIndexOp.Annotations.OPTIONAL, false),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        final SolutionSetHashJoinOp op2 = new SolutionSetHashJoinOp(
                new BOp[] { op },//
                new NV(BOp.Annotations.BOP_ID, 2),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(SolutionSetHashJoinOp.Annotations.OPTIONAL, op.isOptional()),//
                new NV(SolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                new NV(SolutionSetHashJoinOp.Annotations.RELEASE, true),//
                new NV(SolutionSetHashJoinOp.Annotations.LAST_PASS, true),//
                new NV(SolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        final PipelineOp query = op2;

        // The source solutions.
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        final IBindingSet[] bindingSets1 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<String>("Leon"));
            bindingSets1[0] = tmp;
        }
        
        final IBindingSet[] bindingSets2 = new IBindingSet[1];
        {
            final IBindingSet tmp = new ListBindingSet();
            tmp.set(x, new Constant<String>("Mary"));
            tmp.set(y, new Constant<String>("John"));
            bindingSets2[0] = tmp;
        }
        
        // the expected solutions.
        final IBindingSet[] expected = new IBindingSet[] {//
        new ListBindingSet(//
            new IVariable[] { x },//
            new IConstant[] { new Constant<String>("Leon") }//
            ), //
        new ListBindingSet(//
            new IVariable[] { x, y },//
            new IConstant[] { new Constant<String>("Mary"), 
                new Constant<String>("John") }//
        ),//
        };

        final IRunningQuery runningQuery = queryEngine.eval(queryId, query,
                newBindingSetIterator(new IBindingSet[][] { bindingSets1,
                        bindingSets2 }));

        // verify solutions.
        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                runningQuery);
        
    }

}
