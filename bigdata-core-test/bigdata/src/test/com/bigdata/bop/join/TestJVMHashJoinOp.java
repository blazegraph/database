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

import java.util.Map;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.util.Bytes;

/**
 * Test suite for {@link JVMHashJoinOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@SuppressWarnings("rawtypes")
public class TestJVMHashJoinOp extends AbstractHashJoinOpTestCase {

    /**
     * 
     */
    public TestJVMHashJoinOp() {
        super();
    }

    /**
     * @param name
     */
    public TestJVMHashJoinOp(String name) {
        super(name);
    }

    @Override
    protected PipelineOp newJoin(final BOp[] args, final int joinId,
            final IVariable<IV>[] joinVars,
            final Predicate<IV> predOp,
            final UUID queryId,
            final NV... annotations) {
        
        final Map<String,Object> tmp = NV.asMap(
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER), //
                new NV(Predicate.Annotations.BOP_ID, joinId),//
                new NV(PipelineJoin.Annotations.PREDICATE, predOp),//
                new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
//                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.PIPELINED, false),//
                new NV(NamedSetAnnotations.NAMED_SET_REF,
                        NamedSolutionSetRefUtility.newInstance(queryId,
                                getName(), joinVars))//
                );

        if (annotations != null) {
         
            for (NV nv : annotations) {
            
                tmp.put(nv.getName(), nv.getValue());
                
            }
            
        }
        
        final PipelineOp joinOp = new JVMHashJoinOp<IV>(args, tmp);

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
        final IVariable<IV> x = Var.var("x");

        final UUID queryId = UUID.randomUUID();
        
        final IVariable[] joinVars = new IVariable[]{x};

        final NV namedSet = new NV(NamedSetAnnotations.NAMED_SET_REF,
                NamedSolutionSetRefUtility.newInstance(queryId, getName(),
                        joinVars));

        final Predicate<IV> pred = new Predicate<IV>(new IVariableOrConstant[] {
                new Constant<String>("Mary"), Var.var("x") }, NV
                .asMap(new NV[] {//
                        new NV(Predicate.Annotations.RELATION_NAME,
                                new String[] { setup.spoNamespace }),//
                        new NV(Predicate.Annotations.BOP_ID, predId),//
                        new NV(Annotations.TIMESTAMP,
                                ITx.READ_COMMITTED),//
                }));
        
        // w/o variables.
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
//                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            namedSet,//
                    }));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // bad evaluation context.
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.ANY),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            namedSet,//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // missing predicate.
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
//                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            namedSet,//
                    }));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // maxParallel not set to ONE (1).
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 2),//
                            namedSet,//
                    }));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // maxMemory is specified.
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            namedSet,//
                            new NV(PipelineOp.Annotations.MAX_MEMORY,
                                    Bytes.megabyte),//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // must be at once evaluation (pipelined:=true here).
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            namedSet,//
                    }));
            fail("Expecting: " + UnsupportedOperationException.class);
        } catch (UnsupportedOperationException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
                
        // w/o namedSet.
        try {
            new JVMHashJoinOp<IV>(emptyArgs, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(HashJoinAnnotations.JOIN_VARS,new IVariable[]{x}),//
                            new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(AccessPathJoinAnnotations.PREDICATE,pred),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                            namedSet,//
                    }));
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

}
