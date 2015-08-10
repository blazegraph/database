/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.INamedSolutionSetRef;

/**
 * Test suite for {@link HTreeHashIndexOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestHTreeHashIndexOp.java 6010 2012-02-10 20:11:20Z thompsonbry $
 */
public class TestJVMHashIndexOp extends HashIndexOpTestCase {

    /**
     * 
     */
    public TestJVMHashIndexOp() {
    }

    /**
     * @param name
     */
    public TestJVMHashIndexOp(String name) {
        super(name);
    }

    @Override
    protected HashIndexOp newHashIndexOp(final String namespace,
            final BOp[] args, final NV... anns) {

        return new JVMHashIndexOp(args, anns);

    }
    
    @Override
    protected SolutionSetHashJoinOp newSolutionSetHashJoinOp(final BOp[] args,
            final NV... anns) {

        return new JVMSolutionSetHashJoinOp(args, anns);

    }

    /**
     * Correct rejection tests for the constructor (must run on the controller,
     * parallelism is not allowed since the solutions must be written onto an
     * HTree and that is not thread safe for mutation, join variables may be
     * empty but not null, selected may be null, solution set name must be
     * specified; last pass semantics must be requested, etc).
     */
    public void test_hashIndexOp_ctor() {

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[]{Var.var("x")};

        @SuppressWarnings("rawtypes")
        final IVariable[] selected = new IVariable[]{Var.var("y")};
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName, joinVars);

        new HTreeHashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Must run on the query controller.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
//                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Parallel evaluation is not permitted since operator writes on HTree.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
//                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Last pass evaluation must be requested since operator defers outputs
        // until all inputs have been consumed.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // OPTIONAL semantics are supported.
        new HTreeHashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Join vars must be specified.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
//                    new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalStateException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        // Join vars may be an empty [].
        new HTreeHashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, new IVariable[] {}),//
                new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        
        // The selected variables annotation is optional.
        new HTreeHashIndexOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, new IVariable[] {}),//
                new NV(HTreeHashIndexOp.Annotations.SELECT, null),//
                new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        
        // The solution set name must be specified.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected)//
//                    new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalStateException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
    }

}
