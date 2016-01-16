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
package com.bigdata.bop.join;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.INamedSolutionSetRef;

/**
 * Test suite for {@link HTreeSolutionSetHashJoinOp}.
 * 
 * @see TestHTreeHashIndexOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeSolutionSetHashJoin extends TestCase2 {

    public TestHTreeSolutionSetHashJoin() {
    }

    public TestHTreeSolutionSetHashJoin(String name) {
        super(name);
    }

    /**
     * Unit tests for the constructor.
     */
    public void test_solutionSetHashJoin_ctor() {
        
        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[]{Var.var("x")};

//        @SuppressWarnings("rawtypes")
//        final IVariable[] selected = new IVariable[]{Var.var("y")};
        
        final UUID queryId = UUID.randomUUID();
        
        final String solutionSetName = "set1";
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(queryId, solutionSetName, joinVars);

        // release requires lastPass
        // optional requires lastPass
        // lastPass is not compatible with maxParallel 
        // lastPass requires pipelined evaluation
        // selected[] is optional.
        // optional is optional, but requires lastPass.
        
        // Normal join w/o lastPass and release allows maxParallel GT 1.
        new HTreeSolutionSetHashJoinOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 5),//
                new NV(PipelineOp.Annotations.LAST_PASS, false),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, false),//
//                new NV(SolutionSetHashJoinOp.Annotations.OPTIONAL, false),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selected),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Normal join w/ lastPass and release; requires maxParallel EQ 1.
        new HTreeSolutionSetHashJoinOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, true),//
//                new NV(SolutionSetHashJoinOp.Annotations.OPTIONAL, false),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selected),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

        // Optional join; requires maxParallel EQ 1, lastPass.
        new HTreeSolutionSetHashJoinOp(BOp.NOARGS,//
                new NV(BOp.Annotations.BOP_ID, 1),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, true),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(SolutionSetHashJoinOp.Annotations.SELECT, selected),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );

/*
 Why are the next three constructors for HTreeHashIndexOp?
 Commented!

        // Must run on the query controller.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 5),//
                    new NV(PipelineOp.Annotations.LAST_PASS, false),//
//                    new NV(HTreeHashIndexOp.Annotations.RELEASE, false),//
//                    new NV(HTreeHashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        // Last pass requires maxParallel := 1.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 5),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),//
//                    new NV(HTreeHashIndexOp.Annotations.RELEASE, false),//
//                    new NV(HTreeHashIndexOp.Annotations.OPTIONAL, false),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        // Optional requires lastPass, maxParallel=1.
        try {
            new HTreeHashIndexOp(BOp.NOARGS,//
                    new NV(BOp.Annotations.BOP_ID, 1),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, false),//
//                    new NV(HTreeHashIndexOp.Annotations.RELEASE, false),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_TYPE, JoinTypeEnum.Optional),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeHashIndexOp.Annotations.SELECT, selected),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
*/
    }

}
