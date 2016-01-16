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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;

/**
 * Unit tests for {@link HTreeDistinctBindingSetsOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHTreeDistinctBindingSets extends
        AbstractDistinctSolutionsTestCase {

    /**
     * 
     */
    public TestHTreeDistinctBindingSets() {
    }

    /**
     * @param name
     */
    public TestHTreeDistinctBindingSets(String name) {
        super(name);
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
                            NamedSolutionSetRefUtility.newInstance(queryId, getName(), vars)),//
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
                            NamedSolutionSetRefUtility.newInstance(queryId, getName(), vars)),//
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
                            NamedSolutionSetRefUtility.newInstance(queryId, getName(), vars)),//
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
                            NamedSolutionSetRefUtility.newInstance(queryId, getName(), vars)),//
                    new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,new IVariable[]{x}),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
//                    new NV(PipelineOp.Annotations.MAX_PARALLEL,
//                            1),//
                }));
        fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
        	if(log.isInfoEnabled())
        		log.info("Ignoring expected exception: "+ex);
        }

    }

    @Override
    protected PipelineOp newDistinctBindingSetsOp(BOp[] args, NV... anns) {
        
        return new HTreeDistinctBindingSetsOp(args, anns);
        
    }

}
