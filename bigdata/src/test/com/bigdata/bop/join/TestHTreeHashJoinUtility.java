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
 * Created on Oct 14, 2011
 */

package com.bigdata.bop.join;

import java.util.List;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for the {@link HTreeHashJoinUtility}.
 * 
 * TODO Unit test to verify vectoring of left solutions having the same hash
 * code (and look at whether we can vector optional solutions
 * as well).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeHashJoinUtility extends AbstractHashJoinUtilityTestCase {

    /**
     * 
     */
    public TestHTreeHashJoinUtility() {
    }

    /**
     * @param name
     */
    public TestHTreeHashJoinUtility(String name) {
        super(name);
    }
    
    private MemoryManager mmgr;

    @Override
    protected void tearDown() throws Exception {

        if (mmgr != null) {
            mmgr.clear();
            mmgr = null;
        }

        super.tearDown();

    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
    
        /*
         * This wraps an efficient raw store interface around a
         * child memory manager created from the IMemoryManager
         * which is backing the query.
         */
        
        mmgr = new MemoryManager(DirectBufferPool.INSTANCE);

    }

    /**
     * Test helper.
     * 
     * @param optional
     * @param joinVars
     * @param selectVars
     * @param left
     * @param right
     * @param expected
     */
    protected void doHashJoinTest(//
            final boolean optional,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final List<IBindingSet> left, //
            final List<IBindingSet> right,//
            final IBindingSet[] expected//
            ) {

        // Setup a mock PipelineOp for the test.
        final PipelineOp op = new PipelineOp(BOp.NOARGS, NV.asMap(//
                new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                        new String[] { getName() }),//
                new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                new NV(JoinAnnotations.SELECT, selectVars),//
                new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                )) {

            private static final long serialVersionUID = 1L;

            @Override
            public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
                throw new UnsupportedOperationException();
            }

        };

        final HTreeHashJoinUtility state = new HTreeHashJoinUtility(mmgr, op,
                optional);

        try {

            // Load the right solutions into the HTree.
            {

                final BOpStats stats = new BOpStats();

                state.acceptSolutions(
                        new Chunkerator<IBindingSet>(right.iterator()), stats);

                assertEquals(right.size(), state.getRightSolutionCount());

                assertEquals(right.size(), stats.unitsIn.get());

            }

            /*
             * Run the hash join.
             */

            final ICloseableIterator<IBindingSet> leftItr = new CloseableIteratorWrapper<IBindingSet>(
                    left.iterator());

            // Buffer used to collect the solutions.
            final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();

            // Compute the "required" solutions.
            state.hashJoin(leftItr, outputBuffer, true/* leftIsPipeline */);

            if (optional) {

                // Output the optional solutions.
                state.outputOptionals(outputBuffer);

            }

            // Verify the expected solutions.
            assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());

        } finally {

            state.release();

        }

    }

    /**
     * FIXME Write a unit test which verifies that the ivCache is used and that
     * the cached {@link BigdataValue}s are correctly restored when the
     * rightSolutions had cached values and the leftSolutions did not have a
     * value cached for the same IVs. For example, this could be done with a
     * cached value on an IV which is not a join variable and which is only
     * present in the rightSolutions. 
     */
    public void test_cache() {
        
        fail("write test");
        
    }
    
}
