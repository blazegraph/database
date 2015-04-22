/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.analytics;

import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.sail.AbstractSailGraphTestCase;

/**
 * Test class for SSP traversal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSSSP extends AbstractSailGraphTestCase {

    public TestSSSP() {
        
    }
    
    public TestSSSP(String name) {
        super(name);
    }

    /**
     * Test using {@link #setupSmallGraphProblem()}
     */
    public void testSSSP() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final SailConnection cxn = getGraphFixture().getSail()
                    .getConnection();

            try {

                final IGraphAccessor graphAccessor = getGraphFixture()
                        .newGraphAccessor(cxn);

                final IGASContext<SSSP.VS, SSSP.ES, Integer> gasContext = gasEngine
                        .newGASContext(graphAccessor, new SSSP());

                final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = gasContext
                        .getGASState();

                // Initialize the froniter.
                gasState.setFrontier(gasContext, p.getMike());

                // Converge.
                gasContext.call();

                assertEquals(0.0, gasState.getState(p.getMike()).dist());

                assertEquals(1.0, gasState.getState(p.getFoafPerson()).dist());

                assertEquals(1.0, gasState.getState(p.getBryan()).dist());

                assertEquals(2.0, gasState.getState(p.getMartyn()).dist());

            } finally {

                try {
                    cxn.rollback();
                } finally {
                    cxn.close();
                }

            }

        } finally {

            gasEngine.shutdownNow();

        }

    }

    /**
     * Test using {@link #setupSSSPGraphProblem()}
     * 
     * FIXME SSSP: This test needs link weights in the data and to resolve those
     * link weights during traversal. Link weights need to be supported for all
     * backends. The data file needs to have the link weights (they are not in
     * there right now).
     * <p>
     * SSSP must also support floating point distances since the link weights
     * are often non-integer.
     */
    public void _testSSSP2() throws Exception {

        fail("Finish test.");
        
        final SSSPGraphProblem p = setupSSSPGraphProblem();

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final SailConnection cxn = getGraphFixture().getSail()
                    .getConnection();

            try {

                final IGraphAccessor graphAccessor = getGraphFixture()
                        .newGraphAccessor(cxn);

                final IGASContext<SSSP.VS, SSSP.ES, Integer> gasContext = gasEngine
                        .newGASContext(graphAccessor, new SSSP());

                final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = gasContext
                        .getGASState();

                // Initialize the froniter.
                gasState.setFrontier(gasContext, p.v1);

                // Converge.
                gasContext.call();

                assertEquals(0.0, gasState.getState(p.v1).dist());

                assertEquals(1.0, gasState.getState(p.v2).dist());

                assertEquals(1.0, gasState.getState(p.v3).dist());

                assertEquals(1.5, gasState.getState(p.v4).dist());

                assertEquals(1.75, gasState.getState(p.v5).dist());

            } finally {

                try {
                    cxn.rollback();
                } finally {
                    cxn.close();
                }

            }

        } finally {

            gasEngine.shutdownNow();

        }

    }

}
