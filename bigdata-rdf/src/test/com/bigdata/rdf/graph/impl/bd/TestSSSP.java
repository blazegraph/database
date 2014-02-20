/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.graph.impl.bd;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.analytics.SSSP;

/**
 * Test class for SSP traversal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSSSP extends AbstractBigdataGraphTestCase {

    public TestSSSP() {
        
    }
    
    public TestSSSP(String name) {
        super(name);
    }

    public void testSSSP() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final IGraphAccessor graphAccessor = getGraphFixture()
                    .newGraphAccessor(null/* ignored */);

            final IGASContext<SSSP.VS, SSSP.ES, Integer> gasContext = gasEngine
                    .newGASContext(graphAccessor, new SSSP());

            final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = gasContext.getGASState();
            
            // Initialize the froniter.
            gasState.setFrontier(gasContext, p.getMike());

            // Converge.
            gasContext.call();

            assertEquals(0, gasState.getState(p.getMike()).dist());

            assertEquals(1, gasState.getState(p.getFoafPerson()).dist());

            assertEquals(1, gasState.getState(p.getBryan()).dist());

            assertEquals(2, gasState.getState(p.getMartyn()).dist());

        } finally {

            gasEngine.shutdownNow();

        }

    }
    
    /**
     * A unit test based on graph with link weights.
     */
    public void test_sssp_weightedGraph() throws Exception {
        
        final SmallWeightedGraphProblem p = setupSmallWeightedGraphProblem();

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final IGraphAccessor graphAccessor = getGraphFixture()
                    .newGraphAccessor(null/* ignored */);

            final IGASContext<SSSP.VS, SSSP.ES, Integer> gasContext = gasEngine
                    .newGASContext(graphAccessor, new SSSP());

            final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = gasContext.getGASState();
            
            // Initialize the froniter.
            gasState.setFrontier(gasContext, p.getV1());

            // Converge.
            gasContext.call();

            assertEquals(0, gasState.getState(p.getV1()).dist());

            assertEquals(100, gasState.getState(p.getV2()).dist());

            assertEquals(100, gasState.getState(p.getV3()).dist());

            assertEquals(125, gasState.getState(p.getV4()).dist());

            assertEquals(125, gasState.getState(p.getV5()).dist());

        } finally {

            gasEngine.shutdownNow();

        }

    }

}
