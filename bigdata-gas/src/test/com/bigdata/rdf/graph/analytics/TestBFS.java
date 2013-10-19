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
 * Test class for Breadth First Search (BFS) traversal.
 * 
 * @see BFS
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBFS extends AbstractSailGraphTestCase {

    public TestBFS() {
        
    }
    
    public TestBFS(String name) {
        super(name);
    }

    public void testBFS() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final SailConnection cxn = getGraphFixture().getSail()
                    .getConnection();

            try {

                final IGraphAccessor graphAccessor = getGraphFixture()
                        .newGraphAccessor(cxn);

                final IGASContext<BFS.VS, BFS.ES, Void> gasContext = gasEngine
                        .newGASContext(graphAccessor, new BFS());

                final IGASState<BFS.VS, BFS.ES, Void> gasState = gasContext
                        .getGASState();

                // Initialize the froniter.
                gasState.setFrontier(gasContext, p.getMike());

                // Converge.
                gasContext.call();

                assertEquals(0, gasState.getState(p.getMike()).depth());

                assertEquals(1, gasState.getState(p.getFoafPerson()).depth());

                assertEquals(1, gasState.getState(p.getBryan()).depth());

                assertEquals(2, gasState.getState(p.getMartyn()).depth());

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
