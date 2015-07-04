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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Value;
import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.analytics.CC.VS;
import com.bigdata.rdf.graph.impl.sail.AbstractSailGraphTestCase;

/**
 * Test class for Breadth First Search (BFS) traversal.
 * 
 * @see BFS
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestCC extends AbstractSailGraphTestCase {
    
    public TestCC() {
        
    }
    
    public TestCC(String name) {
        super(name);
    }

    public void testCC() throws Exception {

        /*
         * Load two graphs. These graphs are not connected with one another (no
         * shared vertices). This means that each graph will be its own
         * connected component (all vertices in each source graph are
         * connected within that source graph).
         */
        final SmallGraphProblem p1 = setupSmallGraphProblem();
        final SSSPGraphProblem p2 = setupSSSPGraphProblem();
        
        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final SailConnection cxn = getGraphFixture().getSail()
                    .getConnection();

            try {

                final IGraphAccessor graphAccessor = getGraphFixture()
                        .newGraphAccessor(cxn);

                final CC gasProgram = new CC();
                
                final IGASContext<CC.VS, CC.ES, Value> gasContext = gasEngine
                        .newGASContext(graphAccessor, gasProgram);

                final IGASState<CC.VS, CC.ES, Value> gasState = gasContext
                        .getGASState();

                // Converge.
                final IGASStats stats = gasContext.call();

                if(log.isInfoEnabled())
                    log.info(stats);
                
                /*
                 * Check the #of connected components that are self-reported and
                 * the #of vertices in each connected component. This helps to
                 * detect vertices that should have been visited but were not
                 * due to the initial frontier. E.g., "DC" will not be reported
                 * as a connected component of size (1) unless it gets into the
                 * initial frontier (it has no edges, only an attribute).
                 */
                final Map<Value, AtomicInteger> labels = gasProgram
                        .getConnectedComponents(gasState);
                
                // the size of the connected component for this vertex.
                {
                	final VS valueState = gasState.getState(p1.getFoafPerson()); 
                    final Value label = valueState != null?valueState.getLabel():null;
                 
                    assertEquals(4, labels.get(label).get());
                }

                // the size of the connected component for this vertex.
                {
                	final VS valueState = gasState.getState(p2.get_v1());
                    final Value label = valueState != null?valueState.getLabel():null;
                    
                    final AtomicInteger ai = labels.get(label);
                    final int count = ai!=null?ai.get():-1;

                    assertEquals(5, count);
                }

                if (false) {
                    /*
                     * The size of the connected component for this vertex.
                     * 
                     * Note: The vertex sampling code ignores self-loops and
                     * ignores vertices that do not have ANY edges. Thus "DC" is
                     * not put into the frontier and is not visited.
                     */
                    final Value label = gasState.getState(p1.getDC())
                            .getLabel();

                    assertNotNull(label);

                    /*
                     * If DC was not put into the initial frontier, then it will
                     * be missing here.
                     */
                    assertNotNull(labels.get(label));

                    assertEquals(1, labels.get(label).get());

                }

                // the #of connected components.
                assertEquals(2, labels.size());

                /*
                 * Most vertices in problem1 have the same label (the exception
                 * is DC, which is it its own connected component).
                 */
                Value label1 = null;
                for (Value v : p1.getVertices()) {

                    final CC.VS vs = gasState.getState(v);
                    
                    if (log.isInfoEnabled())
                        log.info("v=" + v + ", label=" + vs.getLabel());
                    
                    if(v.equals(p1.getDC())) {
                        /*
                         * This vertex is in its own connected component and is
                         * therefore labeled by itself.
                         */
                        assertEquals("vertex=" + v, v, vs.getLabel());
                        continue;
                    }
                    
                    if (label1 == null) {
                        label1 = vs.getLabel();
                        assertNotNull(label1);
                    }
                    
                    assertEquals("vertex=" + v, label1, vs.getLabel());

                }
                
                // All vertices in problem2 have the same label.
                Value label2 = null;
                for (Value v : p2.getVertices()) {

                    final CC.VS vs = gasState.getState(v);

                    if (log.isInfoEnabled())
                        log.info("v=" + v + ", label=" + vs.getLabel());
                    
                    if (label2 == null) {
                        label2 = vs.getLabel();
                        assertNotNull(label2);
                    }

                    assertEquals("vertex=" + v, label2, vs.getLabel());

                }

                // The labels for the two connected components are distinct.
                assertNotSame(label1, label2);

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
