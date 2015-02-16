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
import com.bigdata.rdf.graph.TraversalDirectionEnum;
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
                assertEquals(null, gasState.getState(p.getMike()).predecessor());

                assertEquals(1, gasState.getState(p.getFoafPerson()).depth());
                assertEquals(p.getMike(), gasState.getState(p.getFoafPerson())
                        .predecessor());

                assertEquals(1, gasState.getState(p.getBryan()).depth());
                assertEquals(p.getMike(), gasState.getState(p.getBryan())
                        .predecessor());

                assertEquals(2, gasState.getState(p.getMartyn()).depth());
                assertEquals(p.getBryan(), gasState.getState(p.getMartyn())
                        .predecessor());

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
     * Variant test in which we choose a vertex (<code>foaf:person</code>) in
     * the middle of the graph and insist on forward directed edges. Since the
     * edges point from the person to the <code>foaf:person</code> vertex, this
     * BSF traversal does not discover any connected vertices.
     */
    public void testBFS_directed_forward() throws Exception {

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
                gasState.setFrontier(gasContext, p.getFoafPerson());

                // directed traversal.
                gasContext
                        .setTraversalDirection(TraversalDirectionEnum.Forward);
                
                // Converge.
                gasContext.call();

                // starting vertex at (0,null).
                assertEquals(0, gasState.getState(p.getFoafPerson()).depth());
                assertEquals(null, gasState.getState(p.getFoafPerson())
                        .predecessor());
                
                // no other vertices are visited.
                assertEquals(-1, gasState.getState(p.getMike()).depth());
                assertEquals(null, gasState.getState(p.getMike()).predecessor());

                assertEquals(-1, gasState.getState(p.getBryan()).depth());
                assertEquals(null, gasState.getState(p.getBryan())
                        .predecessor());

                assertEquals(-1, gasState.getState(p.getMartyn()).depth());
                assertEquals(null, gasState.getState(p.getMartyn())
                        .predecessor());

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
     * Variant test in which we choose a vertex (<code>foaf:person</code>) in
     * the middle of the graph and insist on reverse directed edges. Since the
     * edges point from the person to the <code>foaf:person</code> vertex,
     * forward BSF traversal does not discover any connected vertices. However,
     * since the traversal direction is reversed, the vertices are all one hop
     * away.
     */
    public void testBFS_directed_reverse() throws Exception {

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
                gasState.setFrontier(gasContext, p.getFoafPerson());

                // directed traversal.
                gasContext
                        .setTraversalDirection(TraversalDirectionEnum.Reverse);
                
                // Converge.
                gasContext.call();

                // starting vertex at (0,null).
                assertEquals(0, gasState.getState(p.getFoafPerson()).depth());
                assertEquals(null, gasState.getState(p.getFoafPerson())
                        .predecessor());

                // All other vertices are 1-hop.
                assertEquals(1, gasState.getState(p.getMike()).depth());
                assertEquals(p.getFoafPerson(), gasState.getState(p.getMike())
                        .predecessor());

                assertEquals(1, gasState.getState(p.getBryan()).depth());
                assertEquals(p.getFoafPerson(), gasState.getState(p.getBryan())
                        .predecessor());

                assertEquals(1, gasState.getState(p.getMartyn()).depth());
                assertEquals(p.getFoafPerson(), gasState
                        .getState(p.getMartyn()).predecessor());

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
     * Variant test in which we choose a vertex (<code>foaf:person</code>) in
     * the middle of the graph and insist on directed edges. Since the edges
     * point from the person to the <code>foaf:person</code> vertex, this BSF
     * traversal does not discover any connected vertices.
     */
    public void testBFS_undirected() throws Exception {

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
                gasState.setFrontier(gasContext, p.getFoafPerson());

                // undirected traversal.
                gasContext
                        .setTraversalDirection(TraversalDirectionEnum.Undirected);

                // Converge.
                gasContext.call();

                // starting vertex at (0,null).
                assertEquals(0, gasState.getState(p.getFoafPerson()).depth());
                assertEquals(null, gasState.getState(p.getFoafPerson())
                        .predecessor());

                // All other vertices are 1-hop.
                assertEquals(1, gasState.getState(p.getMike()).depth());
                assertEquals(p.getFoafPerson(), gasState.getState(p.getMike())
                        .predecessor());

                assertEquals(1, gasState.getState(p.getBryan()).depth());
                assertEquals(p.getFoafPerson(), gasState.getState(p.getBryan())
                        .predecessor());

                assertEquals(1, gasState.getState(p.getMartyn()).depth());
                assertEquals(p.getFoafPerson(), gasState
                        .getState(p.getMartyn()).predecessor());

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
