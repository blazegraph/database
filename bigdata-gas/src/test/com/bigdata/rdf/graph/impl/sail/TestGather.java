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
package com.bigdata.rdf.graph.impl.sail;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.FrontierEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.BaseGASProgram;
import com.bigdata.rdf.graph.impl.GASStats;

/**
 * Test class for GATHER.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class TestGather extends AbstractSailGraphTestCase {

    public TestGather() {
        
    }
    
    public TestGather(String name) {
        super(name);
    }

    /**
     * Mock gather class uses a UNION for SUM to test the GATHER semantics.
     * The gathered edge set is then APPLYed to the vertex and becomes the
     * state of that vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class MockGASProgram extends
            BaseGASProgram<Set<Statement>, Set<Statement>, Set<Statement>> {

        private final EdgesEnum gatherEdges;
        
        MockGASProgram(final EdgesEnum gatherEdges) {
            
            this.gatherEdges = gatherEdges;
            
        }
        
        @Override
        public FrontierEnum getInitialFrontierEnum() {

            return FrontierEnum.SingleVertex;
            
        }
        
        @Override
        public EdgesEnum getGatherEdges() {
            return gatherEdges;
        }

        @Override
        public EdgesEnum getScatterEdges() {
            return EdgesEnum.NoEdges;
        }
        
        @Override
        public Factory<Value, Set<Statement>> getVertexStateFactory() {
            return new Factory<Value, Set<Statement>>() {
                @Override
                public Set<Statement> initialValue(Value value) {
                    return new LinkedHashSet<Statement>();
                }
            };
        }

        @Override
        public Factory<Statement, Set<Statement>> getEdgeStateFactory() {

            return null;
            
        }

        @Override
        public void initVertex(
                IGASContext<Set<Statement>, Set<Statement>, Set<Statement>> ctx,
                IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                Value u) {

            // NOP
            
        }

        /**
         * Return the edge as a singleton set.
         */
        @Override
        public Set<Statement> gather(
                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final Value u, final Statement e) {

            return Collections.singleton(e);

        }

        /**
         * Set UNION over the GATHERed edges.
         */
        @Override
        public Set<Statement> sum(
                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final Set<Statement> left, final Set<Statement> right) {

            /*
             * Note: This happens to preserve the visitation order. That is not
             * essential, but it is nice.
             */
            final Set<Statement> tmp = new LinkedHashSet<Statement>(left);
            
            tmp.addAll(right);
            
            return tmp;
            
        }

        /**
         * UNION the gathered edges with those already decorating the vertex.
         */
        @Override
        public Set<Statement> apply(
                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final Value u, final Set<Statement> sum) {
 
            if (sum != null) {

                // Get the state for that vertex.
                final Set<Statement> us = state.getState(u);

                // UNION with the accumulant.
                us.addAll(sum);

                return us;

            }

            // No change.
            return null;

        }

        @Override
        public boolean isChanged(
                IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state, Value u) {

            return true;
            
        }

        @Override
        public void scatter(IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final IGASScheduler sch, Value u, Statement e) {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public boolean nextRound(IGASContext ctx) {

            return true;
            
        }

    };
    
    public void testGather_inEdges() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        // gather no-edges for :mike
        {
            
            final Set<Statement> expected = set();

            doGatherTest(EdgesEnum.NoEdges, expected, p.getMike()/* startingVertex */);
        
        }

        // gather in-edges for :mike
        {
            
            final Set<StatementImpl> expected = set(//
            new StatementImpl(p.getBryan(), p.getFoafKnows(), p.getMike())//
            );

            doGatherTest(EdgesEnum.InEdges, expected, p.getMike()/* startingVertex */);

        }

        // gather out-edges for :mike
        {
            
            final Set<StatementImpl> expected = set(//
                    new StatementImpl(p.getMike(), p.getRdfType(), p.getFoafPerson()),//
                    new StatementImpl(p.getMike(), p.getFoafKnows(), p.getBryan())//
            );

            doGatherTest(EdgesEnum.OutEdges, expected, p.getMike() /* startingVertex */);

        }

        // gather all-edges for :mike 
        {
            
            final Set<StatementImpl> expected = set(//
                    new StatementImpl(p.getBryan(), p.getFoafKnows(), p.getMike()),//
                    new StatementImpl(p.getMike(), p.getRdfType(), p.getFoafPerson()),//
                    new StatementImpl(p.getMike(), p.getFoafKnows(), p.getBryan())//
            );

            doGatherTest(EdgesEnum.AllEdges, expected, p.getMike()/* startingVertex */);

        }

    }

    /**
     * Start on a known vertex. Do one iteration. Verify that the GATHER
     * populated the data structures on the mock object with the appropriate
     * collections.
     * @throws Exception 
     */
    protected void doGatherTest(final EdgesEnum gatherEdges,
            final Set<? extends Statement> expected, final Value startingVertex)
            throws Exception {

        final IGASEngine gasEngine = getGraphFixture()
                .newGASEngine(1/* nthreads */);

        try {

            final SailConnection cxn = getGraphFixture().getSail()
                    .getConnection();

            try {

                final IGraphAccessor graphAccessor = getGraphFixture()
                        .newGraphAccessor(cxn);

                final IGASContext<Set<Statement>, Set<Statement>, Set<Statement>> gasContext = gasEngine
                        .newGASContext(graphAccessor, new MockGASProgram(
                                gatherEdges));

                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> gasState = gasContext
                        .getGASState();

                // Initialize the froniter.
                gasState.setFrontier(gasContext, startingVertex);

                // Do one round.
                gasContext.doRound(new GASStats());

                /*
                 * Lookup the state for the starting vertex (this should be the
                 * only vertex whose state was modified since we did only one
                 * round).
                 */
                final Set<Statement> actual = gasState.getState(startingVertex);

                // Verify against the expected state.
                assertSameEdges(expected, actual);

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
