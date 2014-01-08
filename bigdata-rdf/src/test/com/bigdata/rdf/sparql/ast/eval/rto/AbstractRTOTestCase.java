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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.rto;

import java.util.Arrays;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryEngine.IRunningQueryListener;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.joinGraph.rto.JGraph;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.bop.joinGraph.rto.Path;
import com.bigdata.bop.rdf.joinGraph.TestJoinGraphOnLubm;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Data driven test suite for the Runtime Query Optimizer (RTO).
 * <p>
 * Note: We reduce the stochastic behavior of the algorithm by using non-random
 * sampling techniques. However, the main correctness issues for the RTO are the
 * handling of different kinds of join groups, not the specific join orderings
 * that it produces. The join orderings depend on how the cutoff joins are
 * sampled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 * 
 * @see JGraph
 * @see JoinGraph
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/64">Runtime
 *      Query Optimization</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/258">Integrate
 *      RTO into SAIL</a>
 * 
 *      TODO See the stubbed out test suite for the RTO for some examples of
 *      join groups that it should be handling.
 * 
 *      TODO The RTO also needs to handle FILTERs that require materialization.
 *      This should be the subject of a test suite.
 * 
 *      TODO The RTO should be extended (together with test coverage) to handle
 *      more interesting kinds of join groups (optionls, sub-selects, property
 *      paths, SERVICE calls, etc).
 *      <p>
 *      Note: When handling sub-groups, etc., the RTO needs to flow solutions
 *      into the sub-query.
 * 
 *      TODO When adding an {@link IASTOptimizer} for the RTO, modify this class
 *      to test for the inclusion of the JoinGraphNode for the RTO.
 * 
 *      TODO Automate the larger data scale tests on these data sets as part of
 *      CI and provide automated reporting over time on those performance runs.
 *      Once this is done, there will be no more reason to keep the older
 *      {@link TestJoinGraphOnLubm} and related tests.
 */
public class AbstractRTOTestCase extends AbstractDataDrivenSPARQLTestCase {

    private final static Logger log = Logger.getLogger(AbstractRTOTestCase.class);
    
    /**
     * 
     */
    public AbstractRTOTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRTOTestCase(String name) {
        super(name);
    }

    /**
     * Helper class supports inspection of the terminated {@link IRunningQuery}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    protected static class MyQueryListener implements IRunningQueryListener {

        private final UUID queryId;
        private volatile IRunningQuery q;

        public MyQueryListener(final UUID queryId) {

            if(queryId == null)
                throw new IllegalArgumentException();
            
            this.queryId = queryId;
            
        }
        
        @Override
        public void notify(final IRunningQuery q) {

            if(q.getQueryId().equals(queryId)) {
            
                this.q = q;
                
            }

        }
        
        public IRunningQuery getRunningQuery() {

            final IRunningQuery q = this.q;

            if (q == null)
                fail("Not found.");

            return q;
            
        }
        
    }

    /**
     * Helper to run the test and examine the RTO determined solution.
     * 
     * @param expected
     *            The expected join ordering.
     * @param helper
     */
    protected void assertSameJoinOrder(final int[] expected,
        final TestHelper helper) throws Exception {
        
        /*
         * Assign a UUID to this query so we can get at its outcome.
         */
        final UUID queryId = UUID.randomUUID();

        helper.getASTContainer().setQueryHint(QueryHints.QUERYID,
                queryId.toString());
        
        final QueryEngine queryEngine = QueryEngineFactory
                .getExistingQueryController((IBTreeManager) helper
                        .getTripleStore().getIndexManager());

        // Hook up our listener and run the test.
        final ASTContainer astContainer;
        final MyQueryListener l = new MyQueryListener(queryId);
        try {
            // Register the listener.
            queryEngine.addListener(l);
            // Run the test.
            astContainer = helper.runTest();
        } finally {
            // Unregister the listener.
            queryEngine.removeListener(l);
        }

//        final QueryRoot optimizedAST = astContainer.getOptimizedAST();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        final JoinGraph joinGraph = BOpUtility.getOnly(queryPlan,
                JoinGraph.class);

        assertNotNull(joinGraph);

        // The join path selected by the RTO.
        final Path path = joinGraph.getPath(l.getRunningQuery());

        if (log.isInfoEnabled())
            log.info("path=" + path);

        if (!Arrays.equals(expected, path.getVertexIds()))
            fail("RTO JOIN ORDER" //
                    + ": expected=" + Arrays.toString(expected)//
                    + ", actual=" + Arrays.toString(path.getVertexIds()));

        // joinGraph.getQueryPlan(q)

    }

}
