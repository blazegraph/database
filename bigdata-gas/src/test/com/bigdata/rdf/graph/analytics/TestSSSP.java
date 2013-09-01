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
                gasState.init(p.getMike());

                // Converge.
                gasContext.call();

                assertEquals(0, gasState.getState(p.getMike()).dist());

                assertEquals(1, gasState.getState(p.getFoafPerson()).dist());

                assertEquals(1, gasState.getState(p.getBryan()).dist());

                assertEquals(2, gasState.getState(p.getMartyn()).dist());

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
