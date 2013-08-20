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
package com.bigdata.rdf.graph.analytics;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.PerformanceTest;

/**
 * Test class for SSP traversal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSSSP extends AbstractGraphTestCase {

    public TestSSSP() {
        
    }
    
    public TestSSSP(String name) {
        super(name);
    }

    public void testSSSP() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        final IGASEngine<SSSP.VS, SSSP.ES, Integer> gasEngine = new GASEngine<SSSP.VS, SSSP.ES, Integer>(
                sail.getDatabase(), new SSSP());

        // Initialize the froniter.
        gasEngine.init(p.mike.getIV());

        // Converge.
        gasEngine.call();

        assertEquals(0, gasEngine.getGASContext().getState(p.mike.getIV())
                .dist());

        assertEquals(1, gasEngine.getGASContext()
                .getState(p.foafPerson.getIV()).dist());

        assertEquals(1, gasEngine.getGASContext().getState(p.bryan.getIV())
                .dist());

        assertEquals(2, gasEngine.getGASContext().getState(p.martyn.getIV())
                .dist());
        
    }

    /**
     * Test routine to running against a {@link Journal} in which some data set
     * has already been loaded.
     */
    public static void main(final String[] args) throws Exception {

        new PerformanceTest<SSSP.VS, SSSP.ES, Integer>(args) {

            @Override
            protected IGASProgram<SSSP.VS, SSSP.ES, Integer> newGASProgram() {

                return new SSSP();

            }

        }.call();

    }

}
