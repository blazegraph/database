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
 * Test class for Breadth First Search (BFS) traversal.
 * 
 * @see BFS
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBFS extends AbstractGraphTestCase {

    public TestBFS() {
        
    }
    
    public TestBFS(String name) {
        super(name);
    }

    public void testBFS() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        final IGASEngine<BFS.VS, BFS.ES, Void> gasEngine = new GASEngine<BFS.VS, BFS.ES, Void>(
                sail.getDatabase(), new BFS());

        // Initialize the froniter.
        gasEngine.init(p.mike.getIV());

        // Converge.
        gasEngine.call();

        assertEquals(0, gasEngine.getGASContext().getState(p.mike.getIV())
                .depth());

        assertEquals(1, gasEngine.getGASContext().getState(p.foafPerson.getIV())
                .depth());

        assertEquals(1, gasEngine.getGASContext().getState(p.bryan.getIV())
                .depth());

        assertEquals(2, gasEngine.getGASContext().getState(p.martyn.getIV())
                .depth());

    }
    
    /**
     * Test routine to running against a {@link Journal} in which some data set
     * has already been loaded.
     */
    public static void main(final String[] args) throws Exception {

        new PerformanceTest<BFS.VS, BFS.ES, Void>(args) {

            @Override
            protected IGASProgram<BFS.VS, BFS.ES, Void> newGASProgram() {

                return new BFS();

            }

        }.call();
        
    }

}
