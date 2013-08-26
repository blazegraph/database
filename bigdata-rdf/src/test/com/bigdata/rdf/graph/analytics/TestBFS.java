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

import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.analytics.BFS.ES;
import com.bigdata.rdf.graph.analytics.BFS.VS;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.GASEngine.BigdataGraphAccessor;

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

        final IGASEngine gasEngine = new GASEngine(sail.getDatabase()
                .getIndexManager(), 1/* nthreads */);

        try {

            final BigdataGraphAccessor graphAccessor = ((GASEngine) gasEngine)
                    .newGraphAccessor(sail.getDatabase().getNamespace(), sail
                            .getDatabase().getIndexManager()
                            .getLastCommitTime());

            final IGASContext<BFS.VS, BFS.ES, Void> gasContext = gasEngine
                    .newGASContext(graphAccessor, new BFS());

            final IGASState<VS, ES, Void> gasState = gasContext.getGASState();
            
            // Initialize the froniter.
            gasState.init(p.mike.getIV());

            // Converge.
            gasContext.call();

            assertEquals(0, gasState.getState(p.mike.getIV()).depth());

            assertEquals(1, gasState.getState(p.foafPerson.getIV()).depth());

            assertEquals(1, gasState.getState(p.bryan.getIV()).depth());

            assertEquals(2, gasState.getState(p.martyn.getIV()).depth());

        } finally {

            gasEngine.shutdownNow();

        }
    }
    
}
