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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;

/**
 * Test suite for SPARQL queries with embedded query hints.
 * 
 * @see ASTQueryHintOptimizer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryHints extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestQueryHints() {
    }

    /**
     * @param name
     */
    public TestQueryHints(String name) {
        super(name);
    }


    /**
     * A simple SELECT query with some query hints. This does not verify much
     * beyond the willingness to accept the query hints and silently remove them
     * from the AST model (the query will fail if the query hints are not
     * removed as they will not match anything in the data).
     */
    public void test_query_hints_1() throws Exception {

        new TestHelper("query-hints-1").runTest();
        
    }
    
}
