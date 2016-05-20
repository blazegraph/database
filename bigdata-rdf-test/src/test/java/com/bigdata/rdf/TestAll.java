/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("RDF");

        /*
         * test suite for the rio parser without the triple store integrations.
         */
        suite.addTest(com.bigdata.rdf.rio.TestAll_RIO.suite());

        // test suite for the internal representation of RDF Values.
        suite.addTest( com.bigdata.rdf.internal.TestAll.suite() );

        // vocabulary test suite (w/o triple store)
        suite.addTest( com.bigdata.rdf.vocab.TestAll.suite() );

        // axioms test suite (w/o triple store) [Note: axioms test suite is now proxied.]
        //suite.addTest( com.bigdata.rdf.axioms.TestAll.suite() );

        // test RDF Value and Statement object model (Sesame compliance).
        suite.addTest( com.bigdata.rdf.model.TestAll.suite() );
        
        // test suite for RDF specific operators.
        suite.addTest( com.bigdata.bop.rdf.TestAll.suite() );

        // SPARQL, including the parser, AST, AST optimizers, & AST evaluation.
        suite.addTest(com.bigdata.rdf.sparql.TestAll.suite());

        // test various RDF database implementations.
        suite.addTest( com.bigdata.rdf.store.TestAll.suite() );

        // test the bulk data loader : @todo use proxy tests and move into per-store suites?
        suite.addTest( com.bigdata.rdf.load.TestAll.suite() );

        // test RDF graph mining/analytics
//        suite.addTest(com.bigdata.rdf.graph.TestAll.suite()); // Note: This is in its own maven project.
        suite.addTest(com.bigdata.rdf.graph.impl.bd.TestAll.suite());

        
        // BOp execution
        suite.addTest(com.bigdata.bop.solutions.TestAll.suite());
        
        return suite;
        
    }
    
}
