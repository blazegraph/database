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
package com.bigdata.relation.rdf;


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

        TestSuite suite = new TestSuite("RDF JOINs");

        // test predicate impls.
        suite.addTestSuite(TestSPOPredicate.class);
        
        // @todo test IKeyOrder impl (comparators).
        suite.addTestSuite(TestSPOKeyOrder.class);
  
        /*
         * @todo test ability to insert, update, or remove elements from a
         * relation and the ability to select the right access path given a
         * predicate for that relation and query for those elements (we have
         * to test all this stuff together since testing query requires us 
         * to have some data in the relation).
         */
        suite.addTestSuite(TestSPORelation.class);

        // @todo test some simple rules (evaluate them).
        // suite.addTestSuite( TestRuleRdf01.class );
        
        // test suite for rule re-writes for RDF DB truth maintenance.
        suite.addTestSuite(TestTMUtility.class);
       
        return suite;
        
    }
    
}
