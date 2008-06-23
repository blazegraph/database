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
package com.bigdata.join;


import com.bigdata.join.rdf.TestKeyOrder;
import com.bigdata.join.rdf.TestSPOPredicate;

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

        TestSuite suite = new TestSuite("JOINs");

        // test variable and constant impls.
        suite.addTestSuite(TestVar.class);
        suite.addTestSuite(TestConstant.class);
        
        // test predicate impls.
        suite.addTestSuite(TestPredicate.class);
        suite.addTestSuite(TestSPOPredicate.class);
        
        // @todo test binding set impls.
        suite.addTestSuite(TestBindingSet.class);
        
        // @todo test IKeyOrder impl.
        suite.addTestSuite(TestKeyOrder.class);
        
        // @todo chunked iterator tests.
        // TestChunkedWrappedIterator
        // TestChunkedArrayIterator (fully buffered)
        // TestChunkedIterator (async reader)

        // @todo test blocking buffer with iterator to drain solutions.
        // TestBlockingBuffer
        // @todo test array backed buffer flushing through to a database.
        // TestArrayBuffer

        // @todo test basic access path mechanisms.
        suite.addTestSuite(TestAccessPath.class);

        // @todo access path using fused view : (focusStore+db).
        suite.addTestSuite(TestAccessPathFusedView.class);
        
        // @todo test ability to select the right access path.
        suite.addTestSuite(TestAccessPathFactory.class);
        
        // test suite for basic rule mechanisms.
        suite.addTestSuite( TestRule.class );
       
        // @todo write tests.
        suite.addTestSuite(TestRuleState.class);
        
        // @todo test some simple rules (evaluate them).
        // suite.addTestSuite( TestRuleRdf01.class );
        
        return suite;
        
    }
    
}
