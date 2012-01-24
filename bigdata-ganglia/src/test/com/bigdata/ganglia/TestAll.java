/*
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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.ganglia;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increasing dependency order.
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
     * 
     * TODO There should be a test suite for the XDR package.  It is implicitly
     * tested by testing the ganglia message encode/decode logic.
     */
	public static Test suite() {

		final TestSuite suite = new TestSuite("Ganglia");

		suite.addTestSuite(TestGangliaMunge.class);

		/*
		 * 3.1 wire format test.
		 * 
		 * TODO Same test for pre-3.1 wire format?
		 */
		suite.addTestSuite(TestGangliaMessageEncodeDecode31.class);

		// Test suite verifies correct listener shutdown.
		suite.addTestSuite(TestGangliaListenerShutdown.class);
		
		// Test suite verifies correct service shutdown.
		suite.addTestSuite(TestGangliaServiceShutdown.class);
		
        return suite;
        
    }
    
}
