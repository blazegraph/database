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
package com.bigdata.htree.data;

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

        final TestSuite suite = new TestSuite("HTree data records");

        /*
         * Test w/ all key and value coders suitable for directory pages.
         */
//        suite.addTestSuite(TestNodeDataRecord_Simple.class);
//        suite.addTestSuite(TestNodeDataRecord_FrontCoded.class);
//        suite.addTestSuite(TestNodeDataRecord_CanonicalHuffman.class);

		/*
		 * Test w/ all key and value coders suitable for leaves.
		 * 
		 * @todo test the mutable bucket data record
		 * 
		 * @todo test w/ linked-leaf (order preserving hash functions).
		 * 
		 * @todo test w/ out-of-line tuples (when too large for the page).
		 * 
		 * @todo test w/ overflow pages (when all keys are the same and the page
		 * overflows).
		 * 
		 * FIXME Test with shaped hash code distributions such that lengthMSB is
		 * non-zero (it will be frequently zero if we use random data and a
		 * decent hash function since the bits will tend to be random, but in
		 * practice using int values for keys will result in hash values with
		 * leading zeros using the default Java semantics for the hash of an
		 * int32 value).
		 */
        suite.addTestSuite(TestBucketDataRecord_Simple_Simple.class);
//        suite.addTestSuite(TestLeafDataRecord_FrontCoded_CanonicalHuffman.class);
//        suite.addTestSuite(TestLeafDataRecord_CanonicalHuffman_CanonicalHuffman.class);

        return suite;
        
    }
    
}
