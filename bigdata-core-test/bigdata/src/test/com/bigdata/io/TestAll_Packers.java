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
/*
 * Created on May 26, 2011
 */

package com.bigdata.io;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll_Packers extends TestCase {

    /**
     * 
     */
    public TestAll_Packers() {
    }

    /**
     * @param name
     */
    public TestAll_Packers(String name) {
        super(name);
    }

    public static Test suite() {

        final TestSuite suite = new TestSuite(TestAll.class.getPackage()
                .getName());

        /*
         * TODO Harmonize the DataInputBuffer/ByteArrayBuffer test suites for
         * the packers and the standalone LongPacker and ShortPacker test suites
         * and verify interoperability.
         */
        
        // test packed short support.
        suite.addTestSuite(TestShortPacker.class);
        // test packed long support.
        suite.addTestSuite(TestLongPacker.class);

        // test packed short support.
        suite.addTestSuite(ShortPackerTestCase.class);
        // test packed long support.
        suite.addTestSuite(LongPackerTestCase.class);

        return suite;

    }
    
}
