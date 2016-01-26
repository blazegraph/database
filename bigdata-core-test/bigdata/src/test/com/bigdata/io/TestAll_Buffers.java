/*

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
 * Created on Aug 26, 2009
 */

package com.bigdata.io;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll_Buffers extends TestCase {

    /**
     * 
     */
    public TestAll_Buffers() {
    }

    /**
     * @param arg0
     */
    public TestAll_Buffers(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite(TestAll.class.getPackage()
                .getName());

        /*
         * test fast DataOutput and DataInput implementations.
         */

        // test slice of an input stream.
        suite.addTestSuite(TestSliceInputStream.class);
        // test use of ByteBuffer as an input/output stream.
        suite.addTestSuite(TestByteBufferStreams.class);
        // test fixed-length record w/ absolute access.
        suite.addTestSuite(TestFixedByteArrayBuffer.class);
        // test extensible record w/ absolute and relative and stream-based
        // access.
        suite.addTestSuite(TestByteArrayBuffer.class);
        // test extensible record w/ DataOutput API.
        suite.addTestSuite(TestDataOutputBuffer.class);

        return suite;

    }

}
