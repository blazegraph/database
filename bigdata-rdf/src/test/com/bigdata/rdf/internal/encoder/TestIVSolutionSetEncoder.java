/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

/**
 * Test suite for {@link IVSolutionSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestIVSolutionSetEncoder.java 6033 2012-02-16 16:12:27Z
 *          thompsonbry $
 * 
 *          TODO Pick up the tests from the other two test suites in this
 *          package.
 */
public class TestIVSolutionSetEncoder extends AbstractBindingSetEncoderTestCase {

    /**
     * 
     */
    public TestIVSolutionSetEncoder() {
    }

    /**
     * @param name
     */
    public TestIVSolutionSetEncoder(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
     
        super.setUp();

        // The encoder under test.
        encoder = new IVSolutionSetEncoder(valueFactory);
        
        // The decoder under test (same object as the encoder).
        decoder = new IVSolutionSetDecoder(valueFactory);
        
    }

//    protected void tearDown() throws Exception {
//        
//        super.tearDown();
//        
//        // Clear references.
//        encoder.release();
//        encoder = null;
//        decoder = null;
//        
//    }

}
