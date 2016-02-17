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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;


/**
 * Test suite for {@link IVBindingSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVBindingSetEncoder extends AbstractBindingSetEncoderTestCase {

    /**
     * 
     */
    public TestIVBindingSetEncoder() {
    }

    public TestIVBindingSetEncoder(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
     
        super.setUp();
        
        // The encoder under test.
        encoder = new IVBindingSetEncoder(valueFactory, false/* filter */);
        
        // The decoder under test (same object as the encoder).
        decoder = (IVBindingSetEncoder) encoder;
        
        // The encoder/decoder does not support IVCache resolution.
        testCache = false;
        
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
