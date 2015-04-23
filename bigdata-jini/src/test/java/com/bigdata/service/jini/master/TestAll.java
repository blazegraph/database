/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Jun 26, 2006
 */
package com.bigdata.service.jini.master;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates tests in dependency order.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {
    
    public TestAll() {}
    
    public TestAll(String name) {super(name);}
    
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("master (job) execution");

        /*
         * FIXME There appears to be a problem with this test when run as part
         * of CI where the test does not terminate. I've noted this on the issue
         * referenced below and removed the test from CI. for the moment.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/297#comment:17
         */
//        suite.addTestSuite(TestMappedRDFDataLoadMaster.class);

        return suite;
        
    }
    
}
