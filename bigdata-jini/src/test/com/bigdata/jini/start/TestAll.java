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
/*
 * Created on Jun 26, 2006
 */
package com.bigdata.jini.start;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.service.jini.AbstractServerTestCase;

/**
 * Aggregates tests in dependency order - see {@link AbstractServerTestCase} for
 * <strong>required</strong> system properties in order to run this test suite.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    public TestAll() {
    }

    public TestAll(String name) {
        super(name);
    }

    public static Test suite() {

        final TestSuite suite = new TestSuite("bigdata services manager");

        // 
        suite.addTestSuite(TestServiceConfigurationZNodeEnum.class);

        // test suite for parsing zookeeper server entries.
        suite.addTestSuite(TestZookeeperServerEntry.class);

        // test suite for parsing service configurations, etc.
        suite.addTestSuite(TestServiceConfiguration.class);

        // test suite for starting a bigdata service from a service config.
        suite.addTestSuite(TestServiceStarter.class);
        
        // test suite for managing a logical service using a watcher.
        suite.addTestSuite(TestServiceConfigurationWatcher.class);
        
        return suite;
        
    }
    
}
