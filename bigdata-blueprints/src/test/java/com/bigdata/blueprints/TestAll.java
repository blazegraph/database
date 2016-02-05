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

package com.bigdata.blueprints;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {
    
    /**
     * Static flags to HA and/or Quorum to be excluded, preventing hangs in CI
     */

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
     * Aggregates the tests in increasing dependency order.
     */
    public static Test suite()
    {

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN. If you are using a
         * log4j configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            final Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        final TestSuite suite = new TestSuite("blueprints");

        
        //Factory and client configuration test cases.
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphClientNSS.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphFactoryNSS.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphFactoryFile.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphEmbeddedRepository.class);

        //Blueprints related test cases
        //See BLZG-1415 
        //suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphEmbeddedTransactional.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphClientInMemorySail.class);
        //See BLZG-1415 
        suite.addTestSuite(com.bigdata.blueprints.TestPathConstraints.class);
        
        return suite;
        
    }
    
}
