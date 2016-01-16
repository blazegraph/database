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
 * Created on Feb 4, 2007
 */

package com.bigdata;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
        
        final TestSuite suite = new TestSuite("bigdata");

        // core bigdata packages.
        suite.addTest( com.bigdata.cache.TestAll.suite() );
        suite.addTest( com.bigdata.io.TestAll.suite() );
        suite.addTest( com.bigdata.net.TestAll.suite() );
        suite.addTest( com.bigdata.config.TestAll.suite() );
        //suite.addTest( com.bigdata.util.TestAll.suite() );
        suite.addTest( com.bigdata.util.concurrent.TestAll.suite() );
        suite.addTest( com.bigdata.jsr166.TestAll.suite() );
        suite.addTest( com.bigdata.striterator.TestAll.suite() );
        suite.addTest( com.bigdata.counters.TestAll.suite() );
        suite.addTest( com.bigdata.rawstore.TestAll.suite() );
        suite.addTest( com.bigdata.btree.TestAll.suite() );
        suite.addTest( com.bigdata.htree.TestAll.suite() );
        suite.addTest( com.bigdata.concurrent.TestAll.suite() );
        suite.addTest( com.bigdata.quorum.TestAll.suite() );
        suite.addTest( com.bigdata.ha.TestAll.suite() );
        // Note: this has a dependency on the quorum package.
        suite.addTest(com.bigdata.io.writecache.TestAll.suite());
        suite.addTest( com.bigdata.journal.TestAll.suite() );
        suite.addTest( com.bigdata.rwstore.TestAll.suite() );
        suite.addTest( com.bigdata.resources.TestAll.suite() );
        suite.addTest( com.bigdata.relation.TestAll.suite() );
        suite.addTest( com.bigdata.bop.TestAll.suite() );
        suite.addTest( com.bigdata.relation.rule.eval.TestAll.suite() );
        suite.addTest( com.bigdata.mdi.TestAll.suite() );
        suite.addTest( com.bigdata.service.TestAll.suite() );
//        suite.addTest( com.bigdata.bop.fed.TestAll.suite() );//This was being run 3 times(!)
        suite.addTest( com.bigdata.sparse.TestAll.suite() );
        suite.addTest( com.bigdata.search.TestAll.suite() );
        suite.addTest( com.bigdata.bfs.TestAll.suite() );
//        suite.addTest( com.bigdata.service.mapReduce.TestAll.suite() );

        // Jini integration
        //BLZG-1370 moved to jini package.
        //suite.addTest(com.bigdata.jini.TestAll.suite());

        // RDF
        //Moved into bigdata-rdf-test
        //suite.addTest(com.bigdata.rdf.TestAll.suite());
        //Moved into bigdata-sails-test
 //       suite.addTest(com.bigdata.rdf.sail.TestAll.suite());

        // The REST API test suite.
        //Moved into bigdata-sails-test
  //      suite.addTest(com.bigdata.rdf.sail.webapp.TestAll.suite());

		/*
		 * The Generic Object Model and Graph API (includes remote tests against
		 * the NanoSparqlServer layer).
		 */
        suite.addTest(com.bigdata.gom.TestAll.suite());

        return suite;
        
    }

}
