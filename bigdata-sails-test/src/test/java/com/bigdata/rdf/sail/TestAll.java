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
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    public static Test suite() {

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN unless it has
         * already been set to another value. If you are using a log4j
         * configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        final TestSuite suite = new TestSuite("Sesame 2.x integration");

        // test suite for the SPARQL parse tree => bigdata AST translation.
        suite.addTest(com.bigdata.rdf.sail.sparql.TestAll.suite());
        
        // bootstrap tests for the BigdataSail
        suite.addTestSuite(TestBootstrapBigdataSail.class);

        // run the test suite with statement identifiers enabled.
        suite.addTest(TestBigdataSailWithSids.suite());
        
        // run the test suite without statement identifiers enabled.
        suite.addTest(TestBigdataSailWithoutSids.suite());
        
        // quad store test suite w/ pipeline joins.
        suite.addTest(TestBigdataSailWithQuads.suite());

        // SPARQL Updates
        suite.addTest(com.bigdata.rdf.sail.tck.TestAll.suite());

        // NanoSparqlServer
        suite.addTest(com.bigdata.rdf.sail.webapp.TestAll.suite());
        
        /* FIXME Restore:: quad store in scale-out.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/196 (Journal Leaks Memory)
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/523 (Temporary Journals in CI)
         */
//        suite.addTest(TestBigdataSailEmbeddedFederationWithQuads.suite());
        
        return suite;

    }
    
}
