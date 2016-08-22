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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.sparql.TestVerifyAggregates;

/**
 * Test suite for the {@link BigdataSail} with statement identifiers enabled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataSailWithoutSids extends AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailWithoutSids() {
    }

    public TestBigdataSailWithoutSids(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        final TestBigdataSailWithoutSids delegate = new TestBigdataSailWithoutSids(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate, "SAIL with Triples (no SIDs)");

        // test rewrite of RDF Value => BigdataValue for binding set and tuple expr.
        suite.addTestSuite(TestBigdataValueReplacer.class);

        // test pruning of variables not required for downstream processing.
        suite.addTestSuite(TestPruneBindingSets.class);

        // test of the search magic predicate
        suite.addTestSuite(TestSearchQuery.class);

        // test of high-level query on a graph with statements about statements.
        suite.addTestSuite(TestProvenanceQuery.class);

        suite.addTestSuite(TestUnions.class);
        
        suite.addTestSuite(TestInlineValues.class);
        
        // Validation logic for aggregation operators.
        suite.addTestSuite(TestVerifyAggregates.class);
        
        suite.addTestSuite(TestConcurrentKBCreate.TestWithGroupCommit.class);
        suite.addTestSuite(TestConcurrentKBCreate.TestWithoutGroupCommit.class);

        suite.addTestSuite(TestTxCreate.class);
        suite.addTestSuite(TestCnxnCreate.class);

        suite.addTestSuite(TestChangeSets.class);

        // test suite for the history index.
        suite.addTestSuite(TestHistoryIndex.class);

		suite.addTestSuite(com.bigdata.rdf.sail.TestRollbacks.class);
		suite.addTestSuite(com.bigdata.rdf.sail.TestRollbacksTx.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestRollbacksTM.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestMROWTransactionsNoHistory.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestMROWTransactionsWithHistory.class);
		
        suite.addTestSuite(com.bigdata.rdf.sail.TestMillisecondPrecisionForInlineDateTimes.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket275.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket276.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket422.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket473.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestLexJoinOps.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestMaterialization.class);
        
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket610.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket669.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket693.class);
        
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1388.class);
        
		suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1747.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1753.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1755.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1785.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1788.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1893.class);
        //suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1939.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket2043.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket2043b.class);
        
        suite.addTestSuite(com.bigdata.rdf.sail.webapp.TestBLZG1943.class);
        
        return suite;
        
    }
    
    private Properties properties = null;
    
    @Override
    protected void tearDown(final ProxyBigdataSailTestCase testCase) throws Exception {

        super.tearDown(testCase);
        
        properties = null;
        
    }
    
    @Override
    protected BigdataSail getSail(final Properties properties) {
        
        this.properties = properties;
        
        return new BigdataSail(properties);
        
    }

    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());
/*
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        
        properties.setProperty(Options.QUADS, "false");
*/
        properties.setProperty(Options.TRIPLES_MODE, "true");
        
        return properties;
        
    }
    
    @Override
    protected BigdataSail reopenSail(final BigdataSail sail) {

//        final Properties properties = sail.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return getSail(properties);
        
    }

}
