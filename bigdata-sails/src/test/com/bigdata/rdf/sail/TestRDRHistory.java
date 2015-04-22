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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite {@link RDRHistory}.
 */
public class TestRDRHistory extends ProxyBigdataSailTestCase {

    private static final Logger log = Logger.getLogger(TestRDRHistory.class);
    
    public Properties getProperties() {
        
        return getProperties(MyRDRHistory.class);
        
    }

    public Properties getProperties(final Class<? extends RDRHistory> cls) {
        
        Properties props = super.getProperties();
        
        // no inference
//        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
//        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
//        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
//        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        // turn on RDR history
//        props.setProperty(AbstractTripleStore.Options.RDR_HISTORY_CLASS, cls.getName());
        
        return props;
        
    }

    /**
     * 
     */
    public TestRDRHistory() {
    }

    /**
     * @param arg0
     */
    public TestRDRHistory(String arg0) {
        super(arg0);
    }

    
    /**
     * 
     */
    public void testAdd() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

            final BigdataValueFactory vf = (BigdataValueFactory) sail
                  .getValueFactory();
            final URI s = vf.createURI(":s");
            final URI p = vf.createURI(":p");
            final URI o = vf.createURI(":o");

//            BigdataStatement stmt = vf.createStatement(s, p, o);
//            cxn.add(stmt);
//            cxn.commit();
            
//            if (log.isInfoEnabled()) {
//                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
//            }
            
//            RepositoryResult<Statement> stmts = cxn.getStatements(null, null,
//                  null, false);
//            while (stmts.hasNext()) {
//                Statement res = stmts.next();
//                if (log.isInfoEnabled()) {
//                    log.info(res);
//                }
//            }

        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    public static final class MyRDRHistory extends RDRHistory {

        public static final URI ADDED = new URIImpl(":added");
        
        public static final URI REMOVED = new URIImpl(":removed");
        
        public MyRDRHistory(final AbstractTripleStore database) {
            super(database);
        }

        @Override
        protected URI added() {
            return ADDED;
        }

        @Override
        protected URI removed() {
            return REMOVED;
        }
        
    }

}
