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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.TestSparqlUpdateCommit.CommitCounter;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite {@link RDRHistory}.
 */
public class TestRDRHistory extends ProxyBigdataSailTestCase {

    private static final Logger log = Logger.getLogger(TestRDRHistory.class);
    
    public Properties getProperties() {
        
        return getProperties(RDRHistory.class);
        
    }

    public Properties getProperties(final Class<? extends RDRHistory> cls) {
        
        Properties props = super.getProperties();
        
        // no inference
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        // turn on RDR history
        props.setProperty(AbstractTripleStore.Options.RDR_HISTORY_CLASS, cls.getName());
        
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
     * Test basic add/remove.
     */
    public void testAddAndRemove() throws Exception {

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

            BigdataStatement stmt = vf.createStatement(s, p, o);
            cxn.add(stmt);
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
            }
            
            assertTrue(cxn.size() == 2);

            {
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        s, p, o, true);
                assertTrue(stmts.hasNext());
                stmts.close();
            }

            {
                BigdataBNode sid = vf.createBNode(stmt);
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        sid, RDRHistory.Vocab.ADDED, null, true);
                assertTrue(stmts.hasNext());
                Literal l = (Literal) stmts.next().getObject();
                assertTrue(l.getDatatype().equals(XSD.DATETIME));
                stmts.close();
            }
            
            cxn.remove(stmt);
            cxn.commit();

            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
            }
            
            assertTrue(cxn.size() == 3);

            {
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        s, p, o, true);
                assertFalse(stmts.hasNext());
                stmts.close();
            }

            {
                BigdataBNode sid = vf.createBNode(stmt);
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        sid, RDRHistory.Vocab.REMOVED, null, true);
                assertTrue(stmts.hasNext());
                Literal l = (Literal) stmts.next().getObject();
                assertTrue(l.getDatatype().equals(XSD.DATETIME));
                stmts.close();
            }
            
            cxn.add(stmt);
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
            }
            
            assertTrue(cxn.size() == 4);

            {
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        s, p, o, true);
                assertTrue(stmts.hasNext());
                stmts.close();
            }

            {
                BigdataBNode sid = vf.createBNode(stmt);
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        sid, null, null, true);
                int adds = 0;
                int removes = 0;
                while (stmts.hasNext()) {
                    final Statement result = stmts.next();
                    Literal l = (Literal) result.getObject();
                    assertTrue(l.getDatatype().equals(XSD.DATETIME));
                    final URI action = result.getPredicate();
                    if (action.equals(RDRHistory.Vocab.ADDED)) {
                        adds++;
                    } else if (action.equals(RDRHistory.Vocab.REMOVED)) {
                        removes++;
                    } else {
                        fail();
                    }
                }
                assertTrue(adds == 2);
                assertTrue(removes == 1);
                stmts.close();
            }
            
        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    /**
     * Test custom history handler.
     */
    public void testCustomHistory() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties(CustomRDRHistory.class));

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

            final BigdataValueFactory vf = (BigdataValueFactory) sail
                  .getValueFactory();
            final URI s = vf.createURI(":s");
            final URI p = vf.createURI(":p");
            final URI o = vf.createURI(":o");
            final Literal l = vf.createLiteral("o");

            BigdataStatement stmt1 = vf.createStatement(s, p, o);
            BigdataStatement stmt2 = vf.createStatement(s, p, l);
            cxn.add(stmt1);
            cxn.add(stmt2);
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
            }
            
            assertTrue(cxn.size() == 3);

            {
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        s, p, o, true);
                assertTrue(stmts.hasNext());
                stmts.close();
            }

            {
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        s, p, l, true);
                assertTrue(stmts.hasNext());
                stmts.close();
            }

            {
                BigdataBNode sid = vf.createBNode(stmt1);
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        sid, RDRHistory.Vocab.ADDED, null, true);
                assertFalse(stmts.hasNext());
                stmts.close();
            }
            
            {
                BigdataBNode sid = vf.createBNode(stmt2);
                RepositoryResult<Statement> stmts = cxn.getStatements(
                        sid, RDRHistory.Vocab.ADDED, null, true);
                assertTrue(stmts.hasNext());
                Literal l2 = (Literal) stmts.next().getObject();
                assertTrue(l2.getDatatype().equals(XSD.DATETIME));
                stmts.close();
            }
            
        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    /**
     * Test the SPARQL integration.
     */
    public void testSparqlIntegration() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            
            {
                
                final BigdataSailRepositoryConnection read =
                        repo.getReadOnlyConnection();
                
                try {
                    read.prepareTupleQuery(QueryLanguage.SPARQL, "select * { ?s ?p ?o }").evaluate();
                } finally {
                    read.close();
                }
                
            }
            
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

            {
                final String sparql =
                        "insert { " +
                        "  ?s <:p> \"foo\" . " +
                        "} where { " +
                        "  values (?s) { " +
                        "    (<:s1>) " +
                        "    (<:s2>) " +
                        "  } " +
                        "}";
                
                cxn.prepareUpdate(QueryLanguage.SPARQL, sparql).execute();
                cxn.commit();
            }
            
            {
                final String sparql =
                        "delete { " +
                        "  ?s <:p> ?o . " +
                        "} insert { " +
                        "  ?s <:p> \"bar\" . " +
                        "} where { " +
                        "  ?s <:p> ?o . " +
//                        "  values (?s) { " +
//                        "    (<:s1>) " +
//                        "  } " +
                        "}";
                
                cxn.prepareUpdate(QueryLanguage.SPARQL, sparql).execute();
                cxn.commit();
            }
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore().insert(0,'\n'));
            }
            
            assertTrue(cxn.size() == 10);
            
            {
                final String sparql =
                    "select ?s ?p ?o ?action ?time \n" +
                    "where { \n" +
//                    "  service bd:history { \n" +
//                    "    << ?s ?p ?o >> ?action ?time . \n" +
//                    "  } \n" +
                    "  bind(<< ?s ?p ?o >> as ?sid) . \n" +
                    "  hint:Prior hint:history true . \n" +
                    "  ?sid ?action ?time . \n" +
                    "  values (?s) { \n" +
                    "    (<:s1>) \n" +
                    "  } \n" +
                    "}";
                
                final TupleQueryResult result =
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
                
                int i = 0;
                while (result.hasNext()) {
                    final BindingSet bs = result.next();
                    i++;
                    if (log.isDebugEnabled()) {
                        log.debug(bs);
                    }
                }
                
                assertTrue(i == 3);
            }

            {
                final String sparql =
                    "select ?s ?p ?o ?action ?time \n" +
                    "where { \n" +
//                    "  service bd:history { \n" +
//                    "    << ?s ?p ?o >> ?action ?time . \n" +
//                    "  } \n" +
                    "  bind(<< ?s ?p ?o >> as ?sid) . \n" +
                    "  hint:Prior hint:history true . \n" +
                    "  ?sid ?action ?time . \n" +
                    "}";
                
                final TupleQueryResult result =
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
                
                int i = 0;
                while (result.hasNext()) {
                    final BindingSet bs = result.next();
                    i++;
                    if (log.isDebugEnabled()) {
                        log.debug(bs);
                    }
                }
                
                assertTrue(i == 6);
            }

            {
                final String sparql =
                    "select ?s ?p ?o ?action ?time \n" +
                    "where { \n" +
//                    "  service bd:history { \n" +
//                    "    << ?s <:p> ?o >> <:removed> ?time . \n" +
//                    "  } \n" +
                    "  bind(<< ?s <:p> ?o >> as ?sid) . \n" +
                    "  hint:Prior hint:history true . \n" +
                    "  ?sid <blaze:history:removed> ?time . \n" +
                    "}";
                
                final TupleQueryResult result =
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
                
                int i = 0;
                while (result.hasNext()) {
                    final BindingSet bs = result.next();
                    i++;
                    if (log.isDebugEnabled()) {
                        log.debug(bs);
                    }
                }
                
                assertTrue(i == 2);
            }

        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    /**
     * Test whether the RDRHistory can handle statements that are added
     * and removed in the same commit.
     */
    public void testFullyRedundantEvents() throws Exception {

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
            final Literal o = vf.createLiteral("foo");
            final BigdataStatement stmt = vf.createStatement(s, p, o);
            final BigdataBNode sid = vf.createBNode(stmt);

            cxn.add(stmt);
            cxn.commit();

            assertTrue(cxn.getTripleStore().getAccessPath(sid, null, null).rangeCount(false) == 1);
            
            cxn.remove(stmt);
            cxn.add(stmt);
            cxn.commit();

            assertTrue(cxn.getTripleStore().getAccessPath(sid, null, null).rangeCount(false) == 1);
            
            
        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    /**
     * Test whether the RDRHistory can handle statements that are added
     * and removed in the same commit.
     */
    public void testPartiallyRedundantEvents() throws Exception {

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
            final Literal o = vf.createLiteral("foo");
            final Literal bar = vf.createLiteral("bar");
            final BigdataStatement stmt = vf.createStatement(s, p, o);
            final BigdataStatement stmt2 = vf.createStatement(s, p, bar);
            final BigdataBNode sid = vf.createBNode(stmt);
            final BigdataBNode sid2 = vf.createBNode(stmt2);

            cxn.add(stmt);
            cxn.commit();

            assertTrue(cxn.getTripleStore().getAccessPath(sid, null, null).rangeCount(false) == 1);
            
            cxn.remove(stmt);
            cxn.add(stmt);
            cxn.add(stmt2);
            cxn.commit();

            assertTrue(cxn.getTripleStore().getAccessPath(sid, null, null).rangeCount(false) == 1);
            assertTrue(cxn.getTripleStore().getAccessPath(sid2, null, null).rangeCount(false) == 1);
            
            
        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    

    
    public static class CustomRDRHistory extends RDRHistory {

        public CustomRDRHistory(AbstractTripleStore database) {
            super(database);
        }

        /**
         * Only accept stmts where isLiteral(stmt.o)
         */
        @Override
        protected boolean accept(final IChangeRecord record) {
            return record.getStatement().o().isLiteral();
        }
        
    }
    
}
