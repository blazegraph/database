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

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Test suite for SPARQL* features
 */
public class TestSparqlStar extends ProxyBigdataSailTestCase {

    @Override
    public Properties getProperties() {
        
        return getProperties(RDRHistory.class);
        
    }

    public Properties getProperties(final Class<? extends RDRHistory> cls) {
        
        final Properties props = super.getProperties();
        
        // no inference
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestSparqlStar() {
    }

    /**
     * @param arg0
     */
    public TestSparqlStar(String arg0) {
        super(arg0);
    }

    
    /**
     * Test SPARQL* syntax for subject.
     */
    public void testSubject() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

			assertEquals(0, cxn.getTripleStore().getStatementCount(true));

            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();

            // check insert
            {
                final String updateStr = "insert data { " +
        				"  <x:s> <x:p> \"d\" . " +
        				"  << <x:s> <x:p> \"d\" >> <x:order> \"5\"^^xsd:int . " +
        				"}";
                final Update update = cxn.prepareUpdate(QueryLanguage.SPARQL, updateStr);
    			update.execute();

    			assertEquals(2, cxn.getTripleStore().getStatementCount(true));

        		final URI s = vf.createURI("x:s");
                final URI p = vf.createURI("x:p");
                final Literal o = vf.createLiteral("d");

                final BigdataURI p1 = vf.createURI("x:order");
                final BigdataLiteral o1 = vf.createLiteral(5);
            	
            	final RepositoryResult<Statement> result = cxn.getStatements(s,p,o,true);
            	try {
                int cnt = 0;
            	while (result.hasNext()) {
            		final BigdataStatement resultStmt = (BigdataStatement) result.next();
            		final BigdataBNode bNode = vf.createBNode(resultStmt);
					assertTrue(cxn.hasStatement(bNode, p1, o1, true));
					cnt++;
            	}
				assertEquals(1, cnt);
            	} finally {
            	    result.close();
            	}
            }
            
            // check ask
            {
	            final String selectStr = "ASK { " +
       				"  << <x:s> <x:p> \"d\" >> <x:order> \"5\"^^xsd:int . " +
					"}";
	            final BooleanQuery tq = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, selectStr);
				assertTrue(tq.evaluate());
            }

            // check select
            {
	            final String selectStr = "select ?o { " +
       				"  << <x:s> <x:p> \"d\" >> <x:order> ?o . " +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				final TupleQueryResult tqr = tq.evaluate();
				try {
	            final BigdataValue o2 = vf.createLiteral(5);
					int cnt = 0;
					while (tqr.hasNext()) {
						BindingSet bs = tqr.next();
						assertEquals(o2, bs.getBinding("o").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

            // check bind
            {
	            final String selectStr = "select ?o { " +
       				"  ?s <x:order> ?o . " +
	            	"  bind ( << <x:s> <x:p> \"d\" >> as ?s ) " +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				final TupleQueryResult tqr = tq.evaluate();
				try {
	            final BigdataValue o2 = vf.createLiteral(5);
					int cnt = 0;
					while (tqr.hasNext()) {
						BindingSet bs = tqr.next();
						assertEquals(o2, bs.getBinding("o").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

// Fails due to expansion of TRef into separate statement in the Construct clause
            // check construct
            {
	            final String selectStr = "construct { "+
	       			"  << <x:s> <x:p> \"d\" >> <x:order> ?o . " +
	            	"} where { " +
       				"  << <x:s> <x:p> \"d\" >> <x:order> ?o . " +
					"}";
	            final GraphQuery tg = cxn.prepareGraphQuery(QueryLanguage.SPARQL, selectStr);
				GraphQueryResult tgr = tg.evaluate();
	            final BigdataValue o2 = vf.createLiteral(5);
				try {
					int cnt = 0;
					while (tgr.hasNext()) {
						Statement st = tgr.next();
						assertEquals(o2, st.getObject());
						assertTrue(((BigdataBNode)st.getSubject()).isStatementIdentifier());
						cnt++;
					}
					assertEquals(1, cnt);
				} finally {
					tgr.close();
				}
            }

            // check delete
            {
	            final String deleteStr = "delete data { " +
					"  <x:s> <x:p> \"d\" . " +
					"  << <x:s> <x:p> \"d\" >> <x:order> \"5\"^^xsd:int . " +
					"}";
	            final Update delete = cxn.prepareUpdate(QueryLanguage.SPARQL, deleteStr);
				delete.execute();
	
				assertEquals(0, cxn.getTripleStore().getStatementCount(true));
            }

        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }

    /**
     * Test SPARQL* syntax for object.
     */
    public void testObject() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

			assertEquals(0, cxn.getTripleStore().getStatementCount(true));

			final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();

			// check insert
			{
	            final String updateStr = "insert data { " +
					"  <x:s> <x:p> \"d\" . " +
					"  <x:r> <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"}";
	            final Update update = cxn.prepareUpdate(QueryLanguage.SPARQL, updateStr);
				update.execute();
	
				assertEquals(2, cxn.getTripleStore().getStatementCount(true));

                final URI s = vf.createURI("x:s");
                final URI p = vf.createURI("x:p");
                final Literal o = vf.createLiteral("d");

                final BigdataURI s1 = vf.createURI("x:r");
                final BigdataURI p1 = vf.createURI("x:refers");
            	
                final RepositoryResult<Statement> result = cxn.getStatements(s,p,o,true);
                try {
                int cnt = 0;
            	while (result.hasNext()) {
            		final BigdataStatement resultStmt = (BigdataStatement) result.next();
            		final BigdataBNode bNode = vf.createBNode(resultStmt);
					assertTrue(cxn.hasStatement(s1, p1, bNode, true));
					cnt++;
            	}
				assertEquals(1, cnt);
                } finally {
                    result.close();
                }
            }

            // check ask
            {
	            final String selectStr = "ASK { " +
					"  <x:r> <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"}";
	            final BooleanQuery tq = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, selectStr);
				assertTrue(tq.evaluate());
            }

            // check select
            {
	            final String selectStr = "select ?s { " +
					"  ?s <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				final TupleQueryResult tqr = tq.evaluate();
				try {
	            final BigdataURI s2 = vf.createURI("x:r");
					int cnt = 0;
					while (tqr.hasNext()) {
					    final BindingSet bs = tqr.next();
						assertEquals(s2, bs.getBinding("s").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

            // check bind
            {
	            final String selectStr = "select ?s { " +
					"  ?s <x:refers> ?o . " +
            		"  bind ( << <x:s> <x:p> \"d\" >> as ?o ) " +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				final TupleQueryResult tqr = tq.evaluate();
				try {
	            final BigdataURI s2 = vf.createURI("x:r");
					int cnt = 0;
					while (tqr.hasNext()) {
					    final BindingSet bs = tqr.next();
						assertEquals(s2, bs.getBinding("s").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

            // check construct
            {
	            final String selectStr = "construct { "+
					"  ?s <x:refers> << <x:s> <x:p> \"d\" >> . " +
	            	"} where { " +
					"  ?s <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"}";
	            final GraphQuery tg = cxn.prepareGraphQuery(QueryLanguage.SPARQL, selectStr);
				GraphQueryResult tgr = tg.evaluate();
	            final BigdataURI s2 = vf.createURI("x:r");
				try {
					int cnt = 0;
					while (tgr.hasNext()) {
						Statement st = tgr.next();
						assertEquals(s2, st.getSubject());
						assertTrue(((BigdataBNode)st.getObject()).isStatementIdentifier());
						cnt++;
					}
					assertEquals(1, cnt);
				} finally {
					tgr.close();
				}
            }

			// check delete
	        {
	            final String deteleStr = "delete data { " +
					"  <x:s> <x:p> \"d\" . " +
					"  <x:r> <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"}";
	            final Update delete = cxn.prepareUpdate(QueryLanguage.SPARQL, deteleStr);
	            delete.execute();
	
				assertEquals(0, cxn.getTripleStore().getStatementCount(true));
	        }

        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }

    /**
     * Test recursive SPARQL* syntax.
     */
    public void testRecursion() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

			assertEquals(0, cxn.getTripleStore().getStatementCount(true));

            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            // check insert
            {
                final String updateStr = "insert data { " +
        				"  <x:s> <x:p> \"d\" . " +
        				"  <x:r> <x:refers> << <x:s> <x:p> \"d\" >> . " +
        				"  <x:z> <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
        				"}";
                    final Update update = cxn.prepareUpdate(QueryLanguage.SPARQL, updateStr);
        			update.execute();

        			assertEquals(3, cxn.getTripleStore().getStatementCount(true));

                final URI s = vf.createURI("x:s");
                final URI p = vf.createURI("x:p");
                final Literal o = vf.createLiteral("d");

                final BigdataURI s1 = vf.createURI("x:r");
                final BigdataURI p1 = vf.createURI("x:refers");

                final BigdataURI s2 = vf.createURI("x:z");
                final BigdataURI p2 = vf.createURI("x:recurs");

            	final RepositoryResult<Statement> result = cxn.getStatements(s,p,o,true);
            	try {
            	int cnt = 0;
            	while (result.hasNext()) {
            		final BigdataStatement resultStmt = (BigdataStatement) result.next();
            		final BigdataBNode bNode = vf.createBNode(resultStmt);
                    	final RepositoryResult<Statement> result2 = cxn.getStatements(s1,p1,bNode,true);
                    	try {
                	while (result2.hasNext()) {
                		final BigdataStatement result2Stmt = (BigdataStatement) result2.next();
                		final BigdataBNode bNode2 = vf.createBNode(result2Stmt);
                		assertTrue(cxn.hasStatement(s2, p2, bNode2, true));
                		cnt++;
                	}
                    	} finally {
                    	    result2.close();
                    	}
            	}
				assertEquals(1, cnt);
            	} finally {
            	    result.close();
            	}
            }

            // check ask
            {
	            final String selectStr = "ASK { " +
       				"  <x:z> <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
					"}";
	            final BooleanQuery tq = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, selectStr);
				assertTrue(tq.evaluate());
            }

            // check select
            {
	            final String selectStr = "select ?s { " +
	            	"  ?s <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				TupleQueryResult tqr = tq.evaluate();
	            final BigdataURI s2 = vf.createURI("x:z");
				try {
					int cnt = 0;
					while (tqr.hasNext()) {
						BindingSet bs = tqr.next();
						assertEquals(s2, bs.getBinding("s").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

            // check bind
            {
	            final String selectStr = "select ?s { " +
	            	"  ?s <x:recurs> ?o ." +
	            	"  bind (<< <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >> as ?o ) " +
					"}";
	            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, selectStr);
				TupleQueryResult tqr = tq.evaluate();
	            final BigdataURI s2 = vf.createURI("x:z");
				try {
					int cnt = 0;
					while (tqr.hasNext()) {
						BindingSet bs = tqr.next();
						assertEquals(s2, bs.getBinding("s").getValue());
						cnt ++;
					}
					assertEquals(1, cnt);
				} finally {
					tqr.close();
				}
            }

            // check construct
            {
	            final String selectStr = "construct { "+
	            	"  ?s <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
	            	"} where { " +
	            	"  ?s <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
					"}";
	            final GraphQuery tg = cxn.prepareGraphQuery(QueryLanguage.SPARQL, selectStr);
				GraphQueryResult tgr = tg.evaluate();
	            final BigdataURI s2 = vf.createURI("x:z");
				try {
					int cnt = 0;
					while (tgr.hasNext()) {
						Statement st = tgr.next();
						assertEquals(s2, st.getSubject());
						assertTrue(((BigdataBNode)st.getObject()).isStatementIdentifier());
						cnt++;
					}
					assertEquals(1, cnt);
				} finally {
					tgr.close();
				}
            }

            // check delete
			{
	            final String deleteStr = "delete data { " +
					"  <x:s> <x:p> \"d\" . " +
					"  <x:r> <x:refers> << <x:s> <x:p> \"d\" >> . " +
					"  <x:z> <x:recurs> << <x:r> <x:refers> << <x:s> <x:p> \"d\" >> >>." +
					"}";
	            final Update delete = cxn.prepareUpdate(QueryLanguage.SPARQL, deleteStr);
	            delete.execute();

	            assertEquals(0, cxn.getTripleStore().getStatementCount(true));
			}

        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }

}
