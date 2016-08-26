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

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.rio.RDFFormat;

/**
 * Unit tests the quads aspects of the {@link BigdataSail} implementation.
 * Specify
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code> to run
 * this test suite.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestJoinScope extends QuadsTestCase {

    /**
     * 
     */
    public TestJoinScope() {
    }

    /**
     * @param arg0
     */
    public TestJoinScope(String arg0) {
        super(arg0);
    }

    /**
     * The foaf: namespace.
     */
    final String FOAF = "http://xmlns.com/foaf/0.1/";
    
    /**
     * foaf:name
     */
    final URI FOAF_NAME = new URIImpl(FOAF+"name"); 
    
    /**
     * foaf:mbox
     */
    final URI FOAF_MBOX = new URIImpl(FOAF+"mbox"); 
    
    /**
     * foaf:nick
     */
    final URI FOAF_NICK = new URIImpl(FOAF+"nick"); 
    
    /**
     * foaf:PersonalProfileDocument
     */
    final URI FOAF_PPD = new URIImpl(FOAF+"PersonalProfileDocument"); 
    
    /**
     * foaf:knows
     */
    final URI FOAF_KNOWS = new URIImpl(FOAF+"knows"); 
    
    /**
     * The dc: namespace.
     */
    final String DC = "http://purl.org/dc/elements/1.1/";
    
    /**
     * dc:publisher
     */
    final URI DC_PUBLISHER = new URIImpl(DC+"publisher"); 
    
    
    public void testJoinScope() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final URI graphA = new URIImpl("http://www.bigdata.com/rdf#graphA");
            final URI graphB = new URIImpl("http://www.bigdata.com/rdf#graphB");
            final URI s = new URIImpl("http://www.bigdata.com/rdf#s");
            final URI p1 = new URIImpl("http://www.bigdata.com/rdf#p1");
            final URI o1 = new URIImpl("http://www.bigdata.com/rdf#o1");
            final URI p2 = new URIImpl("http://www.bigdata.com/rdf#p2");
            final URI o2 = new URIImpl("http://www.bigdata.com/rdf#o2");

            URL url = new URL("file:/C:/DOCUME~1/mike/LOCALS~1/Temp/sparql2303/testcases-dawg/data-r2/algebra/var-scope-join-1.ttl");
            cxn.add(url, "", RDFFormat.TURTLE);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
            String query = 
                "PREFIX : <http://example/> " +
                "    SELECT * " +
                "    {  " +
                "      ?X  :name \"paul\" . " +
                "      ?X  :name \"sue\" . " +
                "      ?Y :name \"george\" . " +
                "      OPTIONAL { ?X :email ?Z } " +
                "      OPTIONAL { ?X :address ?A } " +
                "    }";
            
            final SailTupleQuery tupleQuery = (SailTupleQuery) 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            TupleExpr tupleExpr = tupleQuery.getParsedQuery().getTupleExpr();
            
            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            
            compare(result, answer);
            

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
