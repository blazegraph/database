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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;

/**
 * Unit tests the quads aspects of the {@link BigdataSail} implementation.
 * Specify
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code> to run
 * this test suite.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestQuadsAPI extends QuadsTestCase {

    /**
     * 
     */
    public TestQuadsAPI() {
    }

    /**
     * @param arg0
     */
    public TestQuadsAPI(String arg0) {
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
    
    
    /**
     * Test loads data into two graphs and verifies some access to those
     * graphs.
     * @throws Exception 
     */
    public void test_2graphs() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            assertEquals(0, sail.database.getNamedGraphCount());
            
            assertFalse(cxn.getContextIDs().hasNext());
            
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final URI graphA = new URIImpl("http://www.bigdata.com/graphA");
            final URI graphB = new URIImpl("http://www.bigdata.com/graphB");
/**/            
            cxn.add(
                    a,
                    new URIImpl(FOAF+"name"),
                    new LiteralImpl("Alice"),
                    graphA
                    );
            cxn.add(
                    a,
                    new URIImpl(FOAF+"mbox"),
                    new URIImpl("mailto:alice@work.example"),
                    graphA
                    );
            /*
             * Graph B.
             */
            cxn.add(
                    b,
                    new URIImpl(FOAF+"name"),
                    new LiteralImpl("Bob"),
                    graphB
                    );
            cxn.add(
                    b,
                    new URIImpl(FOAF+"mbox"),
                    new URIImpl("mailto:bob@work.example"),
                    graphB
                    );
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            assertEquals(2, sail.database.getNamedGraphCount());
            
            assertSameIterationAnyOrder(new Resource[] { graphA, graphB }, cxn
                    .getContextIDs());

/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSCequality() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            assertEquals(0, sail.database.getNamedGraphCount());
            
            assertFalse(cxn.getContextIDs().hasNext());
            
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final URI graphA = new URIImpl("http://www.bigdata.com/rdf#graphA");
            final URI graphB = new URIImpl("http://www.bigdata.com/rdf#graphB");
            final URI s = new URIImpl("http://www.bigdata.com/rdf#s");
            final URI p1 = new URIImpl("http://www.bigdata.com/rdf#p1");
            final URI o1 = new URIImpl("http://www.bigdata.com/rdf#o1");
            final URI p2 = new URIImpl("http://www.bigdata.com/rdf#p2");
            final URI o2 = new URIImpl("http://www.bigdata.com/rdf#o2");
/**/            
            cxn.add(
                    graphA,
                    p1,
                    o1,
                    graphA
                    );
            
            cxn.add(
                    s,
                    p1,
                    o1,
                    graphA
                    );
            
            cxn.add(
                    graphA,
                    p2,
                    o2,
                    graphA
                    );
            
            cxn.add(
                    s,
                    p2,
                    o2,
                    graphB
                    );
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
//            cxn.flush();//commit();
            cxn.commit();
            
            assertEquals(2, sail.database.getNamedGraphCount());
            
            assertSameIterationAnyOrder(new Resource[] { graphA, graphB }, cxn
                    .getContextIDs());

/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }
            
            String query = 
                "SELECT  * " +
                "WHERE { GRAPH ?g { " +
                "?g <"+p1+"> <"+o1+"> . " +
                "?g <"+p2+"> <"+o2+"> . " +
                "}}";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("g", graphA)));
            
            compare(result, answer);
            

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
