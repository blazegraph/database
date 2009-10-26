/*
 * Copyright SYSTAP, LLC 2006-2009.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
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
public class TestOptionals extends QuadsTestCase {

    /**
     * 
     */
    public TestOptionals() {
    }

    /**
     * @param arg0
     */
    public TestOptionals(String arg0) {
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
     * dc:title
     */
    final URI DC_TITLE = new URIImpl(DC+"title"); 
    
    
    /**
     * Tests mapping of left joins in SPARQL onto optionals in bigdata rules.
     * 
     * @throws Exception 
     */
    public void testLeftJoins() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            assertEquals(0, sail.database.getNamedGraphCount());
            
            assertFalse(cxn.getContextIDs().hasNext());
            
            final URI book1 = new URIImpl("http://www.bigdata.com/rdf#book1");
            final URI book2 = new URIImpl("http://www.bigdata.com/rdf#book2");
            final URI book3 = new URIImpl("http://www.bigdata.com/rdf#book3");
            final URI price = new URIImpl("http://www.bigdata.com/rdf#price");
            final URI XSD_INTEGER = new URIImpl("http://www.w3.org/2001/XMLSchema#integer");
/**/            
            cxn.add(
                    book1,
                    DC_TITLE,
                    new LiteralImpl("TITLE 1")
                    );
            cxn.add(
                    book1,
                    price,
                    new LiteralImpl("10", XSD_INTEGER)
                    );
            cxn.add(
                    book2,
                    DC_TITLE,
                    new LiteralImpl("TITLE 2")
                    );
            cxn.add(
                    book2,
                    price,
                    new LiteralImpl("20", XSD_INTEGER)
                    );
            cxn.add(
                    book3,
                    DC_TITLE,
                    new LiteralImpl("TITLE 3")
                    );
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            String query = 
                "SELECT ?title ?price " +
                "WHERE { " +
                "?book <"+DC_TITLE+"> ?title . " +
                "OPTIONAL { ?book <"+price+"> ?price . } . " +
                "}";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("title", new LiteralImpl("TITLE 1")), new BindingImpl("price", new LiteralImpl("10", XSD_INTEGER))));
            answer.add(createBindingSet(
                    new BindingImpl("title", new LiteralImpl("TITLE 2")), new BindingImpl("price", new LiteralImpl("20", XSD_INTEGER))));
            answer.add(createBindingSet(
                    new BindingImpl("title", new LiteralImpl("TITLE 3"))));
            
            compare(result, answer);
            
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
