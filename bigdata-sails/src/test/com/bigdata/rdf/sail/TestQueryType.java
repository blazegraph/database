/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 20, 2011
 */

package com.bigdata.rdf.sail;

import junit.framework.TestCase2;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.sail.sparql.BigdataSPARQLParser;
import com.bigdata.rdf.store.BD;

/**
 * Test suite for {@link BigdataSPARQLParser}'s ability to report the
 * {@link QueryType} of a SPARQL Query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryType extends TestCase2 {

    /**
     * 
     */
    public TestQueryType() {
    }

    /**
     * @param name
     */
    public TestQueryType(String name) {
        super(name);
    }

    final QueryType fromQuery(final String qs) throws MalformedQueryException {
        
        final String baseURI = "http://www.bigdata.com/";

        return ((IBigdataParsedQuery) new BigdataSPARQLParser().parseQuery(qs,
                baseURI)).getQueryType();
        
    }
    
    public void test_select() throws MalformedQueryException {
    
        final String s = "select ?p ?o where {<http://bigdata.com/foo> ?p ?o}";

        assertEquals(QueryType.SELECT, fromQuery(s));

    }

    public void test_select_with_ask_in_URI() throws MalformedQueryException {
        
        final String s = "select ?p ?o where {<http://blablabla.com/ask_something> ?p ?o}";

        assertEquals(QueryType.SELECT, fromQuery(s));

    }

    public void test_select_with_ask_in_PREFIX() throws MalformedQueryException {
        
        final String s = 
            "prefix bd: <"+BD.NAMESPACE+"> " +
            "prefix foo: <http://www.bigdata.com/test/ask/ns> " +
            "select ?p ?o where {<http://blablabla.com/ask_something> ?p ?o}";

        assertEquals(QueryType.SELECT, fromQuery(s));

    }

    public void test_describe() throws MalformedQueryException  {
    
        final String s = 
                "prefix bd: <"+BD.NAMESPACE+"> " +
                "prefix rdf: <"+RDF.NAMESPACE+"> " +
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
                "describe ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

        assertEquals(QueryType.DESCRIBE, fromQuery(s));

    }

    public void test_construct() throws MalformedQueryException {

        /*
         * Sample query from the SPARQL 1.0 Recommendation.
         */
        final String s = "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>"
                + "PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>"
                + "CONSTRUCT   { <http://example.org/person#Alice> vcard:FN ?name }"
                + "WHERE       { ?x foaf:name ?name }";

        assertEquals(QueryType.CONSTRUCT, fromQuery(s));

    }
    
    public void test_ask() throws MalformedQueryException {

        /*
         * Sample query from the SPARQL 1.0 Recommendation.
         */
        final String s = "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>"
                + "ASK  { ?x foaf:name  \"Alice\" }";

        assertEquals(QueryType.ASK, fromQuery(s));

    }
    
}
