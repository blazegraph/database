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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.store.BD;

/**
 * Unit tests for named graphs. Specify
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code> to
 * run this test suite.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestNamedGraphs extends QuadsTestCase {

    protected static final Logger log = Logger.getLogger(TestNamedGraphs.class);
    
    /**
     * 
     */
    public TestNamedGraphs() {
    }

    /**
     * @param arg0
     */
    public TestNamedGraphs(String arg0) {
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
     * 8.2.1 Specifying the Default Graph
     * 
     * Each FROM clause contains an IRI that indicates a graph to be used to 
     * form the default graph. This does not put the graph in as a named graph.
     * 
     * In this example, the RDF Dataset contains a single default graph and no 
     * named graphs:
     * 
     * # Default graph (stored at http://example.org/foaf/aliceFoaf)
     * @prefix  foaf:  <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT  ?name
     * FROM    <http://example.org/foaf/aliceFoaf>
     * WHERE   { ?x foaf:name ?name }
     * 
     * name
     * "Alice"
     * 
     * If a query provides more than one FROM clause, providing more than one 
     * IRI to indicate the default graph, then the default graph is based on 
     * the RDF merge of the graphs obtained from representations of the 
     * resources identified by the given IRIs.     
     */
    public void test_8_2_1() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.2.1 Specifying the Default Graph");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final URI graph = new URIImpl("http://example.org/foaf/aliceFoaf");
/**/            
            cxn.add(
                    a,
                    new URIImpl(FOAF+"name"),
                    new LiteralImpl("Alice"),
                    graph
                    );
            cxn.add(
                    a,
                    new URIImpl(FOAF+"mbox"),
                    new URIImpl("mailto:alice@work.example"),
                    graph
                    );
            cxn.commit();
/**/            
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "SELECT  ?name " +
                "FROM    <http://example.org/foaf/aliceFoaf> " +
                "WHERE   { ?x foaf:name ?name }";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("name", new LiteralImpl("Alice"))));
            
            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    /**
     * 8.2.3 Combining FROM and FROM NAMED
     * 
     * The FROM clause and FROM NAMED clause can be used in the same query.
     * 
     * # Default graph (stored at http://example.org/dft.ttl)
     * @prefix dc: <http://purl.org/dc/elements/1.1/> .
     * 
     * <http://example.org/bob>    dc:publisher  "Bob Hacker" .
     * <http://example.org/alice>  dc:publisher  "Alice Hacker" .
     * 
     * # Named graph: http://example.org/bob
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Bob" .
     * _:a foaf:mbox <mailto:bob@oldcorp.example.org> .
     * 
     * # Named graph: http://example.org/alice
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * 
     * _:a foaf:name "Alice" .
     * _:a foaf:mbox <mailto:alice@work.example.org> .
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * 
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM NAMED <http://example.org/alice>
     * FROM NAMED <http://example.org/bob>
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * 
     * The RDF Dataset for this query contains a default graph and two named 
     * graphs. The GRAPH keyword is described below.
     * 
     * The actions required to construct the dataset are not determined by the 
     * dataset description alone. If an IRI is given twice in a dataset 
     * description, either by using two FROM clauses, or a FROM clause and a 
     * FROM NAMED clause, then it does not assume that exactly one or 
     * exactly two attempts are made to obtain an RDF graph associated with the 
     * IRI. Therefore, no assumptions can be made about blank node identity in 
     * triples obtained from the two occurrences in the dataset description. 
     * In general, no assumptions can be made about the equivalence of the 
     * graphs. 
     */
    public void test_8_2_3() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.2.3 Combining FROM and FROM NAMED");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final URI bob = new URIImpl("http://example.org/bob");
            final URI alice = new URIImpl("http://example.org/alice");
            final URI graph = new URIImpl("http://example.org/dft.ttl");
            final URI aliceMbox = new URIImpl("mailto:alice@work.example.org");
            final URI bobMbox = new URIImpl("mailto:bob@oldcorp.example.org");
/**/            
            cxn.add(
                    bob,
                    DC_PUBLISHER,
                    new LiteralImpl("Bob Hacker"),
                    graph
                    );
            cxn.add(
                    alice,
                    DC_PUBLISHER,
                    new LiteralImpl("Alice Hacker"),
                    graph
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    bob
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    bobMbox,
                    bob
                    );
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    aliceMbox,
                    alice
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
/**/            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
                "SELECT ?who ?g ?mbox " +
                "FROM <http://example.org/dft.ttl> " +
                "FROM NAMED <http://example.org/alice> " +
                "FROM NAMED <http://example.org/bob> " +
                "WHERE " +
                "{ " +
                "    ?g dc:publisher ?who . " +
                "    GRAPH ?g { ?x foaf:mbox ?mbox } " +
                "}";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results
            
            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("g", alice),
                    new BindingImpl("mbox", aliceMbox),
                    new BindingImpl("who", new LiteralImpl("Alice Hacker"))));
            answer.add(createBindingSet(
                    new BindingImpl("g", bob),
                    new BindingImpl("mbox", bobMbox),
                    new BindingImpl("who", new LiteralImpl("Bob Hacker"))));
            
            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    /**
     * 8.3.1 Accessing Graph Names
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * The query below matches the graph pattern against each of the named 
     * graphs in the dataset and forms solutions which have the src variable 
     * bound to IRIs of the graph being matched. The graph pattern is matched 
     * with the active graph being each of the named graphs in the dataset.
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?src ?bobNick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *     GRAPH ?src
     *     { ?x foaf:mbox <mailto:bob@work.example> .
     *       ?x foaf:nick ?bobNick
     *     }
     *   }
     * 
     * The query result gives the name of the graphs where the information was 
     * found and the value for Bob's nick:
     * src     bobNick
     * <http://example.org/foaf/aliceFoaf>     "Bobby"
     * <http://example.org/foaf/bobFoaf>   "Robert"
     */
    public void test_8_3_1() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.1 Accessing Graph Names");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "SELECT ?src ?bobNick " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "WHERE " +
                "  { " +
                "    GRAPH ?src " +
                "    { ?x foaf:mbox <mailto:bob@work.example> . " +
                "      ?x foaf:nick ?bobNick " +
                "    } " +
                "  }"; 
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("src", alice),
                    new BindingImpl("bobNick", new LiteralImpl("Bobby"))));
            answer.add(createBindingSet(
                    new BindingImpl("src", bob),
                    new BindingImpl("bobNick", new LiteralImpl("Robert"))));
            
            compare(result, answer);

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    /**
     * 8.3.2 Restricting by Graph IRI
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * The query can restrict the matching applied to a specific graph by 
     * supplying the graph IRI. This sets the active graph to the graph named 
     * by the IRI. This query looks for Bob's nick as given in the graph 
     * http://example.org/foaf/bobFoaf.
     * 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX data: <http://example.org/foaf/>
     * 
     * SELECT ?nick
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     *   {
     *      GRAPH data:bobFoaf {
     *          ?x foaf:mbox <mailto:bob@work.example> .
     *          ?x foaf:nick ?nick }
     *   }
     * 
     * which yields a single solution:
     * nick
     * "Robert"
     */
    public void test_8_3_2() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.2 Restricting by Graph IRI");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "PREFIX data: <http://example.org/foaf/> " +
                "SELECT ?nick " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "WHERE " +
                "  { " +
                "     GRAPH data:bobFoaf { " +
                "         ?x foaf:mbox <mailto:bob@work.example> . " +
                "         ?x foaf:nick ?nick } " +
                "  }";
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("nick", new LiteralImpl("Robert"))));
            
            compare(result, answer);

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    /**
     * 8.3.3 Restricting Possible Graph IRIs
     * 
     * The following two graphs will be used in examples:
     * 
     * # Named graph: http://example.org/foaf/aliceFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:a  foaf:name     "Alice" .
     * _:a  foaf:mbox     <mailto:alice@work.example> .
     * _:a  foaf:knows    _:b .
     * 
     * _:b  foaf:name     "Bob" .
     * _:b  foaf:mbox     <mailto:bob@work.example> .
     * _:b  foaf:nick     "Bobby" .
     * _:b  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * # Named graph: http://example.org/foaf/bobFoaf
     * @prefix  foaf:     <http://xmlns.com/foaf/0.1/> .
     * @prefix  rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  rdfs:     <http://www.w3.org/2000/01/rdf-schema#> .
     * 
     * _:z  foaf:mbox     <mailto:bob@work.example> .
     * _:z  rdfs:seeAlso  <http://example.org/foaf/bobFoaf> .
     * _:z  foaf:nick     "Robert" .
     * 
     * <http://example.org/foaf/bobFoaf> rdf:type foaf:PersonalProfileDocument .
     * 
     * A variable used in the GRAPH clause may also be used in another GRAPH 
     * clause or in a graph pattern matched against the default graph in the 
     * dataset.
     *
     * The query below uses the graph with IRI http://example.org/foaf/aliceFoaf 
     * to find the profile document for Bob; it then matches another pattern 
     * against that graph. The pattern in the second GRAPH clause finds the 
     * blank node (variable w) for the person with the same mail box (given by 
     * variable mbox) as found in the first GRAPH clause (variable whom), 
     * because the blank node used to match for variable whom from Alice's FOAF 
     * file is not the same as the blank node in the profile document (they are 
     * in different graphs).
     * 
     * PREFIX  data:  <http://example.org/foaf/>
     * PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>
     * PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * SELECT ?mbox ?nick ?ppd
     * FROM NAMED <http://example.org/foaf/aliceFoaf>
     * FROM NAMED <http://example.org/foaf/bobFoaf>
     * WHERE
     * {
     *   GRAPH data:aliceFoaf
     *   {
     *     ?alice foaf:mbox <mailto:alice@work.example> ;
     *            foaf:knows ?whom .
     *     ?whom  foaf:mbox ?mbox ;
     *            rdfs:seeAlso ?ppd .
     *     ?ppd  a foaf:PersonalProfileDocument .
     *   } .
     *   GRAPH ?ppd
     *   {
     *       ?w foaf:mbox ?mbox ;
     *          foaf:nick ?nick
     *   }
     * }
     * 
     * mbox    nick    ppd
     * <mailto:bob@work.example>   "Robert"    <http://example.org/foaf/bobFoaf>
     * 
     * Any triple in Alice's FOAF file giving Bob's nick is not used to provide 
     * a nick for Bob because the pattern involving variable nick is restricted 
     * by ppd to a particular Personal Profile Document.
     */
    public void test_8_3_3() throws RepositoryException, SailException, 
             MalformedQueryException, QueryEvaluationException, IOException {

        if(log.isInfoEnabled())
            log.info("testing: 8.3.3 Restricting Possible Graph IRIs");
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
        
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode z = new BNodeImpl("_:b");
            final URI alice = new URIImpl("http://example.org/foaf/aliceFoaf");
            final URI bob = new URIImpl("http://example.org/foaf/bobFoaf");
            
            cxn.add(
                    a,
                    FOAF_NAME,
                    new LiteralImpl("Alice"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_MBOX,
                    new URIImpl("mailto:alice@work.example"),
                    alice
                    );
            cxn.add(
                    a,
                    FOAF_KNOWS,
                    b,
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NAME,
                    new LiteralImpl("Bob"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    alice
                    );
            cxn.add(
                    b,
                    FOAF_NICK,
                    new LiteralImpl("Bobby"),
                    alice
                    );
            cxn.add(
                    b,
                    RDFS.SEEALSO,
                    bob,
                    alice
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    alice
                    );
            cxn.add(
                    z,
                    FOAF_MBOX,
                    new URIImpl("mailto:bob@work.example"),
                    bob
                    );
            cxn.add(
                    z,
                    RDFS.SEEALSO,
                    bob,
                    bob
                    );
            cxn.add(
                    z,
                    FOAF_NICK,
                    new LiteralImpl("Robert"),
                    bob
                    );
            cxn.add(
                    bob,
                    RDF.TYPE,
                    FOAF_PPD,
                    bob
                    );
            cxn.commit();
            if(log.isInfoEnabled()) {
                log.info("\n"+sail.getDatabase().dumpStore().toString());
            }
            
            String query = 
                "PREFIX  data:  <http://example.org/foaf/> " +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/> " +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?mbox ?nick ?ppd " +
                "FROM NAMED <http://example.org/foaf/aliceFoaf> " +
                "FROM NAMED <http://example.org/foaf/bobFoaf> " +
                "WHERE " +
                "{ " +
                "  GRAPH data:aliceFoaf " +
                "  { " +
                "    ?alice foaf:mbox <mailto:alice@work.example> ; " +
                "           foaf:knows ?whom . " +
                "    ?whom  foaf:mbox ?mbox ; " +
                "           rdfs:seeAlso ?ppd . " +
                "    ?ppd  a foaf:PersonalProfileDocument . " +
                "  } . " +
                "  GRAPH ?ppd " +
                "  { " +
                "      ?w foaf:mbox ?mbox ; " +
                "         foaf:nick ?nick " +
                "  } " +
                "}"; 
            
            final TupleQuery tupleQuery = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();

            Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet(
                    new BindingImpl("mbox", new URIImpl("mailto:bob@work.example")),
                    new BindingImpl("nick", new LiteralImpl("Robert")),
                    new BindingImpl("ppd", bob)));
            
            compare(result, answer);

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }

    /**
     * A series of tests for querying SPARQL default graphs. The expander should
     * apply the access path to the graph associated with each specified URI,
     * visiting the distinct (s,p,o) tuples found in those graph(s).
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws QueryEvaluationException
     * @throws MalformedQueryException
     */
    public void test_defaultGraphs_accessPathScan() throws RepositoryException,
            SailException, QueryEvaluationException, MalformedQueryException {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);

        try {
            
            if(!sail.getDatabase().isQuads()) {
                
                log.warn("test requires quads.");

                return;
                
            }

            final URI john = new URIImpl("http://www.bigdata.com/john");
            final URI loves= new URIImpl("http://www.bigdata.com/loves");
            final URI mary = new URIImpl("http://www.bigdata.com/mary");
            final URI paul = new URIImpl("http://www.bigdata.com/paul");
            final URI c1 = new URIImpl("http://www.bigdata.com/context1");
            final URI c2 = new URIImpl("http://www.bigdata.com/context2");
            final URI c3 = new URIImpl("http://www.bigdata.com/context3");

            /*
             * Setup the data for the test.
             */
            {
                // add statements to the store.
                cxn.add(john, loves, mary, c1);
                cxn.add(mary, loves, paul, c2);
                cxn.commit();
            }

            {

                /*
                 * One graph in the defaultGraphs set, but the specified graph
                 * has no data. Since there are no joins this is executed
                 * directly by the BigdataSailConnection.
                 */
                
                final String query = 
                    "SELECT  ?x " +
                    "FROM    <"+c3+"> " +
                    "WHERE   { ?x <"+loves+"> ?y }";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                
                tupleQuery.setIncludeInferred(true /* includeInferred */);
  
                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                compare(result, answer);

            }

            {

                /*
                 * One graph in the defaultGraphs set. Since there are no joins
                 * this is executed directly by the BigdataSailConnection.
                 */

                final String query = "SELECT  ?x ?y " + //
                        "FROM    <" + c1 + "> " + //
                        "WHERE   { ?x <" + loves + "> ?y }"//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(new BindingImpl("x", john),
                        new BindingImpl("y", mary)));

                compare(result, answer);

            }

            {

                /*
                 * Two graphs in the defaultGraphs set.
                 */

                final String query = "SELECT  ?x ?y " + //
                        "FROM    <" + c1 + "> " + //
                        "FROM    <" + c2 + "> " + //
                        "WHERE   { ?x <" + loves + "> ?y }"//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(new BindingImpl("x", john),
                        new BindingImpl("y", mary)));

                answer.add(createBindingSet(new BindingImpl("x", mary),
                        new BindingImpl("y", paul)));

                compare(result, answer);

            }

            {

                /*
                 * Query with more restricted predicate bindings (john is bound
                 * as the subject).
                 */

                final String query = "SELECT  ?y " + //
                        "FROM    <" + c1 + "> " + //
                        "FROM    <" + c2 + "> " + //
                        "WHERE   { <"+john+"> <" + loves + "> ?y } "//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(new BindingImpl("y", mary)));

                compare(result, answer);

            }

            {

                /*
                 * Query with more restricted predicate bindings (mary is bound
                 * as the subject).
                 */

                final String query = "SELECT  ?y " + //
                        "FROM    <" + c1 + "> " + //
                        "FROM    <" + c2 + "> " + //
                        "WHERE   { <" + mary + "> <" + loves + "> ?y } "//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(new BindingImpl("y", paul)));

                compare(result, answer);

            }
            
            {

                /*
                 * Query with more restricted predicate bindings (mary is
                 * bound as the object).
                 */

                final String query = "SELECT  ?x " + //
                        "FROM    <" + c1 + "> " + //
                        "FROM    <" + c2 + "> " + //
                        "WHERE   { ?x <" + loves + "> <" + mary + "> } "//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(new BindingImpl("x", john)));

                compare(result, answer);

            }

            {

                /*
                 * Query with more restricted predicate bindings (john is bound
                 * as the subject and paul is bound as the object, so there are
                 * no solutions).
                 */

                final String query = "SELECT  ?x " + //
                        "FROM    <" + c1 + "> " + //
                        "FROM    <" + c2 + "> " + //
                        "WHERE   { <"+john+"> <" + loves + "> <" + paul + "> } "//
                ;

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                compare(result, answer);

            }

        } finally {

            cxn.close();
            sail.__tearDownUnitTest();

        }

    }

    /**
     * Unit test focusing on queries against a default graph comprised of the
     * RDF merge of zero or more graphs where the query involves joins and hence
     * is routed through the {@link BigdataEvaluationStrategyImpl}.
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws QueryEvaluationException
     * @throws MalformedQueryException
     */
    public void test_defaultGraphs_joins() throws RepositoryException,
            SailException, QueryEvaluationException, MalformedQueryException {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                .getConnection();
        cxn.setAutoCommit(false);

        try {

            if (!sail.getDatabase().isQuads()) {

                log.warn("test requires quads.");

                return;

            }

            final URI john = new URIImpl("http://www.bigdata.com/john");
            final URI loves = new URIImpl("http://www.bigdata.com/loves");
            final URI livesIn = new URIImpl("http://www.bigdata.com/liveIn");
            final URI mary = new URIImpl("http://www.bigdata.com/mary");
            final URI paul = new URIImpl("http://www.bigdata.com/paul");
            final URI sam = new URIImpl("http://www.bigdata.com/sam");
            final URI DC = new URIImpl("http://www.bigdata.com/DC");
            final URI VA = new URIImpl("http://www.bigdata.com/VA");
            final URI MD = new URIImpl("http://www.bigdata.com/MD");
            final URI UT = new URIImpl("http://www.bigdata.com/UT");
            final URI c1 = new URIImpl("http://www.bigdata.com/context1");
            final URI c2 = new URIImpl("http://www.bigdata.com/context2");
            final URI c3 = new URIImpl("http://www.bigdata.com/context3");
            final URI c4 = new URIImpl("http://www.bigdata.com/context3");

            /*
             * Setup the data for the test.
             */
            {
                // add statements to the store.
                cxn.add(john, loves, mary, c1);
                cxn.add(john, loves, mary, c2);
                cxn.add(mary, loves, paul, c2);
                cxn.add(john, livesIn, DC, c1);
                cxn.add(mary, livesIn, VA, c2);
                cxn.add(paul, livesIn, MD, c2);
                cxn.add(paul, loves, sam, c4);
                cxn.add(sam, livesIn, UT, c4);
                cxn.commit();
            }

            {

                /*
                 * One graph in the defaultGraphs set, but the specified graph
                 * has no data.  There should be no solutions.
                 */

                final String query = "SELECT  ?x ?y ?z \n" + //
                        "FROM    <" + c3 + ">\n" + //
                        "WHERE   { ?x <" + loves + "> ?y .\n" + //
                        "          ?y <" + loves + "> ?z .\n" + //
                        "}";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                compare(result, answer);

            }

            {

                /*
                 * The data set is not specified. The query will run against the
                 * "default" defaultGraph. For Sesame, I am assuming that the
                 * query is run against the RDF merge of all graphs, including
                 * the null graph. Rather than expanding the access path, we put
                 * a distinct triple filter on the access path. The filter will
                 * strip off the context information and filter for distinct
                 * (s,p,o). If the query has a large result set, then the
                 * distinct filter must spill over from memory onto the disk.
                 * E.g., by using a BTree on a temporary store rather than a
                 * HashSet.
                 * 
                 * Note: In this case there is a duplicate solution based on the
                 * duplicate (john,loves,mary) triple which gets filtered out.
                 */

                final String query = "SELECT  ?x ?y ?z \n" + //
                        "WHERE   { ?x <" + loves + "> ?y .\n" + //
                        "          ?y <" + loves + "> ?z .\n" + //
                        "}";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary), //
                        new BindingImpl("z", paul)//
                        ));
                answer.add(createBindingSet(//
                        new BindingImpl("x", mary),//
                        new BindingImpl("y", paul), //
                        new BindingImpl("z", sam)//
                        ));

                compare(result, answer);

            }

            {

                /*
                 * Two graphs (c1,c2) are used. There should be one solution.
                 * There is a second solution if c4 is considered, but it should
                 * not be since it is not in the default graph.
                 */

                final String query = "SELECT  ?x ?y ?z \n" + //
                        "FROM    <" + c1 + ">\n" + //
                        "FROM    <" + c2 + ">\n" + //
                        "WHERE   { ?x <" + loves + "> ?y .\n" + //
                        "          ?y <" + loves + "> ?z .\n" + //
                        "}";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary), //
                        new BindingImpl("z", paul)//
                        ));

                compare(result, answer);

            }

            {

                /*
                 * Named graph query finds everyone who loves someone and the
                 * graph in which that relationship was asserted.
                 * 
                 * Note: There is an additional occurrence of the relationship
                 * in [c4], but the query does not consider that graph and that
                 * occurrence should not be matched.
                 * 
                 * Note: there is a statement duplicated in two of the named
                 * graphs (john,loves,mary) so we can verify that both
                 * occurrences are reported.
                 * 
                 * Note: This query DOES NOT USE JOINS.  See the next example
                 * below for one that does.
                 */

                final String query = "SELECT  ?g ?x ?y \n" + //
                        "FROM NAMED <" + c1 + ">\n" + //
                        "FROM NAMED <" + c2 + ">\n" + //
                        "WHERE { \n"+
                        "  GRAPH ?g {\n" +
                        "     ?x <" + loves + "> ?y .\n" + //
                        "     }\n"+//
                        "  }";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(//
                        new BindingImpl("g", c1),//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary))//
                        );
                answer.add(createBindingSet(//
                        new BindingImpl("g", c2),//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary))//
                        );
                answer.add(createBindingSet(//
                        new BindingImpl("g", c2),//
                        new BindingImpl("x", mary),//
                        new BindingImpl("y", paul))//
                        );

                compare(result, answer);

            }

            {

                /*
                 * Named graph query finds everyone who loves someone and the
                 * state in which they live.  Each side of the join is drawn
                 * from one of the two named graphs (the graph variables are
                 * bound).
                 */

                final String query = "SELECT ?x ?y ?z \n" + //
                        "FROM NAMED <" + c1 + ">\n" + //
                        "FROM NAMED <" + c2 + ">\n" + //
                        "WHERE { \n"+
                        "  GRAPH <"+c1+"> {\n" +
                        "     ?x <" + loves + "> ?y .\n" + //
                        "     }\n"+//
                        "  GRAPH <"+c2+"> {\n" +
                        "     ?y <" + livesIn + "> ?z .\n" + //
                        "     }\n"+//
                        "  }";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                answer.add(createBindingSet(//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary), //
                        new BindingImpl("z", VA)//
                        ));

                compare(result, answer);

            }

            {

                /*
                 * Named graph query finds everyone who loves someone and the
                 * graph in which that relationship was asserted, and the state
                 * in which they live.
                 * 
                 * Note: There is an additional occurrence of the relationship
                 * in [c4], but the query does not consider that graph and that
                 * occurrence should not be matched.
                 * 
                 * Note: there is a statement duplicated in two of the named
                 * graphs (john,loves,mary) so we can verify that both
                 * occurrences are reported.
                 * 
                 * @todo query pattern involving a defaultGraph and one or more
                 * named graphs.
                 */

                final String query = "SELECT  ?g1 ?x ?y ?g2 ?z \n" + //
                        "FROM NAMED <" + c1 + ">\n" + //
                        "FROM NAMED <" + c2 + ">\n" + //
                        "WHERE { \n"+
                        "  GRAPH ?g1 {\n" +
                        "     ?x <" + loves + "> ?y .\n" + //
                        "     }\n"+//
                        "  GRAPH ?g2 {\n" +
                        "     ?x <" + livesIn + "> ?z .\n" + //
                        "     }\n"+//
                        "  }";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                tupleQuery.setIncludeInferred(true /* includeInferred */);

                final TupleQueryResult result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();

//                cxn.add(john, loves, mary, c1);
//                cxn.add(john, loves, mary, c2);
//                cxn.add(mary, loves, paul, c2);
//                cxn.add(john, livesIn, DC, c1);
//                cxn.add(mary, livesIn, VA, c2);
//                cxn.add(paul, livesIn, MD, c2);
//                cxn.add(paul, loves, sam, c4);
//                cxn.add(sam, livesIn, UT, c4);

// extra result: failed to restrict the named graph search.
// [g2=http://www.bigdata.com/context2;
//  g1=http://www.bigdata.com/context3;
//   z=http://www.bigdata.com/MD;
//   y=http://www.bigdata.com/sam;
//   x=http://www.bigdata.com/paul]
                    
                answer.add(createBindingSet(//
                        new BindingImpl("g1", c1),//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary), //
                        new BindingImpl("g2", c1),//
                        new BindingImpl("z", DC)//
                        ));
                answer.add(createBindingSet(//
                        new BindingImpl("g1", c2),//
                        new BindingImpl("x", john),//
                        new BindingImpl("y", mary),//
                        new BindingImpl("g2", c1),//
                        new BindingImpl("z", DC)//
                        ));
                answer.add(createBindingSet(//
                        new BindingImpl("g1", c2),//
                        new BindingImpl("x", mary),//
                        new BindingImpl("y", paul),//
                        new BindingImpl("g2", c2),//
                        new BindingImpl("z", VA)//
                        ));

                /*
                 * @todo This case is failing because the logic is leaving [c]
                 * unbound when it should be restricting [c] to the specific
                 * graphs in the FROM NAMED clauses.
                 */
                compare(result, answer);

            }

        } finally {

            cxn.close();
            sail.__tearDownUnitTest();

        }

    }
    
    public void testSearchQuery() throws Exception {
        
        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                .getConnection();
        cxn.setAutoCommit(false);

        try {

            if (!sail.getDatabase().isQuads()) {

                log.warn("test requires quads.");

                return;

            }

            final ValueFactory vf = cxn.getValueFactory();
    
            //create context
            final Resource context1 = vf.createURI( "http://example.org" );
    
            //add statement to context1
            cxn.add( vf.createStatement( 
                    vf.createURI("http://example.org#Chris"), 
                    RDFS.LABEL, 
                    vf.createLiteral("Chris") ),  
                    context1);
            cxn.commit();
    
            //add statement to default graph
            cxn.add( vf.createStatement( 
                    vf.createURI("http://example.org#Christian"), 
                    RDFS.LABEL, 
                    vf.createLiteral("Christian") ) );
            cxn.commit();   
    
            {
                //when running this query, we correctly get bindings for both triples
                final String query = 
                    "select ?x ?y " +
                    "where {  " +
                    "    ?y <"+ BD.SEARCH+"> \"Chris\" . " +
                    "    ?x <"+ RDFS.LABEL.stringValue() + "> ?y . " +
                    "}";

                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
//                int i = 1;
//                while (result.hasNext()) {
//                    System.err.println(i++ + "#: " + result.next());
//                }
                
                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(//
                        new BindingImpl("x", vf.createURI("http://example.org#Christian")),//
                        new BindingImpl("y", vf.createLiteral("Christian"))//
                        ));
                answer.add(createBindingSet(//
                        new BindingImpl("x", vf.createURI("http://example.org#Chris")),//
                        new BindingImpl("y", vf.createLiteral("Chris"))//
                        ));

                compare(result, answer);
                
            }
            
            {
                //however, when running this query, we incorrectly get both results as it should only return bindings for the triple added to context 1.  
                String query = 
                    "select ?x ?y " +
                    "where { " +
                    "    graph <http://example.org> { " +
                    "        ?y <"+ BD.SEARCH+"> \"Chris\" . " +
                    "        ?x <"+ RDFS.LABEL.stringValue() + "> ?y ." +
                    "    } . " +
                    "}";
                
                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
//                int i = 1;
//                while (result.hasNext()) {
//                    System.err.println(i++ + "#: " + result.next());
//                }
                
                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(//
                        new BindingImpl("x", vf.createURI("http://example.org#Chris")),//
                        new BindingImpl("y", vf.createLiteral("Chris"))//
                        ));

                compare(result, answer);
                
            }
            
        } finally {

            cxn.close();
            sail.__tearDownUnitTest();

        }
        
    }


}
