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
 * Created on Jan 4, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.xml.XMLWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedList;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import com.bigdata.rdf.store.BD;

/**
 * Unit tests for high-level query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataSailEvaluationStrategyImpl extends ProxyBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailEvaluationStrategyImpl() {
        
    }

    /**
     * @param arg0
     */
    public TestBigdataSailEvaluationStrategyImpl(String arg0) {
        super(arg0);
    }

    public void test_or_equals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final URI property2 = new URIImpl(ns+"property2");
        
        final Literal label = new LiteralImpl("The Label");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                cxn.add(new StatementImpl(mike, property1, label));
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                cxn.add(new StatementImpl(jane, property2, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?p "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s ?p \""+label.getLabel()+"\" . " +
                    "  FILTER(?p = <"+RDFS.LABEL+"> || " +
                    "         ?p = <"+RDFS.COMMENT+"> || " +
                    "         ?p = <"+property1+">) " +
                    "}";
                
                { // evaluate it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    final SPARQLResultsXMLWriter handler = new SPARQLResultsXMLWriter(
                            new XMLWriter(sw));
                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);
                    tupleQuery.evaluate(handler);
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);
                    final TupleQueryResult result = tupleQuery.evaluate();
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike),
                            new BindingImpl("p", property1)));
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_and_equals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final Literal label = new LiteralImpl("The Label");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, label));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, property1, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?type "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> ?type . " +
                    "  ?s ?p \""+label.getLabel()+"\" . " +
                    "  FILTER((?p = <"+RDFS.LABEL+"> || ?p = <"+RDFS.LABEL+">) && " +
                    "         (?type = <"+person+"> || ?type = <"+person+">)) " +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike),
                            new BindingImpl("type", person)));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_and_nequals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI entity = new URIImpl(ns+"Entity");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI place = new URIImpl(ns+"Place");
        
        final URI thing = new URIImpl(ns+"Thing");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(person, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(place, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(thing, RDFS.SUBCLASSOF, entity));
                
                cxn.add(new StatementImpl(person, RDFS.LABEL, 
                        new LiteralImpl(person.getLocalName())));
                
                cxn.add(new StatementImpl(place, RDFS.LABEL, 
                        new LiteralImpl(place.getLocalName())));
                
                cxn.add(new StatementImpl(thing, RDFS.LABEL, 
                        new LiteralImpl(thing.getLocalName())));
                
                cxn.commit();
                
                String query = 
                    "select ?s ?label "+
                    "where { " +
                    "  ?s <"+RDFS.SUBCLASSOF+"> <"+entity+"> . " +
                    // "  ?s <"+RDFS.SUBCLASSOF+"> ?sco . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER(?s != <"+entity+"> && ?s != <"+person+"> && ?s != <"+place+">) " +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", thing),
                            new BindingImpl("label", new LiteralImpl(thing.getLocalName()))));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_filter_literals() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final URI property1 = new URIImpl(ns+"property1");
        
        final URI property2 = new URIImpl(ns+"property2");
        
        final Literal label = new LiteralImpl("The Label");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, label));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, label));
                
                cxn.commit();
                
                String query = 
                    "select ?s "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER(?label = \""+label.getLabel()+"\" || ?label = \""+label.getLabel()+"\")" +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike)));
                    answer.add(createBindingSet(
                            new BindingImpl("s", jane)));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_filter_regex() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.commit();
                
                String query = 
                    "select ?s "+
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER REGEX(?label, 'Mi*', 'i')" +
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike)));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_union() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI person = new URIImpl(ns+"Person");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.commit();
                
                String query = 
                    "select distinct ?s "+
                    "where { {" +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER (?label = \"Mike\")" +
                    "} union { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
                    "  ?s <"+RDFS.LABEL+"> ?label . " +
                    "  FILTER (?label = \"Jane\")" +
                    "} }";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike)));
                    answer.add(createBindingSet(
                            new BindingImpl("s", jane)));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_free_text_search() throws Exception {

        final String ns = "http://www.bigdata.com/rdf#";
        
        final URI search = BD.SEARCH; 
        
        final URI mike = new URIImpl(ns+"Mike");
        
        final URI jane = new URIImpl(ns+"Jane");
        
        final URI bryan = new URIImpl(ns+"Bryan");
        
        final URI person = new URIImpl(ns+"Person");
        
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final Repository repo = new BigdataSailRepository(sail);
            
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                cxn.setAutoCommit(false);
                
                cxn.add(new StatementImpl(mike, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(mike, RDFS.LABEL, new LiteralImpl("Mike")));
                
                cxn.add(new StatementImpl(jane, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(jane, RDFS.LABEL, new LiteralImpl("Jane")));
                
                cxn.add(new StatementImpl(bryan, RDF.TYPE, person));
                
                cxn.add(new StatementImpl(bryan, RDFS.LABEL, new LiteralImpl("Bryan")));
                
                cxn.commit();
                
                System.err.println("<mike> = " + sail.getDatabase().getIV(mike));
                System.err.println("<jane> = " + sail.getDatabase().getIV(jane));
                System.err.println("\"Mike\" = " + sail.getDatabase().getIV(new LiteralImpl("Mike")));
                System.err.println("\"Jane\" = " + sail.getDatabase().getIV(new LiteralImpl("Jane")));
                
                String query = 
                    "select ?s ?label " +
                    "where { " +
                    "  ?s <"+RDF.TYPE+"> <"+person+"> . " +   // [160, 8, 164], [156, 8, 164]
                    "  ?s <"+RDFS.LABEL+"> ?label . " +       // [160, 148, 174], [156, 148, 170] 
                    "  ?label <"+search+"> \"Mi*\" . " +     // [174, 0, 0]
                    "}";
                
                { // evalute it once so i can see it
                    
                    final StringWriter sw = new StringWriter();
                    
                    final SPARQLResultsXMLWriter handler = 
                        new SPARQLResultsXMLWriter(new XMLWriter(sw));
    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    tupleQuery.evaluate(handler);
                    
                    System.err.println(sw.toString());

                }
                
                {
                    
                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    
                    Collection<BindingSet> answer = new LinkedList<BindingSet>();
                    answer.add(createBindingSet(
                            new BindingImpl("s", mike),
                            new BindingImpl("label", new LiteralImpl("Mike"))));
                    
                    compare(result, answer);
                    
                }
                
            } catch(Exception ex) {
                
                cxn.rollback();
                
                throw ex;
                
            } finally {
    
                cxn.close();
    
            }
        
        } finally {
            
            sail.__tearDownUnitTest();
            
        }

    }
    
    public void test_nested_optionals() throws Exception {

        // define the vocabulary
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI jane = new URIImpl(BD.NAMESPACE + "Jane");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI object = new URIImpl(BD.NAMESPACE + "Object");
        final Literal mikeLabel = new LiteralImpl("mike label");
        final Literal mikeComment = new LiteralImpl("mike comment");
        final Literal janeLabel = new LiteralImpl("jane label");
        
        // define the graph
        Graph graph = new GraphImpl();
        graph.add(mike, RDF.TYPE, person);
        graph.add(jane, RDF.TYPE, person);
        graph.add(bryan, RDF.TYPE, person);
        graph.add(mike, RDF.TYPE, object);
        graph.add(jane, RDF.TYPE, object);
        graph.add(bryan, RDF.TYPE, object);
        graph.add(mike, RDFS.LABEL, mikeLabel);
        graph.add(mike, RDFS.COMMENT, mikeComment);
        graph.add(jane, RDFS.LABEL, janeLabel);
        
        // define the query
        String query = 
            "select ?s ?label ?comment " +
            "where { " +
            "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
            "  ?s <"+RDF.TYPE+"> <"+object+"> . " +
            "  OPTIONAL { ?s <"+RDFS.LABEL+"> ?label . } " + 
            "  OPTIONAL { ?s <"+RDFS.COMMENT+"> ?comment . } " + 
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        answer.add(createBindingSet(
                new BindingImpl("s", mike),
                new BindingImpl("label", mikeLabel),
                new BindingImpl("comment", mikeComment)));
        answer.add(createBindingSet(
                new BindingImpl("s", jane),
                new BindingImpl("label", janeLabel)));
        answer.add(createBindingSet(
                new BindingImpl("s", bryan)));
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_open_eq_12() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX     :    <http://example/> " +
            "PREFIX  xsd:    <http://www.w3.org/2001/XMLSchema#> " +
            "SELECT ?x ?v1 ?y ?v2 " +
            "{ " +
            "    ?x :p ?v1 . " +
            "    ?y :p ?v2 . " +
            "    OPTIONAL { ?y :p ?v3 . FILTER( ?v1 != ?v3 || ?v1 = ?v3 )} " +
            "    FILTER (!bound(?v3)) " +
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_join_combo_1() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX :    <http://example/> " +
            "SELECT ?a ?y ?d ?z " +
            "{  " +
            "    ?a :p ?c OPTIONAL { ?a :r ?d }. " + 
            "    ?a ?p 1 { ?p a ?y } UNION { ?a ?z ?p } " + 
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_join_combo_2() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX :    <http://example/> " +
            "SELECT ?x ?y ?z " +
            "{  " +
            "    GRAPH ?g { ?x ?p 1 } { ?x :p ?y } UNION { ?p a ?z } " +
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_join_combo_3() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX :    <http://example/> " +
            "SELECT * " +
            "{  " +
            "    { ?x :p ?y } UNION { ?p :a ?z } " +
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_two_nested_opt() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX :    <http://example/> " +
            "SELECT * " +
            "{  " +
            "    :x1 :p ?v . " +
            "    OPTIONAL " +
            "    { " +
            "      :x3 :q ?w . " +
            "      OPTIONAL { :x2 :p ?v } " +
            "    } " +
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    public void test_two_nested_opt_alt() throws Exception {

        // define the vocabulary
        
        // define the graph
        Graph graph = new GraphImpl();
        
        // define the query
        String query = 
            "PREFIX :    <http://example/> " +
            "SELECT * " +
            "{  " +
            "    :x1 :p ?v . " +
            "    OPTIONAL { :x3 :q ?w } " +
            "    OPTIONAL { :x3 :q ?w  . :x2 :p ?v } " +
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    
    public void test_prune_groups() throws Exception {

        // define the vocabulary
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI jane = new URIImpl(BD.NAMESPACE + "Jane");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI object = new URIImpl(BD.NAMESPACE + "Object");
        final Literal mikeLabel = new LiteralImpl("mike label");
        final Literal mikeComment = new LiteralImpl("mike comment");
        final Literal janeLabel = new LiteralImpl("jane label");
        final URI p1 = new URIImpl(BD.NAMESPACE + "p1");
        final URI p2 = new URIImpl(BD.NAMESPACE + "p2");
        
        // define the graph
        Graph graph = new GraphImpl();
        graph.add(mike, RDF.TYPE, person);
        graph.add(jane, RDF.TYPE, person);
        graph.add(bryan, RDF.TYPE, person);
        graph.add(mike, RDF.TYPE, object);
        graph.add(jane, RDF.TYPE, object);
        graph.add(bryan, RDF.TYPE, object);
        graph.add(mike, RDFS.LABEL, mikeLabel);
        graph.add(mike, RDFS.COMMENT, mikeComment);
        graph.add(jane, RDFS.LABEL, janeLabel);
        
        // define the query
        String query = 
            "select * " +
            "where { " +
            "  ?s <"+RDF.TYPE+"> <"+person+"> . " +
            "  OPTIONAL { " +
            "    ?s <"+p1+"> ?p1 . " +
            "    OPTIONAL {" +
            "      ?p1 <"+p2+"> ?o2 . " +
            "    } " +
            "  } " + 
            "}";
        
        // define the correct answer
        Collection<BindingSet> answer = new LinkedList<BindingSet>();
        answer.add(createBindingSet(
                new BindingImpl("s", mike)));
        answer.add(createBindingSet(
                new BindingImpl("s", jane)));
        answer.add(createBindingSet(
                new BindingImpl("s", bryan)));
        
        // run the test
        runQuery(graph, query, answer);
        
    }
    
    private void runQuery(final Graph data, final String query, 
            final Collection<BindingSet> answer) throws Exception {
        
        final BigdataSail sail = getSail();
        
        try {
        
            // initialize the sail
            sail.initialize();
            final Repository repo = new BigdataSailRepository(sail);
            final RepositoryConnection cxn = repo.getConnection();
            
            try {

                // add the data
                cxn.setAutoCommit(false);
                cxn.add(data);
                cxn.commit();

                { // evalute it once to stderr
                    
                    final StringWriter sw = new StringWriter();
                    final SPARQLResultsXMLWriter handler = new SPARQLResultsXMLWriter(
                            new XMLWriter(sw));
                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);
                    tupleQuery.evaluate(handler);
                    System.err.println(sw.toString());

                }
                
                { // evaluate for correctness

                    // check the answer
                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);
                    final TupleQueryResult result = tupleQuery.evaluate();
                    compare(result, answer);
                    
                }
                
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        
        } finally {
            sail.__tearDownUnitTest();
        }

    }
    
}
