/* 
 *         FIXME This was imported solely to have the class extend TestCase.
 *         I've written to Jeen to request that he make this change in SVN, at
 *         which point we can drop the copy of this class from our SVN
 *         repository.
 *         
 *         @see https://openrdf.atlassian.net/browse/SES-1664 (Request to make private methods public in ComplexSPARQLQueryTest)
 */
/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of compliance tests on SPARQL query functionality which can not be
 * easily executed using the {@link SPARQL11ManifestTest} format. This includes
 * tests on queries with non-deterministic output (e.g. GROUP_CONCAT).
 * 
 * @author Jeen Broekstra
 */
public abstract class ComplexSPARQLQueryTest extends TestCase {

    static final Logger logger = LoggerFactory.getLogger(ComplexSPARQLQueryTest.class);

    private Repository rep;

    protected RepositoryConnection conn;

    protected ValueFactory f;

    protected static final String EX_NS = "http://example.org/";

    private URI bob;

    private URI alice;

    /**
     * @throws java.lang.Exception
     */
//    @Before
    public void setUp()
        throws Exception
    {
        logger.debug("setting up test");
        this.rep = newRepository();
        rep.initialize();

        f = rep.getValueFactory();
        conn = rep.getConnection();

        // Note: commented out - not required. BBT.
//        conn.clear(); // clear existing data from repo

        bob = f.createURI(EX_NS, "bob");
        alice = f.createURI(EX_NS, "alice");

        logger.debug("test setup complete.");
    }

    /**
     * @throws java.lang.Exception
     */
//    @After
    public void tearDown()
        throws Exception
    {
        logger.debug("tearing down...");
        conn.close();
        conn = null;

        rep.shutDown();
        rep = null;

        logger.debug("tearDown complete.");
    }

    // @Test
    public void testNullContext1()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");
        StringBuilder query = new StringBuilder();
        query.append(" SELECT * ");
        query.append(" FROM DEFAULT ");
        query.append(" WHERE { ?s ?p ?o } ");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            while (result.hasNext()) {
                BindingSet bs = result.next();
                assertNotNull(bs);

                Resource s = (Resource)bs.getValue("s");

                assertNotNull(s);
                assertFalse(bob.equals(s)); // should not be present in default
                                                        // graph
                assertFalse(alice.equals(s)); // should not be present in default
                                                        // graph
            }
            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // @Test
    public void testNullContext2()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append(" SELECT * ");
        query.append(" FROM sesame:nil ");
        query.append(" WHERE { ?s ?p ?o } ");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            while (result.hasNext()) {
                BindingSet bs = result.next();
                assertNotNull(bs);

                Resource s = (Resource)bs.getValue("s");

                assertNotNull(s);
                assertFalse(bob.equals(s)); // should not be present in default
                                                        // graph
                assertFalse(alice.equals(s)); // should not be present in default
                                                        // graph
            }
            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // @Test
    public void testGroupConcatDistinct()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");

        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT (GROUP_CONCAT(DISTINCT ?l) AS ?concat)");
        query.append("WHERE { ex:groupconcat-test ?p ?l . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            while (result.hasNext()) {
                BindingSet bs = result.next();
                assertNotNull(bs);

                Value concat = bs.getValue("concat");

                assertTrue(concat instanceof Literal);

                String lexValue = ((Literal)concat).getLabel();

                int occ = countCharOccurrences(lexValue, 'a');
                assertEquals(1, occ);
                occ = countCharOccurrences(lexValue, 'b');
                assertEquals(1, occ);
                occ = countCharOccurrences(lexValue, 'c');
                assertEquals(1, occ);
                occ = countCharOccurrences(lexValue, 'd');
                assertEquals(1, occ);
            }
            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    public void testSameTermRepeatInOptional()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append(" SELECT ?l ?opt1 ?opt2 ");
        query.append(" FROM ex:optional-sameterm-graph ");
        query.append(" WHERE { ");
        query.append("          ?s ex:p ex:A ; ");
        query.append("          { ");
        query.append("              { ");
        query.append("                 ?s ?p ?l .");
        query.append("                 FILTER(?p = rdfs:label) ");
        query.append("              } ");
        query.append("              OPTIONAL { ");
        query.append("                 ?s ?p ?opt1 . ");
        query.append("                 FILTER (?p = ex:prop1) ");
        query.append("              } ");
        query.append("              OPTIONAL { ");
        query.append("                 ?s ?p ?opt2 . ");
        query.append("                 FILTER (?p = ex:prop2) ");
        query.append("              } ");
        query.append("          }");
        query.append(" } ");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                BindingSet bs = result.next();
                count++;
                assertNotNull(bs);

                System.out.println(bs);

                Value l = bs.getValue("l");
                assertTrue(l instanceof Literal);
                assertEquals("label", ((Literal)l).getLabel());

                Value opt1 = bs.getValue("opt1");
                assertNull(opt1);

                Value opt2 = bs.getValue("opt2");
                assertNull(opt2);
            }
            result.close();

            assertEquals(1, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    public void testSameTermRepeatInUnion()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");
        StringBuilder query = new StringBuilder();
        query.append("PREFIX foaf:<http://xmlns.com/foaf/0.1/>\n");
        query.append("SELECT * {\n");
        query.append("    {\n");
        query.append("        ?sameTerm foaf:mbox ?mbox\n");
        query.append("        FILTER sameTerm(?sameTerm,$william)\n");
        query.append("    } UNION {\n");
        query.append("        ?x foaf:knows ?sameTerm\n");
        query.append("        FILTER sameTerm(?sameTerm,$william)\n");
        query.append("    }\n");
        query.append("}");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
            tq.setBinding("william", conn.getValueFactory().createURI("http://example.org/william"));
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                BindingSet bs = result.next();
                count++;
                assertNotNull(bs);

                System.out.println(bs);

                Value mbox = bs.getValue("mbox");
                Value x = bs.getValue("x");

                assertTrue(mbox instanceof Literal || x instanceof URI);
            }
            result.close();

            assertEquals(3, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    public void testSameTermRepeatInUnionAndOptional()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");

        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT * {\n");
        query.append("    {\n");
        query.append("        ex:a ?p ?prop1\n");
        query.append("        FILTER (?p = ex:prop1)\n");
        query.append("    } UNION {\n");
        query.append("          ?s ex:p ex:A ; ");
        query.append("          { ");
        query.append("              { ");
        query.append("                 ?s ?p ?l .");
        query.append("                 FILTER(?p = rdfs:label) ");
        query.append("              } ");
        query.append("              OPTIONAL { ");
        query.append("                 ?s ?p ?opt1 . ");
        query.append("                 FILTER (?p = ex:prop1) ");
        query.append("              } ");
        query.append("              OPTIONAL { ");
        query.append("                 ?s ?p ?opt2 . ");
        query.append("                 FILTER (?p = ex:prop2) ");
        query.append("              } ");
        query.append("          }");
        query.append("    }\n");
        query.append("}");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                BindingSet bs = result.next();
                count++;
                assertNotNull(bs);

                System.out.println(bs);

                Value prop1 = bs.getValue("prop1");
                Value l = bs.getValue("l");

                assertTrue(prop1 instanceof Literal || l instanceof Literal);
                if (l instanceof Literal) {
                    Value opt1 = bs.getValue("opt1");
                    assertNull(opt1);

                    Value opt2 = bs.getValue("opt2");
                    assertNull(opt2);
                }
            }
            result.close();

            assertEquals(2, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    public void testPropertyPathInTree()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");

        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append(" SELECT ?node ?name ");
        query.append(" FROM ex:tree-graph ");
        query.append(" WHERE { ?node ex:hasParent+ ex:b . ?node ex:name ?name . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            while (result.hasNext()) {
                BindingSet bs = result.next();
                assertNotNull(bs);

                System.out.println(bs);

            }
            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    public void testFilterRegexBoolean()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");

        // test case for issue SES-1050
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append(" SELECT *");
        query.append(" WHERE { ");
        query.append("       ?x foaf:name ?name ; ");
        query.append("          foaf:mbox ?mbox . ");
        query.append("       FILTER(EXISTS { ");
        query.append("            FILTER(REGEX(?name, \"Bo\") && REGEX(?mbox, \"bob\")) ");
        // query.append("            FILTER(REGEX(?mbox, \"bob\")) ");
        query.append("            } )");
        query.append(" } ");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            assertTrue(result.hasNext());
            int count = 0;
            while (result.hasNext()) {
                BindingSet bs = result.next();
                count++;
                System.out.println(bs);
            }
            assertEquals(1, count);

            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // @Test
    public void testGroupConcatNonDistinct()
        throws Exception
    {
        loadTestData("/testdata-query/dataset-query.trig");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT (GROUP_CONCAT(?l) AS ?concat)");
        query.append("WHERE { ex:groupconcat-test ?p ?l . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            while (result.hasNext()) {
                BindingSet bs = result.next();
                assertNotNull(bs);

                Value concat = bs.getValue("concat");

                assertTrue(concat instanceof Literal);

                String lexValue = ((Literal)concat).getLabel();

                int occ = countCharOccurrences(lexValue, 'a');
                assertEquals(1, occ);
                occ = countCharOccurrences(lexValue, 'b');
                assertEquals(2, occ);
                occ = countCharOccurrences(lexValue, 'c');
                assertEquals(2, occ);
                occ = countCharOccurrences(lexValue, 'd');
                assertEquals(1, occ);
            }
            result.close();
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithBinding1()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child a owl:Class . ?child rdfs:subClassOf+ ?parent . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            // first execute without binding
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(7, count);
            
            // execute again, but this time setting a binding
            tq.setBinding("parent", OWL.THING);
            
            result = tq.evaluate();
            assertNotNull(result);

            count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(4, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
    
    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithBinding2()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        
        // query without initializing ?child first.
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child rdfs:subClassOf+ ?parent . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            // first execute without binding
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(7, count);
            
            // execute again, but this time setting a binding
            tq.setBinding("parent", OWL.THING);
            
            result = tq.evaluate();
            assertNotNull(result);

            count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(4, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
    
    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithBinding3()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        
        // binding on child instead of parent.
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child rdfs:subClassOf+ ?parent . }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            // first execute without binding
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(7, count);
            
            // execute again, but this time setting a binding
            tq.setBinding("child", f.createURI(EX_NS, "PO_0025117"));
            
            result = tq.evaluate();
            assertNotNull(result);

            count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(2, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithFilter1()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child a owl:Class . ?child rdfs:subClassOf+ ?parent . FILTER (?parent = owl:Thing) }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(4, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
    

    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithFilter2()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child rdfs:subClassOf+ ?parent . FILTER (?parent = owl:Thing) }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(4, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
    
    // @Test
    /**
     * @see http://www.openrdf.org/issues/browse/SES-1091
     * @throws Exception
     */
    public void testArbitraryLengthPathWithFilter3()
        throws Exception
    {
        loadTestData("/testdata-query/alp-testdata.ttl");
        StringBuilder query = new StringBuilder();
        query.append(getNamespaceDeclarations());
        query.append("SELECT ?parent ?child ");
        query.append("WHERE { ?child rdfs:subClassOf+ ?parent . FILTER (?child = <http://example.org/PO_0025117>) }");

        TupleQuery tq = null;
        try {
            tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
        }
        catch (RepositoryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        catch (MalformedQueryException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            TupleQueryResult result = tq.evaluate();
            assertNotNull(result);

            int count = 0;
            while (result.hasNext()) {
                count++;
                BindingSet bs = result.next();
                assertTrue(bs.hasBinding("child"));
                assertTrue(bs.hasBinding("parent"));
            }
            assertEquals(2, count);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
    
    /* private / protected methods */

    private int countCharOccurrences(String string, char ch) {
        int count = 0;
        for (int i = 0; i < string.length(); i++) {
            if (string.charAt(i) == ch) {
                count++;
            }
        }
        return count;
    }

    /**
     * Get a set of useful namespace prefix declarations.
     * 
     * @return namespace prefix declarations for dc, foaf and ex.
     */
    protected String getNamespaceDeclarations() {
        StringBuilder declarations = new StringBuilder();
        declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
        declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
        declarations.append("PREFIX sesame: <" + SESAME.NAMESPACE + "> \n");
        declarations.append("PREFIX ex: <" + EX_NS + "> \n");
        declarations.append("\n");

        return declarations.toString();
    }

    protected abstract Repository newRepository()
        throws Exception;

    protected void loadTestData(String dataFile)
        throws RDFParseException, RepositoryException, IOException
    {
        logger.debug("loading dataset {}", dataFile);
        InputStream dataset = ComplexSPARQLQueryTest.class.getResourceAsStream(dataFile);
        try {
            conn.add(dataset, "", RDFFormat.forFileName(dataFile));
        }
        finally {
            dataset.close();
        }
        logger.debug("dataset loaded.");
    }
}
