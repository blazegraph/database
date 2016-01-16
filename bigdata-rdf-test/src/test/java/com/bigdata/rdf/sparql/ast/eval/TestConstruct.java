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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;

/**
 * Data driven test suite for CONSTRUCT queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6387 2012-07-21 18:37:51Z thompsonbry $
 */
public class TestConstruct extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestConstruct() {
    }

    /**
     * @param name
     */
    public TestConstruct(String name) {
        super(name);
    }

    /**
     * A simple CONSTRUCT query.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * CONSTRUCT {
     *   <http://www.bigdata.com/DC> rdfs:label "DC" .
     *   ?x rdf:type foaf:Person . 
     * } where {
     *   ?x rdf:type foaf:Person 
     * }
     * </pre>
     */
    public void test_construct_1() throws Exception {

        final ASTContainer ast = new TestHelper(
                "construct-1", // testURI,
                "construct-1.rq",// queryFileURL
                "construct-1.trig",// dataFileURL
                "construct-1-result.trig"// resultFileURL
                ).runTest();
        
        final ConstructNode construct = ast.getOptimizedAST().getConstruct();

        assertNotNull(construct);

        assertFalse(construct.isNativeDistinct());

    }

    /**
     * A simple CONSTRUCT query using a native DISTINCT filter.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * CONSTRUCT {
     *   <http://www.bigdata.com/DC> rdfs:label "DC" .
     *   ?x rdf:type foaf:Person . 
     * } where {
     *   # Enable the native DISTINCT SPO filter.
     *   hint:Query hint:nativeDistinctSPO true .
     *   ?x rdf:type foaf:Person 
     * }
     * </pre>
     */
    public void test_construct_1a() throws Exception {

        final ASTContainer ast = new TestHelper(
                "construct-1", // testURI,
                "construct-1a.rq",// queryFileURL
                "construct-1.trig",// dataFileURL
                "construct-1-result.trig"// resultFileURL
                ).runTest();

        final ConstructNode construct = ast.getOptimizedAST().getConstruct();

        assertNotNull(construct);

        assertTrue(construct.isNativeDistinct());
        
    }

    /**
     * A simple CONSTRUCT query using a native DISTINCT filter (enabled
     * via the "analytic" query hint).
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * CONSTRUCT {
     *   <http://www.bigdata.com/DC> rdfs:label "DC" .
     *   ?x rdf:type foaf:Person . 
     * } where {
     *   # Enable the native DISTINCT SPO filter.
     *   hint:Query hint:analytic true .
     *   ?x rdf:type foaf:Person 
     * }
     * </pre>
     */
    public void test_construct_1b() throws Exception {

        final ASTContainer ast = new TestHelper(
                "construct-1", // testURI,
                "construct-1b.rq",// queryFileURL
                "construct-1.trig",// dataFileURL
                "construct-1-result.trig"// resultFileURL
                ).runTest();

        final ConstructNode construct = ast.getOptimizedAST().getConstruct();

        assertNotNull(construct);

        assertTrue(construct.isNativeDistinct());
        
    }

    /**
     * A CONSTRUCT without a template and having a ground triple in the WHERE
     * clause. For this variant of the test, the triple is not in the KB.
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * CONSTRUCT WHERE {
     *   <http://www.bigdata.com/DC> rdfs:label "MD" 
     * }
     * </pre>
     */
    public void test_construct_2() throws Exception {

        new TestHelper(
                "construct-2", // testURI,
                "construct-2.rq",// queryFileURL
                "construct-2.trig",// dataFileURL
                "construct-2-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A CONSTRUCT without a template and having a ground triple in the WHERE
     * clause. For this variant of the test, the triple is in the KB and should
     * be in the CONSTRUCT result.
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * CONSTRUCT WHERE {
     *   <http://www.bigdata.com/DC> rdfs:label "DC" 
     * }
     * </pre>
     */
    public void test_construct_3() throws Exception {

        new TestHelper(
                "construct-3", // testURI,
                "construct-3.rq",// queryFileURL
                "construct-3.trig",// dataFileURL
                "construct-3-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A construct with told triples in the CONSTRUCT clause and no WHERE
     * clause.
     * 
     * <pre>
     * PREFIX : <http://www.bigdata.com/>
     * 
     * CONSTRUCT {
     *   :Bryan :likes :RDFS
     * } where {
     * }
     * </pre>
     */
    public void test_construct_without_where_clause() throws Exception {
       
        new TestHelper(
                "construct-without-where-clause", // testURI,
                "construct-without-where-clause.rq",// queryFileURL
                "construct-without-where-clause.trig",// dataFileURL
                "construct-without-where-clause-result.trig"// resultFileURL
                ).runTest();
    }
    
    /**
     * A CONSTRUCT query where the constructed statements can have blank nodes
     * because a variable becomes bound to a blank node in the data.
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * CONSTRUCT WHERE {
     * <http://example.org/bob> rdfs:label ?o
     * }
     * </pre>
     */
    public void test_construct_5() throws Exception {

        new TestHelper(
                "construct-5", // testURI,
                "construct-5.rq",// queryFileURL
                "construct-5.trig",// dataFileURL
                "construct-5-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * Return the non-ground triple pattern templates from the CONSTRUCT node of
     * the query.
     * 
     * @param queryRoot
     *            The query.
     *            
     * @return The non-ground triple patterns in the CONSTRUCT template.
     */
    private static List<StatementPatternNode> getConstructTemplates(
            final QueryRoot queryRoot) {

        final List<StatementPatternNode> templates = new LinkedList<StatementPatternNode>();

        final ConstructNode construct = queryRoot.getConstruct();

        for (StatementPatternNode pat : construct) {

            if (!pat.isGround()) {

                /*
                 * A statement pattern that we will process for each solution.
                 */

                templates.add(pat);

            }

        }
    
        return templates;

    }
    
    /**
     * Unit test for method identifying whether a CONSTRUCT template and WHERE
     * clause will obviously result in distinct triples without the application
     * of a DISTINCT SPO filter.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person . 
     * } where {
     *   ?x rdf:type foaf:Person 
     * }
     * </pre>
     * 
     * For this query against a QUADS mode database, the access path for the
     * sole triple pattern in the WHERE clause will be a DEFAULT GRAPH access
     * path (the RDF MERGE of the triples across all named graphs that are
     * visible). That results in an obviously distinct construct.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public void test_isObviouslyDistinct_01() throws Exception {
     
        final ASTContainer ast = new TestHelper(
                "construct-isObviouslyDistinct-01", // testURI,
                "construct-isObviouslyDistinct-01.rq",// queryFileURL
                "construct-isObviouslyDistinct-01.trig",// dataFileURL
                "construct-isObviouslyDistinct-01-result.trig"// resultFileURL
                ).runTest();

        final QueryRoot optimizedQuery = ast.getOptimizedAST();

        assertTrue(ASTConstructIterator.isObviouslyDistinct(store.isQuads(),
                getConstructTemplates(optimizedQuery),
                optimizedQuery.getWhereClause()));

    }
    
    /**
     * Unit test for method identifying whether a CONSTRUCT template and WHERE
     * clause will obviously result in distinct triples without the application
     * of a DISTINCT SPO filter.
     * 
     * <pre>
     * CONSTRUCT WHERE {?s ?p ?o}
     * </pre>
     * 
     * For this query against a QUADS mode database, the access path for the
     * sole triple pattern in the WHERE clause will be a DEFAULT GRAPH access
     * path (the RDF MERGE of the triples across all named graphs that are
     * visible). That results in an obviously distinct construct.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public void test_isObviouslyDistinct_02() throws Exception {
     
        final ASTContainer ast = new TestHelper(
                "construct-isObviouslyDistinct-02", // testURI,
                "construct-isObviouslyDistinct-02.rq",// queryFileURL
                "construct-isObviouslyDistinct-02.trig",// dataFileURL
                "construct-isObviouslyDistinct-02-result.trig"// resultFileURL
                ).runTest();

        final QueryRoot optimizedQuery = ast.getOptimizedAST();

        assertTrue(ASTConstructIterator.isObviouslyDistinct(store.isQuads(),
                getConstructTemplates(optimizedQuery),
                optimizedQuery.getWhereClause()));

    }
    
    /**
     * Unit test for method identifying whether a CONSTRUCT template and WHERE
     * clause will obviously result in distinct triples without the application
     * of a DISTINCT SPO filter.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person . 
     * } where {
     *   ?x rdf:type foaf:Person . 
     *   ?x rdfs:label ?y 
     * }
     * </pre>
     * 
     * For this query, the <em>possibility</em> of multiple bindings on
     * <code>y</code> for a given binding on <code>x</code> means that the
     * CONSTRUCT is NOT obviously distinct.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public void test_isObviouslyDistinct_03() throws Exception {
     
        final ASTContainer ast = new TestHelper(
                "construct-isObviouslyDistinct-03", // testURI,
                "construct-isObviouslyDistinct-03.rq",// queryFileURL
                "construct-isObviouslyDistinct-03.trig",// dataFileURL
                "construct-isObviouslyDistinct-03-result.trig"// resultFileURL
                ).runTest();

        final QueryRoot optimizedQuery = ast.getOptimizedAST();

        assertFalse(ASTConstructIterator.isObviouslyDistinct(store.isQuads(),
                getConstructTemplates(optimizedQuery),
                optimizedQuery.getWhereClause()));

    }
    
    
    /**
     * Unit test for method identifying whether a CONSTRUCT template and WHERE
     * clause will obviously result in distinct triples without the application
     * of a DISTINCT SPO filter.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x foaf:accountName ?y . 
     * } where {
     *   ?x rdf:label ?y
     * }
     * </pre>
     * 
     * For this query, both the template and the WHERE clause are a single
     * triple pattern and all variables are used in both places.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public void test_isObviouslyDistinct_04() throws Exception {
     
        final ASTContainer ast = new TestHelper(
                "construct-isObviouslyDistinct-04", // testURI,
                "construct-isObviouslyDistinct-04.rq",// queryFileURL
                "construct-isObviouslyDistinct-04.trig",// dataFileURL
                "construct-isObviouslyDistinct-04-result.trig"// resultFileURL
                ).runTest();

        final QueryRoot optimizedQuery = ast.getOptimizedAST();

        assertTrue(ASTConstructIterator.isObviouslyDistinct(store.isQuads(),
                getConstructTemplates(optimizedQuery),
                optimizedQuery.getWhereClause()));

    }

    /**
     * Unit test for method identifying whether a CONSTRUCT template and WHERE
     * clause will obviously result in distinct triples without the application
     * of a DISTINCT SPO filter.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?s ?p ?o
     * }
     * WHERE {
     *   GRAPH <http://www.bigdata.com/foo> {
     *     ?s ?p ?o
     *   }
     * }
     * </pre>
     * 
     * For this query, both the template and the WHERE clause are a single
     * triple pattern and all variables are used in both places.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
     *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
     */
    public void test_isObviouslyDistinct_05() throws Exception {

        final ASTContainer ast = new TestHelper(
                "construct-isObviouslyDistinct-05", // testURI,
                "construct-isObviouslyDistinct-05.rq",// queryFileURL
                "construct-isObviouslyDistinct-05.trig",// dataFileURL
                "construct-isObviouslyDistinct-05-result.trig"// resultFileURL
        ).runTest();

        final QueryRoot optimizedQuery = ast.getOptimizedAST();

        assertTrue(ASTConstructIterator.isObviouslyDistinct(store.isQuads(),
                getConstructTemplates(optimizedQuery),
                optimizedQuery.getWhereClause()));

    }

}
