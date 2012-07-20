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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstructNode;

/**
 * Data driven test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBasicQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBasicQuery() {
    }

    /**
     * @param name
     */
    public TestBasicQuery(String name) {
        super(name);
    }

    /**
     * A SELECT query consisting of a single statement pattern.
     */
    public void test_select_1() throws Exception {
        
        new TestHelper("select-1").runTest();
        
    }
    
    /**
     * A simple SELECT query.
     */
    public void test_select_2() throws Exception {
        
        new TestHelper("select-2").runTest();
        
    }
    
    /**
     * A simple ASK query.
     */
    public void test_ask() throws Exception {

        new TestHelper("ask").runTest();
        
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
     * A simple DESCRIBE query of a constant.
     */
    public void test_describe_1() throws Exception {

        new TestHelper(
                "describe-1", // testURI,
                "describe-1.rq",// queryFileURL
                "describe-1.trig",// dataFileURL
                "describe-1-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A simple DESCRIBE query of a variable with a where clause.
     */
    public void test_describe_2() throws Exception {

        new TestHelper(
                "describe-2", // testURI,
                "describe-2.rq",// queryFileURL
                "describe-2.trig",// dataFileURL
                "describe-2-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A simple DESCRIBE query of a constant plus a variable with a where clause.
     */
    public void test_describe_3() throws Exception {

        new TestHelper(
                "describe-3", // testURI,
                "describe-3.rq",// queryFileURL
                "describe-3.trig",// dataFileURL
                "describe-3-result.trig"// resultFileURL
                ).runTest();
        
    }

    // Note: Was TestDescribe#testSingleDescribe()
    public void test_describe_4() throws Exception {

        new TestHelper(
                "describe-4", // testURI,
                "describe-4.rq",// queryFileURL
                "describe-4.trig",// dataFileURL
                "describe-4-result.trig"// resultFileURL
                ).runTest();
        
    }

    // Note: Was TestDescribe#testMultiDescribe()
    public void test_describe_5() throws Exception {

        new TestHelper(
                "describe-5", // testURI,
                "describe-5.rq",// queryFileURL
                "describe-5.trig",// dataFileURL
                "describe-5-result.trig"// resultFileURL
                ).runTest();
        
    }

}
