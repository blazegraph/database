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

import java.util.Properties;

import com.bigdata.rdf.store.AbstractTripleStore;


/**
 * Data driven test suite for SPARQL 1.1 BIND & VALUES clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a> * 
 * @version $Id$
 */
public class TestBindings extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBindings() {
    }

    /**
     * @param name
     */
    public TestBindings(String name) {
        super(name);
    }

    /**
     * TCK test for the BINDINGS clause.
     * 
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?book ?title ?price
     * {
     *    ?book dc:title ?title ;
     *          ns:price ?price .
     * }
     * BINDINGS ?book {
     *  (:book1)
     * }
     * </pre>
     * 
     * <pre>
     * @prefix dc:   <http://purl.org/dc/elements/1.1/> .
     * @prefix :     <http://example.org/book/> .
     * @prefix ns:   <http://example.org/ns#> .
     * 
     * :book1  dc:title  "SPARQL Tutorial" .
     * :book1  ns:price  42 .
     * :book2  dc:title  "The Semantic Web" .
     * :book2  ns:price  23 .
     * </pre>
     */
    public void test_sparql11_bindings_01() throws Exception {

        new TestHelper("sparql11-bindings-01", // testURI,
                "sparql11-bindings-01.rq",// queryFileURL
                "sparql11-bindings-01.ttl",// dataFileURL
                "sparql11-bindings-01.srx"// resultFileURL
        ).runTest();

    }

    /**
     * TCK test for the BINDINGS clause.
     * 
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?title ?price
     * {
     *    ?book dc:title ?title ;
     *          ns:price ?price .
     * }
     * BINDINGS ?book {
     *  (:book1)
     * }
     * </pre>
     * 
     * <pre>
     * @prefix dc:   <http://purl.org/dc/elements/1.1/> .
     * @prefix :     <http://example.org/book/> .
     * @prefix ns:   <http://example.org/ns#> .
     * 
     * :book1  dc:title  "SPARQL Tutorial" .
     * :book1  ns:price  42 .
     * :book2  dc:title  "The Semantic Web" .
     * :book2  ns:price  23 .
     * </pre>
     */
    public void test_sparql11_bindings_02() throws Exception {

        new TestHelper("sparql11-bindings-02", // testURI,
                "sparql11-bindings-02.rq",// queryFileURL
                "sparql11-bindings-02.ttl",// dataFileURL
                "sparql11-bindings-02.srx"// resultFileURL
        ).runTest();

    }

    /**
     * This is a variant of Federated Query <code>service04</code> where the
     * remote end point is treated as a named graph. This makes it easier to
     * examine the JOIN evaluation logic, which should be the same regardless of
     * whether or not we use a SERVICE call.
     * <p>
     * This is the result when run as a SERVICE call.
     * 
     * <pre>
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/a, o1="alan@example.org" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/a, o1="Alan" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/a, o1="alan@example.org" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/a, o1="Alan" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/b, o1="bob@example.org" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/b, o1="Bob" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/c, o1="alice@example.org" }
     * SOLUTION:   7b4c0689-2141-4523-8eae-e46338f8953d    N/A -1  -1  8   { o2=http://example.org/b, s=http://example.org/c, o1="Alice" }
     * ============ test4 =======================
     * Unexpected bindings: 
     * [o2=http://example.org/b;s=http://example.org/b;o1="bob@example.org"]
     * [o2=http://example.org/b;s=http://example.org/b;o1="Bob"]
     * </pre>
     * 
     * And the result when run as a named graph query:
     * 
     * <pre>
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/a, o1="alan@example.org" }
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/a, o1="Alan" }
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/b, o1="bob@example.org" }
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/b, o1="Bob" }
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/c, o1="alice@example.org" }
     * SOLUTION:   1fe3bae3-10d0-481c-bb20-182b0f80813a    N/A -1  -1  6   { o2=http://example.org/b, s=http://example.org/c, o1="Alice" }
     * ===================================
     * Unexpected bindings: 
     * [o2=http://example.org/b;s=http://example.org/b;o1="bob@example.org"]
     * [o2=http://example.org/b;s=http://example.org/b;o1="Bob"]
     * </pre>
     * 
     * Thus, the query has the same behavior when run locally and run against a
     * remote graph (actually, it produces duplicate solutions for "alan" when
     * run locally which appears to be because the SERVICE call actually does
     * one more hash join).
     * <p>
     * I suspect that the underlying problem has to do with bottom up
     * evaluation.
     * <p>
     * I have modified the expected result to include the solutions for "bob"
     * until someone can justify why they should be pruned in a manner which
     * exposes the underlying "bottom up" evaluation semantics issue.
     */
    public void test_sparql11_bindings_04() throws Exception {

        new TestHelper("sparql11-bindings-04").runTest();
        
    }
    

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT ?s WHERE {
     *   OPTIONAL { 
     *     ?s ?p ?o
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over singleton graph.
     */
    public void testBindingsAndBottomUp01a() throws Exception {
       
       new TestHelper("bindingsAndBottomUp01a",// testURI,
             "bindingsAndBottomUp01a.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp01a.srx"// resultFileURL
       ).runTest();
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT ?s WHERE {
     *   BIND(1 AS ?s)
     *   OPTIONAL { 
     *     ?s ?p ?o
     *   }
     * }
     * </pre>
     * 
     * over singleton graph.
     */
    public void testBindingsAndBottomUp01b() throws Exception {
    
       new TestHelper("bindingsAndBottomUp01b",// testURI,
             "bindingsAndBottomUp01b.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp01b.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT ?s WHERE {
     *   OPTIONAL { 
     *     ?s ?p ?o
     *   }
     * } VALUES ?o { <http://example.com/o> }
     * </pre>
     * 
     * over singleton graph with triple 
     * <http://example.com/s> <http://example.com/p> <http://example.com/o> .
     */
    public void testBindingsAndBottomUp01c() throws Exception {
       
       new TestHelper("bindingsAndBottomUp01c",// testURI,
             "bindingsAndBottomUp01c.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp01c.srx"// resultFileURL
       ).runTest();
       
    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT ?s WHERE {
     *   BIND(<http://example.com/o> AS ?o)
     *   OPTIONAL { 
     *     ?s ?p ?o
     *   }
     * }
     * </pre>
     * 
     * over singleton graph with triple 
     * <http://example.com/s> <http://example.com/p> <http://example.com/o> .
     */
    public void testBindingsAndBottomUp01d() throws Exception {
    
       new TestHelper("bindingsAndBottomUp01d",// testURI,
             "bindingsAndBottomUp01d.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp01d.srx"// resultFileURL
       ).runTest();       

    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT ?s WHERE {
     *   OPTIONAL { 
     *     OPTIONAL {
     *       ?s ?p ?o
     *     }
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over singleton graph.
     */
    public void testBindingsAndBottomUp02a() throws Exception {
    
       new TestHelper("bindingsAndBottomUp02a",// testURI,
             "bindingsAndBottomUp02a.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp02a.srx"// resultFileURL
       ).runTest();       

    }

    /**
     * Evaluation of query
     * 
     * SELECT ?s WHERE {
     *   BIND(1 AS ?s)
     *   OPTIONAL { 
     *     OPTIONAL {
     *       ?s ?p ?o
     *     }
     *   }
     * } 
     * 
     * over singleton graph.
     */
    public void testBindingsAndBottomUp02b() throws Exception {
       
       new TestHelper("bindingsAndBottomUp02b",// testURI,
             "bindingsAndBottomUp02b.rq",// queryFileURL
             "bindingsAndBottomUp.trig",// dataFileURL
             "bindingsAndBottomUp02b.srx"// resultFileURL
       ).runTest();       
       
    }        
    
    /**
     * Evaluation of query
     * 
     * SELECT * WHERE {
     *   BIND(1 AS ?s)
     *   {
     *     BIND(2 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     * } 
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03a() throws Exception {

       new TestHelper("bindingsAndBottomUp03a",// testURI,
             "bindingsAndBottomUp03a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03a.srx"// resultFileURL
       ).runTest();       
       
    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(2 AS ?s2)
     *   FILTER(!bound(?s))
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03b() throws Exception {

       new TestHelper("bindingsAndBottomUp03b",// testURI,
             "bindingsAndBottomUp03b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03b.srx"// resultFileURL
       ).runTest();       
       
    }
    
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   {
     *     BIND(2 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03c() throws Exception {

       new TestHelper("bindingsAndBottomUp03c",// testURI,
             "bindingsAndBottomUp03c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03c.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   {
     *     {
     *       BIND(2 AS ?s2)
     *       FILTER(!bound(?s))
     *     }
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03d() throws Exception {

       new TestHelper("bindingsAndBottomUp03d",// testURI,
             "bindingsAndBottomUp03d.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03d.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   {
     *     BIND(2 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     *   UNION
     *   {
     *     BIND(3 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03e() throws Exception {

       new TestHelper("bindingsAndBottomUp03e",// testURI,
             "bindingsAndBottomUp03e.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03e.srx"// resultFileURL
       ).runTest();       
       
    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?s)
     *   {
     *     {
     *       BIND(2 AS ?s2)
     *       FILTER(!bound(?s))
     *     }
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03f() throws Exception {

       new TestHelper("bindingsAndBottomUp03f",// testURI,
             "bindingsAndBottomUp03f.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03f.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?s)
     *   {
     *     BIND(2 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     *   UNION
     *   {
     *     BIND(3 AS ?s2)
     *     FILTER(!bound(?s))
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03g() throws Exception {

       new TestHelper("bindingsAndBottomUp03g",// testURI,
             "bindingsAndBottomUp03g.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03g.srx"// resultFileURL
       ).runTest();       
       
    }


    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   {
     *     SELECT * WHERE {
     *       BIND(2 AS ?s2)
     *       FILTER(!bound(?s))
     *     }
     *   }
     * } VALUES ?s { 1 }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03h() throws Exception {

       new TestHelper("bindingsAndBottomUp03h",// testURI,
             "bindingsAndBottomUp03h.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03h.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?s)
     *   {
     *     SELECT * WHERE {
     *       BIND(2 AS ?s2)
     *       FILTER(!bound(?s))
     *     }
     *   }
     * }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03i() throws Exception {

       new TestHelper("bindingsAndBottomUp03i",// testURI,
             "bindingsAndBottomUp03i.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03i.srx"// resultFileURL
       ).runTest();       
       
    }  
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(2 AS ?s2)
     *   VALUES ?s { 1 }
     *   FILTER(!bound(?s))
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03j() throws Exception {

       new TestHelper("bindingsAndBottomUp03j",// testURI,
             "bindingsAndBottomUp03j.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03j.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(2 AS ?s2)
     *   BIND(1 AS ?s)
     *   FILTER(!bound(?s))
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp03k() throws Exception {

       new TestHelper("bindingsAndBottomUp03k",// testURI,
             "bindingsAndBottomUp03k.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp03k.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   FILTER(!bound(?x))
     *   {
     *     BIND(1 AS ?x)
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp04a() throws Exception {

       new TestHelper("bindingsAndBottomUp04a",// testURI,
             "bindingsAndBottomUp04a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp04a.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   FILTER(!bound(?x))
     *   {
     *     VALUES ?x { 1 }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp04b() throws Exception {

       new TestHelper("bindingsAndBottomUp04b",// testURI,
             "bindingsAndBottomUp04b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp04b.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   FILTER(!bound(?x))
     *   {
     *     BIND(2 AS ?x2)
     *     {
     *        BIND(1 AS ?x)
     *     }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp04c() throws Exception {

       new TestHelper("bindingsAndBottomUp04c",// testURI,
             "bindingsAndBottomUp04c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp04c.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   FILTER(!bound(?x))
     *   {
     *     VALUES ?x2 { 2 }
     *     {
     *       VALUES ?x { 1 }
     *     }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp04d() throws Exception {

       new TestHelper("bindingsAndBottomUp04d",// testURI,
             "bindingsAndBottomUp04d.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp04d.srx"// resultFileURL
       ).runTest();       
       
    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   FILTER(!bound(?x) || !bound(?y))
     *   {
     *     BIND(1 AS ?x)
     *     {
     *       BIND(2 AS ?y)
     *     }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp04e() throws Exception {

       new TestHelper("bindingsAndBottomUp04e",// testURI,
             "bindingsAndBottomUp04e.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp04e.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(5*?x AS ?y)
     *   {
     *     BIND(1 AS ?x)
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp05a() throws Exception {

       new TestHelper("bindingsAndBottomUp05a",// testURI,
             "bindingsAndBottomUp05a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp05a.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(5*?x AS ?y)
     *   {
     *     {
     *       VALUES ?x { 1 }
     *     }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp05b() throws Exception {

       new TestHelper("bindingsAndBottomUp05b",// testURI,
             "bindingsAndBottomUp05b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp05b.srx"// resultFileURL
       ).runTest();       
       
    }

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT * WHERE {
     *       ?s ?p ?o
     *     } VALUES ?p { <http://p> }
     *   }
     * } VALUES ?p { <http://p> }
     * </pre>
     * 
     * over triple <http://s> <http://p> <http://o> .
     * @throws Exception
     */
    public void testBindingsWithSubquery01() throws Exception {

       new TestHelper("bindingsWithSubquery01",// testURI,
             "bindingsWithSubquery01.rq",// queryFileURL
             "bindingsWithSubquery01.trig",// dataFileURL
             "bindingsWithSubquery01.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT * WHERE {
     *       ?s ?p2 ?o2
     *     } VALUES ?p2 { <http://p2> }
     *   }
     * } VALUES ?p { <http://p> }
     * </pre>
     * 
     * over triples    
     * 
     * <pre>
     * <http://s> <http://p> <http://o> . 
     * <http://s> <http://p2> <http://o2> .   
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery02() throws Exception {

       new TestHelper("bindingsWithSubquery02",// testURI,
             "bindingsWithSubquery02.rq",// queryFileURL
             "bindingsWithSubquery02.trig",// dataFileURL
             "bindingsWithSubquery02.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT ?s WHERE {
     *       ?s ?p ?o .
     *     }
     *   }
     *   BIND(<http://o> AS ?o)
     * }
     * 
     * </pre>
     * 
     * over triples
     * 
     * <pre>
     * <http://s> <http://p> <http://o> .
     * <http://s> <http://p2> <http://o2> .
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery03a() throws Exception {

       new TestHelper("bindingsWithSubquery03a",// testURI,
             "bindingsWithSubquery03a.rq",// queryFileURL
             "bindingsWithSubquery03a.trig",// dataFileURL
             "bindingsWithSubquery03a.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT ?s WHERE {
     *       ?s ?p ?o .
     *       FILTER(?o=<http://o2>)
     *     }
     *   }
     *   BIND(<http://o> AS ?o)
     * }
     * 
     * </pre>
     * 
     * over triples
     * 
     * <pre>
     * <http://s> <http://p> <http://o> .
     * <http://s> <http://p2> <http://o2> .
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery03b() throws Exception {

       new TestHelper("bindingsWithSubquery03b",// testURI,
             "bindingsWithSubquery03b.rq",// queryFileURL
             "bindingsWithSubquery03b.trig",// dataFileURL
             "bindingsWithSubquery03b.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT ?s WHERE {
     *       ?s ?p ?o .
     *       FILTER(?o=<http://o2>)
     *     }     
     *   }
     *   BIND(<http://o> AS ?o)
     * } 
     * </pre>
     * 
     * over triples
     * 
     * <pre>
     * <http://s> <http://p> <http://o> .
     * <http://s> <http://p2> <http://o2> .
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery04() throws Exception {

       new TestHelper("bindingsWithSubquery04",// testURI,
             "bindingsWithSubquery04.rq",// queryFileURL
             "bindingsWithSubquery04.trig",// dataFileURL
             "bindingsWithSubquery04.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT ?s WHERE {
     *       BIND(<http://p2> AS ?p)
     *       ?s ?p ?o .
     *     }
     *   }
     *   BIND(<http://o> AS ?o)
     * }
     * </pre>
     * 
     * over triples
     * 
     * <pre>
     * <http://s> <http://p> <http://o> .
     * <http://s> <http://p2> <http://o2> .
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery05() throws Exception {

       new TestHelper("bindingsWithSubquery05",// testURI,
             "bindingsWithSubquery05.rq",// queryFileURL
             "bindingsWithSubquery05.trig",// dataFileURL
             "bindingsWithSubquery05.srx"// resultFileURL
       ).runTest();       

    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s ?p ?o
     *   {
     *     SELECT ?s WHERE {
     *       BIND(<http://p2> AS ?p)
     *       ?s ?p ?o .
     *     }
     *   }
     *   BIND(<http://o> AS ?o)
     * } VALUES ?p { <http://p> } 
     * </pre>
     * 
     * over triples
     * 
     * <pre>
     * <http://s> <http://p> <http://o> .
     * <http://s> <http://p2> <http://o2> .
     * </pre>
     * 
     * @throws Exception
     */
    public void testBindingsWithSubquery06() throws Exception {

       new TestHelper("bindingsWithSubquery06",// testURI,
             "bindingsWithSubquery06.rq",// queryFileURL
             "bindingsWithSubquery06.trig",// dataFileURL
             "bindingsWithSubquery06.srx"// resultFileURL
       ).runTest();       

    }
        
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?x)
     *   {
     *     {
     *       BIND(5*?x AS ?y)
     *     }
     *   }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndBottomUp05c() throws Exception {

       new TestHelper("bindingsAndBottomUp05c",// testURI,
             "bindingsAndBottomUp05c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndBottomUp05c.srx"// resultFileURL
       ).runTest();       
       
    }
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?a)
     *   BIND(2 AS ?b)
     * } VALUES ?c { "c1" "c2" }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01a() throws Exception {

       new TestHelper("bindingsAndValuesMix01a",// testURI,
             "bindingsAndValuesMix01a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01a.srx"// resultFileURL
       ).runTest();

    }    

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?a)
     *   {  
     *     BIND(2 AS ?b)
     *     {
     *       BIND(3 AS ?c)
     *     }
     *   }
     * } VALUES ?d { "d1" }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01b() throws Exception {

       new TestHelper("bindingsAndValuesMix01b",// testURI,
             "bindingsAndValuesMix01b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01b.srx"// resultFileURL
       ).runTest();
    }    

    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   BIND(1 AS ?a)
     *   OPTIONAL {  
     *     BIND(2 AS ?b)
     *   }
     * } VALUES ?c { "c1" }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01c() throws Exception {

       new TestHelper("bindingsAndValuesMix01c",// testURI,
             "bindingsAndValuesMix01c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01c.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   VALUES (?a ?b) { ("a1" "b1") ("a2" "b2") }
     *   BIND("b1" AS ?b)
     * } VALUES (?a ?c) { ("a1" "c1") ("a2" "c2") }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01d() throws Exception {

       new TestHelper("bindingsAndValuesMix01d",// testURI,
             "bindingsAndValuesMix01d.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01d.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   VALUES (?a ?b) { ("a1" "b1") ("a2" "b2") }
     * } VALUES (?a ?c) { ("a1" "c1") ("a2" "c2") }
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01e() throws Exception {

       new TestHelper("bindingsAndValuesMix01e",// testURI,
             "bindingsAndValuesMix01e.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01e.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Evaluation of query
     * 
     * <pre>
     * SELECT * WHERE {
     *   VALUES (?a ?b) { ("a1" "b1") ("a2" "b2") }
     *   VALUES (?a ?c) { ("a1" "c1") ("a2" "c2") }
     * } 
     * </pre>
     * 
     * over empty graph.
     */
    public void testBindingsAndValuesMix01f() throws Exception {

       new TestHelper("bindingsAndValuesMix01e",// testURI,
             "bindingsAndValuesMix01e.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "bindingsAndValuesMix01e.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-48">
     * Query fails to project subquery variables</a>.
     */
    public void test_ticket_bg48a() throws Exception {
       new TestHelper("ticket_bg48a",// testURI,
             "ticket_bg48a.rq",// queryFileURL
             "ticket_bg48.trig",// dataFileURL
             "ticket_bg48.srx"// resultFileURL
       ).runTest();          
    }
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-48">
     * Query fails to project subquery variables</a>.
     */
    public void test_ticket_bg48b() throws Exception {
       new TestHelper("ticket_bg48b",// testURI,
             "ticket_bg48b.rq",// queryFileURL
             "ticket_bg48.trig",// dataFileURL
             "ticket_bg48.srx"// resultFileURL
       ).runTest();          
    }
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-48">
     * Query fails to project subquery variables</a>.
     */
    public void test_ticket_bg48c() throws Exception {
       new TestHelper("ticket_bg48c",// testURI,
             "ticket_bg48c.rq",// queryFileURL
             "ticket_bg48.trig",// dataFileURL
             "ticket_bg48.srx"// resultFileURL
       ).runTest();          
    }
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-50">
     * Queries with multiple VALUES clauses</a>.
     */
    public void test_ticket_bg50a() throws Exception {
       new TestHelper("ticket_bg50a",// testURI,
             "ticket_bg50a.rq",// queryFileURL
             "ticket_bg50.trig",// dataFileURL
             "ticket_bg50.srx"// resultFileURL
       ).runTest();           
    }
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-50">
     * Queries with multiple VALUES clauses</a>.
     */
    public void test_ticket_bg50b() throws Exception {
       new TestHelper("ticket_bg50b",// testURI,
             "ticket_bg50b.rq",// queryFileURL
             "ticket_bg50.trig",// dataFileURL
             "ticket_bg50.srx"// resultFileURL
       ).runTest();           
    }
    
    /**
     * Problem with multiple VALUES clauses, as described in 
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-50">
     * Queries with multiple VALUES clauses</a>.
     */
    public void test_ticket_bg50c() throws Exception {
       new TestHelper("ticket_bg50c",// testURI,
             "ticket_bg50c.rq",// queryFileURL
             "ticket_bg50.trig",// dataFileURL
             "ticket_bg50.srx"// resultFileURL
       ).runTest();           
    }
    
    /**
     * Duplicate in VALUES get replicated problem
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1299">
     * duplicates in VALUES get replicated</a>.
     */
    public void test_ticket_bg1299() throws Exception {
       new TestHelper("ticket_bg1299",// testURI,
             "ticket_bg1299.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1299.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Testing proper use of VALUES clause inside named subqueries.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1296">
     * named subquery and VALUES expression</a>.
     */
    public void test_ticket_bg1296a() throws Exception {
       new TestHelper("ticket_bg1296a",// testURI,
             "ticket_bg1296a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1296.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Testing proper use of VALUES clause inside named subqueries.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1296">
     * named subquery and VALUES expression</a>.
     */
    public void test_ticket_bg1296b() throws Exception {
       new TestHelper("ticket_bg1296b",// testURI,
             "ticket_bg1296b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1296.srx"// resultFileURL
       ).runTest();
    }    

    /**
     * Testing proper use of VALUES clause inside named subqueries.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1296">
     * named subquery and VALUES expression</a>.
     */
    public void test_ticket_bg1296c() throws Exception {
       new TestHelper("ticket_bg1296c",// testURI,
             "ticket_bg1296c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1296.srx"// resultFileURL
       ).runTest();
    }    

    /**
     * Testing proper use of VALUES clause inside named subqueries.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1296">
     * named subquery and VALUES expression</a>.
     */
    public void test_ticket_bg1296d() throws Exception {
       new TestHelper("ticket_bg1296d",// testURI,
             "ticket_bg1296d.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1296.srx"// resultFileURL
       ).runTest();
    }    

    /**
     * Combination of VALUES clause and SERVICE keyword
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1256">
     * Service call with values clauses create a cross product</a>
     */
    public void test_ticket_bg1256() throws Exception {
       new TestHelper("ticket_bg1256",// testURI,
             "ticket_bg1256.rq",// queryFileURL
             "ticket_bg1256.trig",// dataFileURL
             "ticket_bg1256.srx"// resultFileURL
       ).runTest();
    }   
    
    /**
     * Strategies for VALUES+BIND queries
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1141">
     * Strategies for VALUES+BIND queries</a>
     */
    public void test_ticket_bg1141() throws Exception {
       new TestHelper("ticket_bg1141",// testURI,
             "ticket_bg1141.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg1141.srx"// resultFileURL
       ).runTest();
    }

    
    /**
     * Isues with placement of BIND.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-876">
     * BIND not executed before SERVICE call</a>, comment from
     * 07/Jan/14 7:42 PM.
     */
    public void test_ticket_bg876c() throws Exception {
       new TestHelper("ticket_bg876c",// testURI,
             "ticket_bg876c.rq",// queryFileURL
             "ticket_bg876c.trig",// dataFileURL
             "ticket_bg876c.srx"// resultFileURL
       ).runTest();       
    }
    
    /**
     * Isues with placement of BIND.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-876">
     * BIND not executed before SERVICE call</a>, comment from
     * 07/Jan/14 7:42 PM.
     */
    public void test_ticket_bg876d() throws Exception {
       new TestHelper("ticket_bg876d",// testURI,
             "ticket_bg876d.rq",// queryFileURL
             "ticket_bg876d.trig",// dataFileURL
             "ticket_bg876d.srx"// resultFileURL
       ).runTest();       
    }
    
    /**
     * Isues with placement of BIND.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-876">
     * BIND not executed before SERVICE call</a>, comment from
     * 07/Jan/14 7:43 PM.
     */
    public void test_ticket_bg876e() throws Exception {
       new TestHelper("ticket_bg876e",// testURI,
             "ticket_bg876e.rq",// queryFileURL
             "ticket_bg876e.trig",// dataFileURL
             "ticket_bg876e.srx"// resultFileURL
       ).runTest();       
    }    
    
    public void test_nested_values01() throws Exception {
       new TestHelper("nested_values01",// testURI,
             "nested_values01.rq",// queryFileURL
             "nested_values.trig",// dataFileURL
             "nested_values01.srx"// resultFileURL
       ).runTest();           
    }
    
    public void test_nested_values02() throws Exception {
       new TestHelper("nested_values02",// testURI,
             "nested_values02.rq",// queryFileURL
             "nested_values.trig",// dataFileURL
             "nested_values02.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_nested_values03() throws Exception {
       new TestHelper("nested_values03",// testURI,
             "nested_values03.rq",// queryFileURL
             "nested_values.trig",// dataFileURL
             "nested_values03.srx"// resultFileURL
       ).runTest();          
    }
    
    public void test_nested_values04() throws Exception {
       new TestHelper("nested_values04",// testURI,
             "nested_values04.rq",// queryFileURL
             "nested_values.trig",// dataFileURL
             "nested_values04.srx"// resultFileURL
       ).runTest();          
    }
    
    public void test_nested_values05() throws Exception {
       new TestHelper("nested_values05",// testURI,
             "nested_values05.rq",// queryFileURL
             "nested_values.trig",// dataFileURL
             "nested_values05.srx"// resultFileURL
       ).runTest();          
    }
    
    /**
     * Some of the test cases require the FTS index, so we need a custom
     * properties definition here.
     */
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "true");

        return properties;

    }    
}
