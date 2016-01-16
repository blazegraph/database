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

package com.bigdata.rdf.sparql.ast.eval.service;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

/**
 * Data driven test suite for full text search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Correct rejection test when a magic predicate for search
 *          appears outside of a named query clause.
 * 
 *          TODO Correct rejection test when the subject of a magic predicate
 *          for search is a constant.
 * 
 *          TODO Correct rejection test when an unknown magic predicate in the
 *          {@link BD#SEARCH_NAMESPACE} is observed.
 * 
 *          TODO Add unit tests for slicing by rank and relevance.
 */
public class TestSearch extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestSearch() {
    }

    /**
     * @param name
     */
    public TestSearch(String name) {
        super(name);
    }

    /**
     * A simple full text search query. The query is transformed from
     * 
     * <pre>
     * PREFIX bd: <http://www.bigdata.com/rdf/search#>
     * 
     * SELECT ?subj ?score 
     * 
     *   WITH {
     *    SELECT ?subj ?score
     *     WHERE {
     *       ?lit bd:search "mike" .
     *       ?lit bd:relevance ?score .
     *       ?subj ?p ?lit .
     *       }
     *     ORDER BY DESC(?score)
     *     LIMIT 10
     *     OFFSET 0
     *   } as %searchSet1
     * 
     * WHERE {
     *    
     *    include %searchSet1
     *    
     * }
     * </pre>
     * 
     * <pre>
     * PREFIX bd: <http://www.bigdata.com/rdf/search#>
     * WITH {
     *   QueryType: SELECT
     *   SELECT ( VarNode(subj) AS VarNode(subj) ) ( VarNode(score) AS VarNode(score) )
     *     JoinGroupNode {
     *       SERVICE <ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search])> {
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search]), ConstantNode(TermId(0L)[mike]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#relevance]), VarNode(score), DEFAULT_CONTEXTS)
     *         }
     *       }
     *       StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), DEFAULT_CONTEXTS)
     *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=5
     *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=SPOC
     *     }
     *   order by com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(score))[ ascending=false]
     *   slice(limit=10)
     * } AS %searchSet1 JOIN ON () DEPENDS ON ()
     * QueryType: SELECT
     * SELECT ( VarNode(subj) AS VarNode(subj) ) ( VarNode(score) AS VarNode(score) )
     *   JoinGroupNode {
     *     INCLUDE %searchSet1 JOIN ON ()
     *   }
     * </pre>
     */
    public void test_search_1() throws Exception {
        
        new TestHelper("search-1").runTest();
        
    }

    /**
     * A simple full text search query (variant of the above using the
     * <code>SPARQL 1.1</code> SERVICE syntax).
     */
    public void test_search_service_1() throws Exception {
        
        new TestHelper("search-service-1").runTest();
        
    }

    /**
     * A full text search query based on the in-depth example in the full
     * text index core package ("child proofing" example).
     */
    public void test_search_2() throws Exception {
        
        new TestHelper("search-2").runTest();
        
    }

    /**
     * An example with an additional join in the named query and a join in the
     * main query as well.
     */
    public void test_search_3() throws Exception {

        new TestHelper("search-3").runTest();

    }

    /**
     * Unit test for a prefix match ("mi*").
     * 
     * <pre>
     * # Search query.
     * PREFIX bds: <http://www.bigdata.com/rdf/search#>
     * 
     * SELECT ?subj ?label 
     * WHERE {
     *       ?lit bds:search "mi*" .
     *       ?lit bds:relevance ?cosine .
     *       ?subj ?p ?label .
     * }
     * </pre>
     */
    public void test_search_prefix_match() throws Exception {
        
        new TestHelper(
                "search-prefix-match",// testURI
                "search-prefix-match.rq",// query
                "search-prefix-match.trig",//data
                "search-prefix-match.srx" // expected results
                ).runTest();

    }

    /**
     * Unit test for a prefix match ("mi*").
     * 
     * <pre>
     * # Search query.
     * PREFIX bds: <http://www.bigdata.com/rdf/search#>
     * 
     * SELECT ?subj ?label 
     *   WITH {
     *    SELECT ?subj ( ?lit as ?label )
     *     WHERE {
     *       ?lit bds:search "mi*" .
     *       ?lit bds:relevance ?cosine .
     *       ?subj ?p ?lit .
     *       }
     *   } as %searchSet1
     * WHERE {
     *    include %searchSet1
     * }
     * </pre>
     */
    public void test_search_prefix_match2() throws Exception {
        
        new TestHelper(
                "search-prefix-match2",// testURI
                "search-prefix-match2.rq",// query
                "search-prefix-match.trig",//data
                "search-prefix-match.srx" // expected results
                ).runTest();
        
    }

    /**
     * Unit test for a search where all tokens in the query must match.
     */
    public void test_search_match_all_terms() throws Exception {
        
        new TestHelper("search-match-all-terms").runTest();
        
    }

    /**
     * Unit test with named graphs (quads) using graph graph pattern and a
     * subject join in the named subquery. In this case we do not need to do
     * anything more to filter out literals matched which do not appear in
     * statements for the named graph.
     * <p>
     * This version has an unbound graph variable.
     */
    public void test_search_named_graphs1() throws Exception {
        
        new TestHelper("search-named-graphs1").runTest();
        
    }
    
    /**
     * Variant of the test above in which the graph variable is bound to the
     * graph in which the search result is visible.
     */
    public void test_search_named_graphs2() throws Exception {
        
        new TestHelper("search-named-graphs2").runTest();
        
    }

    /**
     * Variant of the test above in which the graph variable is bound to a graph
     * in which the search result is not visible.
     */
    public void test_search_named_graphs3() throws Exception {
        
        new TestHelper("search-named-graphs3").runTest();
        
    }

    /**
     * Variant of the test above in there are search results in more than one
     * graph.
     */
    public void test_search_named_graphs4() throws Exception {
        
        new TestHelper("search-named-graphs4").runTest();
        
    }

    /**
     * Unit test with named graphs (quads) using a graph graph pattern but not
     * having a subject join in the named subquery. In this case we need to take
     * action in order to ensure that literal matches are filtered out if they
     * do not appear in a statement for the named graph. Verify that a subject
     * join is automatically added into the named query using an anonymous
     * variable for the subject and predicate positions to impose that
     * constraint.
     */
    public void test_search_named_graphs5() throws Exception {
        
        new TestHelper("search-named-graphs5").runTest();
        
    }

    /**
     * Run the query with no graphs specified.
     * 
     * <pre>
     * SELECT ?s 
     * WHERE {
     *     ?s bd:search "Alice" .
     * }
     * </pre>
     * <p>
     * Note: The first and second condition in the old test were identical. The
     * first condition was modified to run with no graphs specified when the
     * tests were migrated to this format.
     * <p>
     * Note: This is the first of a series of tests which are a port of
     * com.bigdata.rdf.sail.TestSearchQuery#testWithNamedGraphs(). Some errors
     * in that test were corrected when the tests were migrated to this format.
     */
    public void test_search_named_graphs10a() throws Exception {

        new TestHelper(//
                "search-named-graphs10", // testURI
                "search-named-graphs10a.rq", // queryFileURI
                "search-named-graphs10.trig",// dataFileURI
                "search-named-graphs10a.srx"// resultFileURI
        ).runTest();

    }

    /**
     * Run the query with graphA specified as the default graph.
     * 
     * <pre>
     * SELECT ?s 
     * FROM :graphA
     * WHERE {
     *     ?s bd:search "Alice" .
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_search_named_graphs10b() throws Exception {

        new TestHelper(//
                "search-named-graphs10", // testURI
                "search-named-graphs10b.rq", // queryFileURI
                "search-named-graphs10.trig",// dataFileURI
                "search-named-graphs10b.srx"// resultFileURI
        ).runTest();

    }

    /**
     * Run the query with graphB specified as the default graph
     * 
     * <pre>
     * SELECT ?s 
     * FROM :graphB
     * WHERE {
     *     ?s bd:search "Alice" .
     * }
     * </pre>
     */
    public void test_search_named_graphs10c() throws Exception {

        new TestHelper(//
                "search-named-graphs10", // testURI
                "search-named-graphs10c.rq", // queryFileURI
                "search-named-graphs10.trig",// dataFileURI
                "search-named-graphs10c.srx"// resultFileURI
        ).runTest();

    }

    /**
     * Run the query with graphB specified as the default graph and also pull
     * out the subject from the search variable.
     * 
     * <pre>
     * SELECT ?s ?o 
     * FROM :graphB
     * WHERE {
     *     ?s rdfs:label ?o .
     *     ?o bd:search "Alice" .
     * }
     * </pre>
     */
    public void test_search_named_graphs10d() throws Exception {

        new TestHelper(//
                "search-named-graphs10", // testURI
                "search-named-graphs10d.rq", // queryFileURI
                "search-named-graphs10.trig",// dataFileURI
                "search-named-graphs10d.srx"// resultFileURI
        ).runTest();

    }

    /**
     * Run the query with graphA specified as the default graph and also pull
     * out the subject from the search variable.
     * 
     * <pre>
     * SELECT ?s ?o 
     * FROM :graphA
     * WHERE {
     *     ?s rdfs:label ?o .
     *     ?o bd:search "Alice" .
     * }
     * </pre>
     * <p>
     * Note: Note: this was running against graphB in the old test suite, but we
     * do that in the case above. It has been modified to run against graphA in
     * this test suite.
     */
    public void test_search_named_graphs10e() throws Exception {

        new TestHelper(//
                "search-named-graphs10", // testURI
                "search-named-graphs10e.rq", // queryFileURI
                "search-named-graphs10.trig",// dataFileURI
                "search-named-graphs10e.srx"// resultFileURI
        ).runTest();

    }

    /**
     * Unit test for a query with magic search predicates in the main WHERE
     * clause. These predicates should be translated into a {@link ServiceNode}
     * which is then lifted into a named subquery.
     */
    public void test_search_main_where_clause() throws Exception {
        
        new TestHelper("search-main-where-clause").runTest();
        
    }
    
    /**
     * Unit test with explicit {@link BD#MIN_RELEVANCE} of zero and a prefix
     * match search.
     */
    public void test_search_min_relevance() throws Exception {

        new TestHelper("search-min-relevance").runTest();

    }
    
    /**
     * Ported from TestNamedGraphs in the sails package.
     */
    public void test_search_query() throws Exception {
      
      if(!store.isQuads())
          return;

      new TestHelper(
          "search-query",// testURI
          "search-query.rq", // queryURI
          "search-query.trig", // dataURI
          "search-query.srx" // resultURI
          ).runTest();

  }
    
//    /**
//     * TODO Unit test for search-in-search pattern:
//     * 
//     * <pre>
//     * select ?snippet
//     * where {
//     * 
//     *   ?o bd:search "foo" .
//     *   ?snippet ?p ?o .
//     *   
//     *   ?snippet ?p1 ?o1 .
//     *   filter(isLiteral(?o1)) .
//     *   filter(regex(str(?o1),"bar")) .
//     * 
//     * }
//     * You could achieve the same result by running it like this:
//     * 
//     * select ?snippet
//     * where {
//     *   
//     *   ?o1 bd:search "foo" .
//     *   ?snippet ?p1 ?o1 .
//     * 
//     *   ?o2 bd:search "bar" .
//     *   ?snippet ?p2 ?o2 .
//     * 
//     * }
//     * 
//     * You'd want to do a hash join of the two groups of two predicates, joining on ?snippet.
//     * there needs to be pagination on both sides of the two searches or the query will croak.
//     * 
//     * So for something like snippet merge:
//     * 
//     * select ?snippet1 ?snippet2 ?mergeVal
//     * where {
//     * 
//     *   ?o1 bd:search "foo" .
//     *   ?o1 bd:rank ?rank1 .
//     *   ?snippet1 ?p1 ?o1 .
//     *   ?snippet1 <mergePredicate1> ?mergeVal .
//     * 
//     *   optional {
//     * 
//     *     ?o2 bd:search "bar" .
//     *     ?o2 bd:rank ?rank2 .
//     *     ?snippet2 ?p2 ?o2 .
//     *     ?snippet2 <mergePredicate2> ?mergeVal .
//     * 
//     *   }
//     * 
//     * }
//     * 
//     * I page through the right side of the hash join (the "bar" search) rank by rank. 
//     * When I hit the end of the right side I move one page forward on the left side
//     * and then do the whole paging of the right side over again. Right now I have no
//     * way to do an N-way merge, because the pagination would start to take forever.
//     * 
//     * ?rank1, ?rank2
//     * 1, 1
//     * 1, 2
//     * 1, 3
//     * 1, 4
//     * 2, 1
//     * 2, 2
//     * 2, 3
//     * 2, 4
//     * </pre>
//     */
//    public void test_search_in_search() {
//        fail("write test");
//    }
    
}
