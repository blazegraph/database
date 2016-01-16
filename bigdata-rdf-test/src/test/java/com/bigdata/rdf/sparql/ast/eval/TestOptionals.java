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
 * Created on Oct 11, 2011
 */
package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for OPTIONAL groups. Unlike the TCK, this test suite is focused on
 * the semantics of well-formed OPTIONAL groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOptionals extends AbstractDataDrivenSPARQLTestCase {

    public TestOptionals() {
    }

    public TestOptionals(String name) {
        super(name);
    }

    /**
     * Unit test for a simple optional (one where the statement pattern can
     * be lifted into the parent group).
     */
    public void test_simple_optional_01() throws Exception {

        new TestHelper(
                "simple-optional-01",// testURI
                "simple-optional-01.rq", // queryURI
                "simple-optional-01.ttl", // dataURI
                "simple-optional-01.srx" // resultURI
                ).runTest();
        
    }
    
    /**
     * Unit test for an optional which is too complex to be handled as a simple
     * optional (it uses a filter which can not be lifted since it requires a
     * materialized variable).
     */
    public void test_optional_with_filter() throws Exception {

        new TestHelper(
                "optional-with-filter-01",// testURI
                "optional-with-filter-01.rq", // queryURI
                "optional-with-filter-01.ttl", // dataURI
                "optional-with-filter-01.srx" // resultURI
                ).runTest();
        
    }

    /**
     * The FILTER will always fail. This means that the OPTIONAL group will
     * never produce any solutions. Thus only the original solutions from the
     * statement pattern outside of the optional will be reported as solutions
     * for the query. This tests the correct eventual triggering of the optional
     * hash join at the end of the optional group.
     * <p>
     * Note: If this test fails, then it is likely that the optional hash join
     * was not triggered for its last pass evaluation. For this test to pass the
     * optional hash join needs to be triggered for last pass evaluation even
     * though no solutions reached the optional hash join through normal
     * evaluation (they were all failed by the filter).
     */
    public void test_optional_with_filter_that_fails() throws Exception {

        new TestHelper("optional-with-filter-that-fails-01",// testURI
                "optional-with-filter-that-fails-01.rq", // queryURI
                "optional-with-filter-that-fails-01.ttl", // dataURI
                "optional-with-filter-that-fails-01.srx" // resultURI
        ).runTest();

    }

    /**
     * Unit test for an optional which is too complex to be handled as a simple
     * optional (it involves more than one statement pattern).
     * 
     * <pre>
     * select ?x ?o ?y ?z
     * where {
     *   ?x rdf:type foaf:Person
     *   OPTIONAL {
     *   ?x rdfs:label ?o
     *   }
     *   OPTIONAL {
     *   ?x foaf:knows ?y .
     *   ?y foaf:knows ?z .
     *   FILTER ( ?z != ?x )
     *   }
     * }
     * </pre>
     */
    public void test_complex_optional_01() throws Exception {
        
        new TestHelper(
                "complex-optional-01",// testURI
                "complex-optional-01.rq", // queryURI
                "complex-optional-01.ttl", // dataURI
                "complex-optional-01.srx"  // resultURI
//                ,true// laxCardinality
//                ,false // checkOrder
                ).runTest();
        
    }


    /**
     * Unit test for <a href="http://sourceforge.net/apps/trac/bigdata/ticket/712">trac 712</a>
     * 
     * <pre>
     * select ?x 
     * where {
     *   OPTIONAL {
     *    FILTER ( false )
     *   }
     * }
     * </pre>
     */
    public void test_non_matching_optional_01() throws Exception {
        
        new TestHelper(
                "non-matching-optional-01",// testURI
                "non-matching-optional-01.rq", // queryURI
                "non-matching-optional-01.ttl", // dataURI
                "non-matching-optional-01.srx"  // resultURI
//                ,true// laxCardinality
//                ,false // checkOrder
                ).runTest();
        
    }

    /**
     * Unit test for <a href="http://sourceforge.net/apps/trac/bigdata/ticket/712">trac 712</a>
     * 
     * <pre>
     * select ?x 
     * where {
     *   BIND ( 1 as ?y )
     *   OPTIONAL {
     *     FILTER ( false )
     *   }
     * }
     * </pre>
     */
    public void test_non_matching_optional_02() throws Exception {
        
        new TestHelper(
                "non-matching-optional-02",// testURI
                "non-matching-optional-02.rq", // queryURI
                "non-matching-optional-01.ttl", // dataURI - same as before
                "non-matching-optional-01.srx"  // resultURI - same as before
//                ,true// laxCardinality
//                ,false // checkOrder
                ).runTest();
        
    }
    /**
     * <pre>
     * select *
     * where {
     *   ?s rdf:type :Person .
     *   OPTIONAL {
     *     ?s :p1 ?p1 .
     *     OPTIONAL {
     *       ?p1 :p2 ?o2 .
     *     }
     *   }
     * }
     * </pre>
     * 
     * Note: This test was ported from
     * TestBigdataSailEvaluationStrategyImpl#test_prune_groups()
     */
    public void test_prune_groups() throws Exception {
        
        new TestHelper("prune_groups").runTest();
        
    }

    /**
     * <pre>
     * select ?s ?label ?comment
     * where {
     *   ?s rdf:type :Person .
     *   ?s rdf:type :Object .
     *   OPTIONAL { ?s rdfs:label ?label . } 
     *   OPTIONAL { ?s rdfs:comment ?comment . } 
     * }
     * </pre>
     * 
     * Note: This test was ported from
     * TestBigdataSailEvaluationStrategyImpl#test_prune_groups()
     */
    public void test_nested_optionals() throws Exception {
        
        new TestHelper("nested_optionals").runTest();
        
    }

    /*
     * Tests ported from com.bigdata.rdf.sail.TestOptionals.
     */
    
    /**
     * <pre>
     * select *
     * where {
     *   ?a :knows ?b .
     *   OPTIONAL {
     *     ?b :knows ?c .
     *     ?c :knows ?d .
     *   }
     * }
     * </pre>
     */
    public void test_optionals_simplest() throws Exception {

        new TestHelper("optionals_simplest").runTest();

    }

    /**
     * <pre>
     * select *
     * where {
     *   ?a :knows ?b .
     *   OPTIONAL {
     *     ?b :knows ?c .
     *     ?c :knows ?d .
     *     filter(?d != :leon)
     *   }
     * }
     * </pre>
     */
    public void test_optionals_simplestWithFilter() throws Exception {

        new TestHelper("optionals_simplestWithFilter").runTest();

    }

    /**
     * <pre>
     * select *
     * where {
     *   ?a :knows ?b .
     *   OPTIONAL {
     *     ?b :knows ?c .
     *     ?c :knows ?d .
     *     filter(?d != :paul)
     *   }
     * }
     * </pre>
     */
    public void test_optionals_simplestWithConditional() throws Exception {

        new TestHelper("optionals_simplestWithConditional").runTest();

    }
    
    /**
     * Test case for issue #1079.
     * 
     * <pre>
     * select * where {
     *   optional { 
     *     <http://someURI> rdfs:label ?labelPrefered_l_ru. 
     *     filter( langMatches(lang(?labelPrefered_l_ru),"de") )
     *   }
     *   optional { 
     *     <http://someURI> rdfs:label ?labelPrefered_l_ru. 
     *     filter( langMatches(lang(?labelPrefered_l_ru),"en") )
     *   }
     * }
     * </pre>
     */
    public void test_optionals_emptyWhereClause() throws Exception {

        new TestHelper("optionals_emptyWhereClause").runTest();

    }
    
}
