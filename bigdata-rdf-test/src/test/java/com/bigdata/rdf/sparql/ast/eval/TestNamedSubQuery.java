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

/**
 * Data driven test suite for named subquery evaluation (that is, for queries
 * which are explicitly written using the named subquery syntax).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNamedSubQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestNamedSubQuery() {
    }

    /**
     * @param name
     */
    public TestNamedSubQuery(String name) {
        super(name);
    }

    /**
     * This is a version of {@link TestSubQuery#test_sparql_subselect()} which
     * uses the same data and has the same results, but which uses a named
     * subquery rather than a SPARQL 1.1 subselect.
     * 
     * <pre>
     * select ?x ?o
     *   with {
     *     select ?x where { ?x rdf:type foaf:Person }
     *   } AS %namedSet1
     * where {
     *   ?x rdfs:label ?o
     *   INCLUDE %namedSet1 
     * }
     * </pre>
     */
    public void test_named_subquery() throws Exception {

        new TestHelper("named-subquery").runTest();

    }

//    /**
//     * This is a variant {@link #test_named_subquery()} in which the JOIN ON
//     * query hint is used to explicitly specify NO join variables.
//     */
//    public void test_named_subquery_noJoinVars() throws Exception {
//
//        new TestHelper("named-subquery-noJoinVars").runTest();
//
//    }

    /**
     * Test that only projected variables are included in named-subquery
     * results.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s ?x
     * WITH{
     *     SELECT ?s ?x { ?s :p ?x }
     * }as %set1
     * WITH{
     *     SELECT ?s ?fake1 ?fake2 { ?x :q ?s . LET (?fake1 := 1) . LET (?fake2 := 2) . }
     * }as %set2
     * WHERE {
     *      INCLUDE %set1
     *      INCLUDE %set2
     * }
     * </pre>
     */
    public void test_named_subquery_scope() throws Exception {

        new TestHelper("named-subquery-scope").runTest();

    }

    /**
     * <pre>
     * prefix : <http://www.bigdata.com/>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?a ?x ?y
     * WITH {
     *   SELECT ?a ?x {?a :x ?x}
     * } as %set1
     * WITH {
     *   SELECT ?a ?y {?a :y ?y}
     * } as %set2
     * WHERE {
     *    OPTIONAL {INCLUDE %set1}.
     *    OPTIONAL {INCLUDE %set2}.
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_double_optional_include() throws Exception {

        new TestHelper("double-optional-include").runTest();
        
    }
    
    /**
     * Unit test verifies that the BINDINGS clause is presented to the named
     * subquery. This is done by referencing the variable bound the BINDINGS
     * clause only within the named subquery and not the main where clause.
     * Thus, if the query is to be restricted, the named subquery needs to be
     * imposing that restriction.
     * 
     * <pre>
     * select ?x ?o
     *   with {
     *     select ?x where { ?x rdf:type foaf:Person }
     *   } AS %namedSet1
     * where {
     *   ?x rdfs:label ?o
     *   INCLUDE %namedSet1 
     * }
     * bindings ?x
     * {
     *   ( <http://www.bigdata.com/Mike> )
     * }
     * </pre>
     */
    public void test_named_subquery_bindings() throws Exception {

        new TestHelper("named-subquery-bindings").runTest();

    }
    
    /**
     * Unit test verifies that the named subquery is considering the
     * exogenous variables when it's the first operator in the query and
     * choosing the appropriate join variables to build the hash index.  You
     * need to enable logging to verify that the JOIN_VARS are set correctly
     * on the include (you should see VarNode(x)).
     * 
     * <pre>
     * select ?x ?o
     *   with {
     *     select ?x where { ?x rdf:type foaf:Person }
     *   } AS %namedSet1
     * where {
     *   INCLUDE %namedSet1 
     *   ?x rdfs:label ?o
     * }
     * bindings ?x
     * {
     *   ( <http://www.bigdata.com/Mike> )
     * }
     * </pre>
     */
    public void test_named_subquery_bindings_2() throws Exception {

        new TestHelper("named-subquery-bindings-2").runTest();

    }
    
}
