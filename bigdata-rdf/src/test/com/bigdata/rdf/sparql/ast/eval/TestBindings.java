/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Data driven test suite for SPARQL 1.1 BINDINGS clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit test to verify that static analysis correctly leverages
 *          when a variable is known bound to a constant across multiple
 *          solutions in the BINDINGS clause and when it takes on different
 *          values in different solutions in the BINDINGS clause.
 * 
 *          TODO Unit test when a variable is bound in some solutions in the
 *          BINDINGS clause but not in other solutions in that BINDINGS clause.
 *          The variable can not be treated as "known" bound in the latter case.
 * 
 *          TODO Unit test when a variable which is known bound in the BINDINGS
 *          clause is not projected into a sub-select. The test should verify
 *          that a variable by the same name in the sub-select is in fact a
 *          distinct variable.
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
    
}
