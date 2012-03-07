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

/**
 * Data driven test suite for SPARQL 1.1 BINDINGS clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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

}
