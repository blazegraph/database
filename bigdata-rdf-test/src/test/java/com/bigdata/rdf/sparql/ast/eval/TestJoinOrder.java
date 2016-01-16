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
 * Data driven test suite for SPARQL 1.1 BIND & VALUES clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a> * 
 * @version $Id$
 */
public class TestJoinOrder extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestJoinOrder() {
    }

    /**
     * @param name
     */
    public TestJoinOrder(String name) {
        super(name);
    }

    /**
     * Query
     * 
     * <pre>
     * SELECT * WHERE {
     *   <http://s> <http://p1> ?a
     *   OPTIONAL { <http://s> <http://p1> ?b }
     *   <http://s> <http://p2> ?b             
     * }
     * </pre>
     * 
     * with data
     * 
     * <pre>
     * <http://s> <http://p1> 1 .
     * <http://s> <http://p2> 2 .
     * </pre>
     * 
     * must yield the empty result (i.e., the OPTIONAL is evaluated prior to
     * the last triple pattern).
     */
    public void testJoinOrderOptional01() throws Exception {

        new TestHelper("join-order-optional-01", // testURI,
                "join-order-optional-01.rq",// queryFileURL
                "join-order-optional-01.ttl",// dataFileURL
                "join-order-optional-01.srx"// resultFileURL
        ).runTest();

    }
    
    /**
     * Query
     * 
     * <pre>
     * SELECT * WHERE {
     *   <http://s> <http://p2> ?b             
     *   OPTIONAL { <http://s> <http://p1> ?b }
     *   <http://s> <http://p1> ?a
     * }
     * </pre>
     * 
     * with data
     * 
     * <pre>
     * <http://s> <http://p1> 1 .
     * <http://s> <http://p2> 2 .
     * </pre>
     * 
     * must yield the result { ?a -> 1, b -> 2 }
     */
    public void testJoinOrderOptional02() throws Exception {

        new TestHelper("join-order-optional-02", // testURI,
                "join-order-optional-02.rq",// queryFileURL
                "join-order-optional-02.ttl",// dataFileURL
                "join-order-optional-02.srx"// resultFileURL
        ).runTest();

    }
    
}
