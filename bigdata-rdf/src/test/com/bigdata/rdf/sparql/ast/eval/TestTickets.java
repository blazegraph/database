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
 * Created on Sep 29, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for tickets at <href a="http://sourceforge.net/apps/trac/bigdata">
 * trac </a>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTickets extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestTickets() {
    }

    /**
     * @param name
     */
    public TestTickets(String name) {
        super(name);
    }

    /**
     * <pre>
     * SELECT * WHERE {{}}
     * </pre>
     * 
     * @throws Exception
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/384">
     *      IndexOutOfBoundsException during query evaluation </a>
     */
    public void test_ticket_384() throws Exception {

        new TestHelper("test_ticket_384").runTest();

    }
    

    public void test_ticket_739() throws Exception {

        new TestHelper("ticket739-optpp",// testURI,
                "ticket739-optpp.rq",// queryFileURL
                "ticket739-optpp.ttl",// dataFileURL
                "ticket739-optpp.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_739a() throws Exception {

        new TestHelper("ticket739A-optpp",// testURI,
                "ticket739A-optpp.rq",// queryFileURL
                "ticket739-optpp.ttl",// dataFileURL
                "ticket739-optpp.srx"// resultFileURL
                ).runTest();

    }



    public void test_ticket_739b() throws Exception {

        new TestHelper("ticket739B-optpp",// testURI,
                "ticket739B-optpp.rq",// queryFileURL
                "ticket739-optpp.ttl",// dataFileURL
                "ticket739-optpp.srx"// resultFileURL
                ).runTest();

    }

    public void test_ticket_739c() throws Exception {

        new TestHelper("ticket739B-optpp",// testURI,
                "ticket739C-optpp.rq",// queryFileURL
                "ticket739-optpp.ttl",// dataFileURL
                "ticket739-optpp.srx"// resultFileURL
                ).runTest();

    }

    public void test_ticket_739d() throws Exception {

        new TestHelper("ticket739D-optpp",// testURI,
                "ticket739D-optpp.rq",// queryFileURL
                "ticket739D-optpp.ttl",// dataFileURL
                "ticket739D-optpp.srx"// resultFileURL
                ).runTest();

    }
    public void test_ticket_739e() throws Exception {

        new TestHelper("ticket739E-optpp",// testURI,
                "ticket739E-optpp.rq",// queryFileURL
                "ticket739D-optpp.ttl",// dataFileURL
                "ticket739D-optpp.srx"// resultFileURL
                ).runTest();

    }
    public void test_ticket_747() throws Exception {

        new TestHelper("ticket747-bound",// testURI,
                "ticket747-bound.rq",// queryFileURL
                "ticket747-bound.ttl",// dataFileURL
                "ticket747-bound.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_747a() throws Exception {

        new TestHelper("ticket747A-bound",// testURI,
                "ticket747A-bound.rq",// queryFileURL
                "ticket747-bound.ttl",// dataFileURL
                "ticket747A-bound.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_747b() throws Exception {

        new TestHelper("ticket747B-bound",// testURI,
                "ticket747B-bound.rq",// queryFileURL
                "ticket747-bound.ttl",// dataFileURL
                "ticket747-bound.srx"// resultFileURL
                ).runTest();

    }

    public void test_ticket_747c() throws Exception {

        new TestHelper("ticket747-bound",// testURI,
                "ticket747C-bound.rq",// queryFileURL
                "ticket747-bound.ttl",// dataFileURL
                "ticket747-bound.srx"// resultFileURL
                ).runTest();

    }
    public void test_ticket_747d() throws Exception {

        new TestHelper("ticket747B-bound",// testURI,
                "ticket747D-bound.rq",// queryFileURL
                "ticket747-bound.ttl",// dataFileURL
                "ticket747-bound.srx"// resultFileURL
                ).runTest();

    }
    public void test_ticket_748() throws Exception {

        new TestHelper("ticket748-subselect",// testURI,
                "ticket748-subselect.rq",// queryFileURL
                "ticket748-subselect.ttl",// dataFileURL
                "ticket748-subselect.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_748a() throws Exception {

        new TestHelper("ticket748A-subselect",// testURI,
                "ticket748A-subselect.rq",// queryFileURL
                "ticket748-subselect.ttl",// dataFileURL
                "ticket748-subselect.srx"// resultFileURL
                ).runTest();

    }

    public void test_ticket_two_subselects_748() throws Exception {

        new TestHelper("ticket748-two-subselects",// testURI,
                "ticket748-two-subselects.rq",// queryFileURL
                "ticket748-two-subselects.ttl",// dataFileURL
                "ticket748-two-subselects.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_two_subselects_748a() throws Exception {

        new TestHelper("ticket748A-two-subselects",// testURI,
                "ticket748A-two-subselects.rq",// queryFileURL
                "ticket748-two-subselects.ttl",// dataFileURL
                "ticket748-two-subselects.srx"// resultFileURL
                ).runTest();

    }


    public void test_ticket_bad_projection_748() throws Exception {

        new TestHelper("ticket748-bad-projection",// testURI,
                "ticket748-bad-projection.rq",// queryFileURL
                "ticket748-bad-projection.ttl",// dataFileURL
                "ticket748-bad-projection.srx"// resultFileURL
                ).runTest();

    }
    /**
     * <pre>
     * PREFIX ex: <http://example.org/>
     * 
     * SELECT DISTINCT ?sub WHERE {
     *   ?sub ex:hasName ?name.
     * } order by DESC(?name)
     * </pre>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/563">
     *      DISTINCT ORDER BY</a>
     */
    public void test_ticket_563() throws Exception {

        new TestHelper("ticket563-DistinctOrderBy",// testURI,
                "ticket563-DistinctOrderBy.rq",// queryFileURL
                "ticket563-DistinctOrderBy.n3",// dataFileURL
                "ticket563-DistinctOrderBy.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }
   

    public void test_ticket_min736() throws Exception {

        new TestHelper("aggregate-min",// testURI,
                "aggregate-min.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-min.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    public void test_ticket_max736() throws Exception {

        new TestHelper("aggregate-max",// testURI,
                "aggregate-max.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-max.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    public void test_ticket_min736_1() throws Exception {

        new TestHelper("aggregate-min1",// testURI,
                "aggregate-min1.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-min1.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    public void test_ticket_max736_1() throws Exception {

        new TestHelper("aggregate-max1",// testURI,
                "aggregate-max1.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-max1.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    public void test_ticket_min736_2() throws Exception {

        new TestHelper("aggregate-min2",// testURI,
                "aggregate-min2.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-min2.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    public void test_ticket_max736_2() throws Exception {

        new TestHelper("aggregate-max2",// testURI,
                "aggregate-max2.rq",// queryFileURL
                "aggregate-min-max.ttl",// dataFileURL
                "aggregate-max2.srx",// resultFileURL
                true // checkOrder
        ).runTest();

    }

    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/806>
     *      Incorrect AST generated for OPTIONAL { SELECT }</a>
     */
    public void test_ticket_806() throws Exception {
        
        new TestHelper("ticket-806",// testURI,
                "ticket-806.rq",// queryFileURL
                "ticket-806.trig",// dataFileURL
                "ticket-806.srx",// resultFileURL
                false// checkOrder
        ).runTest();
        
    }

    public void test_ticket_765() throws Exception {
        new TestHelper("ticket-765",// testURI,
                "ticket-765.rq",// queryFileURL
                "ticket-765.trig",// dataFileURL
                "ticket-765.srx",// resultFileURL
                false // checkOrder (because only one solution)
        ).runTest();
    }
    
    
    /**
     * Original test case associated with ticket 832.
     * 
     * @throws Exception
     */
    public void test_ticket_832a() throws Exception {
       new TestHelper("ticket_832a",// testURI,
             "ticket_832a.rq",// queryFileURL
             "ticket_832a.trig",// dataFileURL
             "ticket_832a.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Propagation of named graph specification inside subqueries,
     * simple one level propagation.
     * 
     * @throws Exception
     */
    public void test_ticket_832b() throws Exception {
       new TestHelper("ticket_832b",// testURI,
             "ticket_832b.rq",// queryFileURL
             "ticket_832b.trig",// dataFileURL
             "ticket_832b.srx"// resultFileURL
       ).runTest();
    }

    /**
     * Propagation of named graph specifications inside subqueries,
     * advanced two-level propagation.
     * 
     * @throws Exception
     */
    public void test_ticket_832c() throws Exception {
       new TestHelper("ticket_832c",// testURI,
             "ticket_832c.rq",// queryFileURL
             "ticket_832c.trig",// dataFileURL
             "ticket_832c.srx"// resultFileURL
       ).runTest();
    }

    /**
     * Propagation of named graph specifications inside FILTER NOT EXISTS
     * clauses, as reported in bug #792/#888
     * 
     * @throws Exception
     */
    public void test_ticket_792a() throws Exception {
       new TestHelper("ticket_792a",// testURI,
             "ticket_792a.rq",// queryFileURL
             "ticket_792.trig",// dataFileURL
             "ticket_792a.srx"// resultFileURL
       ).runTest();
    }

    /**
     * Propagation of named graph specifications inside FILTER NOT EXISTS
     * clauses, as reported in bug #792/#888 (inverse test)
     * 
     * @throws Exception
     */
    public void test_ticket_792b() throws Exception {
       new TestHelper("ticket_792b",// testURI,
             "ticket_792b.rq",// queryFileURL
             "ticket_792.trig",// dataFileURL
             "ticket_792b.srx"// resultFileURL
       ).runTest();
    }

    /**
     * Propagation of named graph specifications inside FILTER EXISTS
     * clauses, as reported in bug #792/#888 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_792c() throws Exception {
       new TestHelper("ticket_792c",// testURI,
             "ticket_792c.rq",// queryFileURL
             "ticket_792.trig",// dataFileURL
             "ticket_792c.srx"// resultFileURL
       ).runTest();
    }

    /**
     * Propagation of named graph specifications inside FILTER EXISTS
     * clauses, as reported in bug #792/#888 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_792d() throws Exception {
       new TestHelper("ticket_792d",// testURI,
             "ticket_792d.rq",// queryFileURL
             "ticket_792.trig",// dataFileURL
             "ticket_792d.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071a() throws Exception {
       new TestHelper("ticket_1071a",// testURI,
             "ticket_1071a.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071a.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071b() throws Exception {
       new TestHelper("ticket_1071b",// testURI,
             "ticket_1071b.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071b.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071c() throws Exception {
       new TestHelper("ticket_1071c",// testURI,
             "ticket_1071c.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071c.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071d() throws Exception {
       new TestHelper("ticket_1071d",// testURI,
             "ticket_1071d.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071d.srx"// resultFileURL
       ).runTest();
    } 
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071e() throws Exception {
       new TestHelper("ticket_1071e",// testURI,
             "ticket_1071e.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071e.srx"// resultFileURL
       ).runTest();
    } 
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071f() throws Exception {
       new TestHelper("ticket_1071f",// testURI,
             "ticket_1071f.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071f.srx"// resultFileURL
       ).runTest();
    } 
        
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071g() throws Exception {
       new TestHelper("ticket_1071g",// testURI,
             "ticket_1071g.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071g.srx"// resultFileURL
       ).runTest();
    } 

    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835>
     * Query solutions are duplicated and increase by adding graph patterns</a>
     */
    public void test_ticket_835a() throws Exception {
       new TestHelper("ticket_835a",// testURI,
             "ticket_835a.rq",// queryFileURL
             "ticket_835.trig",// dataFileURL
             "ticket_835.srx"// resultFileURL
       ).runTest();       
    }
    
    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835>
     * Query solutions are duplicated and increase by adding graph patterns</a>
     */
    public void test_ticket_835b() throws Exception {
       new TestHelper("ticket_835b",// testURI,
             "ticket_835b.rq",// queryFileURL
             "ticket_835.trig",// dataFileURL
             "ticket_835.srx"// resultFileURL
       ).runTest();    
    }

    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835>
     * Query solutions are duplicated and increase by adding graph patterns</a>
     */
    public void test_ticket_835c() throws Exception {
       new TestHelper("ticket_835c",// testURI,
             "ticket_835c.rq",// queryFileURL
             "ticket_835.trig",// dataFileURL
             "ticket_835.srx"// resultFileURL
       ).runTest();    
    }
    
    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835>
     * Query solutions are duplicated and increase by adding graph patterns</a>.
     * Related test case using a complex join group instead of subquery.
     */
    public void test_ticket_835d() throws Exception {
       new TestHelper("ticket_835d",// testURI,
             "ticket_835d.rq",// queryFileURL
             "ticket_835.trig",// dataFileURL
             "ticket_835.srx"// resultFileURL
       ).runTest();    
    }

}