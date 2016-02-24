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
 * Created on Sep 29, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for tickets at <href a="http://sourceforge.net/apps/trac/bigdata">
 * trac </a>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
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
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071h() throws Exception {
       new TestHelper("ticket_1071h",// testURI,
             "ticket_1071h.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071h.srx"// resultFileURL
       ).runTest();
    }     
    
    /**
     * BIND + UNION + OPTIONAL combination fails, 
     * as reported in bug #1071 (associated test)
     * 
     * @throws Exception
     */
    public void test_ticket_1071i() throws Exception {
       new TestHelper("ticket_1071i",// testURI,
             "ticket_1071i.rq",// queryFileURL
             "ticket_1071.trig",// dataFileURL
             "ticket_1071i.srx"// resultFileURL
       ).runTest();
    }     
    
    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835">
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
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835">
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
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835">
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
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/835">
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
    
    /**
     * Covering GRAPH ?x {}
     * 
     * @see <a href="http://trac.bigdata.com/ticket/709">
     * select ?g { Graph ?g {} } incorrect</a> 
     * @see <a href="http://trac.bigdata.com/ticket/429">
     * Optimization for GRAPH uri {} and GRAPH ?foo {}</a>.
     */
    public void test_ticket_709() throws Exception {
       new TestHelper("ticket_709",// testURI,
             "ticket_709.rq",// queryFileURL
             "ticket_709.trig",// dataFileURL
             "ticket_709.srx"// resultFileURL
       ).runTest();    
    } 

    /**
     * Covering GRAPH <uri> {} with in dictionary existing and matching URI
     * 
     * @see <a href="http://trac.bigdata.com/ticket/429">
     * Optimization for GRAPH uri {} and GRAPH ?foo {}</a>.
     */
    public void test_ticket_429a() throws Exception {
       new TestHelper("ticket_429a",// testURI,
             "ticket_429a.rq",// queryFileURL
             "ticket_429.trig",// dataFileURL
             "ticket_429a.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * Covering GRAPH <uri> {} with non-existing and (thus) non-matching URI
     *
     * @see <a href="http://trac.bigdata.com/ticket/429">
     * Optimization for GRAPH uri {} and GRAPH ?foo {}</a>.
     */
    public void test_ticket_429b() throws Exception {
       new TestHelper("ticket_429b",// testURI,
             "ticket_429b.rq",// queryFileURL
             "ticket_429.trig",// dataFileURL
             "ticket_429b.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * Covering GRAPH <uri> {} with in dictionary existing but non-matching URI
     *
     * @see <a href="http://trac.bigdata.com/ticket/429">
     * Optimization for GRAPH uri {} and GRAPH ?foo {}</a>.
     */
    public void test_ticket_429c() throws Exception {
       new TestHelper("ticket_429c",// testURI,
             "ticket_429c.rq",// queryFileURL
             "ticket_429.trig",// dataFileURL
             "ticket_429b.srx"// resultFileURL (not matching: reuse 429b)
       ).runTest();    
    } 
    
    /**
     * Nested OPTIONAL-BIND construct
     * 
     * @throws Exception
     */
    public void test_ticket_933a() throws Exception {
       new TestHelper("ticket_933a",// testURI,
             "ticket_933a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_933ac.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * Nested OPTIONAL-BIND construct, advanced
     * 
     * @throws Exception
     */
    public void test_ticket_933b() throws Exception {
       new TestHelper("ticket_933b",// testURI,
             "ticket_933b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_933bd.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * Similiar to 933a, but with statement patterns instead of BIND clause.
     * 
     * @throws Exception
     */
    public void test_ticket_933c() throws Exception {
       new TestHelper("ticket_933c",// testURI,
             "ticket_933c.rq",// queryFileURL
             "ticket_933cd.trig",// dataFileURL
             "ticket_933ac.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * Similiar to 933b, but with statement patterns instead of BIND clause.
     * 
     * @throws Exception
     */
    public void test_ticket_933d() throws Exception {
       new TestHelper("ticket_933d",// testURI,
             "ticket_933d.rq",// queryFileURL
             "ticket_933cd.trig",// dataFileURL
             "ticket_933bd.srx"// resultFileURL
       ).runTest();    
    } 

    /**
     * Optional translation approach issues mentioned in ticket #933.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/801">
     * Adding Optional removes solutions</a>.
     */
    public void test_ticket_933e() throws Exception {
       new TestHelper("ticket_933e",// testURI,
             "ticket_933e.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_933e.srx"// resultFileURL
       ).runTest();    
    } 
    
    /**
     * {@link NotMaterializedException} in combination with LET expressions.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1331">
     * Duplicate LET expression leading to NotMaterializedException</a>. 
     */
    public void test_ticket_blzg_1331a() throws Exception {
       new TestHelper("ticket_blzg_1331a",// testURI,
             "ticket_blzg_1331a.rq",// queryFileURL
             "ticket_blzg_1331.trig",// dataFileURL
             "ticket_blzg_1331a.srx"// resultFileURL
       ).runTest();    
    }
    
    /**
     * {@link NotMaterializedException} in combination with LET expressions.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1331">
     * Duplicate LET expression leading to NotMaterializedException</a>. 
     */
    public void test_ticket_blzg_1331b() throws Exception {
       new TestHelper("ticket_blzg_1331b",// testURI,
             "ticket_blzg_1331b.rq",// queryFileURL
             "ticket_blzg_1331.trig",// dataFileURL
             "ticket_blzg_1331b.srx"// resultFileURL
       ).runTest();    
    }
    
    /**
     * Double nesting of FILTER NOT EXISTS.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1281">
     * FILTER FILTER != not working</a>
     */
    public void test_ticket_blzg_1281a() throws Exception {
       new TestHelper("ticket_blzg_1281a",// testURI,
             "ticket_blzg_1281a.rq",// queryFileURL
             "ticket_blzg_1281a.trig",// dataFileURL
             "ticket_blzg_1281a.srx"// resultFileURL
       ).runTest();    

    }
    
    /**
     * Double nesting of FILTER NOT EXISTS.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1281">
     * FILTER FILTER != not working</a>
     */
    public void test_ticket_blzg_1281b() throws Exception {
       new TestHelper("ticket_blzg_1281b",// testURI,
             "ticket_blzg_1281b.rq",// queryFileURL
             "ticket_blzg_1281b.trig",// dataFileURL
             "ticket_blzg_1281b.srx"// resultFileURL
       ).runTest();    

    }

    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346a() throws Exception {

       new TestHelper("ticket_bg1346a",// testURI,
               "ticket_bg1346a.rq",// queryFileURL
               "ticket_bg1346.trig",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }

    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346b() throws Exception {

       new TestHelper("ticket_bg1346b",// testURI,
               "ticket_bg1346b.rq",// queryFileURL
               "ticket_bg1346.trig",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }
    
    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346c() throws Exception {

       new TestHelper("ticket_bg1346c",// testURI,
               "ticket_bg1346c.rq",// queryFileURL
               "ticket_bg1346.trig",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }
    
    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346d() throws Exception {

       new TestHelper("ticket_bg1346d",// testURI,
               "ticket_bg1346d.rq",// queryFileURL
               "ticket_bg1346.ttl",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }
    
    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346e() throws Exception {

       new TestHelper("ticket_bg1346e",// testURI,
               "ticket_bg1346e.rq",// queryFileURL
               "ticket_bg1346.ttl",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }
    
    /**
     * DistinctTermScanOp is not retrieving all data.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1346">
     * DistinctTermScanOp is not retrieving all data</a>
     */
    public void test_ticket_1346f() throws Exception {

       new TestHelper("ticket_bg1346f",// testURI,
               "ticket_bg1346f.rq",// queryFileURL
               "ticket_bg1346.ttl",// dataFileURL
               "ticket_bg1346.srx"// resultFileURL
               ).runTest();
    }
    
    /**
     * Placement of filters in presence of other FILTER NOT EXISTS
     * clauses.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1284">
     * optional / filter ! bound interaction malfunction</a>
     */
    public void test_ticket_blzg_1284a() throws Exception {
       new TestHelper("ticket_blzg_1284a",// testURI,
             "ticket_blzg_1284a.rq",// queryFileURL
             "ticket_blzg_1284.trig",// dataFileURL
             "ticket_blzg_1284a.srx"// resultFileURL
       ).runTest();
    }    
    
    /**
     * Placement of filters in presence of other FILTER NOT EXISTS
     * clauses.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-1284">
     * optional / filter ! bound interaction malfunction</a>
     */
    public void test_ticket_blzg_1284b() throws Exception {
       new TestHelper("ticket_blzg_1284b",// testURI,
             "ticket_blzg_1284b.rq",// queryFileURL
             "ticket_blzg_1284.trig",// dataFileURL
             "ticket_blzg_1284b.srx"// resultFileURL
       ).runTest();
    }   
    
    /**
     * Unsound translation of FILTER (NOT) EXISTS.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021a() throws Exception {
       new TestHelper("ticket_blzg_1021a",// testURI,
             "ticket_blzg_1021a.rq",// queryFileURL
             "ticket_blzg_1021.trig",// dataFileURL
             "ticket_blzg_1021.srx"// resultFileURL
       ).runTest();
    }   
    
    /**
     * Unsound translation of FILTER (NOT) EXISTS.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021b() throws Exception {
       new TestHelper("ticket_blzg_1021b",// testURI,
             "ticket_blzg_1021b.rq",// queryFileURL
             "ticket_blzg_1021.trig",// dataFileURL
             "ticket_blzg_1021.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Unsound translation of FILTER (NOT) EXISTS.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021c() throws Exception {
       new TestHelper("ticket_blzg_1021c",// testURI,
             "ticket_blzg_1021c.rq",// queryFileURL
             "ticket_blzg_1021.trig",// dataFileURL
             "ticket_blzg_1021.srx"// resultFileURL
       ).runTest();
    }   
    
    /**
     * Unsound translation of FILTER (NOT) EXISTS.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021d() throws Exception {
       new TestHelper("ticket_blzg_1021d",// testURI,
             "ticket_blzg_1021d.rq",// queryFileURL
             "ticket_blzg_1021.trig",// dataFileURL
             "ticket_blzg_1021.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021e() throws Exception {
       new TestHelper("ticket_blzg_1021e",// testURI,
             "ticket_blzg_1021e.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021f() throws Exception {
       new TestHelper("ticket_blzg_1021f",// testURI,
             "ticket_blzg_1021f.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021g() throws Exception {
       new TestHelper("ticket_blzg_1021g",// testURI,
             "ticket_blzg_1021g.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021gh.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021h() throws Exception {
       new TestHelper("ticket_blzg_1021h",// testURI,
             "ticket_blzg_1021h.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021gh.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021i() throws Exception {
       new TestHelper("ticket_blzg_1021i",// testURI,
             "ticket_blzg_1021i.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021j() throws Exception {
       new TestHelper("ticket_blzg_1021j",// testURI,
             "ticket_blzg_1021j.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021k() throws Exception {
       new TestHelper("ticket_blzg_1021k",// testURI,
             "ticket_blzg_1021k.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Translation of complex FILTER expressions.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1021">
     * optimizer = None and FILTER EXISTS problem</a>
     */
    public void test_ticket_blzg_1021l() throws Exception {
       new TestHelper("ticket_blzg_1021l",// testURI,
             "ticket_blzg_1021l.rq",// queryFileURL
             "ticket_blzg_1021efgh.trig",// dataFileURL
             "ticket_blzg_1021ef.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * Filter Not Exists RC1 Broken.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1380">
     * Filter Not Exists RC1 Broken</a>
     */
    public void test_ticket_blzg_1380() throws Exception {
       new TestHelper("ticket_blzg_1380",// testURI,
             "ticket_blzg_1380.rq",// queryFileURL
             "ticket_blzg_1380.trig",// dataFileURL
             "ticket_blzg_1380.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1300">
     * SUM(DISTINCT $a) does not take DISTINCT into account</a>
     */
    public void test_ticket_blzg_1300() throws Exception {
       new TestHelper("ticket_blzg_1300",// testURI,
             "ticket_blzg_1300.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_blzg_1300.srx"// resultFileURL
       ).runTest();
    }    
    
      /**
     * * does not include var only set in values
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1113">
     * * does not include var only set in values</a>
     */
    public void test_ticket_blzg_1113a() throws Exception {
       new TestHelper("ticket_blzg_1113a",// testURI,
             "ticket_blzg_1113a.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_blzg_1113.srx"// resultFileURL
       ).runTest();
    }
 
    /**
     * * does not include var only set in values
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1113">
     * * does not include var only set in values</a>
     */
    public void test_ticket_blzg_1113b() throws Exception {
       new TestHelper("ticket_blzg_1113b",// testURI,
             "ticket_blzg_1113b.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_blzg_1113.srx"// resultFileURL
       ).runTest();
    }
    
    /**
     * * does not include var only set in values
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1113">
     * * does not include var only set in values</a>
     */
    public void test_ticket_blzg_1113c() throws Exception {
       new TestHelper("ticket_blzg_1113c",// testURI,
             "ticket_blzg_1113c.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_blzg_1113.srx"// resultFileURL
       ).runTest();
    }

    /**
     * * does not include var only set in values
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1113">
     * * does not include var only set in values</a>
     */
    public void test_ticket_blzg_1113d() throws Exception {
       new TestHelper("ticket_blzg_1113d",// testURI,
             "ticket_blzg_1113d.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_blzg_1113.srx"// resultFileURL
       ).runTest();
    }
    
    public void test_ticket_blzg_1475a() throws Exception {
       new TestHelper(
          "ticket_blzg_1475a",// testURI,
          "ticket_blzg_1475a.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-noresult.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_ticket_blzg_1475b() throws Exception {
       new TestHelper(
          "ticket_blzg_1475b",// testURI,
          "ticket_blzg_1475b.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-noresult.srx"// resultFileURL
       ).runTest();       
    }

    public void test_ticket_blzg_1475c() throws Exception {
       new TestHelper(
          "ticket_blzg_1475c",// testURI,
          "ticket_blzg_1475c.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-result.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_ticket_blzg_1475d() throws Exception {
       new TestHelper(
          "ticket_blzg_1475d",// testURI,
          "ticket_blzg_1475d.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-noresult.srx"// resultFileURL
       ).runTest();       
    }

    public void test_ticket_blzg_1475e() throws Exception {
       new TestHelper(
          "ticket_blzg_1475e",// testURI,
          "ticket_blzg_1475e.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-result.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_ticket_blzg_1475f() throws Exception {
       new TestHelper(
          "ticket_blzg_1475f",// testURI,
          "ticket_blzg_1475f.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-result.srx"// resultFileURL
       ).runTest();       
    }

    public void test_ticket_blzg_1475g() throws Exception {
       new TestHelper(
          "ticket_blzg_1475g",// testURI,
          "ticket_blzg_1475g.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-result.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_ticket_blzg_1475h() throws Exception {
       new TestHelper(
          "ticket_blzg_1475h",// testURI,
          "ticket_blzg_1475h.rq",// queryFileURL
          "ticket_blzg_1475.trig",// dataFileURL
          "ticket_blzg_1475-result.srx"// resultFileURL
       ).runTest();       
    }
    
    
    public void test_ticket_blzg_1493() throws Exception {
       new TestHelper(
          "ticket_blzg_1493",// testURI,
          "ticket_blzg_1493.rq",// queryFileURL
          "ticket_blzg_1493.trig",// dataFileURL
          "ticket_blzg_1493.srx"// resultFileURL
       ).runTest();       
    }
    
    public void test_ticket_blzg_1494a() throws Exception {
        new TestHelper(
           "ticket_blzg_1494a",// testURI,
           "ticket_blzg_1494a.rq",// queryFileURL
           "ticket_blzg_1494.trig",// dataFileURL
           "ticket_blzg_1494.srx"// resultFileURL
        ).runTest();       
     }
    

    public void test_ticket_blzg_1494b() throws Exception {
        new TestHelper(
           "ticket_blzg_1494b",// testURI,
           "ticket_blzg_1494b.rq",// queryFileURL
           "ticket_blzg_1494.trig",// dataFileURL
           "ticket_blzg_1494.srx"// resultFileURL
        ).runTest();       
     }
    
    public void test_ticket_blzg_1495() throws Exception {
        new TestHelper(
           "ticket_blzg_1495",// testURI,
           "ticket_blzg_1495.rq",// queryFileURL
           "ticket_blzg_1495.trig",// dataFileURL
           "ticket_blzg_1495.srx"// resultFileURL
        ).runTest();       
     }

    /**
     * Query having *no* bottom-up issues.
     * 
     * @throws Exception
     */
    public void test_ticket_1463a() throws Exception {
       new TestHelper(
             "ticket_bg1463a",// testURI,
             "ticket_bg1463a.rq",// queryFileURL
             "ticket_bg1463.trig",// dataFileURL
             "ticket_bg1463a.srx"// resultFileURL
          ).runTest();
    }
    
    /**
     * Query having bottom-up issues.
     * 
     * @throws Exception
     */
    public void test_ticket_1463b() throws Exception {
       new TestHelper(
             "ticket_bg1463b",// testURI,
             "ticket_bg1463b.rq",// queryFileURL
             "ticket_bg1463.trig",// dataFileURL
             "ticket_bg1463b.srx"// resultFileURL
          ).runTest();       
    }
    
    /**
     * Same as 1463a, just nested into subquery.
     * 
     * @throws Exception
     */
    public void test_ticket_1463c() throws Exception {
       new TestHelper(
             "ticket_bg1463c",// testURI,
             "ticket_bg1463c.rq",// queryFileURL
             "ticket_bg1463.trig",// dataFileURL
             "ticket_bg1463c.srx"// resultFileURL
          ).runTest();   
    }

    /**
     * Same as 1463b, just nested into subquery.
     * 
     * @throws Exception
     */
    public void test_ticket_1463d() throws Exception {
       new TestHelper(
             "ticket_bg1463d",// testURI,
             "ticket_bg1463d.rq",// queryFileURL
             "ticket_bg1463.trig",// dataFileURL
             "ticket_bg1463d.srx"// resultFileURL
          ).runTest();   
    }

    /**
     * Ticket 1627: minus fails when preceded by property path.
     * 
     * @throws Exception
     */
    public void test_ticket_1627a() throws Exception {
       new TestHelper(
             "ticket_bg1627a",// testURI,
             "ticket_bg1627a.rq",// queryFileURL
             "ticket_bg1627.trig",// dataFileURL
             "ticket_bg1627a.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1627b: minus fails when preceded by property path.
     * Variant of the query where MINUS is indeed eliminated.
     * 
     * @throws Exception
     */
    public void test_ticket_1627b() throws Exception {
       new TestHelper(
             "ticket_bg1627b",// testURI,
             "ticket_bg1627b.rq",// queryFileURL
             "ticket_bg1627.trig",// dataFileURL
             "ticket_bg1627b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1627c: minus fails when preceded by property path
     * - variant of 1627a with different style of property path.
     * 
     * @throws Exception
     */
    public void test_ticket_1627c() throws Exception {
       new TestHelper(
             "ticket_bg1627c",// testURI,
             "ticket_bg1627c.rq",// queryFileURL
             "ticket_bg1627.trig",// dataFileURL
             "ticket_bg1627c.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1627d: minus fails when preceded by property path
     * - variant of 1627d with different style of property path.
     * 
     * @throws Exception
     */
    public void test_ticket_1627d() throws Exception {
       new TestHelper(
             "ticket_bg1627d",// testURI,
             "ticket_bg1627d.rq",// queryFileURL
             "ticket_bg1627.trig",// dataFileURL
             "ticket_bg1627d.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1498: Query hint optimizer:None ignored for property
     * path queries. This is related to ticket 1627, caused
     * by appending property path decompositions at the end. It
     * has been fixed along the lines with the previous one.
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void test_ticket_1498() throws Exception {
       final ASTContainer container = new TestHelper(
             "ticket_bg1498",// testURI,
             "ticket_bg1498.rq",// queryFileURL
             "ticket_bg1498.trig",// dataFileURL
             "ticket_bg1498.srx"// resultFileURL
          ).runTest();   
       
       /**
        * We assert that, in the optimized AST and given the input
        * 
        * SELECT (count(*) as ?c)
        * WHERE {
        *   hint:Query hint:optimizer "None" .
        *   ?s <http://p1>/<http://p2> "A" . 
        *   ?s rdf:type <http://T> . 
        * }
        * 
        * the triples pattern ?s rdf:type <http://T> is still at
        * the last position, i.e. the optimizer hint is considered.
        */
       final GraphPatternGroup<IGroupMemberNode> whereClause = 
           container.getOptimizedAST().getWhereClause();
       final BOp lastInGroup = whereClause.get(whereClause.arity()-1);
       
       final StatementPatternNode sp = (StatementPatternNode)lastInGroup;
       final VarNode subj = (VarNode)sp.get(0);
       final ConstantNode pred = (ConstantNode)sp.get(1);
       final ConstantNode obj = (ConstantNode)sp.get(2);
       
       // the statement pattern node is the only node (?s, <const>, <const>),
       // so the checks below are sufficient to verify our claim
       assertTrue(subj.getValueExpression().getName().equals("s"));
       assertTrue(pred.isConstant());
       assertTrue(obj.isConstant());
    }
    
    /**
     * Ticket 1648: proper handling of join constraints in merge join.
     * 
     * @throws Exception
     */
    public void test_ticket_1648a() throws Exception {
       new TestHelper(
             "ticket_bg1648a",// testURI,
             "ticket_bg1648a.rq",// queryFileURL
             "ticket_bg1648.trig",// dataFileURL
             "ticket_bg1648.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1648: proper handling of join constraints in merge join.
     * 
     * @throws Exception
     */
    public void test_ticket_1648b() throws Exception {
       new TestHelper(
             "ticket_bg1648b",// testURI,
             "ticket_bg1648b.rq",// queryFileURL
             "ticket_bg1648.trig",// dataFileURL
             "ticket_bg1648.srx"// resultFileURL
       ).runTest();
    }

    /*
     * Ticket 1524: MINUS being ignored.
     * 
     * @throws Exception
     */
    public void test_ticket_1542a() throws Exception {
       new TestHelper(
             "ticket_bg1542a",// testURI,
             "ticket_bg1542a.rq",// queryFileURL
             "ticket_bg1542.trig",// dataFileURL
             "ticket_bg1542a.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1524: MINUS being ignored. Analogous test
     * case for OPTIONAL case.
     * 
     * @throws Exception
     */
    public void test_ticket_1542b() throws Exception {
       new TestHelper(
             "ticket_bg1542b",// testURI,
             "ticket_bg1542b.rq",// queryFileURL
             "ticket_bg1542.trig",// dataFileURL
             "ticket_bg1542b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (analytic mode).
     * 
     * @throws Exception
     */
    public void test_ticket_611a_analytic() throws Exception {
       new TestHelper(
             "ticket_bg611a_analytic",// testURI,
             "ticket_bg611a_analytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611a.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (non-analytic equivalent).
     * 
     * @throws Exception
     */
    public void test_ticket_611a_nonanalytic() throws Exception {
       new TestHelper(
             "ticket_bg611a_nonanalytic",// testURI,
             "ticket_bg611a_nonanalytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611a.srx"// resultFileURL
          ).runTest();   
    }
    
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (non-analytic equivalent).
     * 
     * @throws Exception
     */
    public void test_ticket_611b_nonanalytic() throws Exception {
       new TestHelper(
             "ticket_bg611b_nonanalytic",// testURI,
             "ticket_bg611b_nonanalytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (analytic mode).
     * 
     * @throws Exception
     */
    public void test_ticket_611b_analytic() throws Exception {
       new TestHelper(
             "ticket_bg611b_analytic",// testURI,
             "ticket_bg611b_analytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (non-analytic equivalent).
     * 
     * @throws Exception
     */
    public void test_ticket_611c_nonanalytic() throws Exception {
       new TestHelper(
             "ticket_bg611c_nonanalytic",// testURI,
             "ticket_bg611c_nonanalytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611c.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (analytic mode).
     * 
     * @throws Exception
     */
    public void test_ticket_611c_analytic() throws Exception {
       new TestHelper(
             "ticket_bg611c_analytic",// testURI,
             "ticket_bg611c_analytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611c.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (non-analytic equivalent).
     * 
     * @throws Exception
     */
    public void test_ticket_611d_nonanalytic() throws Exception {
       new TestHelper(
             "ticket_bg611d_nonanalytic",// testURI,
             "ticket_bg611d_nonanalytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611d.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (analytic mode).
     * 
     * @throws Exception
     */
    public void test_ticket_611d_analytic() throws Exception {
       new TestHelper(
             "ticket_bg611d_analytic",// testURI,
             "ticket_bg611d_analytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611d.srx"// resultFileURL
          ).runTest();   
    }
    
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (non-analytic equivalent).
     * 
     * @throws Exception
     */
    public void test_ticket_611e_nonanalytic() throws Exception {
       new TestHelper(
             "ticket_bg611e_nonanalytic",// testURI,
             "ticket_bg611e_nonanalytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611e.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 611: Mock IV / TermId hashCode()/equals() problems
     * (analytic mode).
     * 
     * @throws Exception
     */
    public void test_ticket_611e_analytic() throws Exception {
       new TestHelper(
             "ticket_bg611e_analytic",// testURI,
             "ticket_bg611e_analytic.rq",// queryFileURL
             "empty.trig",// dataFileURL
             "ticket_bg611e.srx"// resultFileURL
          ).runTest();   
    }

    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * with literal in object position (analytic version). Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643a_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643a_analytic",// testURI,
             "ticket_bg1643a_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643a.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service,
     * with literal in object position (non-analytic equivalent). Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643a_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643a_nonanalytic",// testURI,
           "ticket_bg1643a_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643a.srx"// resultFileURL
          ).runTest();   
    }
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * with URI in object position (analytic version). Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643b_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643b_analytic",// testURI,
             "ticket_bg1643b_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service,
     * with URI in object position (non-analytic equivalent). Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643b_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643b_nonanalytic",// testURI,
           "ticket_bg1643b_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643b.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (analytic mode). Version with MINUS. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643c_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643c_analytic",// testURI,
             "ticket_bg1643c_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643c.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (non-analytic equivalent). Version with MINUS. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643c_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643c_nonanalytic",// testURI,
           "ticket_bg1643c_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643c.srx"// resultFileURL
          ).runTest();   
    }    
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (analytic mode). Version with URI where FILTER removes element. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643d_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643d_analytic",// testURI,
             "ticket_bg1643d_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643d.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (non-analytic equivalent).  Version with URI where FILTER removes element. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643d_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643d_nonanalytic",// testURI,
           "ticket_bg1643d_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643d.srx"// resultFileURL
          ).runTest();   
    }  
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (analytic mode).  Version with literal where FILTER removes element. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643e_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643e_analytic",// testURI,
             "ticket_bg1643e_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643e.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (non-analytic equivalent). Version with literal where FILTER removes element.  Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643e_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643e_nonanalytic",// testURI,
           "ticket_bg1643e_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643e.srx"// resultFileURL
          ).runTest();   
    }  
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (analytic mode).Version with literal where FILTER does not apply
     * because of datatype in literal. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643f_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643f_analytic",// testURI,
             "ticket_bg1643f_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643f.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (non-analytic equivalent). Version with literal where FILTER does not apply
     * because of datatype in literal.  Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643f_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643f_nonanalytic",// testURI,
           "ticket_bg1643f_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643f.srx"// resultFileURL
          ).runTest();   
    }  
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (analytic mode).Version with literal where FILTER applies
     * because of datatype in literal. Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643g_analytic() throws Exception {
       new TestHelper(
             "ticket_bg1643g_analytic",// testURI,
             "ticket_bg1643g_analytic.rq",// queryFileURL
             "ticket_bg1643.trig",// dataFileURL
             "ticket_bg1643g.srx"// resultFileURL
          ).runTest();   
    }
    
    /**
     * Ticket 1643: DELETE does not delete properly with Wikidata Query service
     * (non-analytic equivalent). Version with literal where FILTER applies
     * because of datatype in literal.  Related to BLZG-611.
     * 
     * @throws Exception
     */
    public void test_ticket_1643g_nonanalytic() throws Exception {
       new TestHelper(
           "ticket_bg1643g_nonanalytic",// testURI,
           "ticket_bg1643g_nonanalytic.rq",// queryFileURL
           "ticket_bg1643.trig",// dataFileURL
           "ticket_bg1643g.srx"// resultFileURL
          ).runTest();   
    }  
    
   /**
    * BLZG-1683: Required property relationName for HTreeDistinctBindingSetsOp
    * (analytic version).
    */
   public void test_ticket_1683a() throws Exception {
      new TestHelper(
          "ticket_bg1683a",// testURI,
          "ticket_bg1683a.rq",// queryFileURL
          "ticket_bg1683.trig",// dataFileURL
          "ticket_bg1683.srx"// resultFileURL
         ).runTest();   
   }
   
   /**
    * BLZG-1683: Required property relationName for HTreeDistinctBindingSetsOp
    * (non-analytic version).
    */
   public void test_ticket_1683b() throws Exception {
      new TestHelper(
          "ticket_bg1683b",// testURI,
          "ticket_bg1683b.rq",// queryFileURL
          "ticket_bg1683.trig",// dataFileURL
          "ticket_bg1683.srx"// resultFileURL
         ).runTest();   
   }
   
   /**
    * BLZG-852: MINUS and UNION.
    */
   public void test_ticket_852a() throws Exception {

      new TestHelper(
          "ticket_bg852a",// testURI,
          "ticket_bg852a.rq",// queryFileURL
          "empty.trig",// dataFileURL
          "ticket_bg852a.srx"// resultFileURL
         ).runTest();   
   }



   /**
    * BLZG-852: MINUS and UNION.
    */
   public void test_ticket_852b() throws Exception {

      new TestHelper(
          "ticket_bg852b",// testURI,
          "ticket_bg852b.rq",// queryFileURL
          "empty.trig",// dataFileURL
          "ticket_bg852b.srx"// resultFileURL
         ).runTest();   
   }
   
   /**
    * BLZG-1748: Regressions in date comparison
    */
   public void test_ticket_1748a() throws Exception {

       new TestHelper(
           "ticket_bg1748a",// testURI,
           "ticket_bg1748a.rq",// queryFileURL
           "ticket_bg1748.trig",// dataFileURL
           "ticket_bg1748.srx"// resultFileURL
          ).runTest();   
    }
   
   /**
    * BLZG-1748: Regressions in date comparison
    */
   public void test_ticket_1748b() throws Exception {

       new TestHelper(
           "ticket_bg1748b",// testURI,
           "ticket_bg1748b.rq",// queryFileURL
           "ticket_bg1748.trig",// dataFileURL
           "ticket_bg1748.srx"// resultFileURL
          ).runTest();   
    }

}
