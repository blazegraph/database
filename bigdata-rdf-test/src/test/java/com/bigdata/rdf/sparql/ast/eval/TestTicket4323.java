/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;
/*
 * Nov 24, 2016
 */

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

/**
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-4323">
 * hint:runFirst not considered for Subqueries and Named Subquery Includes</a>
 * 
 * @author <a href="mailto:ms@blazegraph.com">Michael Schmidt</a>
 */
public class TestTicket4323 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket4323() {
    }

    public TestTicket4323(String name) {
        super(name);
    }

    /** 
     * Test case asserting proper treatment of runFirst query hint for subquery.
     */
    public void test_ticket_4323a() throws Exception {
        
        final TestHelper h = 
        	new TestHelper(
        		"test_ticket_4323a", // testURI,
        		"test_ticket_4323a.rq",// queryFileURL
        		"test_ticket_4323.trig",// dataFileURL
        		"test_ticket_4323.srx"// resultFileURL
        	);
        
        // run the test, asserting that the produced result is correct
        final ASTContainer ast = h.runTest();
        
        // make sure that the first pattern in the where clause is a subquery,
        // as requested per the hint:Prior hint:runFirst "true" query hint
        final QueryRoot optimizedAst = ast.getOptimizedAST();
        final GraphPatternGroup<?> whereClause = optimizedAst.getWhereClause();
        final Object firstChild = whereClause.getChildren().get(0);
        assertTrue(firstChild instanceof SubqueryRoot);

    }
    
    /** 
     * Test case asserting proper treatment of runFirst query hint for complex
     * subquery, which is transformed into a named subquery include. Technically,
     * we just add a LIMIT to the subquery, to make sure it is translated as an NSI.
     */
    public void test_ticket_4323b() throws Exception {
        
        final TestHelper h = 
        	new TestHelper(
        		"test_ticket_4323b", // testURI,
        		"test_ticket_4323b.rq",// queryFileURL
        		"test_ticket_4323.trig",// dataFileURL
        		"test_ticket_4323.srx"// resultFileURL
        	);
        
        // run the test, asserting that the produced result is correct
        final ASTContainer ast = h.runTest();
        
        // make sure that the first pattern in the where clause is an NSI,
        // as requested per the hint:Prior hint:runFirst "true" query hint
        final QueryRoot optimizedAst = ast.getOptimizedAST();
        final GraphPatternGroup<?> whereClause = optimizedAst.getWhereClause();
        final Object firstChild = whereClause.getChildren().get(0);
        assertTrue(firstChild instanceof NamedSubqueryInclude);

    }
}
