/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

/**
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 */
public class TestDistinctTermScanOptimizer extends
		AbstractDataDrivenSPARQLTestCase {

	public TestDistinctTermScanOptimizer() {
	}

	public TestDistinctTermScanOptimizer(String name) {
		super(name);
	}

	public void test_ticket_1035_a() throws Exception {
		
		fail("write tests");
		
	}
	
//	/**
//	 * <pre>
//	 * SELECT (COUNT(*) as ?count) {?s ?p ?o}
//	 * </pre>
//	 */
//	public void test_ticket_1037_a() throws Exception {
//
//		new TestHelper("ticket_1037_a", // testURI,
//				"ticket_1037_a.rq",// queryFileURL
//				"ticket_1037.trig",// dataFileURL
//				"ticket_1037.srx"// resultFileURL
//		).runTest();
//
//		// TODO Expand test coverage.
//		fail("Verify that the correct operators were used.");
//		
//	}

}
