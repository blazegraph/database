/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 16, 2012
 */
package com.bigdata.bop.join;

import com.bigdata.rdf.sparql.ast.eval.TestInclude;

import junit.framework.TestCase2;

/**
 * Test suite for low to no cardinality joins against a named solution set.
 * 
 * @author thompsonbry
 * 
 *         FIXME Verify that we are handling SELECT and CONSTRAINTS as well as
 *         the operator specific annotations (NAME, SPARQL_CACHE). Look at the
 *         existing test suites for hash joins for examples that we can setup
 *         here.
 * 
 *         FIXME Verify that the output of this join operator is order
 *         preserving (that could be done in a data driven unit test at the
 *         SPARQL layer for INCLUDE).
 */
public class TestNamedSolutionSetScanOp extends TestCase2 {

	public TestNamedSolutionSetScanOp() {
	}

	public TestNamedSolutionSetScanOp(String name) {
		super(name);
	}

	/**
	 * Note: There are some tests at the data-driven level.
	 * 
	 * @see TestInclude
	 */
	public void test_something() {
		fail("write tests");
	}
	
}
