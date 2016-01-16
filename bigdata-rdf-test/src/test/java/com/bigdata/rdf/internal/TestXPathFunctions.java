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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.internal;

import com.bigdata.rdf.sparql.ast.FunctionRegistry;

import junit.framework.TestCase2;

/**
 * Test suite for various xpath numeric functions.
 * 
 * <strong>The xpath functions are very picky. Check the specs when you work on
 * this test suite!</strong>
 * 
 * @see http://www.w3.org/TR/xpath-functions/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestXPathFunctions extends TestCase2 {

    /**
     * 
     */
    public TestXPathFunctions() {
    }

    /**
     * @param name
     */
    public TestXPathFunctions(String name) {
        super(name);
    }

    /**
     * Unit test for {@link FunctionRegistry#ABS}. 
     * 
     * @see http://www.w3.org/TR/xpath-functions/#func-abs
     * @see http://www.w3.org/TR/xquery-semantics/#sec_fn_abs_ceil_floor_round
     */
    public void test_xpath_abs_01() {
//        fail("write test");
    	// covered in TCK
    }
    
    /**
     * Unit test for {@link FunctionRegistry#CEIL}. 
     * 
     * @see http://www.w3.org/TR/xpath-functions/#func-ceiling
     * @see http://www.w3.org/TR/xquery-semantics/#sec_fn_abs_ceil_floor_round
     */
    public void test_xpath_ceiling_01() {
//        fail("write test");
    	// covered in TCK
    }

    /**
     * Unit test for {@link FunctionRegistry#FLOOR}. 
     * 
     * @see http://www.w3.org/TR/xpath-functions/#func-floor
     * @see http://www.w3.org/TR/xquery-semantics/#sec_fn_abs_ceil_floor_round
     */
    public void test_xpath_floor_01() {
//        fail("write test");
    	// covered in TCK
    }

    /**
     * Unit test for {@link FunctionRegistry#round}. 
     * 
     * @see http://www.w3.org/TR/xpath-functions/#func-round
     * @see http://www.w3.org/TR/xquery-semantics/#sec_fn_abs_ceil_floor_round
     */
    public void test_xpath_round_01() {
//        fail("write test");
    	// covered in TCK
    }

}
