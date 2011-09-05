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
 * Data driven test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBasicQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBasicQuery() {
    }

    /**
     * @param name
     */
    public TestBasicQuery(String name) {
        super(name);
    }

    /**
     * A simple SELECT query.
     */
    public void test_select() throws Exception {
        
        new TestHelper("select").runTest();
        
    }
    
    /**
     * A simple ASK query.
     */
    public void test_ask() throws Exception {

        new TestHelper("ask").runTest();
        
    }

    /**
     * A simple CONSTRUCT query.
     */
    public void test_construct() throws Exception {

        new TestHelper(
                "construct", // testURI,
                "construct.rq",// queryFileURL
                "construct.trig",// dataFileURL
                "construct-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A simple DESCRIBE query.
     */
    public void test_describe() throws Exception {

        new TestHelper(
                "describe", // testURI,
                "describe.rq",// queryFileURL
                "describe.trig",// dataFileURL
                "describe-result.trig"// resultFileURL
                ).runTest();
        
    }

}
