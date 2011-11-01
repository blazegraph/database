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
     * A SELECT query consisting of a single statement pattern.
     */
    public void test_select_1() throws Exception {
        
        new TestHelper("select-1").runTest();
        
    }
    
    /**
     * A simple SELECT query.
     */
    public void test_select_2() throws Exception {
        
        new TestHelper("select-2").runTest();
        
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
     * A construct with told triples in the CONSTRUCT clause and no WHERE
     * clause.
     */
    public void test_construct_without_where_clause() throws Exception {
       
        new TestHelper(
                "construct-without-where-clause", // testURI,
                "construct-without-where-clause.rq",// queryFileURL
                "construct-without-where-clause.trig",// dataFileURL
                "construct-without-where-clause-result.trig"// resultFileURL
                ).runTest();
    }
    
    /**
     * A simple DESCRIBE query of a constant.
     */
    public void test_describe_1() throws Exception {

        new TestHelper(
                "describe-1", // testURI,
                "describe-1.rq",// queryFileURL
                "describe-1.trig",// dataFileURL
                "describe-1-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A simple DESCRIBE query of a variable with a where clause.
     */
    public void test_describe_2() throws Exception {

        new TestHelper(
                "describe-2", // testURI,
                "describe-2.rq",// queryFileURL
                "describe-2.trig",// dataFileURL
                "describe-2-result.trig"// resultFileURL
                ).runTest();
        
    }

    /**
     * A simple DESCRIBE query of a constant plus a variable with a where clause.
     */
    public void test_describe_3() throws Exception {

        new TestHelper(
                "describe-3", // testURI,
                "describe-3.rq",// queryFileURL
                "describe-3.trig",// dataFileURL
                "describe-3-result.trig"// resultFileURL
                ).runTest();
        
    }

    // Note: Was TestDescribe#testSingleDescribe()
    public void test_describe_4() throws Exception {

        new TestHelper(
                "describe-4", // testURI,
                "describe-4.rq",// queryFileURL
                "describe-4.trig",// dataFileURL
                "describe-4-result.trig"// resultFileURL
                ).runTest();
        
    }

    // Note: Was TestDescribe#testMultiDescribe()
    public void test_describe_5() throws Exception {

        new TestHelper(
                "describe-5", // testURI,
                "describe-5.rq",// queryFileURL
                "describe-5.trig",// dataFileURL
                "describe-5-result.trig"// resultFileURL
                ).runTest();
        
    }

}
