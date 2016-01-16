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



public class TestPropertyPaths extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestPropertyPaths() {
    }

    /**
     * @param name
     */
    public TestPropertyPaths(String name) {
        super(name);
    }

//    private void property_path_test(String name) throws Exception {
//
//        new TestHelper(
//                "property-path-734-" + name,         // testURI,
//                "property-path-734-" + name + ".rq", // queryFileURL
//                "property-path-734.ttl",             // dataFileURL
//                "property-path-734.srx"              // resultFileURL,
//                ).runTest();
//    }
//
//    private void property_path_using_workaround_test(String name) throws Exception {
//
//        new TestHelper(
//                "property-path-734-B-" + name,         // testURI,
//                "property-path-734-B-" + name + ".rq", // queryFileURL
//                "property-path-734-B.ttl",             // dataFileURL
//                "property-path-734-B.srx"              // resultFileURL,
//                ).runTest();
//    }

    public void test_inVar_outConst_notBound() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-1.rq", 		// queryFileURL
                "property-paths-2.ttl",       // dataFileURL
                "property-paths-1.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outConst_inBound() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-1.rq", 		// queryFileURL
                "property-paths.ttl",       // dataFileURL
                "property-paths-1.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outVar_inBound() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-2.rq", 		// queryFileURL
                "property-paths.ttl",       // dataFileURL
                "property-paths-2.srx"        // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outVar_outBound() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-3.rq", 		// queryFileURL
                "property-paths-2.ttl",       // dataFileURL
                "property-paths-3.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outVar_bothBound() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-3.rq", 		// queryFileURL
                "property-paths.ttl",       // dataFileURL
                "property-paths-3.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inConst_outConst() throws Exception {

        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-4.rq", 		// queryFileURL
                "property-paths.ttl",       // dataFileURL
                "property-paths-3.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outVar_noSharedVars() throws Exception {
    	
        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-6.rq", 		// queryFileURL
                "property-paths.ttl",       // dataFileURL
                "property-paths-6.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_inVar_outVar_someSharedVars() throws Exception {
    	
        new TestHelper(
                "property-paths",         	// testURI,
                "property-paths-7.rq", 		// queryFileURL
                "property-paths-7.ttl",       // dataFileURL
                "property-paths-7.srx"      // resultFileURL,
                ).runTest();
        
    }
    
    public void test_cycle() throws Exception {

       new TestHelper(
             "property-paths8",            // testURI,
             "property-paths-8.rq",       // queryFileURL
             "property-paths-8.ttl",       // dataFileURL
             "property-paths-8.srx"      // resultFileURL,
             ).runTest();

    }
    
    public void test_multiplicity() throws Exception {
       
       new TestHelper(
             "property-paths9",            // testURI,
             "property-paths-9.rq",       // queryFileURL
             "property-paths-9.ttl",       // dataFileURL
             "property-paths-9.srx"      // resultFileURL,
             ).runTest();

    }
    
    
}
