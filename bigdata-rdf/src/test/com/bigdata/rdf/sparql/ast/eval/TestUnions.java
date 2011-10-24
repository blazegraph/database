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
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for UNION.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnions extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestUnions() {
    }

    /**
     * @param name
     */
    public TestUnions(String name) {
        super(name);
    }

    /**
     * <pre>
     * SELECT ?p ?o
     * WHERE { 
     *     {
     *         <http://www4.wiwiss.fu-berlin.de/dailymed/resource/drugs/1080> ?p ?o . 
     *     } UNION { 
     *         <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01254> ?p ?o . 
     *     } UNION { 
     *         <http://www4.wiwiss.fu-berlin.de/sider/resource/drugs/3062316> ?p ?o . 
     *     }
     * }
     * </pre>
     */
    public void test_union_01() throws Exception {

        new TestHelper("test_union_01").runTest();
        
        /*
         * FIXME Mike, please modify the data files to have some expected
         * results so it test the UNION execution as well (the data and results
         * files are both currently empty).
         */
        fail("Modify the data files to have some expected results.");

    }

    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Mike")
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Jane")
     * } }
     * </pre>
     * 
     * Note: This is a port of
     * {@link TestBigdataEvaluationStrategyImpl#test_union()}.
     * 
     * @throws Exception
     */
    public void test_union_02() throws Exception {

        new TestHelper("union_02").runTest();
        
    }
    
    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Mike")
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     * } }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_union_03() throws Exception {

        new TestHelper("union_03").runTest();
        
    }

    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Jane")
     * } }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_union_04() throws Exception {

        new TestHelper("union_04").runTest();
        
    }    
    
}
