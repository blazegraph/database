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
 * Created on Oct 31, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for FILTER evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFilters extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestFilters() {
    }

    /**
     * @param name
     */
    public TestFilters(String name) {
        super(name);
    }

    /**
     * <pre>
     * select ?s ?p
     * where { 
     *   ?s rdf:type foaf:Person .
     *   ?s ?p "The Label" .
     *   FILTER(?p = rdfs:label || 
     *          ?p = rdfs:comment || 
     *          ?p = :property1
     *          )
     * }
     * </pre>
     * 
     * Note: This is a port of
     * TestBigdataEvaluationStrategyImpl#test_or_equals()
     */
    public void test_filters_or_equals() throws Exception {
    
        new TestHelper("filters_or_equals").runTest();
        
    }

    /**
     * <pre>
     * select ?s ?type
     * where { 
     *   ?s rdf:type ?type .
     *   ?s ?p "The Label" .
     *   FILTER((?p = rdfs:label || ?p = rdfs:label) && 
     *          (?type = foaf:Person || ?type = foaf:Person))
     * }
     * </pre>
     * 
     * Note: Pretty sure there was a TupleExpr optimizer that would roll single
     * equality tests directly into the statement patterns themselves. The
     * complex filter was probably to bypass that.
     * <p>
     * Note: This is a port of
     * TestBigdataEvaluationStrategyImpl#test_and_equals()
     */
    public void test_and_equals() throws Exception {
        
        new TestHelper("filters_and_equals").runTest();
    
    }

    /**
     * <pre>
     * select ?s ?label
     * where {
     *   ?s rdfs:subClassOF :Entity .
     *   ?s rdfs:label ?label .
     *   FILTER(?s != :Entity && ?s != :Person && ?s != :Place) 
     * }
     * </pre>
     * 
     * Note: This is a port of
     * TestBigdataEvaluationStrategyImpl#test_and_nequals()
     */
    public void test_and_nequals() throws Exception {
    
        new TestHelper("filters_and_nequals").runTest();
        
    }

    /**
     * <pre>
     * select ?s
     * where {
     *   ?s rdf:type foaf:Person .
     *   ?s rdfs:label ?label . 
     *   FILTER(?label = "The Label" || ?label = "The Label")
     * }
     * </pre>
     * 
     * Note: This is a port of
     * TestBigdataEvaluationStrategyImpl#test_filter_literals()
     */
    public void test_filter_literals() throws Exception {
        
        new TestHelper("filter_literals").runTest();
        
    }

    /**
     * <pre>
     * select ?s
     * where {
     *   ?s rdf:type foaf:Person .
     *   ?s rdfs:label ?label .
     *   FILTER REGEX(?label, 'Mi*', 'i')
     * }
     * </pre>
     * 
     * Note: This is a port of
     * TestBigdataEvaluationStrategyImpl#test_filter_literals()
     */
    public void test_filter_regex() throws Exception {
        
        new TestHelper("filter_regex").runTest();
    
    }

    /**
     * Test correct behavior of redundant filters (fix of issue #972)
     * 
     * <pre>
     * select ?s
     * { 
     *   { SELECT ?s WHERE { ?s ?p ?o } }
     *   FILTER ( ?s != <eg:b>)
     *   FILTER ( ?s != <eg:b>) 
     * }
     * </pre>
     * @throws Exception
     */
    public void test_redundant_filter() throws Exception {
        
        new TestHelper("filter-redundant").runTest();
    
    }
}
