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
 * Created on Nov 16, 2016
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.List;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.paths.ArbitraryLengthPathOp;
import com.bigdata.rdf.sparql.ast.QueryHints;

/**
 * Test suite for https://jira.blazegraph.com/browse/BLZG-2089, which
 * introduces a fresh query hint to select the gearing choice for property
 * paths.
 * 
 * @author <a href="mailto:ms@blazegraph.com">Michael Schmidt</a>
 */
public class TestGearingQueryHint extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGearingQueryHint() {
    }

    /**
     * @param name
     */
    public TestGearingQueryHint(String name) {
        super(name);
    }

    /**
     * Simple s p* ?o pattern without user-defined gearing hint.
     */
    public void test_hint_gearing_none_01() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-01",// testURI,
            "hint-gearing-none-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);
    }
    
    /**
     * Simple s p* ?o pattern with user-defined forward gearing hint.
     */
    public void test_hint_gearing_forward_01() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-01",// testURI,
            "hint-gearing-forward-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        );
        
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);
    }
    
    /**
     * Simple s p* ?o pattern with user-defined reverse gearing hint.
     */
    public void test_hint_gearing_reverse_01() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-01",// testURI,
            "hint-gearing-reverse-01.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-01.srx"// resultFileURL
        );
        
        h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
   
    /**
     * Simple ?s p* o pattern without user-defined gearing hint.
     */
    public void test_hint_gearing_none_02() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-02",// testURI,
            "hint-gearing-none-02.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-02.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);

    }
    
    /**
     * Simple ?s p* o pattern with user-defined forward gearing hint.
     */
    public void test_hint_gearing_forward_02() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-02",// testURI,
            "hint-gearing-forward-02.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-02.srx"// resultFileURL
        );
            
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);
    }
    
    /**
     * Simple ?s p* o pattern with user-defined reverse gearing hint.
     */
    public void test_hint_gearing_reverse_02() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-02",// testURI,
            "hint-gearing-reverse-02.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-02.srx"// resultFileURL
        );
        
        h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
    
    /**
     * Simple ?s p* ?o pattern without user-defined gearing hint.
     */
    public void test_hint_gearing_none_03() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-03",// testURI,
            "hint-gearing-none-03.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-03.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);

    }
    
    /**
     * Simple ?s p* ?o pattern with user-defined forward gearing hint.
     */
    public void test_hint_gearing_forward_03() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-03",// testURI,
            "hint-gearing-forward-03.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-03.srx"// resultFileURL
        );
        
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);    	
    }
    
    /**
     * Simple ?s p* ?o pattern with user-defined reverse gearing hint.
     */
    public void test_hint_gearing_reverse_03() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-03",// testURI,
            "hint-gearing-reverse-03.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-03.srx"// resultFileURL
        );
        
        h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
    
    /**
     * Simple s p* o pattern without user-defined gearing hint.
     * Case where the pattern does match.
     */
    public void test_hint_gearing_none_04a() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-04a",// testURI,
            "hint-gearing-none-04a.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04a.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);
    }
    
    /**
     * Simple s p* o pattern with user-defined forward gearing hint.
     * Case where the pattern does match.
     */
    public void test_hint_gearing_forward_04a() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-04a",// testURI,
            "hint-gearing-forward-04a.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04a.srx"// resultFileURL
        );
        
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);
    }
    
    /**
     * Simple s p* o pattern with user-defined reverse gearing hint.
     * Case where the pattern does match.
     */
    public void test_hint_gearing_reverse_04a() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-04a",// testURI,
            "hint-gearing-reverse-04a.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04a.srx"// resultFileURL
        );
        
        h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
    
    /**
     * Simple s p* o pattern without user-defined gearing hint.
     * Case where the pattern does NOT match.
     */
    public void test_hint_gearing_none_04b() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-04b",// testURI,
            "hint-gearing-none-04b.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04b.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);
    }
    
    /**
     * Simple s p* o pattern with user-defined forward gearing hint.
     * Case where the pattern does NOT match.
     */
    public void test_hint_gearing_forward_04b() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-04b",// testURI,
            "hint-gearing-forward-04b.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04b.srx"// resultFileURL
        );
        
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);    	
    }
    
    /**
     * Simple s p* o pattern with user-defined reverse gearing hint.
     * Case where the pattern does NOT match.
     */
    public void test_hint_gearing_reverse_04b() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-04b",// testURI,
            "hint-gearing-reverse-04b.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04b.srx"// resultFileURL
        );
        
        h.runTest();

    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
    
    /**
     * Simple s p* s pattern without user-defined gearing hint.
     */
    public void test_hint_gearing_none_04c() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-none-04c",// testURI,
            "hint-gearing-none-04c.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04c.srx"// resultFileURL
        );
    	
    	h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), null /* no gearing hint specified */);
    }
    
    /**
     * Simple s p* s pattern with user-defined forward gearing hint.
     * Case where the pattern does NOT match.
     */
    public void test_hint_gearing_forward_04c() throws Exception {

    	final TestHelper h = new TestHelper(
            "hint-gearing-forward-04c",// testURI,
            "hint-gearing-forward-04c.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04c.srx"// resultFileURL
        );
        
    	h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);
    }
    
    /**
     * Simple s p* s pattern with user-defined reverse gearing hint.
     */
    public void test_hint_gearing_reverse_04c() throws Exception {

        final TestHelper h = new TestHelper(
            "hint-gearing-reverse-04c",// testURI,
            "hint-gearing-reverse-04c.rq",// queryFileURL
            "hint-gearing.trig",// dataFileURL
            "hint-gearing-04c.srx"// resultFileURL
        );
        
        h.runTest();
        
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_REVERSE);
    }
    
    /**
     * Query in the style of the original ticket triggering this feature:
     * https://jira.blazegraph.com/browse/BLZG-2089
     * 
     * The test makes sure that the query hint is properly attached.
     * 
     * @throws Exception
     */
    public void test_original_ticket() throws Exception {
    	
        final TestHelper h = new TestHelper(
            "hint-gearing-blzg2089",// testURI,
            "hint-gearing-blzg2089.rq",// queryFileURL
            "hint-gearing-blzg2089.trig",// dataFileURL
            "hint-gearing-blzg2089.srx"// resultFileURL
        );
            
        h.runTest();
    	
    	assertQueryHintSet(h.getASTContainer().getQueryPlan(), QueryHints.GEARING_FORWARD);
    }

    /**
     * Asserts that the gearing hint is set as specified (either forward, reverse, or not set aka null)
     * in the queryPlan's single {@link ArbitraryLengthPathOp}. Fails if this condition is violated.
     * 
     * @param queryPlan
     * @param expectedGearing
     */
    private void assertQueryHintSet(final PipelineOp queryPlan, final String expectedGearing) {
    	
    	final List<ArbitraryLengthPathOp> apOps = BOpUtility.toList(queryPlan, ArbitraryLengthPathOp.class);
    	final ArbitraryLengthPathOp apOp = apOps.get(0);
    	
    	assertEquals(expectedGearing, apOp.getProperty(QueryHints.GEARING));

    }
}
