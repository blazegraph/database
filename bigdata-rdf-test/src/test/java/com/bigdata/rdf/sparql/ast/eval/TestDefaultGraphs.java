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
 * Created on Nov 19, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.filter.BOpResolver;
import com.bigdata.bop.join.AccessPathJoinAnnotations;
import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.spo.SPOPredicate;

/**
 * Unit tests for default graph semantics ported from the old
 * TestDefaultGraphAccessPath class. The data set for these tests is:
 * 
 * <pre>
 * :c1 {
 *   :john :loves :mary .
 * }
 * :c2 {
 *   :mary :loves :paul .
 * }
 * :c4 {
 *   :paul :loves :sam .
 * }
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDefaultGraphs extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestDefaultGraphs() {
    }

    /**
     * @param name
     */
    public TestDefaultGraphs(String name) {
        super(name);
    }

    /**
     * No graphs in the defaultGraphs set (none of the specified graphs in the
     * default graph exist in the database).
     * 
     * <pre>
     * SELECT ?s ?p ?o
     * FROM :c0
     * WHERE {
     *     ?s ?p ?o
     * }
     * </pre>
     */
    public void test_defaultGraphs_01a() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01a",// testURI
                "default-graphs-01a.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01a.srx" // resultURI
                ).runTest();

    }

    /**
     * One graph in the defaultGraphs set.
     * <p>
     * Note: This case gets handled specially.
     */
    public void test_defaultGraphs_01b() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01b",// testURI
                "default-graphs-01b.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01b.srx" // resultURI
                ).runTest();

    }

    /**
     * Two graphs in the defaultGraphs set.
     */
    public void test_defaultGraphs_01c() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01c",// testURI
                "default-graphs-01c.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01c.srx" // resultURI
                ).runTest();

    }

    /**
     * Two graphs in the defaultGraphs set, but the query uses a more restricted
     * bindings (john is bound as the subject).
     */
    public void test_defaultGraphs_01d() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01d",// testURI
                "default-graphs-01d.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01d.srx" // resultURI
                ).runTest();

    }

    /**
     * Two graphs in the defaultGraphs set, but the query uses a more restricted
     * bindings (mary is bound as the subject).
     */
    public void test_defaultGraphs_01e() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01e",// testURI
                "default-graphs-01e.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01e.srx" // resultURI
                ).runTest();

    }
    
    /**
     * Two graphs in the defaultGraphs set, but the query uses a more restricted
     * bindings (mary is bound as the object).
     */
    public void test_defaultGraphs_01f() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01f",// testURI
                "default-graphs-01f.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01f.srx" // resultURI
                ).runTest();

    }

    /**
     * Query with more restricted bindings (john is bound as the subject and
     * paul is bound as the object, so there are no solutions).
     */
    public void test_defaultGraphs_01g() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01g",// testURI
                "default-graphs-01g.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01g.srx" // resultURI
                ).runTest();

    }

    /**
     * Default graph is null (that is, it is not specified at the SPARQL level
     * or at the protocol level and therefore defaults to addressing ALL graphs
     * in the kb instance).
     * <p>
     * Note: This case gets handled specially.
     */
    public void test_defaultGraphs_01h() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "default-graphs-01h",// testURI
                "default-graphs-01h.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01h.srx" // resultURI
                ).runTest();

    }

    /**
     * Default graph is null (that is, it is not specified at the SPARQL level
     * or at the protocol level and therefore defaults to addressing ALL graphs
     * in the kb instance). In addition, we have a query hint disabling the
     * default graph access filter.
     * 
     * Given that there are no duplicate triples (i.e., no triple resides in
     * more than one named graph), this query still gives us the correct result. 
     * We also check that there is no distinct filter in the query plan.
     */
    public void test_defaultGraphs_01i() throws Exception {

        if(!store.isQuads())
            return;
        
        final ASTContainer astContainer = new TestHelper(
                "default-graphs-01i",// testURI
                "default-graphs-01i.rq", // queryURI
                "default-graphs-01.trig", // dataURI
                "default-graphs-01i.srx" // resultURI
                ).runTest();
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        final BOp pipelineJoin = queryPlan.get(0).get(0); // first is chunked materialization op
        final SPOPredicate spoPred = (SPOPredicate)pipelineJoin.annotations().get(AccessPathJoinAnnotations.PREDICATE);
        final Object filter = spoPred.annotations().get(IPredicate.Annotations.ACCESS_PATH_FILTER);

        // distinct filter disabled via query hint
        assertFalse(filter instanceof NativeDistinctFilter);

    }
    
    /**
     * We add another, duplicate triple to the dataset, namely:
     * :c5 { :paul :loves :sam . } (which already occurs in :c4) and submit a
     * simple query extracting all triples (without query hint). This test 
     * ensures that, in the standard configuration, the distinct filter is
     * applied and our result contains the triple only once.
     * We also check that there is a distinct filter in the query plan.
     */

    public void test_defaultGraphs_01j() throws Exception {

        if(!store.isQuads())
            return;
        
        final ASTContainer astContainer = new TestHelper(
                "default-graphs-01j",// testURI
                "default-graphs-01j.rq", // queryURI
                "default-graphs-01jk.trig", // dataURI
                "default-graphs-01j.srx" // resultURI
                ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();
        final BOp pipelineJoin = queryPlan.get(0).get(0); // first is chunked materialization op
        final SPOPredicate spoPred = (SPOPredicate)pipelineJoin.annotations().get(AccessPathJoinAnnotations.PREDICATE);
        final Object filter = spoPred.annotations().get(IPredicate.Annotations.ACCESS_PATH_FILTER);

        // distinct filter must be enabled by default
        assertTrue(filter.toString().contains("DistinctFilter()"));

    }
    
    
    /**
     * We add another, duplicate triple to the dataset, namely:
     * :c5 { :paul :loves :sam . } (which already occurs in :c4) and submit a
     * simple query extracting all triples with query hint disabling the default
     * path access filter. Consequently, we get the wrong result, with the triple
     * above being contained twice in the result.
     */
    public void test_defaultGraphs_01k() throws Exception {

        if(!store.isQuads())
            return;
        
        final ASTContainer astContainer = new TestHelper(
                "default-graphs-01k",// testURI
                "default-graphs-01k.rq", // queryURI
                "default-graphs-01jk.trig", // dataURI
                "default-graphs-01k.srx" // resultURI
                ).runTest();
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        final BOp pipelineJoin = queryPlan.get(0).get(0); // first is chunked materialization op
        final SPOPredicate spoPred = (SPOPredicate)pipelineJoin.annotations().get(AccessPathJoinAnnotations.PREDICATE);
        final Object filter = spoPred.annotations().get(IPredicate.Annotations.ACCESS_PATH_FILTER);

        // distinct filter disabled via query hint
        assertFalse(filter instanceof NativeDistinctFilter);

    }
    
}
