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
 * Created on June 21, 2016
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.JVMNamedSubqueryOp;
import com.bigdata.bop.join.HTreeSolutionSetHashJoinOp;
import com.bigdata.bop.join.HashIndexOpBase;
import com.bigdata.bop.join.JVMSolutionSetHashJoinOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.bop.solutions.HTreeDistinctBindingSetsOp;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;
import com.bigdata.bop.solutions.PipelinedAggregationOp;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.rdf.sparql.ast.ASTContainer;

/**
 * Test suite for the propagation of maxParallel query hint to different
 * operators in the query execution plan. Associated tickets
 * - https://jira.blazegraph.com/browse/BLZG-1958:
 *   Implement ChunkedMaterialization inside query to exploit parallelization
 * - https://jira.blazegraph.com/browse/BLZG-1960: 
 *   Propagate maxParallel hint to non-single threaded operators
 * 
 * Extends {@link TestQueryHints} class, which provides some convenience
 * methods for visiting query hints.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestMaxParallelQueryHint extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestMaxParallelQueryHint() {
    }

    /**
     * @param name
     */
    public TestMaxParallelQueryHint(final String name) {
        super(name);
    }

    /**
     * Test propagation of maxParallel query hint for simple query:
     * 
     * <code>
       SELECT ?s ?o
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p ?o
       }
       </code>
     */
    public void test_maxParallel_simple_triple_pattern() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-01", // testURI,
                "max-parallel-01.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-01.srx"// resultFileURL
             ).runTest();        
        
        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10);

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
    }

    /**
     * Test propagation of maxParallel query hint for simple query:
     * 
     * <code>
       SELECT ?s
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p1 <http://o1> .
         ?s ?p2 <http://o2> .
         ?s ?p3 <http://o3> .
       }
       </code>
     */
    public void test_maxParallel_multiple_triple_patterns() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-02", // testURI,
                "max-parallel-02.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-02.srx"// resultFileURL
            ).runTest();        


        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10); // first triple pattern
        assertTrue(joinItr.next().getMaxParallel()==10); // second triple pattern
        assertTrue(joinItr.next().getMaxParallel()==10); // third triple pattern

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
    }

    /**
     * Test propagation of maxParallel query hint, asserting that the hint
     * is not propagated to the JVMHashJoinOp.
     * 
     * <code>
       SELECT ?s ?p2
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p1 <http://o1> .
         OPTIONAL {
           ?s ?p2 <http://o2> .
           ?s ?p3 <http://o3> .
         }
       }
       </code>
     */
    public void test_maxParallel_jvm_hash_join_op() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-03", // testURI,
                "max-parallel-03.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-03.srx"// resultFileURL
            ).runTest();        


        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10); // first triple pattern

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<HashIndexOpBase> jvmHashIndexOpItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), HashIndexOpBase.class);
        assertTrue(jvmHashIndexOpItr.next().getMaxParallel()==1 /* ISingleThreaded ! */);
        
        // assert projection join carries max query hint
        final Iterator<JVMSolutionSetHashJoinOp> hashJoinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), JVMSolutionSetHashJoinOp.class);
        assertTrue(hashJoinItr.next().getMaxParallel()==10);

    }
    
    /**
     * Test propagation of maxParallel query hint, asserting that the hint
     * is not propagated to the JVMHashJoinOp.
     * 
     * <code>
       SELECT ?s ?p2
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p1 <http://o1> .
         OPTIONAL {
           ?s ?p2 <http://o2> .
           ?s ?p3 <http://o3> .
         }
       }
       </code>
     */
    public void test_maxParallel_htree_hash_join_op() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-04", // testURI,
                "max-parallel-04.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-04.srx"// resultFileURL
            ).runTest();        


        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10); // first triple pattern

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<HashIndexOpBase> jvmHashIndexOpItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), HashIndexOpBase.class);
        assertTrue(jvmHashIndexOpItr.next().getMaxParallel()==1 /* ISingleThreaded ! */);
        
        // assert projection join carries max query hint
        final Iterator<HTreeSolutionSetHashJoinOp> hashJoinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), HTreeSolutionSetHashJoinOp.class);
        assertTrue(hashJoinItr.next().getMaxParallel()==10);
        
    }    
    
    
    /**
     * Test propagation of maxParallel query hint for JVM distinct
     * (which is multi-threaded)
     * 
     * <code>
       SELECT DISTINCT ?s ?o
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p ?o
       }
       </code>
     */
    public void test_maxParallel_jvm_distinct() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-05", // testURI,
                "max-parallel-05.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-05.srx"// resultFileURL
             ).runTest();        
        
        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10);

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<JVMDistinctBindingSetsOp> distinctItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), JVMDistinctBindingSetsOp.class);
        assertTrue(distinctItr.next().getMaxParallel()==10);
    }
    
    /**
     * Test non-propagation of maxParallel query hint for JVM distinct
     * (which is single-threaded)
     * 
     * <code>
       SELECT DISTINCT ?s ?o
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s ?p ?o
       }
       </code>
     */
    public void test_maxParallel_htree_distinct() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-06", // testURI,
                "max-parallel-06.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-06.srx"// resultFileURL
             ).runTest();        
        
        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10);

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<HTreeDistinctBindingSetsOp> distinctItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), HTreeDistinctBindingSetsOp.class);
        assertTrue(distinctItr.next().getMaxParallel()==1 /* ISingleThreaded! */);
    }
    
    /**
     * Test non-propagation of maxParallel query hint to {@link PipelinedAggregationOp}
     * (which is single-threaded)
     * 
     * <code>
       SELECT (COUNT(*) AS ?cnt)
       WHERE {
         hint:Query hint:maxParallel 10 .
         ?s <http://p1> ?o1 .
         ?s <http://p2> ?o2 .
       }
       </code>
     */
    public void test_pipelined_aggregation_op() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-07", // testURI,
                "max-parallel-07.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-07.srx"// resultFileURL
             ).runTest();        
        
        // assert pipeline join carries max query hint
        @SuppressWarnings("rawtypes")
        final Iterator<PipelineJoin> joinItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelineJoin.class);
        assertTrue(joinItr.next().getMaxParallel()==10); // first join
        assertTrue(joinItr.next().getMaxParallel()==10); // second join

        // assert projection join carries max query hint
        final Iterator<ProjectionOp> projectionItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ProjectionOp.class);
        assertTrue(projectionItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<ChunkedMaterializationOp> chunkedMatItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), ChunkedMaterializationOp.class);
        assertTrue(chunkedMatItr.next().getMaxParallel()==10);
        
        // assert projection join carries max query hint
        final Iterator<PipelinedAggregationOp> aggregationItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), PipelinedAggregationOp.class);
        assertTrue(aggregationItr.next().getMaxParallel()==1 /* ISingleThreaded! */);
    }
    
    /**
     * Test non-propagation of maxParallel query hint to {@link JVMNamedSubqueryOp}
     * (which is single-threaded)
     * 
     * <code>
       SELECT ?s
       WITH {
         SELECT * WHERE { ?s <http://p1> <http://o1> }
       } AS %sub1
       WHERE {
         INCLUDE %sub1
         ?s <http://p2> <http://o2>
       }
       </code>
     */
    public void test_jvm_named_subquery_op() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-08", // testURI,
                "max-parallel-08.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-08.srx"// resultFileURL
             ).runTest();        
        
        // assert projection join carries max query hint
        final Iterator<JVMNamedSubqueryOp> namedSubqueryItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), JVMNamedSubqueryOp.class);
        assertTrue(namedSubqueryItr.next().getMaxParallel()==1 /* ISingleThreaded! */);
    }
    

    /**
     * Test non-propagation of maxParallel query hint to {@link JVMNamedSubqueryOp}
     * (which is single-threaded)
     * 
     * <code>
       SELECT ?s
       WITH {
         SELECT * WHERE { ?s <http://p1> <http://o1> }
       } AS %sub1
       WHERE {
         hint:Query hint:analytic "true" .
         INCLUDE %sub1
         ?s <http://p2> <http://o2>
       }
       </code>
     */
    public void test_htree_named_subquery_op() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(//
                "max-parallel-09", // testURI,
                "max-parallel-09.rq",// queryFileURL
                "max-parallel.trig",// dataFileURL
                "max-parallel-09.srx"// resultFileURL
             ).runTest();        
        
        // assert projection join carries max query hint
        final Iterator<HTreeNamedSubqueryOp> namedSubqueryItr = BOpUtility.visitAll(
                astContainer.getQueryPlan(), HTreeNamedSubqueryOp.class);
        assertTrue(namedSubqueryItr.next().getMaxParallel()==1 /* ISingleThreaded! */);
    }
    
}
