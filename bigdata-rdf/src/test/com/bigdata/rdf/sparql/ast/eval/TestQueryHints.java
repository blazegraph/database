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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.ServiceCallJoin;
import com.bigdata.bop.join.AbstractHashJoinOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.service.OpenrdfNativeMockServiceFactory;
import com.bigdata.rdf.sparql.ast.hints.QueryHintException;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.TestASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.util.InnerCause;

/**
 * Test suite for SPARQL queries with embedded query hints.
 * 
 * @see ASTQueryHintOptimizer
 * @see TestASTQueryHintOptimizer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryHints extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestQueryHints() {
    }

    /**
     * @param name
     */
    public TestQueryHints(String name) {
        super(name);
    }

    /**
     * A simple SELECT query with some query hints. This verifies that the query
     * hints are correctly applied to the AST and/or the generated query plan.
     */
    public void test_query_hints_01() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-01")
                .runTest();

        /*
         * Check the optimized AST. The magic predicates which correspond to
         * query hints should have be removed and various annotations made to
         * the AST which capture the semantics of those query hints.
         */
        {

            final JoinGroupNode whereClause = (JoinGroupNode) astContainer
                    .getOptimizedAST().getWhereClause();

            /*
             * The hint to disable the join order optimizer should show up on
             * the JoinGroupNodes. For the sample query, that means just the
             * top-level WHERE clause.
             */
            assertEquals(QueryOptimizerEnum.None,
                    whereClause.getProperty(QueryHints.OPTIMIZER));

            /*
             * The query/group scope pipeline operator hints are not found on
             * the JoinGroupNode.
             * 
             * TODO Probably these hints SHOULD be on the JoinGroupNode so they
             * can impact the pipeline operators used to enter and/leave a join
             * group, but even then that would not apply for the top-level of
             * the WHERE clause.
             */
            assertNull(whereClause
                    .getQueryHint(PipelineOp.Annotations.MAX_PARALLEL));
            assertNull(whereClause
                    .getQueryHint(PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY));
            assertNull(whereClause
                    .getQueryHint(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY));
            assertNull(whereClause
                    .getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));

            // There are two statement pattern nodes left (after removing the
            // query hint SPs).
            assertEquals(2, whereClause.arity());

            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(0);

                assertEquals(RDFS.LABEL, sp.p().getValue());

                assertEquals(10,
                        Integer.parseInt(sp
                                .getQueryHint(
                                        PipelineOp.Annotations.MAX_PARALLEL,
                                        "1"/* defaultValue */)));

                assertEquals(20, Integer.parseInt(sp
                        .getQueryHint(
                                PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                                "1"/* defaultValue */)));

                assertEquals(
                        20,
                        Integer.parseInt(sp
                                .getQueryHint(
                                        BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                                        "5"/* defaultValue */)));

                // The join order optimizer query hint should not be on the SP.
                assertNull(sp.getProperty(QueryHints.OPTIMIZER));

            }

            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(1);

                assertEquals(RDF.TYPE, sp.p().getValue());

                assertEquals(1000,
                        Integer.parseInt(sp
                                .getQueryHint(BufferAnnotations.CHUNK_CAPACITY,
                                        "100"/* defaultValue */)));

                assertEquals(
                        20,
                        Integer.parseInt(sp
                                .getQueryHint(
                                        BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                                        "5"/* defaultValue */)));

                // The join order optimizer query hint should not be on the SP.
                assertNull(sp.getProperty(QueryHints.OPTIMIZER));

            }

        } // end check of the AST.

        /*
         * Check the query plan. Various query hints should have be transferred
         * onto pipeline operators so they can effect the evaluation of those
         * operators when they execute on the query engine. The join order was
         * turned off, so we can expect to see the joins in the same order that
         * the SPs were given in the query.
         */
        {

            @SuppressWarnings("rawtypes")
            final Iterator<PipelineJoin> jitr = BOpUtility.visitAll(
                    astContainer.getQueryPlan(), PipelineJoin.class);

            /*
             * Note: The joins are coming back from visitAll() out of the
             * expected order. This puts them into the expected slots.
             */
            @SuppressWarnings("rawtypes")
            final PipelineJoin[] joins = new PipelineJoin[2];
            joins[1] = jitr.next();
            joins[0] = jitr.next();
            assertFalse(jitr.hasNext());
            {
             
                @SuppressWarnings("rawtypes")
                final PipelineJoin join = joins[0];
                
                assertEquals(RDFS.LABEL, ((IV<?,?>)join.getPredicate().get(1/* p */)
                        .get()).getValue());
                
                assertEquals(10, join.getMaxParallel());

                assertEquals(20, Integer.parseInt(join
                        .getProperty(
                                PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                                "1"/* defaultValue */)));

                assertEquals(20, join.getChunkOfChunksCapacity());

            }

            {

                @SuppressWarnings("rawtypes")
                final PipelineJoin join = joins[1];

                assertEquals(RDF.TYPE, ((IV<?,?>)join.getPredicate().get(1/* p */)
                        .get()).getValue());

                assertEquals(10, join.getMaxParallel());

                assertEquals(1000, join.getChunkCapacity());

                assertEquals(20, join.getChunkOfChunksCapacity());

            }

        }

    }

    /**
     * Unit test verifies the correct handling {@link QueryHints#QUERYID}.
     */
    public void test_query_hints_02() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-02")
                .runTest();

        final String expectedIdStr = "458aed8b-2c76-4d29-9cf0-d18d6a74bc0e";

        final UUID expectedId = UUID.fromString(expectedIdStr);

        final String actualIdStr = astContainer.getQueryHint(QueryHints.QUERYID);
        
        // Verify that the right queryId was assigned to the ASTContainer.
        assertEquals(expectedIdStr, actualIdStr);

        // Verify that the statement pattern conveying the query hint was
        // removed.
        assertEquals(2, astContainer.getOptimizedAST().getWhereClause().arity());

        // Verify that the queryId was included in the generated query plan.
        assertEquals(
                expectedId,
                astContainer
                        .getQueryPlan()
                        .getProperty(
                                com.bigdata.bop.engine.QueryEngine.Annotations.QUERY_ID));

    }

    /**
     * Unit test for correct rejection of an unknown query hint.
     */
    public void test_query_hints_03() throws Exception {

        try {
        
            new TestHelper("query-hints-03").runTest();

        } catch (Throwable t) {
            
            assertTrue(InnerCause.isInnerCause(t, QueryHintException.class));
        
        }
        
    }

    /**
     * Part one of a two part unit test which verifies that we lift out a
     * {@link SubqueryRoot} which is marked the {@link QueryHints#RUN_ONCE}
     * query hint. In this version of the test, the query hint is not present
     * and we verify that the sub-select is NOT lifted out.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     *   ?x rdfs:label ?o .
     *   { 
     *      SELECT ?x { ?x rdf:type foaf:Person }
     *   }
     * </pre>
     */
    public void test_query_hints_04a() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-04a")
                .runTest();

        final NamedSubqueriesNode namedSubqueries = astContainer
                .getOptimizedAST().getNamedSubqueries();

        assertNull(namedSubqueries);

    }
    
    /**
     * Part one of a two part unit test which verifies that we lift out a
     * {@link SubqueryRoot} which is marked the {@link QueryHints#RUN_ONCE}
     * query hint. In this version of the test, the query hint is present and we
     * verify that the sub-select is lifted out.
     */
    public void test_query_hints_04b() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-04b")
                .runTest();

        final NamedSubqueriesNode namedSubqueries = astContainer
                .getOptimizedAST().getNamedSubqueries();

        assertNotNull(namedSubqueries);

        assertEquals(1, namedSubqueries.arity());
        
    }

    /**
     * Unit test for the query hints {@link AST2BOpBase.Annotations#HASH_JOIN}
     * and {@link IPredicate.Annotations#KEY_ORDER}.
     */
    public void test_query_hints_05() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-05")
                .runTest();

        /*
         * Check the optimized AST. The magic predicates which correspond to
         * query hints should have be removed and various annotations made to
         * the AST which capture the semantics of those query hints.
         */
        {

            final JoinGroupNode whereClause = (JoinGroupNode) astContainer
                    .getOptimizedAST().getWhereClause();

            /*
             * The hint to disable the join order optimizer should show up on
             * the JoinGroupNodes. For the sample query, that means just the
             * top-level WHERE clause.
             */
            assertEquals(QueryOptimizerEnum.None,
                    whereClause.getProperty(QueryHints.OPTIMIZER));

            // There are two statement pattern nodes left (after removing the
            // query hint SPs).
            assertEquals(2, whereClause.arity());

            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(0);

                assertEquals(RDFS.LABEL, sp.p().getValue());

            }

            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(1);

                assertEquals(RDF.TYPE, sp.p().getValue());

                assertEquals(SPOKeyOrder.PCSO.toString(),
                        sp.getQueryHint(IPredicate.Annotations.KEY_ORDER));

                assertEquals("true",
                        sp.getQueryHint(QueryHints.HASH_JOIN));

            }

        } // end check of the AST.

        /*
         * Check the query plan.
         */
        {

            // Should be one pipeline join.
            {

                @SuppressWarnings("rawtypes")
                final Iterator<PipelineJoin> jitr = BOpUtility.visitAll(
                        astContainer.getQueryPlan(), PipelineJoin.class);
                
                assertTrue(jitr.hasNext());
                
                @SuppressWarnings("rawtypes")
                final PipelineJoin join = jitr.next();
                
                assertEquals(RDFS.LABEL, ((IV<?,?>)join.getPredicate().get(1/* p */)
                        .get()).getValue());
                
                assertFalse(jitr.hasNext());

            }

            // Should be one hash join.
            {
            
                @SuppressWarnings("rawtypes")
                final Iterator<AbstractHashJoinOp> jitr = BOpUtility.visitAll(
                        astContainer.getQueryPlan(), AbstractHashJoinOp.class);
                
                assertTrue(jitr.hasNext());
                
                @SuppressWarnings("rawtypes")
                final AbstractHashJoinOp join = jitr.next();
                
                assertEquals(RDF.TYPE, ((IV<?,?>) join.getPredicate().get(1/* p */)
                        .get()).getValue());

                final IPredicate<?> pred = join.getPredicate();
                
                assertEquals(SPOKeyOrder.PCSO, pred.getKeyOrder());
                
                assertFalse(jitr.hasNext());
                
            }

        }
        
    }
    
    /**
     * Unit test for {@link QueryHints#CHUNK_SIZE}.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     *   
     *   # Turn off the join order optimizer.
     *   hint:Query hint:optimizer "None".
     *   
     *   # Disable analytic query for the test.
     *   hint:Query hint:analytic "false".
     *   
     *   ?x rdfs:label ?o .
     *   # Override the vector size for the previous join.
     *   hint:Prior hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity 1001 .
     *   
     *   { 
     *      SELECT ?x {
     *           ?x rdf:type foaf:Person .
     *           # Override the vector size for previous join.
     *           hint:Prior hint:chunkSize 251 .
     *          }
     *   }
     * 
     * }
     * </pre>
     * 
     * TODO We have no tests for {@link QueryHints#CHUNK_SIZE} for anything
     * other than {@link PipelineJoin} and {@link ServiceCall}. Work through the
     * code and tests required to get the query hint into place for other kinds
     * of joins as well.
     */
    public void test_query_hints_06() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-06")
                .runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        // Check the PipelineJoins.
        {
            @SuppressWarnings("rawtypes")
            final Iterator<PipelineJoin> itr = BOpUtility.visitAll(queryPlan,
                    PipelineJoin.class);

            {
                final PipelineJoin<?> join = itr.next();

                // Make sure this is the right join.
                assertTrue(join.getPredicate().get(2/* s */).isConstant());

                assertEquals(join.toString(), 251, join.getChunkCapacity());
            }

            {
                final PipelineJoin<?> join = itr.next();

                // Make sure this is the right join.
                assertEquals(Var.var("o"), join.getPredicate().get(2/* o */));

                assertEquals(join.toString(), 1001,
                        join.getChunkCapacity());
            }
        }

    }
    
    /**
     * Unit test for {@link QueryHints#AT_ONCE} when applied to a
     * {@link PipelineJoin}.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     *   
     *   # Turn off the join order optimizer.
     *   hint:Query hint:optimizer "None".
     *   
     *   # Disable analytic query for the test.
     *   hint:Query hint:analytic false.
     *   
     *   ?x rdfs:label ?o .
     *   hint:Prior hint:atOnce true .
     *   
     *   { 
     *      SELECT ?x {
     *           ?x rdf:type foaf:Person .
     *           hint:Prior hint:atOnce false .
     *          }
     *   }
     * 
     * }
     * </pre>
     * 
     * TODO We have no tests for {@link QueryHints#AT_ONCE} for anything other
     * than {@link PipelineJoin} (this test) and {@link ServiceNode} (below).
     * Work through the code and tests required to get the query hint into place
     * for other kinds of joins as well.
     */
    public void test_query_hints_07() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-07")
                .runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        // Check the PipelineJoins.
        {
            @SuppressWarnings("rawtypes")
            final Iterator<PipelineJoin> itr = BOpUtility.visitAll(queryPlan,
                    PipelineJoin.class);

            {

                final PipelineJoin<?> join = itr.next();

                // Make sure this is the right join.
                assertTrue(join.getPredicate().get(2/* s */).isConstant());

                assertEquals(join.toString(), true,
                        join.isPipelinedEvaluation());

            }

            {

                final PipelineJoin<?> join = itr.next();

                // Make sure this is the right join.
                assertEquals(Var.var("o"), join.getPredicate().get(2/* o */));

                assertEquals(join.toString(), false,
                        join.isPipelinedEvaluation());

            }

        }

    }
    
    /**
     * Unit test for {@link QueryHints#AT_ONCE} when applied to a SERVICE node.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x
     * WHERE {
     *   
     *   SERVICE ?x {
     *      ?x rdf:type foaf:Person .
     *   }
     *   hint:Prior hint:atOnce true.
     * 
     * }
     * </pre>
     */
    public void test_query_hints_08() throws Exception {

        /*
         * The assumption is that operators are pipelined unless explicitly
         * overridden.
         */
        assertTrue(PipelineOp.Annotations.DEFAULT_PIPELINED);

        final URI serviceURI = new URIImpl("http://www.bigdata.com/mockService");
        
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Mike"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Bryan"));
            serviceSolutions.add(bset);
        }
        
        ServiceRegistry.getInstance().add(serviceURI,
                new OpenrdfNativeMockServiceFactory(serviceSolutions));

        final ASTContainer astContainer;
        try {

            astContainer = new TestHelper("query-hints-08").runTest();

        } finally {

            ServiceRegistry.getInstance().remove(serviceURI);

        }

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        {

            final Iterator<ServiceCallJoin> itr = BOpUtility.visitAll(
                    queryPlan, ServiceCallJoin.class);

            {

                final ServiceCallJoin join = itr.next();

                assertFalse(itr.hasNext());

                assertEquals(join.toString(), false,
                        join.isPipelinedEvaluation());

            }

        }

    }

    
    /**
     * Unit test for {@link QueryHints#AT_ONCE} when applied to a SERVICE node.
     * This verifies that we can turn off "at-once" evaluation for a SERVICE.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x
     * WHERE {
     *   
     *   SERVICE ?x {
     *      ?x rdf:type foaf:Person .
     *   }
     *   hint:Prior hint:atOnce false.
     * 
     * }
     * </pre>
     */
    public void test_query_hints_08b() throws Exception {

        /*
         * The assumption is that operators are pipelined unless explicitly
         * overridden.
         */
        assertTrue(PipelineOp.Annotations.DEFAULT_PIPELINED);

        final URI serviceURI = new URIImpl("http://www.bigdata.com/mockService");
        
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Mike"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Bryan"));
            serviceSolutions.add(bset);
        }
        
        ServiceRegistry.getInstance().add(serviceURI,
                new OpenrdfNativeMockServiceFactory(serviceSolutions));

        final ASTContainer astContainer;
        try {

            astContainer = new TestHelper("query-hints-08b").runTest();

        } finally {

            ServiceRegistry.getInstance().remove(serviceURI);

        }

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        {

            final Iterator<ServiceCallJoin> itr = BOpUtility.visitAll(
                    queryPlan, ServiceCallJoin.class);

            {

                final ServiceCallJoin join = itr.next();

                assertFalse(itr.hasNext());

                assertEquals(join.toString(), true,
                        join.isPipelinedEvaluation());

            }

        }

    }

    /**
     * Unit test for {@link QueryHints#CHUNK_SIZE} when applied to a SERVICE node.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x
     * WHERE {
     *   
     *   SERVICE ?x {
     *      ?x rdf:type foaf:Person .
     *   }
     *   hint:Prior hint:chunkSize 1001 .
     * 
     * }
     * </pre>
     */
    public void test_query_hints_09() throws Exception {

        final URI serviceURI = new URIImpl("http://www.bigdata.com/mockService");
        
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Mike"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("x",new URIImpl("http://www.bigdata.com/Bryan"));
            serviceSolutions.add(bset);
        }
        
        ServiceRegistry.getInstance().add(serviceURI,
                new OpenrdfNativeMockServiceFactory(serviceSolutions));

        final ASTContainer astContainer;
        try {

            astContainer = new TestHelper("query-hints-09").runTest();

        } finally {

            ServiceRegistry.getInstance().remove(serviceURI);

        }

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        {

            final Iterator<ServiceCallJoin> itr = BOpUtility.visitAll(
                    queryPlan, ServiceCallJoin.class);

            {

                final ServiceCallJoin join = itr.next();

                assertFalse(itr.hasNext());

                assertEquals(join.toString(), 1001, join.getChunkCapacity());

            }

        }

    }

}
