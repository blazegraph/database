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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.controller.INamedSubqueryOp;
import com.bigdata.bop.controller.ServiceCallJoin;
import com.bigdata.bop.join.HashJoinOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.SolutionSetHashJoinOp;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.service.OpenrdfNativeMockServiceFactory;
import com.bigdata.rdf.sparql.ast.hints.QueryHintException;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.TestASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Test suite for SPARQL queries with embedded query hints.
 * 
 * @see ASTQueryHintOptimizer
 * @see TestASTQueryHintOptimizer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *          TODO Unit test to verify that hint:optimistic actually changes the
 *          value so the query runs differently (that is, test the query hint
 *          not the interpretation of the hint by the static join order
 *          optimizer). (I believe that this was hitting one of the BSBM, or
 *          maybe the BSBM BI queries.)
 * 
 *          TODO Unit test query hints for APs both at the BGP scope and in the
 *          Query scope (where they bind on AST2BOpContext). (E.g., the use of
 *          remote access paths, hash joins, sample limits, etc.).
 * 
 *          TODO Unit test to verify that query hints are being attached to the
 *          {@link ConditionalRoutingOp}.
 */
public class TestQueryHints extends AbstractDataDrivenSPARQLTestCase {

    private static final Logger log = Logger.getLogger(TestQueryHints.class);

    /**
     * 
     */
    public TestQueryHints() {
    }

    /**
     * @param name
     */
    public TestQueryHints(final String name) {
        super(name);
    }

    /**
     * Return all {@link BOp}s in which the specified property name appears
     * either as a direct annotation of that {@link BOp} or as a property in the
     * query hints properties object for that {@link BOp}.
     * 
     * @param op
     *            The root operator.
     * @param name
     *            The name of the desired property.
     *            
     * @return An iterator visiting the operators having a binding for that
     *         property.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<BOp> visitQueryHints(final BOp op, final String name) {
        return new Striterator(BOpUtility.preOrderIteratorWithAnnotations(op))
                .addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public boolean isValid(final Object arg0) {
                        if (!(arg0 instanceof BOp)) {
                            return false;
                        }
                        final BOp tmp = (BOp) arg0;
                        if (tmp.getProperty(name) != null) {
                            // Exists as bare property.
                            if (log.isDebugEnabled())
                                log.debug("Found as bare property: name="
                                        + name + ", op=" + tmp.toShortString()
                                        + "#" + tmp.hashCode());
                            return true;
                        }
                        final Properties queryHints = (Properties) tmp
                                .getProperty(ASTBase.Annotations.QUERY_HINTS);
                        if (queryHints != null) {
                            if (queryHints.getProperty(name) != null) {
                                // exists as query hint property.
                                if (log.isDebugEnabled())
                                    log.debug("Found as query hint: name="
                                            + name + ", op="
                                            + tmp.toShortString() + "#"
                                            + tmp.hashCode());
                                return true;
                            }
                        }
                        return false;
                    }
                });
    }

    /**
     * Return the number of {@link BOp}s for which the specified property name
     * appears either as a direct annotation of that {@link BOp} or as a
     * property in the query hints properties object for that {@link BOp}.
     * 
     * @param op
     *            The root operator.
     * @param name
     *            The name of the desired property.
     *            
     * @return The number of occurrences of that query hint.
     */
    static private int countQueryHints(final BOp op, final String name) {
        int n = 0;
        final Iterator<BOp> itr = visitQueryHints(op, name);
        while (itr.hasNext()) {
            itr.next();
            n++;
        }
        return n;
    }

    /**
     * A simple SELECT query with some query hints. This verifies that the query
     * hints are correctly applied to the AST and/or the generated query plan.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # disable join order optimizer
     *   hint:Group hint:optimizer "None" .
     * 
     *   # query hint binds for the group.
     *   hint:Group hint:maxParallel 10 .
     * 
     *   # query hint binds for the group and any subgroups.
     *   hint:GroupAndSubGroups hint:com.bigdata.bop.PipelineOp.pipelineQueueCapacity 20 .
     * 
     *   # query hint binds for the (sub)query but does not effect other (sub)queries.
     *   hint:SubQuery hint:com.bigdata.relation.accesspath.BlockingBuffer.chunkOfChunksCapacity 20 .
     * 
     *   ?x rdfs:label ?o .
     * 
     *   ?x rdf:type foaf:Person .
     * 
     *   # query hint binds for the immediately proceeding basic graph pattern.
     *   hint:Prior hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity 1000 .
     * 
     *   # query hint binds for the immediately proceeding basic graph pattern.
     *   #hint:Prior hint:com.bigdata.bop.IPredicate.fullyBufferedReadThreshold 5000 .
     * 
     * }
     * </pre>
     */
    public void test_query_hints_01() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-01")
                .runTest();

        /*
         * Verify that the PREFIX "hint:" was implicitly declared.
         */
        assertEquals(QueryHints.NAMESPACE, astContainer.getOriginalAST()
                .getPrefixDecls().get("hint"));

        /*
         * Check the optimized AST. The magic predicates which correspond to
         * query hints should have be removed and various annotations made to
         * the AST which capture the semantics of those query hints.
         */
        {

            if (log.isInfoEnabled())
                log.info(BOpUtility.toString(astContainer.getOptimizedAST()));

            final JoinGroupNode whereClause = (JoinGroupNode) astContainer
                    .getOptimizedAST().getWhereClause();

            /*
             * The hint to disable the join order optimizer should show up on
             * the JoinGroupNodes. For the sample query, that means just the
             * top-level WHERE clause.
             */
            assertEquals(QueryOptimizerEnum.None,
                    whereClause.getQueryOptimizer());

            /*
             * The Query/Group scope pipeline operator hints are found on the
             * JoinGroupNode.
             */
            assertEquals("10",
                    whereClause
                            .getQueryHint(PipelineOp.Annotations.MAX_PARALLEL,
                                    "-1"/* defaultValue */));
            assertEquals("20",
                    whereClause.getQueryHint(
                            PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                            "-1"/* defaultValue */));
            assertEquals("20",
                    whereClause.getQueryHint(
                            PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                            "-1"/* defaultValue */));

            /*
             * Note: This query hint is bound *only* to the prior SP (Scope :=
             * Prior) in the query. That binding is tested below.
             */
            assertNull(whereClause
                    .getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertEquals(
                    1,
                    countQueryHints(astContainer.getOptimizedAST(),
                            PipelineOp.Annotations.CHUNK_CAPACITY));

            /*
             * hint:optimizer only appears on join groups. The only join group
             * in this query is the top-level whereClause.
             */
            assertSameIteratorAnyOrder(
                    new Object[] { whereClause },
                    visitQueryHints(astContainer.getOptimizedAST(),
                            QueryHints.OPTIMIZER));

            // There are two statement pattern nodes left (after removing the
            // query hint SPs).
            assertEquals(2, whereClause.arity());

            // The 1st SP.
            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(0);

                assertEquals(RDFS.LABEL, sp.p().getValue());

                assertEquals(10,
                        Integer.parseInt(sp
                                .getQueryHint(
                                        PipelineOp.Annotations.MAX_PARALLEL,
                                        "-1"/* defaultValue */)));

                assertEquals(20, Integer.parseInt(sp
                        .getQueryHint(
                                PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                                "-1"/* defaultValue */)));

                assertEquals(20, Integer.parseInt(sp
                        .getQueryHint(
                                BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                                "-1"/* defaultValue */)));

                /*
                 * The join order optimizer query hint is not be on the SPs (it
                 * is only applied to JoinGroupNode instances).
                 */
                assertNull(sp.getProperty(QueryHints.OPTIMIZER));

            }

            // The 2nd SP.
            {

                final StatementPatternNode sp = (StatementPatternNode) whereClause
                        .get(1);

                assertEquals(RDF.TYPE, sp.p().getValue());

                assertEquals(1000,
                        Integer.parseInt(sp
                                .getQueryHint(BufferAnnotations.CHUNK_CAPACITY,
                                        "-1"/* defaultValue */)));

                assertEquals(20, Integer.parseInt(sp
                        .getQueryHint(
                                BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                                "-1"/* defaultValue */)));

                /*
                 * The join order optimizer query hint is not be on the SPs (it
                 * is only applied to JoinGroupNode instances).
                 */
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

            if (log.isInfoEnabled())
                log.info(BOpUtility.toString(astContainer.getQueryPlan()));

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

                final IPredicate<?> pred = join.getPredicate();

                if (log.isInfoEnabled())
                    log.info("pred=" + BOpUtility.toString(pred));

                assertEquals(RDFS.LABEL,
                        ((IV<?, ?>) pred.get(1/* p */).get()).getValue());

                assertEquals(10, join.getMaxParallel());

                assertEquals(20, Integer.parseInt(join
                        .getProperty(
                                PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY,
                                "-1"/* defaultValue */)));

                assertEquals(20, join.getChunkOfChunksCapacity());

                /*
                 * Verify that the annotations were also applied to the
                 * Predicate associated with the PipelineJoin operator. This is
                 * necessary for the AccessPath class to override its defaults
                 * for vectoring based on the query hints for the
                 * StatementPatternNode.
                 */

                // This annotation is not given for this SP.
                assertNull(pred
                        .getProperty(PipelineOp.Annotations.CHUNK_CAPACITY));
                assertEquals(
                        "20",
                        pred.getProperty(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY));
                // Note: This hint was given, but it is not copied to the Predicate.
                assertNull(
                        pred.getProperty(PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY));
                // Note: This annotation was not given.
                assertNull(pred
                        .getProperty(IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD));

            }

            {

                @SuppressWarnings("rawtypes")
                final PipelineJoin join = joins[1];

                final IPredicate<?> pred = join.getPredicate();

                if (log.isInfoEnabled())
                    log.info("pred=" + BOpUtility.toString(pred));
                
                assertEquals(RDF.TYPE,
                        ((IV<?, ?>) pred.get(1/* p */).get()).getValue());

                assertEquals(10, join.getMaxParallel());

                assertEquals(1000, join.getChunkCapacity());

                assertEquals(20, join.getChunkOfChunksCapacity());

                /*
                 * Verify that the annotations were also applied to the
                 * Predicate associated with the PipelineJoin operator. This is
                 * necessary for the AccessPath class to override its defaults
                 * for vectoring based on the query hints for the
                 * StatementPatternNode.
                 */

                assertEquals("1000",
                        pred.getProperty(PipelineOp.Annotations.CHUNK_CAPACITY));
                assertEquals(
                        "20",
                        pred.getProperty(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY));
                // Note: This hint was given, but it is not copied to the Predicate.
                assertNull(pred
                        .getProperty(PipelineOp.Annotations.PIPELINE_QUEUE_CAPACITY));
                // Note: this hint was not given.
                assertNull(pred
                        .getProperty(IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD));

            }

            /*
             * The projection should be the first operator in the query plan.
             * 
             * Note: The SELECT is outside of the join group in which these
             * query hints were given. Therefore, only those query hints whose
             * scope is Query or SubQuery should be applied to the ProjectionOp.
             */
            {

                final Iterator<ProjectionOp> projItr = BOpUtility.visitAll(
                        astContainer.getQueryPlan(), ProjectionOp.class);

                assertTrue(projItr.hasNext());

                final ProjectionOp projection = projItr.next();

                // Not transferred since Scope:=Group.
                assertNull(projection
                        .getProperty(PipelineOp.Annotations.MAX_PARALLEL));

                // Not transferred since Scope:=Group.
                assertNull(projection
                        .getProperty(PipelineOp.Annotations.CHUNK_CAPACITY));

                // Scope:=SubQuery, so transferred to the SELECT clause.
                assertNotNull(projection
                        .getProperty(PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY));
                assertEquals(20, projection.getChunkOfChunksCapacity());

                assertFalse(projItr.hasNext());

            }
            
        }

    }

    /**
     * Unit test verifies the correct handling {@link QueryHints#QUERYID}.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # Set the queryId.
     *   hint:Query hint:queryId "458aed8b-2c76-4d29-9cf0-d18d6a74bc0e" .
     * 
     *   ?x rdfs:label ?o .
     * 
     *   ?x rdf:type foaf:Person .
     * 
     * }
     * </pre>
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
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # An unknown query hint. This should result in a QueryHintException.
     *   hint:Query hint:unknown "true" .
     * 
     *   ?x rdfs:label ?o .
     * 
     *   ?x rdf:type foaf:Person .
     * 
     * }
     * </pre>
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
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * 
     * SELECT ?x ?o
     * WHERE {
     * 
     *   # disable join order optimizer
     *   hint:Group hint:optimizer "None" .
     * 
     *   # One-bound using P___ index.
     *   ?x rdfs:label ?o .
     * 
     *   # Two-bound.  Uses POCS index by default, which is optimal.
     *   ?x rdf:type foaf:Person .
     * 
     *   # Request a hash join against the statement index.
     *   hint:Prior hint:hashJoin "true" .
     * 
     *   # Override the statement index.
     *   hint:Prior hint:com.bigdata.bop.IPredicate.keyOrder "PCSO" .
     * 
     * }
     * </pre>
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
                    whereClause.getQueryOptimizer());

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
                final Iterator<HashJoinOp> jitr = BOpUtility.visitAll(
                        astContainer.getQueryPlan(), HashJoinOp.class);
                
                assertTrue(jitr.hasNext());
                
                @SuppressWarnings("rawtypes")
                final HashJoinOp join = jitr.next();
                
                assertEquals(RDF.TYPE, ((IV<?,?>) join.getPredicate().get(1/* p */)
                        .get()).getValue());

                final IPredicate<?> pred = join.getPredicate();
                
                assertEquals(SPOKeyOrder.PCSO, pred.getKeyOrder());
                
                assertFalse(jitr.hasNext());
                
            }

        }
        
    }
    
    /**
     * Unit test for the {@link IPredicate.Annotations#KEY_ORDER} query hint on
     * a pipeline join (versus a hash join).
     * <p>
     * Note: This test does NOT verify that the pipeline join actually obeys
     * that query hint. You need to verify that with a unit test of the pipeline
     * join evaluation and check to see that the right index is used for each
     * join.
     */
    public void test_query_hints_05a() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "query-hints-05a",
                "query-hints-05a.rq",
                "query-hints-05.trig",
                "query-hints-05.srx"
                )
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
                    whereClause.getQueryOptimizer());

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

//                assertEquals("true",
//                        sp.getQueryHint(QueryHints.HASH_JOIN));

            }

        } // end check of the AST.

        /*
         * Check the query plan.
         */
        {

            /*
             * Should be two pipeline joins.   
             * 
             * The first pipeline join is (?x rdfs:label ?o)
             * 
             * The second pipeline join is (?x rdf:type foaf:Person)
             */
            @SuppressWarnings("rawtypes")
            final PipelineJoin[] joins = BOpUtility.toList(
                    BOpUtility.visitAll(astContainer.getQueryPlan(),
                            PipelineJoin.class)).toArray(new PipelineJoin[0]);
            
            assertEquals("#joins", 2, joins.length);
            {

                @SuppressWarnings("rawtypes")
                final PipelineJoin join = joins[1];
                
                assertEquals(RDFS.LABEL, ((IV<?,?>)join.getPredicate().get(1/* p */)
                        .get()).getValue());
                
            }

            {
            
                @SuppressWarnings("rawtypes")
                final PipelineJoin join = joins[0];
                
                assertEquals(RDF.TYPE, ((IV<?,?>) join.getPredicate().get(1/* p */)
                        .get()).getValue());

                final IPredicate<?> pred = join.getPredicate();
                
                assertEquals(SPOKeyOrder.PCSO, pred.getKeyOrder());
                
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
     *   # Set the global chunkSize.  This will apply to the SELECT, sub-SELECT
     *   # (including the hash index and hash join operators used to realize the
     *   # sub-select), the SliceOp, etc.  This gets overridden for both SPs in 
     *   # the query.
     *   hint:Query hint:chunkSize "5" .
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
     *          } limit 10
     *   }
     *   
     * } limit 20
     * </pre>
     */
    public void test_query_hints_06() throws Exception {

        final ASTContainer astContainer = new TestHelper("query-hints-06")
                .runTest();

        // Check the optimized AST.
        {

            if (log.isInfoEnabled())
                log.info(BOpUtility.toString(astContainer.getOptimizedAST()));

            // The top-level QueryRoot.
            final QueryRoot queryRoot = astContainer.getOptimizedAST();

            // The top-level WHERE clause.
            final JoinGroupNode whereClause = (JoinGroupNode) queryRoot
                    .getWhereClause();

            /*
             * There should be just one subquery. It could be a
             * NamedSubqueryRoot or a SubqueryRoot [we need to know this to know
             * whether there should be an INCLUDE in the optimized AST].
             */
            final SubqueryBase subqueryRoot;
            {
                final Iterator<SubqueryBase> it = BOpUtility.visitAll(
                        astContainer.getOptimizedAST(), SubqueryBase.class);
                assertTrue(it.hasNext());
                subqueryRoot = it.next();
                assertFalse(it.hasNext());
            }
            
            // The subquery was lifted out as a NamedSubqueryRoot.
            assertNotNull(astContainer.getOptimizedAST().getNamedSubqueries());

            // The INCLUDE for that NamedSubquery
            final NamedSubqueryInclude nsi = BOpUtility.getOnly(whereClause,
                    NamedSubqueryInclude.class);

            // Extract the SPs. There should be TWO (2).
            final List<StatementPatternNode> sps = BOpUtility.toList(queryRoot,
                    StatementPatternNode.class);

            // Only 2 left after we remove the query hints.
            assertEquals(2, sps.size());

            // There is only one SP in the top-level WHERE clause.
            final StatementPatternNode queryRootSP = BOpUtility.getOnly(
                    whereClause, StatementPatternNode.class);
            
            // There is only one SP in the named subquery WHERE clause.
            final StatementPatternNode subqueryRootSP = BOpUtility.getOnly(
                    subqueryRoot.getWhereClause(), StatementPatternNode.class);

            /*
             * The hint to disable the join order optimizer should show up on
             * the JoinGroupNodes. For the sample query, that means both the
             * top-level WHERE clause and the WHERE clause on the sub-query.
             */
            assertSameIteratorAnyOrder(
                    new Object[] { whereClause, subqueryRoot.getWhereClause() },
                    visitQueryHints(astContainer.getOptimizedAST(),
                            QueryHints.OPTIMIZER));

            /*
             * The hint:chunkSize annotation is NOT found. It was turned into
             * the PipelineOp.Annotations.CHUNK_CAPACITY annotation.
             */
            assertEquals(
                    0,
                    countQueryHints(astContainer.getOptimizedAST(),
                            QueryHints.CHUNK_SIZE));

            /*
             * The hint:chunkSize annotation was applied the the entire query,
             * including the sub-query.
             */
            assertNotNull(queryRoot.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(queryRoot.getProjection().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(queryRoot.getWhereClause().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(queryRootSP.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(queryRoot.getSlice().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            
            /**
             * Note: The ASTSparql11SubqueryOptimizer runs *after* the
             * ASTQueryHintOptimizer. It needs to explicitly copy over the query
             * hints from the original subquery.
             * 
             * @see <a
             *      href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
             *      Clean up query hints </a>
             */
            assertNotNull(nsi.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(subqueryRoot.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));

            assertNotNull(subqueryRoot.getProjection().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(subqueryRoot.getWhereClause().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(subqueryRootSP.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertNotNull(subqueryRoot.getSlice().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));

//            assertSameIteratorAnyOrder(
//                    new Object[] { //
//                            // Main Query.
//                            queryRoot,//
//                            queryRoot.getProjection(), //
//                            whereClause, //
//                            queryRootSP,//
//                            nsi,//
//                            queryRoot.getSlice(),//
//                            // Subquery.
//                            subqueryRoot,//
//                            subqueryRoot.getProjection(),//
//                            subqueryRoot.getWhereClause(), //
//                            subqueryRootSP,//
//                            subqueryRoot.getSlice(),//
//                    },//
//                    visitQueryHints(queryRoot,
//                            PipelineOp.Annotations.CHUNK_CAPACITY));

            /*
             * hint:chunkSize was used to set a global default. we spot check a
             * few AST nodes here.
             */
            assertEquals("5",queryRoot.getProjection().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertEquals("5",subqueryRoot.getProjection().getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));

            /*
             * hint:chunkSize was also used to override that global default. we
             * check those overrides next.
             */            
            assertEquals("1001",queryRootSP.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            assertEquals("251",subqueryRootSP.getQueryHint(PipelineOp.Annotations.CHUNK_CAPACITY));
            
        } // end check of the AST.
        
        /*
         * Check the query plan.
         * 
         * Note: This is where we make sure that the vectoring overrides for
         * hint:chunkSize actually made it into the physical query plan.
         */
        {
         
            final PipelineOp queryPlan = astContainer.getQueryPlan();

            if (log.isInfoEnabled())
                log.info(BOpUtility.toString(astContainer.getQueryPlan()));

            // Check the PipelineJoins.
            {
                @SuppressWarnings("rawtypes")
                final Iterator<PipelineJoin> itr = BOpUtility.visitAll(
                        queryPlan, PipelineJoin.class);

                {
                    final PipelineJoin<?> join = itr.next();

                    // Make sure this is the right join.
                    assertEquals(Var.var("o"), join.getPredicate()
                            .get(2/* o */));

                    assertEquals(join.toString(), 1001, join.getChunkCapacity());
                }

                {
                    final PipelineJoin<?> join = itr.next();

                    // Make sure this is the right join.
                    assertTrue(join.getPredicate().get(2/* s */).isConstant());

                    assertEquals(join.toString(), 251, join.getChunkCapacity());
                }

            }
            
            // Check the SliceOps.
            {

                final List<SliceOp> sliceOps = BOpUtility.toList(queryPlan,
                        SliceOp.class);

                assertEquals(2, sliceOps.size());

                final SliceOp queryRootSliceOp = sliceOps.get(0).getLimit() == 20L ? sliceOps
                        .get(0) : sliceOps.get(1);

                final SliceOp subqueryRootSliceOp = sliceOps.get(0).getLimit() == 10L ? sliceOps
                        .get(0) : sliceOps.get(1);

                // make we got distinct AST nodes.
                assertFalse(queryRootSliceOp == subqueryRootSliceOp);

                assertEquals(5, queryRootSliceOp.getChunkCapacity());

                assertEquals(5, subqueryRootSliceOp.getChunkCapacity());

            }
            
            // Check the ProjectionOps.
            {

                final List<ProjectionOp> projectionOps = BOpUtility.toList(queryPlan,
                        ProjectionOp.class);

                assertEquals(2, projectionOps.size());

                final ProjectionOp queryRootProjectionOp = projectionOps.get(0)
                        .getVariables().length == 2 ? projectionOps.get(0)
                        : projectionOps.get(1);

                final ProjectionOp subqueryRootProjectionOp = projectionOps
                        .get(0).getVariables().length == 1 ? projectionOps
                        .get(0) : projectionOps.get(1);

                // make we got distinct AST nodes.
                assertFalse(queryRootProjectionOp == subqueryRootProjectionOp);

                assertEquals(5, queryRootProjectionOp.getChunkCapacity());

                /*
                 * Note: The ASTSparql11SubqueryOptimizer needs to explicitly
                 * copy across the query hints to the new named subquery root in
                 * order for the query hints to show up on the subquery's
                 * ProjectionNode.
                 */
                assertEquals(5, subqueryRootProjectionOp.getChunkCapacity());

            }

            /*
             * Check the operators that realize the named subquery evaluation
             * and its INCLUDE into the generated query plan.
             */

            final INamedSubqueryOp namedSubqueryOp = BOpUtility.getOnly(
                    queryPlan, INamedSubqueryOp.class);
            
            assertEquals(5, ((PipelineOp) namedSubqueryOp).getChunkCapacity());
            
            final SolutionSetHashJoinOp solutionSetHashJoin = BOpUtility
                    .getOnly(queryPlan, SolutionSetHashJoinOp.class);

            assertEquals(5, solutionSetHashJoin.getChunkCapacity());

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

    /**
     * Unit test for {@link QueryHints#MAX_PARALLEL} when applied to a SERVICE node.
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
     *   hint:Prior hint:maxParallel 1001 .
     * 
     * }
     * </pre>
     * 
     * TODO Test for PipelineJoin and other things as well.
     */
    public void test_query_hints_10() throws Exception {

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

            astContainer = new TestHelper("query-hints-10").runTest();

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

                assertEquals(join.toString(), 13, join.getMaxParallel());

            }

        }

    }

}
