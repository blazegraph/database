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
import java.util.UUID;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.hints.QueryHintException;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.TestASTQueryHintOptimizer;
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
     * A simple SELECT query with some query hints. This does not verify much
     * beyond the willingness to accept the query hints and silently remove them
     * from the AST model (the query will fail if the query hints are not
     * removed as they will not match anything in the data).
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
                
                assertEquals(RDFS.LABEL, ((IV)join.getPredicate().get(1/* p */)
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

                assertEquals(RDF.TYPE, ((IV)join.getPredicate().get(1/* p */)
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
        
            new TestHelper("query-hints-03");

        } catch (Throwable t) {
            
            assertTrue(InnerCause.isInnerCause(t, QueryHintException.class));
        
        }
        
    }
    
}
