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
 * Created on Mar 16, 2012
 */

package com.bigdata.rdf.sparql.ast.eval.update;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.rdf.update.ChunkedResolutionOp;
import com.bigdata.bop.rdf.update.CommitOp;
import com.bigdata.bop.rdf.update.InsertStatementsOp;
import com.bigdata.bop.rdf.update.ParseOp;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.InsertData;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;

/**
 * Bootstrapped test suite for core UPDATE functionality based on BOP
 * evaluation.
 * <p>
 * Note: We are not using BOP evaluation for SPARQL UPDATE at this time, so this
 * test is NOT being run in CI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestUpdateBootstrap.java 6167 2012-03-20 17:47:30Z thompsonbry
 *          $
 */
public class TestUpdateBootstrap extends AbstractASTEvaluationTestCase {

    private static final Logger log = Logger
            .getLogger(TestUpdateBootstrap.class);
    
    /**
     * 
     */
    public TestUpdateBootstrap() {
    }

    /**
     * @param name
     */
    public TestUpdateBootstrap(String name) {
        super(name);
    }

    /**
     * Unit test for inserting ground triples or quads.
     * 
     * <pre>
     * @prefix : <http://www.bigdata.com/> .
     * @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
     * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
     * INSERT DATA
     * { 
     *   GRAPH :g1 {
     *     :Mike rdf:type foaf:Person .
     *     :Bryan rdf:type foaf:Person .
     *     :Mike rdfs:label "Mike" .
     *     :Bryan rdfs:label "Bryan" .
     *     :DC rdfs:label "DC" .
     *   }
     * }
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_insert_data() throws Exception {

        final UpdateRoot updateRoot = new UpdateRoot();
        final ISPO[] data;
        {

            final InsertData op = new InsertData();

            updateRoot.addChild(op);

            final IV g1 = makeIV(valueFactory.createURI("http://www.bigdata.com/g1"));
            final IV mike = makeIV(valueFactory.createURI("http://www.bigdata.com/Mike"));
            final IV bryan = makeIV(valueFactory.createURI("http://www.bigdata.com/Bryan"));
            final IV dc = makeIV(valueFactory.createURI("http://www.bigdata.com/DC"));
            final IV rdfType = makeIV(valueFactory.asValue(RDF.TYPE));
            final IV rdfsLabel = makeIV(valueFactory.asValue(RDFS.LABEL));
            final IV foafPerson = makeIV(valueFactory.createURI("http://xmlns.com/foaf/0.1/Person"));
            final IV mikeL = makeIV(valueFactory.createLiteral("Mike"));
            final IV bryanL = makeIV(valueFactory.createLiteral("Bryan"));
            final IV dcL = makeIV(valueFactory.createLiteral("DC"));

            data = new ISPO[] { //
                    new SPO(mike, rdfType, foafPerson, g1, StatementEnum.Explicit),//
                    new SPO(bryan, rdfType, foafPerson, g1, StatementEnum.Explicit),//
                    new SPO(mike, rdfsLabel, mikeL, g1, StatementEnum.Explicit),//
                    new SPO(bryan, rdfsLabel, bryanL, g1, StatementEnum.Explicit),//
                    new SPO(dc, rdfsLabel, dcL, g1, StatementEnum.Explicit),//
            };
            
            /*
             * FIXME Finish this test up (again). It is running into a conflict
             * with the Value to IV resolution.  It seems that it should be using
             * BigdataValues here since the lexicon add/resolve step has not yet
             * been executed (it runs below). Note that this code is old. It was
             * a mock up for the evaluation of SPARQL UPDATE on the query engine.
             * We do not evaluate SPARQL update that way, so this class could just
             * go away.
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/573">
             * NullPointerException when attempting to INSERT DATA containing a
             * blank node </a>
             */
            //op.setData(data);
            fail("Finish test");
            
        }

        /*
         * Turn that AST Update operation into a pipeline bop to add the terms
         * and write the statements. This will be done by AST2BOpUtility, but I
         * can mock the translation target up first.
         */
 
        final ASTContainer astContainer = new ASTContainer(updateRoot);
        
        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        final long txId = ITx.UNISOLATED;
        int bopId = 1;
        final int resolutionId = bopId++;
        final int insertStatementsId = bopId++;
        final int commitId = bopId++;
        PipelineOp left = null;
        
        /*
         * Resolve/add terms against the lexicon.
         */
        left = new ChunkedResolutionOp(leftOrEmpty(left), NV.asMap(//
                new NV(BOp.Annotations.BOP_ID, resolutionId),//
                new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, txId),//
                new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                        new String[] { context.getLexiconNamespace() })//
                ));

        /*
         * Insert statements.
         * 
         * Note: namespace is the triple store, not the spo relation. This is
         * because insert is currently on the triple store for historical SIDs
         * support.
         * 
         * Note: This already does TM for SIDs mode.
         */
        left = new InsertStatementsOp(leftOrEmpty(left), NV.asMap(new NV(
                BOp.Annotations.BOP_ID, insertStatementsId),//
                new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, txId),//
                new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                        new String[] { context.getNamespace() })//
                ));
        
        /*
         * Commit.
         */
        left = new CommitOp(leftOrEmpty(left), NV.asMap(//
                new NV(BOp.Annotations.BOP_ID, commitId),//
                new NV(CommitOp.Annotations.TIMESTAMP, txId),//
                new NV(CommitOp.Annotations.PIPELINED, false)//
                ));

        /*
         * Populate the source solutions with the statements to be asserted.
         */
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {

            final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
                    .var("o"), c = Var.var("c");

            for(ISPO spo : data) {

                final ListBindingSet bset = new ListBindingSet();
                
                bset.set(s, new Constant(spo.s()));
                
                bset.set(p, new Constant(spo.p()));
                
                bset.set(o, new Constant(spo.o()));
                
                if (spo.c() != null)
                    bset.set(c, new Constant(spo.c()));
                
                bsets.add(bset);
                
            }

        }
        
        // Run the update.
        final IRunningQuery future = context.queryEngine.eval(left,
                bsets.toArray(new IBindingSet[bsets.size()]));
        
        // Look for errors.
        future.get();

        if (log.isInfoEnabled())
            log.info(store.dumpStore());
        
        {
            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI mike = f.createURI("http://www.bigdata.com/Mike");
            final BigdataURI bryan = f.createURI("http://www.bigdata.com/Bryan");
            final BigdataURI dc = f.createURI("http://www.bigdata.com/DC");
            final BigdataURI g1 = f.createURI("http://www.bigdata.com/g1");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfsLabel = f.asValue(RDFS.LABEL);
            final BigdataURI foafPerson = f.createURI("http://xmlns.com/foaf/0.1/Person");
            final BigdataLiteral mikeL = f.createLiteral("Mike");
            final BigdataLiteral bryanL = f.createLiteral("Bryan");
            final BigdataLiteral DCL = f.createLiteral("DC");

            final BigdataValue[] values = new BigdataValue[] { mike, bryan, dc,
                    g1, rdfType, rdfsLabel, foafPerson, mikeL, bryanL, DCL };

            // Batch resolve (read-only).
            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);
            
            // Verify IV assignment.
            for (BigdataValue v : values) {

                final IV<?, ?> iv = v.getIV();

                // a real IV.
                assertFalse(iv.isNullIV());
                
            }

            /*
             * Verify statements in the store.
             */
            assertTrue(store.hasStatement(mike, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(bryan, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(mike, rdfsLabel, mikeL, g1));
            assertTrue(store.hasStatement(bryan, rdfsLabel, bryanL, g1));
            assertTrue(store.hasStatement(dc, rdfsLabel, DCL, g1));

            /*
             * Check the mutation counters for the lexicon.
             */
            {

                final BOpStats stats = future.getStats().get(resolutionId);

                /*
                 * Note: This assumes that rdf:type and rdfs:label are not
                 * defined by the vocabulary.  We have to subtract out each
                 * Value which was declared by the vocabulary or is otherwise
                 * represented as a fully inline IV.
                 */
                
                long expectedCount = values.length;

                for (int i = 0; i < values.length; i++) {

                    if (values[i].getIV().isInline())
                        expectedCount--;

                }

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());
            
            }
            
            /*
             * Check the mutation counters for the statements relation.
             */
            {

                final BOpStats stats = future.getStats()
                        .get(insertStatementsId);

                final long expectedCount = 5;

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());

            }

        }
        
    }
    
    /**
     * Unit test for LOAD with quads.
     */
    public void test_load_data() throws Exception {
        
        final UpdateRoot updateRoot = new UpdateRoot();
        final LoadGraph op;
        {

            op = new LoadGraph();

            updateRoot.addChild(op);

            op.setSourceGraph(new ConstantNode(
                    makeIV(new URIImpl(
                            "file:src/test/java/com/bigdata/rdf/sparql/ast/eval/update/load_01.trig"))));

            /*
             * Note: This is loading into an unspecified graph since the target
             * was not set.
             */
            
        }

        /*
         * Turn that AST Update operation into a pipeline bop to add the terms
         * and write the statements. This will be done by AST2BOpUtility, but I
         * can mock the translation target up first.
         */
 
        final ASTContainer astContainer = new ASTContainer(updateRoot);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        final long txId = ITx.UNISOLATED;
        int bopId = 1;
        final int parseId = bopId++;
        final int resolutionId = bopId++;
        final int insertStatementsId = bopId++;
        final int commitId = bopId++;
        PipelineOp left = null;

        /*
         * Parse the file.
         * 
         * Note: After the parse step, the remainder of the steps are just like
         * INSERT DATA.
         */
        {
            final Map<String, Object> anns = new HashMap<String, Object>();

            anns.put(BOp.Annotations.BOP_ID, parseId);

            // required.
            anns.put(ParseOp.Annotations.SOURCE_URI, op.getSourceGraph()
                    .getValue());

            // optional.
            if (op.getTargetGraph() != null)
                anns.put(ParseOp.Annotations.TARGET_URI, op.getTargetGraph());

            // required.
            anns.put(ParseOp.Annotations.TIMESTAMP, txId);
            anns.put(ParseOp.Annotations.RELATION_NAME,
                    new String[] { context.getNamespace() });

            /*
             * Note: 100k is the historical default for the data loader. We
             * generally want to parse a lot of data at once and vector it in
             * big chunks.
             */
            anns.put(ParseOp.Annotations.CHUNK_CAPACITY, 100000);
            
            left = new ParseOp(leftOrEmpty(left), anns);

        }

        /*
         * Resolve/add terms against the lexicon.
         */
        left = new ChunkedResolutionOp(leftOrEmpty(left), NV.asMap(//
                new NV(ChunkedResolutionOp.Annotations.BOP_ID, resolutionId),//
                new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, txId),//
                new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                        new String[] { context.getLexiconNamespace() })//
                ));

        /*
         * Insert statements.
         */
        left = new InsertStatementsOp(leftOrEmpty(left), NV.asMap(new NV(
                BOp.Annotations.BOP_ID, insertStatementsId),//
                new NV(InsertStatementsOp.Annotations.TIMESTAMP, txId),//
                new NV(InsertStatementsOp.Annotations.RELATION_NAME,
                        new String[] { context.getNamespace() })//
                ));
        
        /*
         * Commit.
         */
        left = new CommitOp(leftOrEmpty(left), NV.asMap(//
                new NV(BOp.Annotations.BOP_ID, commitId),//
                new NV(CommitOp.Annotations.TIMESTAMP, txId),//
                new NV(CommitOp.Annotations.PIPELINED, false)//
                ));
        
        // Run the update.
        final IRunningQuery future = context.queryEngine.eval(left);
        
        // Look for errors.
        future.get();

        if (log.isInfoEnabled())
            log.info(store.dumpStore());
        
        {
            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI mike = f.createURI("http://www.bigdata.com/Mike");
            final BigdataURI bryan = f.createURI("http://www.bigdata.com/Bryan");
            final BigdataURI dc = f.createURI("http://www.bigdata.com/DC");
            final BigdataURI g1 = f.createURI("http://www.bigdata.com/g1");
            final BigdataURI rdfType = f.asValue(RDF.TYPE);
            final BigdataURI rdfsLabel = f.asValue(RDFS.LABEL);
            final BigdataURI foafPerson = f.createURI("http://xmlns.com/foaf/0.1/Person");
            final BigdataLiteral mikeL = f.createLiteral("Mike");
            final BigdataLiteral bryanL = f.createLiteral("Bryan");
            final BigdataLiteral DCL = f.createLiteral("DC");

            final BigdataValue[] values = new BigdataValue[] { mike, bryan, dc,
                    g1, rdfType, rdfsLabel, foafPerson, mikeL, bryanL, DCL };

            // Batch resolve (read-only).
            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);
            
            // Verify IV assignment.
            for (BigdataValue v : values) {

                final IV<?, ?> iv = v.getIV();

                // a real IV.
                assertFalse(iv.isNullIV());
                
            }

            /*
             * Verify statements in the store.
             */
            assertTrue(store.hasStatement(mike, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(bryan, rdfType, foafPerson, g1));
            assertTrue(store.hasStatement(mike, rdfsLabel, mikeL, g1));
            assertTrue(store.hasStatement(bryan, rdfsLabel, bryanL, g1));
            assertTrue(store.hasStatement(dc, rdfsLabel, DCL, g1));

            /*
             * Check the mutation counters for the lexicon.
             */
            {

                final BOpStats stats = future.getStats().get(resolutionId);

                /*
                 * Note: This assumes that rdf:type and rdfs:label are not
                 * defined by the vocabulary.  We have to subtract out each
                 * Value which was declared by the vocabulary or is otherwise
                 * represented as a fully inline IV.
                 */
                
                long expectedCount = values.length;

                for (int i = 0; i < values.length; i++) {

                    if (values[i].getIV().isInline())
                        expectedCount--;

                }

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());
            
            }
            
            /*
             * Check the mutation counters for the statements relation.
             */
            {

                final BOpStats stats = future.getStats()
                        .get(insertStatementsId);

                final long expectedCount = 5;

                assertEquals("mutationCount", expectedCount,
                        stats.mutationCount.get());

            }

        }
        
    }

    /**
     * Return either <i>left</i> wrapped as the sole member of an array or
     * {@link BOp#NOARGS} iff <i>left</i> is <code>null</code>.
     * 
     * @param left
     *            The prior operator in the pipeline (optional).
     * @return The array.
     */
    static protected BOp[] leftOrEmpty(final PipelineOp left) {

        return left == null ? BOp.NOARGS : new BOp[] { left };

    }

}
