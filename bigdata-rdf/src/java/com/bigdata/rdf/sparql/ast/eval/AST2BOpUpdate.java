/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 17, 2012
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.rdf.update.ChunkedResolutionOp;
import com.bigdata.bop.rdf.update.CommitOp;
import com.bigdata.bop.rdf.update.InsertStatementsOp;
import com.bigdata.bop.rdf.update.ParseOp;
import com.bigdata.bop.rdf.update.RemoveStatementsOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractGraphDataUpdate;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.UpdateType;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * Class handles SPARQL update query plan generation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO When translating, the notion that we might translate either
 *          incrementally or all operations in the sequence at once is related
 *          to the notion of interactive evaluation which would help to enable
 *          the RTO.
 * 
 *          FIXME Where are the DataSet(s) for the update operations in the
 *          sequence coming from? I assume that they will be attached to each
 *          {@link Update}.
 * 
 *          FIXME DELETE DATA with triples addresses the "defaultGraph". Since
 *          that is the RDF merge of all named graphs for the bigdata quads
 *          mode, the operation needs to be executed separately for each
 *          {@link ISPO} in which the context position is not bound. For such
 *          {@link ISPO}s, we need to remove everything matching the (s,p,o) in
 *          the quads store (c is unbound).
 *          <p>
 *          This is not being handled coherently right now. We are handling this
 *          by binding [c] to the nullGraph when removing data, but it looks
 *          like some triples are being allowed in without [c] being bound.
 */
public class AST2BOpUpdate extends AST2BOpUtility {

    private static final Logger log = Logger.getLogger(AST2BOpUpdate.class);
    
    /**
     * 
     */
    public AST2BOpUpdate() {
        super();
    }
    
    /**
     * The s,p,o, and c variable names used for binding sets which model
     * {@link Statement}s.
     */
    private static final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
            .var("o"), c = Var.var("c");

    /**
     * Convert the query (generates an optimized AST as a side-effect).
     * 
     * TODO Top-level optimization pass over the {@link UpdateRoot}. We have to
     * resolve IVs, etc., etc. See if we can just use the
     * {@link DefaultOptimizerList} and modify the {@link IASTOptimizer}s that
     * we need to work against update as well as query. (We will need a common
     * API for the WHERE clause). We will need to expand the AST optimizer test
     * suite for this.
     * 
     * TODO Focus on getting the assertion/retraction patterns right in the
     * different database modes).
     */
    protected static void optimizeUpdateRoot(final AST2BOpContext context) {

        final ASTContainer astContainer = context.astContainer;

        // TODO Clear the optimized AST.
        // astContainer.clearOptimizedUpdateAST();

        /*
         * TODO Build up the optimized AST for the UpdateRoot for each Update to
         * be executed. Maybe do this all up front before we run anything since
         * we might reorder or regroup some operations (e.g., parallelized LOAD
         * operations, parallelized INSERT data operations, etc).
         */
        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();

        /*
         * Evaluate each update operation in the optimized UPDATE AST in turn.
         */
        for (Update op : updateRoot) {

        }

    }

    /**
     * Generate physical plan for the update operations (attached to the AST as
     * a side-effect).
     * 
     * FIXME Follow the general pattern of {@link AST2BOpUtility}, refactoring
     * the update plans from TestUpdateBootstrap into this class and adding
     * data-driven UPDATE tests as we go.
     * 
     * @throws Exception
     */
    protected static PipelineOp convertUpdate(final AST2BOpContext context) throws Exception {

        final ASTContainer astContainer = context.astContainer;

        // FIXME Change this to the optimized AST.
        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();

        /*
         * Evaluate each update operation in the optimized UPDATE AST in turn.
         */
        PipelineOp left = null;
        for (Update op : updateRoot) {

            left = convert(left, op, context);

        }

        /*
         * Commit.
         * 
         * Note: Not required on cluster.
         * 
         * Note: Not required unless the end of the UpdateRoot or we desired a
         * checkpoint on the sequences of operations.
         * 
         * TODO The commit really can not happen until the update plan(s) were
         * known to execute successfully. We could do that with an AT_ONCE
         * annotation on the CommitOp or we could just invoke commit() at
         * appropriate checkpoints in the UPDATE operation.
         */
        if (!context.isCluster()) {
            left = new CommitOp(leftOrEmpty(left), NV.asMap(//
                    new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                    new NV(CommitOp.Annotations.TIMESTAMP, context
                            .getTimestamp()),//
                    new NV(CommitOp.Annotations.PIPELINED, false)//
                    ));
        }

        // Set as annotation on the ASTContainer.
        astContainer.setQueryPlan(left);
        
        return left;

    }

    /**
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception 
     */
    private static PipelineOp convert(PipelineOp left, final Update op,
            final AST2BOpContext context) throws Exception {

        final UpdateType updateType = op.getUpdateType();

        switch (updateType) {
//        case Add:
//            break;
//        case Copy:
//            break;
        case Create: {
            left = convertCreateGraph(left, (CreateGraph) op, context);
            break;
        }
//        case Move:
//            break;
        case Clear:
        case Drop:
            left = convertClearOrDropGraph(left, (DropGraph) op, context);
            break;
        case InsertData:
        case DeleteData:
            left = convertGraphDataUpdate(left, (AbstractGraphDataUpdate) op,
                    context);
            break;
        case Load:
            left = convertLoadGraph(left, (LoadGraph) op, context);
            break;
//        case DeleteInsert:
//            break;
        default:
            throw new UnsupportedOperationException("updateType=" + updateType);
        }

        return left;
        
    }

    /**
     * <pre>
     * LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception
     */
    private static PipelineOp convertLoadGraph(PipelineOp left,
            final LoadGraph op, final AST2BOpContext context) throws Exception {

        /*
         * Parse the file.
         * 
         * Note: After the parse step, the remainder of the steps are just like
         * INSERT DATA.
         */
        {

            final Map<String, Object> anns = new HashMap<String, Object>();

            anns.put(BOp.Annotations.BOP_ID, context.nextId());

            // required.
            anns.put(ParseOp.Annotations.SOURCE_URI, op.getSourceGraph()
                    .getValue());

            if(op.isSilent())
                anns.put(ParseOp.Annotations.SILENT, true);

            // optional.
            if (op.getTargetGraph() != null)
                anns.put(ParseOp.Annotations.TARGET_URI, op.getTargetGraph());

            // required.
            anns.put(ParseOp.Annotations.TIMESTAMP, context.getTimestamp());
            anns.put(ParseOp.Annotations.RELATION_NAME,
                    new String[] { context.getNamespace() });

            /*
             * TODO 100k is the historical default for the data loader. We
             * generally want to parse a lot of data at once and vector it in
             * big chunks. However, we could have a lot more parallelism with
             * the query engine. So, if there are multiple source URIs to be
             * loaded, then we might want to reduce the vector size (or maybe
             * not, probably depends on the JVM heap).
             */
            anns.put(ParseOp.Annotations.CHUNK_CAPACITY, 100000);

            left = new ParseOp(leftOrEmpty(left), anns);

        }

        /*
         * Append the pipeline operations to add/resolve IVs against the lexicon
         * and insert/delete statemetns.
         */
        left = addInsertOrDeleteDataPipeline(left, true/* insert */, context);

        /*
         * Execute the update.
         */
        executeUpdate(left, null/* bindingSets */, context);

        // Return null since pipeline was evaluated.
        return null;
        
    }

    /**
     * Note: Bigdata does not support empty graphs, so {@link UpdateType#Clear}
     * and {@link UpdateType#Drop} have the same semantics.
     * 
     * <pre>
     * DROP ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     */
    private static PipelineOp convertClearOrDropGraph(PipelineOp left,
            final DropGraph op, final AST2BOpContext context) {

        final TermNode targetGraphNode = op.getTargetGraph();

        final Scope scope = op.getScope();

        if (targetGraphNode != null) {

            /*
             * Addressing a specific graph.
             */

            final BigdataURI targetGraph = (BigdataURI) targetGraphNode
                    .getValue();

            if (targetGraph == null) {
                /*
                 * Deleting with a null would remove everything, so make sure
                 * this is a valid URI.
                 */
                throw new AssertionError();
            }

            context.db.removeStatements(null/* s */, null/* p */, null/* o */,
                    targetGraph);

        } else if (scope != null) {

            /*
             * Addressing either the defaultGraph or the named graphs.
             * 
             * FIXME We need access to the data set for this. Where is it
             * attached on the UPDATE AST?
             */
        
            throw new UnsupportedOperationException();

        } else {  
            
            /*
             * Addressing ALL graphs.
             */

            context.db.removeStatements(null/* s */, null/* p */, null/* o */,
                    null/* c */);
            
        }

        return left;
        
    }

    /**
     * If the graph already exists (context has at least one statement), then
     * this is an error (unless SILENT). Otherwise it is a NOP.
     * 
     * @param left
     * @param op
     * @param context
     * @return
     */
    private static PipelineOp convertCreateGraph(final PipelineOp left,
            final CreateGraph op, final AST2BOpContext context) {

        if (!op.isSilent()) {

            final IV<?, ?> c = ((CreateGraph) op).getTargetGraph().getValue()
                    .getIV();

            if (context.db.getAccessPath(null/* s */, null/* p */, null/* o */,
                    c).rangeCount(false/* exact */) != 0) {

                throw new RuntimeException("Graph exists: " + c.getValue());

            }

        }

        return left;

    }

    /**
     * <pre>
     * INSERT DATA -or- DELETE DATA
     * </pre>
     * 
     * @param left
     * @param op
     * @param context
     * @return
     * @throws Exception
     */
    private static PipelineOp convertGraphDataUpdate(PipelineOp left,
            final AbstractGraphDataUpdate op, final AST2BOpContext context)
            throws Exception {

        /*
         * Convert the statements to be asserted or retracted into an
         * IBindingSet[].
         */
        final IBindingSet[] bindingSets;
        {

            // Note: getTargetGraph() is not defined for INSERT/DELETE DATA.
            final ConstantNode c = null;// op.getTargetGraph();

            @SuppressWarnings("rawtypes")
            IV targetGraphIV = null;

            if (c != null) {

                targetGraphIV = c.getValue().getIV();

            }

            if (targetGraphIV == null && context.isQuads()) {

                /*
                 * 
                 * TODO Extract nullGraphIV into AST2BOpContext and cache.
                 * Ideally, this should always be part of the Vocabulary and the
                 * IVCache should be set (which is always true for the
                 * vocabulary).
                 */
                final BigdataURI nullGraph = context.db.getValueFactory()
                        .asValue(BD.NULL_GRAPH);
                context.db.addTerm(nullGraph);
                targetGraphIV = nullGraph.getIV();
                targetGraphIV.setValue(nullGraph);

            }

            bindingSets = getData(op.getData(), targetGraphIV,
                    context.isQuads());

        }

        final boolean insert;
        switch (op.getUpdateType()) {
        case InsertData:
            insert = true;
            break;
        case DeleteData:
            insert = false;
            break;
        default:
            throw new UnsupportedOperationException(op.getUpdateType().name());
        }

        /*
         * Append the pipeline operations to add/resolve IVs against the lexicon
         * and insert/delete statemetns.
         */

        left = addInsertOrDeleteDataPipeline(left, insert, context);

        /*
         * Execute the update.
         */
        executeUpdate(left, bindingSets, context);
        
        // Return null since pipeline was evaluated.
        return null;

    }

    /**
     * @param left
     * @param b
     * @param context
     * @return
     */
    private static PipelineOp addInsertOrDeleteDataPipeline(PipelineOp left,
            final boolean insert, final AST2BOpContext context) {
        
        /*
         * Resolve/add terms against the lexicon.
         * 
         * TODO Must do SIDs support. Probably pass the database mode in as an
         * annotation. See StatementBuffer.
         */
        left = new ChunkedResolutionOp(leftOrEmpty(left), NV.asMap(
                //
                new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                        .getTimestamp()),//
                new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                        new String[] { context.getLexiconNamespace() })//
                ));

        /*
         * Insert / remove statements.
         * 
         * Note: namespace is the triple store, not the spo relation. This is
         * because insert is currently on the triple store for historical SIDs
         * support.
         * 
         * Note: This already does TM for SIDs mode.
         * 
         * TODO This must to TM for the subject-centric text index.
         * 
         * TODO This must be able to do TM for triples+inference.
         */
        if (insert) {
            left = new InsertStatementsOp(leftOrEmpty(left), NV.asMap(
                    new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                    new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                            .getTimestamp()),//
                    new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                            new String[] { context.getNamespace() })//
                    ));
        } else {
            left = new RemoveStatementsOp(leftOrEmpty(left), NV.asMap(
                    new NV(BOp.Annotations.BOP_ID, context.nextId()),//
                    new NV(ChunkedResolutionOp.Annotations.TIMESTAMP, context
                            .getTimestamp()),//
                    new NV(ChunkedResolutionOp.Annotations.RELATION_NAME,
                            new String[] { context.getNamespace() })//
                    ));
        }

        return left;
    }

    /**
     * Convert an {@link ISPO}[] into an {@link IBindingSet}[].
     * 
     * @param data
     *            The {@link ISPO}[].
     * @param targetGraph
     *            The target graph (optional, but required if quads).
     * @param quads
     *            <code>true</code> iff the target {@link AbstractTripleStore}
     *            is in quads mode.
     * 
     * @return The {@link IBindingSet}[].
     * 
     *         TODO Either we need to evaluate this as a query NOW or this needs
     *         to be pumped into a hash index associated with the query plan in
     *         order to be available when there is more than one INSERT DATA or
     *         REMOVE DATA operation (or simply more than one UPDATE operation).
     *         <p>
     *         That hash index could be joined into the solutions immediate
     *         before we undertake the chunked resolution operation which then
     *         flows into the add/remove statements operation.
     *         <p>
     *         Variables in the query can not be projected into this operation
     *         without causing us to insert/delete the cross product of those
     *         variables, which has no interesting effect.
     *         <p>
     *         The advantage of running one plan per {@link Update} is that the
     *         data can be flowed naturally into the {@link IRunningQuery}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static IBindingSet[] getData(final ISPO[] data,
            final IV<?, ?> targetGraph, final boolean quads) {

        final IBindingSet[] bsets = new IBindingSet[data.length];

        for (int i = 0; i < data.length; i++) {

            final ISPO spo = data[i];

            final IBindingSet bset = bsets[i] = new ListBindingSet();

            bset.set(s, new Constant(spo.s()));

            bset.set(p, new Constant(spo.p()));

            bset.set(o, new Constant(spo.o()));

            Constant g = null;

            if (spo.c() != null)
                g = new Constant(spo.c());

            if (quads && g == null) {

                g = new Constant(targetGraph);

            }

            if (g != null) {

                bset.set(c, g);

            }

        }

        return bsets;
    
    }

    /**
     * Execute the update plan.
     * 
     * @param left
     * @param bindingSets
     *            The source solutions.
     * @param context
     * 
     * @throws UpdateExecutionException
     */
    static private void executeUpdate(final PipelineOp left,
            IBindingSet[] bindingSets, final AST2BOpContext context)
            throws Exception {

        if (left == null)
            throw new IllegalArgumentException();

        if(bindingSets == null) {
            bindingSets = EMPTY_BINDING_SETS;
        }
        
        if (context == null)
            throw new IllegalArgumentException();

        IRunningQuery runningQuery = null;
        try {

            // Submit update plan for evaluation.
            runningQuery = context.queryEngine.eval(left, bindingSets);

            // Wait for the update plan to complete.
            runningQuery.get();

        } finally {

            if (runningQuery != null) {
            
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            
        }

    }

    private static final IBindingSet[] EMPTY_BINDING_SETS = new IBindingSet[0];
    
}
