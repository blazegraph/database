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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.GraphQueryResultImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.sail.SailException;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.sail.RunningQueryCloseableIterator;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Helper class for evaluating SPARQL queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEvalHelper {

    /**
     * A logger whose sole purpose is to log the SPARQL queries which are being
     * evaluated.
     */
    private static final Logger log = Logger.getLogger(ASTEvalHelper.class);
    
    /**
     * Setup the input binding set which will be fed into the query pipeline.
     * 
     * @param bs
     *            The given source binding set (optional).
     * 
     * @return Either the argument or an empty {@link IBindingSet} if the
     *         argument was <code>null</code>.
     */
    static private IBindingSet wrapSource(final IBindingSet bs)
            throws SailException {

        if (bs != null) {
        
            /*
             * A single input binding set will be fed into the query pipeline
             * using the supplied bindings.
             */

            return bs;

        } else {
            
            /*
             * A single empty input binding set will be fed into the query
             * pipeline.
             */
            
            return new ListBindingSet();
            
        }
        
    }
    
    /**
     * Batch resolve {@link Value}s to {@link IV}s.
     * 
     * @param store
     *            The KB instance.
     * @param bs
     *            The binding set (may be empty or <code>null</code>).
     * 
     * @return A binding set having resolved {@link IV}s.
     * 
     * @throws QueryEvaluationException
     */
    static private BindingSet batchResolveIVs(final AbstractTripleStore store,
            final BindingSet bs) throws QueryEvaluationException {

        if (bs == null) {
            // Use an empty binding set.
            return new QueryBindingSet();
        }

        if (bs.size() == 0) {
            // Fast path if empty.
            return bs;
        }

        final Object[] tmp = new BigdataValueReplacer(store).replaceValues(
                null/* dataset */, new BindingSet[] { bs });

        final BindingSet[] a = (BindingSet[]) tmp[1];

        return a[0];

    }
    
    /**
     * Evaluate a boolean query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     *            
     * @return <code>true</code> if there are any solutions to the query.
     * 
     * @throws QueryEvaluationException
     */
    static public boolean evaluateBooleanQuery(
            final AbstractTripleStore store,
            final ASTContainer astContainer, final BindingSet bs)
            throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet bset = toBindingSet(batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bset);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        // Note: We do not need to materialize anything for ASK.
        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        CloseableIteration<BindingSet, QueryEvaluationException> itr = null;
        try {
            itr = ASTEvalHelper.evaluateQuery(
                    astContainer,
                    context,
                    bset
                    , materializeProjectionInQuery//
                    , new IVariable[0]// required
                    );
            return itr.hasNext();
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
    }
        
    /**
     * Evaluate a SELECT query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     *            
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public TupleQueryResult evaluateTupleQuery(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet bs) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet bset = toBindingSet(batchResolveIVs(store, bs));

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bset);

        // Get the projection for the query.
        final IVariable<?>[] projected = astContainer.getOptimizedAST()
                .getProjection().getProjectionVars();

        final List<String> projectedSet = new LinkedList<String>();

        for (IVariable<?> var : projected)
            projectedSet.add(var.getName());

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        return new TupleQueryResultImpl(projectedSet,
                ASTEvalHelper.evaluateQuery(
                        astContainer,
                        context,
                        bset
                        ,materializeProjectionInQuery//
                        , projected//
                        ));

    }
    
    /**
     * Evaluate a CONSTRUCT/DESCRIBE query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param bs
     *            The initial solution to kick things off.
     * 
     * @throws QueryEvaluationException
     */
    public static GraphQueryResult evaluateGraphQuery(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet bs) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();
        
        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet bset = toBindingSet(batchResolveIVs(store, bs));
        
        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, bset);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
        
        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        return new GraphQueryResultImpl(//
                optimizedQuery.getPrefixDecls(), //
                new ASTConstructIterator(store, //
                        optimizedQuery.getConstruct(), //
                        ASTEvalHelper.evaluateQuery(
                                astContainer,
                                context,
                                bset//
                                ,materializeProjectionInQuery//
                                ,optimizedQuery.getProjection()
                                        .getProjectionVars()//
                                )));

    }
    
    /**
     * Evaluate a query plan (core method).
     * 
     * @param database
     *            The {@link AbstractTripleStore} view against which the query
     *            will be evaluated.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The source binding set.
     * @param materializeProjectionInQuery
     *            When <code>true</code>, the projection was materialized within
     *            query plan. When <code>false</code>, this method will take
     *            responsibility for that materialization step.
     * @param required
     *            The variables which must be materialized. Only materialized
     *            variables will be reported in the output solutions. This MAY
     *            be <code>null</code> to materialize all variables in the
     *            solutions. If MAY be empty to materialize NONE of the
     *            variables in the solutions (in which case all solutions will
     *            be empty).
     * 
     * @return An iteration which may be used to read Sesame {@link BindingSet}s
     *         containing the solutions for the query.
     * 
     * @throws QueryEvaluationException
     */
    static private CloseableIteration<BindingSet, QueryEvaluationException> evaluateQuery(
            final ASTContainer astContainer,
            final AST2BOpContext ctx,            
            final IBindingSet bs, 
            final boolean materializeProjectionInQuery,
            final IVariable<?>[] required) throws QueryEvaluationException {

        if(log.isInfoEnabled()) {
            // Log the SPARQL query string.
            log.info(astContainer.getQueryString());
        }
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        
        IRunningQuery runningQuery = null;
        try {

            // Submit query for evaluation.
            runningQuery = ctx.queryEngine.eval(queryPlan,wrapSource(bs));

            /*
             * Wrap up the native bigdata query solution iterator as Sesame
             * compatible iteration with materialized RDF Values.
             */
            return iterator(runningQuery, ctx.db,
                    materializeProjectionInQuery, required);

        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            throw new QueryEvaluationException(t);
        }

    }

    /**
     * Convert a Sesame {@link BindingSet} into a bigdata {@link IBindingSet}.
     * 
     * @param src
     *            The {@link BindingSet}.
     *            
     * @return The {@link IBindingSet}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static IBindingSet toBindingSet(final BindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final ListBindingSet bindingSet = new ListBindingSet();

        final Iterator<Binding> itr = src.iterator();

        while(itr.hasNext()) {

            final Binding binding = itr.next();

            final IVariable<IV> var = com.bigdata.bop.Var.var(binding.getName());
            
            final IV iv = ((BigdataValue) binding.getValue()).getIV();
            
            final IConstant<IV> val = new Constant<IV>(iv);
            
            bindingSet.set(var, val);
            
        }
        
        return bindingSet;
        
    }
    
    /**
     * Wrap {@link IRunningQuery} with the logic to materialize {@link IV}s as
     * RDF {@link Value}s.
     * 
     * @param runningQuery
     *            The {@link IRunningQuery}.
     * @param db
     *            The view of the {@link AbstractTripleStore} against which the
     *            query is running.
     * @param materializeProjectionInQuery
     *            When <code>true</code>, the projection was materialized within
     *            query plan. When <code>false</code>, this method will take
     *            responsibility for that materialization step.
     * @param required
     *            The variables which must be materialized (optional).
     * 
     * @return A Sesame {@link CloseableIteration} which will drain
     *         {@link BindingSet}s of materialized RDF {@link Value}s.
     */
    private static CloseableIteration<BindingSet, QueryEvaluationException> iterator(
            final IRunningQuery runningQuery, final AbstractTripleStore db,
            final boolean materializeProjectionInQuery,
            final IVariable<?>[] required) {
    
        /*
         * FIXME We should not dechunk just to rechunk here. This is not very
         * efficient.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/483 (Eliminate
         * unnecessary chunking and dechunking)
         */
        
        // Dechunkify the running query and monitor the Sesame iterator.
        final ICloseableIterator<IBindingSet> it1 = iterator(runningQuery);
        
        // Wrap in an IChunkedOrderedIterator
        final IChunkedOrderedIterator<IBindingSet> it2 = 
            new ChunkedWrappedIterator<IBindingSet>(it1);

        final CloseableIteration<BindingSet, QueryEvaluationException> it3; 
        if(materializeProjectionInQuery) {
        
            /*
             * The projection of the query is being materialized by the query
             * plan. All we have to do here is convert bigdata IBindingSets
             * consisting of IVs having cached BigdataValues to Sesame
             * BindingSets.
             */
            
            // Convert IVs in IBindingSets to Sesame BindingSets with Values.
            it3 = new Bigdata2Sesame2BindingSetIterator(it2);

        } else {
        
            /*
             * The projection of the query was not materialized by the query
             * plan. We need to layer in a chunked iterator which handles that
             * materialization step and then do the conversion into Sesame
             * BindingSet objects.
             */
            
            /*
             * Note: This captures the historical behavior, which was based on
             * the AbstractTripleStore's configuration properties for
             * chunkCapacity, chunkOfChunksCapacity, and the chunkTimeout. Those
             * properties still affect the rules engine but do not otherwise
             * effect query performance. Most query operators use the PipelineOp
             * annotations to control these properties. RDF Value
             * materialization is the exception. To correct this, I have lifted
             * out all these parameters here so we can override it based on
             * query annotations.
             * 
             * There are two basic code paths for RDF Value materialization: One
             * is the ChunkedMateralizationOp (it handles the "chunk" you feed
             * it as a "chunk" and is used for materialization for FILTERs). The
             * other is the BigdataBindingSetResolverator. Both call through to
             * LexiconRelation#getTerms().
             * 
             * Regarding [termsChunkSize] and [blobsChunkSize], on a cluster,
             * the operation is parallelized (by the ClientIndexView) on a
             * cluster regardless so there is no reason to ever run materialized
             * with more than one thread. Shard local resolution can be enabled
             * by setting [materializeProjectionInQuery:=true], but at the cost
             * of doing the materialization after a SLICE (if there is one in
             * the query). However, when running through the
             * BigdataBindingSetResolverator, there will be exactly one thread
             * materializing RDF values (because the iterator pattern is single
             * threaded) unless the chunkSize exceeds this threshold.
             */
            
            // Historical values.
//            final int chunkCapacity = db.getChunkCapacity();
//            final int chunkOfChunksCapacity = db.getChunkOfChunksCapacity();
//            final long chunkTimeout = db.getChunkTimeout();
//            final int termsChunkSize = 4000;
//            final int blobsChunkSize = 4000;
            
            // Values set based on query hints.
            final BOp query = runningQuery.getQuery();
            final int chunkCapacity = query.getProperty(
                    PipelineOp.Annotations.CHUNK_CAPACITY,
                    PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);
            final int chunkOfChunksCapacity = query.getProperty(
                    PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                    PipelineOp.Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);
            final long chunkTimeout = query.getProperty(
                    PipelineOp.Annotations.CHUNK_TIMEOUT,
                    PipelineOp.Annotations.DEFAULT_CHUNK_TIMEOUT);
            final int termsChunkSize = chunkCapacity;
            final int blobsChunkSize = chunkCapacity;
            
            // Convert bigdata binding sets to Sesame binding sets.
            it3 = new Bigdata2Sesame2BindingSetIterator(
                    // Materialize IVs as RDF Values.
                    new BigdataBindingSetResolverator(db, it2, required,
                            chunkCapacity, chunkOfChunksCapacity, chunkTimeout,
                            termsChunkSize, blobsChunkSize).start(db
                            .getExecutorService()));

        }
     
        return it3;
        
    }

    /**
     * Dechunkify the running query and monitor the Sesame iterator.
     * 
     * @param runningQuery
     *            The {@link IRunningQuery}.
     *            
     * @return An {@link ICloseableIterator} which has been dechunkified.
     */
    private static ICloseableIterator<IBindingSet> iterator(
            final IRunningQuery runningQuery) {

        // The iterator draining the query solutions.
        final IAsynchronousIterator<IBindingSet[]> it1 = runningQuery
                .iterator();

        // Dechunkify the original iterator
        final ICloseableIterator<IBindingSet> it2 = 
            new Dechunkerator<IBindingSet>(it1);

        // Monitor IRunningQuery and cancel if Sesame iterator is closed.
        final ICloseableIterator<IBindingSet> it3 =
            new RunningQueryCloseableIterator<IBindingSet>(runningQuery, it2);

        return it3;
        
    }
    
}
