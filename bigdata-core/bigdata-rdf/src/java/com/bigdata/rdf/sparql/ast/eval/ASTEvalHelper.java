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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
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
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.rdf.join.ChunkedMaterializationIterator;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.RunningQueryCloseableIterator;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.DeleteInsertGraph;
import com.bigdata.rdf.sparql.ast.DescribeModeEnum;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.cache.DescribeBindingsCollector;
import com.bigdata.rdf.sparql.ast.cache.DescribeCacheUpdater;
import com.bigdata.rdf.sparql.ast.cache.IDescribeCache;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution.DeferredResolutionResult;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;

import cutthecrap.utils.striterators.ICloseableIterator;
import info.aduna.iteration.CloseableIteration;

/**
 * Helper class for evaluating SPARQL queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEvalHelper {

    /**
     * A logger whose sole purpose is to log the SPARQL queries which are being
     * evaluated <strong>DO NOT USE THIS FOR OTHER PURPOSES !!! </strong>
     */
    private static final Logger log = Logger.getLogger(ASTEvalHelper.class);

    /**
     * Evaluate a boolean query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param globallyScopedBS
     *            The initial solution to kick things off.
     *            
     * @return <code>true</code> if there are any solutions to the query.
     * 
     * @throws QueryEvaluationException
     */
    static public boolean evaluateBooleanQuery(
            final AbstractTripleStore store,
            final ASTContainer astContainer,
            final BindingSet globallyScopedBS,
            final Dataset dataset)
            throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        final DeferredResolutionResult resolved;
        try {
        	// @see https://jira.blazegraph.com/browse/BLZG-1176
            resolved = ASTDeferredIVResolution.resolveQuery(
                store, astContainer, globallyScopedBS, dataset, context);
        } catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }

        if (resolved.dataset != null) {
            astContainer.getOriginalAST().setDataset(
                new DatasetNode(resolved.dataset, false/* update */));
        }


        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] globallyScopedBSAsList = toBindingSet(resolved.bindingSet) ;

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, globallyScopedBSAsList);

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
                    materializeProjectionInQuery,
                    new IVariable[0]// required
                    );
            return itr.hasNext();
        } finally {
            if (itr != null) {
                /**
                 * Ensure query is terminated. An interrupt during hasNext()
                 * should cause the query to terminate through itr.close().
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
                 *      BlockingBuffer.close() does not unblock threads </a>
                 */
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
     * @param globallyScopedBS
     *            The initial solution to kick things off.
     *            
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public TupleQueryResult evaluateTupleQuery(
            final AbstractTripleStore store,
            final ASTContainer astContainer,
            final QueryBindingSet globallyScopedBS,
            final Dataset dataset) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        final QueryRoot optimizedQuery = 
                optimizeQuery(astContainer, context, globallyScopedBS, dataset);
        
        // Get the projection for the query.
        final IVariable<?>[] projected = astContainer.getOptimizedAST()
                .getProjection().getProjectionVars();

        final List<String> projectedSet = new LinkedList<String>();

        for (IVariable<?> var : projected)
            projectedSet.add(var.getName());

        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        final CloseableIteration<BindingSet, QueryEvaluationException> itr = ASTEvalHelper
                .evaluateQuery(astContainer, context, 
                        materializeProjectionInQuery, projected);

        TupleQueryResult r = null;
        try {
            r = new TupleQueryResultImpl(projectedSet, itr);
            return r;
        } finally {
            if (r == null) {
                /**
                 * Ensure query is terminated if assignment to fails. E.g., if
                 * interrupted during the ctor.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
                 *      BlockingBuffer.close() does not unblock threads </a>
                 */
                itr.close();
            }
        }

    }

    /**
     * Evaluate a SELECT query without converting the results into openrdf
     * solutions.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The {@link ASTContainer}.
     * @param globallyScopedBS
     *            The initial solution to kick things off.
     * @param materialize
     *            When <code>true</code>, {@link IV}s will be materialized
     *            (their {@link IVCache} association will be set to the
     *            corresponding RDF {@link Value}). When <code>false</code>,
     *            this materialization step will be skipped. However, it is
     *            possible that {@link IV}s in the query plan will be
     *            materialized anyway (for example, materialization might be
     *            required to support FILTERs in the query).
     * 
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public ICloseableIterator<IBindingSet[]> evaluateTupleQuery2(
            final AbstractTripleStore store, final ASTContainer astContainer,
            final QueryBindingSet globallyScopedBS, final boolean materialize)
            throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] globallyScopedBSAsList = toBindingSet(globallyScopedBS) ;

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, globallyScopedBSAsList);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();
        
        // true iff we can materialize the projection inside of the query plan.
        final boolean materializeProjectionInQuery = materialize && context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        final List<String> projectedSet;

        if (materialize) {

            /*
             * Add a materialization step.
             */
            
            // Get the projection for the query.
            final IVariable<?>[] projected = astContainer.getOptimizedAST()
                    .getProjection().getProjectionVars();

            projectedSet = new LinkedList<String>();

            for (IVariable<?> var : projected)
                projectedSet.add(var.getName());

        } else {
        
            /*
             * Do not add a materialization step.
             */

            projectedSet = null;
            
        }

        doSparqlLogging(context);
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        
        IRunningQuery runningQuery = null;
        try {

            // Submit query for evaluation.
            runningQuery = context.queryEngine.eval(queryPlan, globallyScopedBSAsList);

            // The iterator draining the query solutions.
            final ICloseableIterator<IBindingSet[]> it1 = runningQuery
                    .iterator();

            final ICloseableIterator<IBindingSet[]> it2;

            if (materialize && !materializeProjectionInQuery
                    && !projectedSet.isEmpty()) {

                /*
                 * Materialize IVs as RDF Values.
                 * 
                 * Note: This is the code path when we want to materialize the
                 * IVs and we can not do so within the query plan because the
                 * query uses a SLICE. If we want to materialize IVs and there
                 * is no slice, then the materialization step is done inside of
                 * the query plan.
                 * 
                 * Note: This does not materialize the IVCache for inline IVs.
                 * The assumption is that the consumer is bigdata aware and can
                 * use inline IVs directly.
                 */
                
                // The variables to be materialized.
                final IVariable<?>[] vars = projectedSet
                        .toArray(new IVariable[projectedSet.size()]);

                // Wrap with chunked materialization logic.
                it2 = new ChunkedMaterializationIterator(vars,
                        context.db.getLexiconRelation(),
                        false/* materializeInlineIVs */, it1);

            } else {
                
                it2 = it1;
                
            }
            
            return it2;

        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            throw new QueryEvaluationException(t);
        }

    }
    
    /**
     * Optimize a SELECT query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The {@link ASTContainer}.
     * @param globallyScopedBS
     *            The initial solution to kick things off.
     *            
     * @return An optimized AST.
     * 
     * @throws QueryEvaluationException
     */
    static public QueryRoot optimizeQuery(
            final ASTContainer astContainer,
            final AST2BOpContext context,
            final QueryBindingSet globallyScopedBS,
            final Dataset dataset) throws QueryEvaluationException {

        final AbstractTripleStore store = context.getAbstractTripleStore();

        final DeferredResolutionResult resolved;
        try {
            // @see https://jira.blazegraph.com/browse/BLZG-1176
            resolved = ASTDeferredIVResolution.resolveQuery(
                store, astContainer, globallyScopedBS, dataset, context);
        } catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }

        if (resolved.dataset != null) {
            astContainer.getOriginalAST().setDataset(
                new DatasetNode(resolved.dataset, false/* update */));
        }

        // Clear the optimized AST.
        astContainer.clearOptimizedAST();

        // Batch resolve Values to IVs and convert to bigdata binding set.
        final IBindingSet[] globallyScopedBSAsList = toBindingSet(resolved.bindingSet) ;

        // Convert the query (generates an optimized AST as a side-effect).
        AST2BOpUtility.convert(context, globallyScopedBSAsList);

        // The optimized AST.
        final QueryRoot optimizedQuery = astContainer.getOptimizedAST();

        return optimizedQuery;
        
    }

    /**
     * Evaluate a CONSTRUCT/DESCRIBE query.
     * <p>
     * Note: For a DESCRIBE query, this also updates the DESCRIBE cache.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param astContainer
     *            The {@link ASTContainer}.
     * @param globallyScopedBS
     *            The initial solution to kick things off.
     * 
     * @throws QueryEvaluationException
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
     *      DESCRIBE CACHE </a>
     */
    public static GraphQueryResult evaluateGraphQuery(
            final AbstractTripleStore store,
            final ASTContainer astContainer,
            final QueryBindingSet globallyScopedBS,
            final Dataset dataset) throws QueryEvaluationException {

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        // true iff the original query was a DESCRIBE.
        final boolean isDescribe = astContainer.getOriginalAST().getQueryType() == QueryType.DESCRIBE;
        
        /*
         * A mapping that is used to preserve a consistent assignment from blank
         * node IDs to BigdataBNodes scoped to the subgraph reported by the
         * top-level DESCRIBE query.
         */
        final Map<String, BigdataBNode> bnodes = (isDescribe ? new LinkedHashMap<String, BigdataBNode>()
                : null);

        final IDescribeCache describeCache;
        final Set<IVariable<?>> describeVars;
        if (isDescribe && context.describeCache != null) {

            /*
             * The DESCRIBE cache is enabled.
             */
            
            describeCache = context.getDescribeCache();

            /*
             * The set of variables that were in the original DESCRIBE
             * projection. This can include both variables explicitly given in
             * the query (DESCRIBE ?foo WHERE {...}) and variables bound to
             * constants by an AssignmentNode (DESCRIBE uri).
             */
            describeVars = astContainer.getOriginalAST().getProjectedVars(
                    new LinkedHashSet<IVariable<?>>());

        } else {
            
            // DESCRIBE cache is not enabled.
            describeCache = null;
            describeVars = null;
            
        }
        
        final QueryRoot optimizedQuery = 
                optimizeQuery(astContainer, context, globallyScopedBS, dataset);
        
        final boolean materializeProjectionInQuery = context.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        // The effective DescribeMode.
//      final DescribeModeEnum describeMode = optimizedQuery.getProjection()
//              .getDescribeMode() == null ? QueryHints.DEFAULT_DESCRIBE_MODE
//              : optimizedQuery.getProjection().getDescribeMode();
        final DescribeModeEnum describeMode = context
                .getDescribeMode(optimizedQuery.getProjection());

        final int describeIterationLimit = context
                .getDescribeIterationLimit(optimizedQuery.getProjection());

        final int describeStatementlimit = context
                .getDescribeStatementLimit(optimizedQuery.getProjection());

        // The final result to be returned.
        GraphQueryResult result = null;
        
        // Solutions to the WHERE clause (as projected).
        final CloseableIteration<BindingSet, QueryEvaluationException> solutions = ASTEvalHelper
                .evaluateQuery(astContainer, context, materializeProjectionInQuery//
                        , optimizedQuery.getProjection().getProjectionVars()//
                );

        try {
        
        final CloseableIteration<BindingSet, QueryEvaluationException> solutions2;
        final Set<BigdataValue> describedResources;
        if (describeCache != null) {

            /**
             * If we are maintaining the DESCRIBE cache, then we need to know
             * the distinct bindings that the projected variables in the
             * original DESCRIBE query take on in the solutions. Those bound
             * values identify the resources that were actually described by the
             * query. This is necessary to handle cases such as
             * <code>DESCRIBE ?foo WHERE {...}</code> or <code>DESCRIBE *</code>
             * 
             * Note: We only do this for the top-level DESCRIBE. This step is
             * NOT done the embedded DESCRIBE query(s) issued for Concise
             * Bounded Description since we are only interested in caching the
             * original resources that were being described.
             * 
             * Note: The [describedResources] is a ConcurrentHashSet in order to
             * provide thread safety since new bindings for the DESCRIBE
             * variable(s) may be discovered concurrent with new constructed
             * statements being observed. We need to have the new bindings
             * become immediately visible in order to avoid missing any
             * statements involving a resource in the original projection. The
             * [describedResources] were "described" and must be updated in the
             * DESCRIBE cache.
             */
     
            // Concurrency safe set.
            describedResources = Collections
                    .newSetFromMap(new ConcurrentHashMap<BigdataValue, Boolean>());

            // Collect the bindings on those variables.
            solutions2 = new DescribeBindingsCollector(//
                    describeVars,// what to collect
                    describedResources,// where to put the bindings.
                    solutions// source solutions
            );

        } else {

            // Pass through original iterator.
            solutions2 = solutions;
            describedResources = null;
        
        }

        // Constructed Statements.
        final CloseableIteration<BigdataStatement, QueryEvaluationException> src =
                new ASTConstructIterator(context, store, //
                        optimizedQuery.getConstruct(), //
                        optimizedQuery.getWhereClause(),//
                        bnodes,//
                        solutions2//
                        );

        final CloseableIteration<BigdataStatement, QueryEvaluationException> src2;
        if (isDescribe) {
        	switch (describeMode) {
        	case SymmetricOneStep: // No expansion step.
        	case ForwardOneStep: // No expansion step.
        		src2 = src;
        		break;
        	case CBD:
        	case SCBD:
        		//        case CBDNR:
        		//        case SCBDNR: 
        	{
        		/*
        		 * Concise Bounded Description (of any flavor) requires a fixed
        		 * point expansion.
        		 * 
        		 * TODO CBD : The expansion should monitor a returned iterator so
        		 * the query can be cancelled by the openrdf client. Right now the
        		 * expansion is performed before the iteration is returned to the
        		 * client, so there is no opportunity to cancel a running CBD
        		 * DESCRIBE.
        		 */
        		src2 = new CBD(store, describeMode, describeIterationLimit,
        				describeStatementlimit, bnodes).computeClosure(src);
        		break;
        	}
        	default:
        		throw new UnsupportedOperationException("describeMode="
        				+ describeMode);
        	}
        } else {
        	src2 = src;
        }
        final CloseableIteration<BigdataStatement, QueryEvaluationException> src3;

        if (describeCache != null) {

            /*
             * Wrap the Statement iteration with logic that will update the
             * DESCRIBE cache.
             * 
             * Note: [describedResources] is the set of BigdataValues that were
             * "described" by the query and will have an entry asserted in the
             * cache.
             * 
             * TODO We do not need to update cache entries unless they have been
             * invalidated (or are based on the open web and have become stale
             * or invalidated by finding new assertions relevant to those
             * resources during an open web query).
             */

            src3 = new DescribeCacheUpdater(describeCache, describedResources,
                    src2);

        } else {

            src3 = src2;

        }
        
        result = new GraphQueryResultImpl(//
                optimizedQuery.getPrefixDecls(), //
                src3);
        } finally {
            if (result == null) {
                /**
                 * Cancel the query since we are not returning the
                 * GraphTupleQuery result object to the caller.
                 * 
                 * Note: This provides only partial resolution of the following
                 * ticket. There are other operations than the underlying query
                 * that would need to be canceled. I have NOT verified that
                 * closing the underlying query is sufficient to unwind those
                 * operations. Also, the CBD support is not written to be
                 * interruptable at this time (see the TODO above).
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/715"
                 *      > Interrupt of thread submitting a query for evaluation
                 *      does not always terminate the AbstractRunningQuery </a>
                 */
                solutions.close();
            }
        }
        
        return result;

    }

    /**
     * Evaluate a query plan (core method).
     * <p>
     * As explained in some depth at <a
     * href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
     * BlockingBuffer.close() does not unblock threads </a> and <a
     * href="http://trac.blazegraph.com/ticket/864"> Semantics of interrupting a
     * running query</a>, (a) you can not interrupted the thread that submits a
     * query until the {@link CloseableIteration} has been returned to the
     * caller submitting that query; (b)
     * <p>
     * (a) If you interrupt the thread submitting the query, the query may
     * actually execute. This can occur because the interrupt can arise between
     * the time at which the query begins to execute on the {@link QueryEngine}
     * and the time at which the {@link IRunningQuery} object is bound up inside
     * of the returned {@link CloseableIteration} and returned to the caller.
     * Until the caller has possession of the {@link CloseableIteration}, an
     * interrupt will not cause the associated {@link IRunningQuery} to be
     * terminated. See <a
     * href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
     * BlockingBuffer.close() does not unblock threads </a>
     * <p>
     * (b) If you interrupt the thread draining the solutions from the
     * {@link CloseableIteration} or otherwise cause
     * {@link CloseableIteration#close()} to become invoked, then the
     * {@link IRunningQuery} will be interrupted. Per <a
     * href="http://trac.blazegraph.com/ticket/864"> Semantics of interrupting a
     * running query</a>, that interrupt is interpreted as <em>normal</em>
     * termination (this supports the use case of LIMIT and is built deeply into
     * the {@link QueryEngine} semantics). In order for the application to
     * distinguish between a case where it has interrupted the query and a case
     * where the query has been interrupted by a LIMIT, the application MUST
     * notice when it decides to interrupt a given query and then discard the
     * outcome of that query.
     * 
     * @param astContainer
     *            The query model.
     * @param ctx
     *            The evaluation context.
     * @param bindingSets
     *            The source solution set(s).
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
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/707">
     *      BlockingBuffer.close() does not unblock threads </a>
     * @see <a href="http://trac.blazegraph.com/ticket/864"> Semantics of
     *      interrupting a running query</a>
     */
    static //private Note: Exposed to CBD class.
    CloseableIteration<BindingSet, QueryEvaluationException> evaluateQuery(
            final ASTContainer astContainer,
            final AST2BOpContext ctx,            
            final boolean materializeProjectionInQuery,
            final IVariable<?>[] required) throws QueryEvaluationException {

        doSparqlLogging(ctx);
        
        final PipelineOp queryPlan = astContainer.getQueryPlan();
        
        IRunningQuery runningQuery = null;
        try {

            // Optional attributes to be attached to the query.
            final Map<Object, Object> queryAttributes = ctx
                    .getQueryAttributes();

            // Submit query for evaluation.
            runningQuery = ctx.queryEngine.eval(queryPlan, 
                  astContainer.getOptimizedASTBindingSets(), queryAttributes);
            runningQuery.setStaticAnalysisStats(ctx.getStaticAnalysisStats());

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
     *            The {@link BindingSet} (optional).
     * 
     * @return The {@link IBindingSet}. When the source is null or empty, an
     *         empty {@link ListBindingSet} is returned.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static IBindingSet[] toBindingSet(final BindingSet src) {
        
        if (src == null || src.size() == 0) {
            
            return new IBindingSet[] { new ListBindingSet() };

        }

        final ListBindingSet bindingSet = new ListBindingSet();

        final Iterator<Binding> itr = src.iterator();

        while (itr.hasNext()) {

            final Binding binding = itr.next();

            final IVariable<IV> var = com.bigdata.bop.Var.var(binding.getName());
            
            final IV iv = ((BigdataValue) binding.getValue()).getIV();
            
            final IConstant<IV> val = new Constant<IV>(iv);
            
            bindingSet.set(var, val);
            
        }
        
        return new IBindingSet[]{ bindingSet };

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
         * The basic API alignment problem is that the IRunningQuery#iterator()
         * visits IBindingSet[] chunks while the BigdataBindingSetResolverator
         * and Bigdata2SesameBindingSetIterator are IChunked(Ordered)Iterators.
         * That is, they implement #nextChunk(). A very simple class could be
         * used to align an IBindingSet[] returned by next() with nextChunk(). I
         * would be surprised if this class did not already exist (in fact, the
         * class is ChunkedArraysIterator -or- ChunkConsumerIterator).
         * 
         * The other issue is that RunningQueryCloseableIterator would APPEAR to
         * be redundant with QueryResultIterator. However, correct termination
         * is a tricky business and the current layering obviously works. The
         * differences in those two classes appear to be (a) whether or not we
         * invoke cancel() on the IRunningQuery when the iterator is closed and
         * (b) whether or not we are buffering the last element visited. It is
         * quite possible that RunningQueryCloseableIterator simply layers on
         * one or two fixes which SHOULD be incorporated into the
         * QueryResultIterator.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/483 (Eliminate
         * unnecessary chunking and dechunking)
         */
        
        // Dechunkify the running query and monitor the Sesame iterator.
        final ICloseableIterator<IBindingSet> it1 = iterator(runningQuery);
        
        final BOp query = runningQuery.getQuery();
        
        final int chunkCapacity = query.getProperty(
                PipelineOp.Annotations.CHUNK_CAPACITY,
                PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);

        // Wrap in an IChunkedOrderedIterator
        final IChunkedOrderedIterator<IBindingSet> it2 = new ChunkedWrappedIterator<IBindingSet>(
                it1, chunkCapacity, IBindingSet.class);

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
//            final BOp query = runningQuery.getQuery();
//            final int chunkCapacity = query.getProperty(
//                    PipelineOp.Annotations.CHUNK_CAPACITY,
//                    PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);
            final int chunkOfChunksCapacity = query.getProperty(
                    PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                    PipelineOp.Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);
            final long chunkTimeout = query.getProperty(
                    PipelineOp.Annotations.CHUNK_TIMEOUT,
                    (long)PipelineOp.Annotations.DEFAULT_CHUNK_TIMEOUT);
            final int termsChunkSize = chunkCapacity;
            final int blobsChunkSize = chunkCapacity;
            
            // Convert bigdata binding sets to Sesame binding sets.
            it3 = new Bigdata2Sesame2BindingSetIterator(
                    // Materialize IVs as RDF Values.
                    new BigdataBindingSetResolverator(db, it2,
                            runningQuery.getQueryId(), required, chunkCapacity,
                            chunkOfChunksCapacity, chunkTimeout,
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
        final ICloseableIterator<IBindingSet[]> it1 = runningQuery
                .iterator();

        // Dechunkify the original iterator
        final ICloseableIterator<IBindingSet> it2 = 
            new Dechunkerator<IBindingSet>(it1);

        // Monitor IRunningQuery and cancel if Sesame iterator is closed.
        final ICloseableIterator<IBindingSet> it3 =
            new RunningQueryCloseableIterator<IBindingSet>(runningQuery, it2);

        return it3;
        
    }

    /**
     * Evaluate a SPARQL UPDATE request (core method).
     * 
     * @param astContainer
     *            The query model.
     * @param ctx
     *            The evaluation context.
     * @param dataset
     *            A dataset which will override the data set declaration for
     *            each {@link DeleteInsertGraph} operation in the update
     *            sequence (optional).
     * @param includeInferred
     *            if inferences should be included in various operations.
     *
     * @return The timestamp of the commit point.
     *            
     * @throws SailException
     * 
     * TODO timeout for update?
     */
    static public long executeUpdate(//
            final BigdataSailRepositoryConnection conn,//
            final ASTContainer astContainer,//
            final Dataset dataset,
            final boolean includeInferred,//
            final QueryBindingSet bs
            ) throws UpdateExecutionException {

        if(conn == null)
            throw new IllegalArgumentException();

        if(astContainer == null)
            throw new IllegalArgumentException();
        
        final DeferredResolutionResult resolved;
        try {
        	// @see https://jira.blazegraph.com/browse/BLZG-1176
            resolved = ASTDeferredIVResolution.resolveUpdate(conn.getTripleStore(), astContainer, bs, dataset);
        } catch (MalformedQueryException e) {
            throw new UpdateExecutionException(e.getMessage(), e);
        }

        try {

            if (dataset != null) {

                /*
                 * Apply the optional data set override.
                 */

                applyDataSet(conn.getTripleStore(), astContainer, resolved.dataset);
                
            }

            final AST2BOpUpdateContext ctx = new AST2BOpUpdateContext(
                    astContainer, conn);

            doSparqlLogging(ctx);

            // Propagate attribute.
            ctx.setIncludeInferred(includeInferred);
            
            // Batch resolve Values to IVs and convert to bigdata binding set.
            final IBindingSet[] bindingSets = toBindingSet(resolved.bindingSet) ;
            
            // Propagate bindings
            ctx.setQueryBindingSet(bs);
            ctx.setBindings(bindingSets);
            ctx.setDataset(dataset);

            /*
             * Convert the query (generates an optimized AST as a side-effect).
             */
            AST2BOpUpdate.optimizeUpdateRoot(ctx);

            /*
             * Generate and execute physical plans for the update operations.
             */
            AST2BOpUpdate.convertUpdate(ctx);

            return ctx.getCommitTime();
            
        } catch (Exception ex) {

            ex.printStackTrace();
            
            throw new UpdateExecutionException(ex);

        }

    }
    
    /**
     * Apply the {@link Dataset} to each {@link DeleteInsertGraph} in the UPDATE
     * request.
     * <p>
     * The openrdf API here is somewhat at odds with the current LCWD for SPARQL
     * UPDATE. In order to align them, setting the {@link Dataset} here causes
     * it to be applied to each {@link DeleteInsertGraph} operation in the
     * {@link UpdateRoot}. Note that the {@link Dataset} has no effect exception
     * for the {@link DeleteInsertGraph} operation in SPARQL 1.1 UPDATE (that is
     * the only operation which has a WHERE clause and which implements the
     * {@link IDataSetNode} interface).
     * 
     * @param tripleStore
     * @param astContainer
     * @param dataset
     * 
     * @see <a href="http://www.openrdf.org/issues/browse/SES-963"> Dataset
     *      assignment in update sequences not properly scoped </a>
     */
    static private void applyDataSet(final AbstractTripleStore tripleStore,
            final ASTContainer astContainer, final Dataset dataset) {

        if (tripleStore == null)
            throw new IllegalArgumentException();
        
        if (astContainer == null)
            throw new IllegalArgumentException();

        if (dataset == null)
            throw new IllegalArgumentException();
        
        /*
         * Batch resolve RDF Values to IVs and then set on the query model.
         */

//        final Object[] tmp = new BigdataValueReplacer(tripleStore)
//                .replaceValues(dataset, null/* bindings */);

        /*
         * Set the data set on the original AST.
         */
        
//        final Dataset resolvedDataset = (Dataset) tmp[0];

        final UpdateRoot updateRoot = astContainer.getOriginalUpdateAST();
        
        for (Update op : updateRoot) {
        
            if (op instanceof IDataSetNode) {
            
                final IDataSetNode node = ((IDataSetNode) op);

                node.setDataset(new DatasetNode(dataset, true/* update */));
                
            }
            
        }

    }

    /**
     * Log SPARQL Query and SPARQL UPDATE requests.
     * <p>
     * Note: The SPARQL syntax is logged whenever possible. However, we
     * sometimes generate the AST directly, in which case the SPARQL syntax is
     * not available and the AST is logged instead.
     * 
     * @param ctx
     */
    private static void doSparqlLogging(final AST2BOpContext ctx) {

        if (!log.isInfoEnabled())
            return;

        /*
         * Log timestamp of the view and the SPARQL query string.
         */

        setupLoggingContext(ctx);

        final ASTContainer astContainer = ctx.astContainer;

        final String queryString = astContainer.getQueryString();

        if (queryString != null) {

            /*
             * Log the query string when it is available.
             * 
             * Note: We sometimes generate the AST directly, in which case there
             * is no query string.
             */
            
            log.info(queryString);
            
        } else {

            /*
             * If there is no query string, then log the AST instead.
             */
            
            if (astContainer.isQuery()) {
            
                log.info(astContainer.getOriginalAST());
                
            } else {
                
                log.info(astContainer.getOriginalUpdateAST());
                
            }
            
        }

        clearLoggingContext();

    }

    private static void setupLoggingContext(final IEvaluationContext context) {

        MDC.put("tx", TimestampUtility.toString(context.getTimestamp()));

    }
    
    private static void clearLoggingContext() {
        
        MDC.remove("tx");
    }
    
}
