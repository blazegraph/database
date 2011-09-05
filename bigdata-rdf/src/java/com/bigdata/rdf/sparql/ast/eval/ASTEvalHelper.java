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

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.Bigdata2Sesame2BindingSetIterator;
import com.bigdata.rdf.sail.BigdataEvaluationStrategyImpl3;
import com.bigdata.rdf.sail.RunningQueryCloseableIterator;
import com.bigdata.rdf.sail.sop.UnsupportedOperatorException;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * FIXME Class has code that is being migrated from
 * {@link BigdataEvaluationStrategyImpl3}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEvalHelper {

//    private static final Logger log = Logger
//            .getLogger(BigdataEvaluationStrategyImpl3.class);
//
//    protected final BigdataTripleSource tripleSource = null;
//
//    protected final Dataset dataset = null;
//
//    final private CloseableIteration<BindingSet, QueryEvaluationException> bindingSets = null;
//
//    private final AbstractTripleStore database = null;
//
//    /**
//     * Translate the Sesame {@link TupleExpr} into a bigdata query plan and
//     * evaluate that query plan.
//     * 
//     * @param root
//     *            The level of the {@link TupleExpr} at which we intercept the
//     *            Sesame evaluation.
//     * @param bs
//     *            The initial {@link BindingSet}.
//     * 
//     * @return An iterator from which the solutions may be drained.
//     * 
//     * @throws UnsupportedOperatorException
//     * @throws UnrecognizedValueException
//     * @throws QueryEvaluationException
//     * 
//     *             FIXME SPARQL 1.0 and SPARQL 1.1 : This is where we need to
//     *             handle {@link QueryRoot}. However, we are only invoking this
//     *             method right now for the top-level construction which we are
//     *             willing to recognize. For example: UNION or JOIN.
//     */
//    private CloseableIteration<BindingSet, QueryEvaluationException> 
//        doEvaluateNatively(final TupleExpr root, final BindingSet bs)
//            throws UnsupportedOperatorException, UnrecognizedValueException, 
//                    QueryEvaluationException {
//        
//        final SOpTreeBuilder stb = new SOpTreeBuilder();
//        
//        /*
//         * The sesame operator tree
//         */
//        SOpTree sopTree;
//
//        /*
//         * Turn the Sesame operator tree into something a little easier
//         * to work with.
//         */
//        sopTree = stb.collectSOps(root);
//        
//        /*
//         * We need to prune groups that contain terms that do not appear in
//         * our lexicon.
//         */
//        final Collection<SOpGroup> groupsToPrune = new LinkedList<SOpGroup>();
//        
//        /*
//         * We need to prune Sesame filters that we cannot translate into native
//         * constraints (ones that require lexicon joins).  We also need to 
//         * prune search metadata tails.
//         */
//        final Collection<SOp> sopsToPrune = new LinkedList<SOp>();
//        
//        /*
//         * deal with free text search tails first. need to match up search
//         * metadata tails with the searches themselves. ie:
//         * 
//         * select *
//         * where {
//         *   ?s bd:search "foo" .
//         *   ?s bd:relevance ?score .
//         * }
//         */
//        // the statement patterns for metadata about the searches
//        final Map<Var, Set<StatementPattern>> searchMetadata =
//            new LinkedHashMap<Var, Set<StatementPattern>>();
//        // do a first pass to gather up the actual searches and take them out
//        // of the master list of statement patterns
//        for (SOp sop : sopTree) {
//            final QueryModelNode op = sop.getOperator();
//            if (op instanceof StatementPattern) {
//                final StatementPattern sp = (StatementPattern) op;
//                final Value s = sp.getSubjectVar().getValue();
//                final Value p = sp.getPredicateVar().getValue();
//                final Value o = sp.getObjectVar().getValue();
//                if (s == null && p != null && o != null && 
//                        BD.SEARCH.equals(p)) {
//                    searchMetadata.put(sp.getSubjectVar(), 
//                            new LinkedHashSet<StatementPattern>());
//                }
//            }
//        }
//        // do a second pass to get the search metadata
//        for (SOp sop : sopTree) {
//            final QueryModelNode op = sop.getOperator();
//            if (op instanceof StatementPattern) {
//                final StatementPattern sp = (StatementPattern) op;
//                final Value s = sp.getSubjectVar().getValue();
//                final Value p = sp.getPredicateVar().getValue();
//                if (s == null && p != null && 
//                        (BD.RELEVANCE.equals(p) || 
//                            BD.RANK.equals(p) ||
//                            BD.MIN_RANK.equals(p) ||
//                            BD.MAX_RANK.equals(p) ||
//                            BD.MIN_RELEVANCE.equals(p) || 
//                            BD.MAX_RELEVANCE.equals(p) || 
//                            BD.MATCH_ALL_TERMS.equals(p))) {
//                    final Var sVar = sp.getSubjectVar();
//                    Set<StatementPattern> metadata = searchMetadata.get(sVar);
//                    if (metadata != null) {
//                        metadata.add(sp);
//                    }
//                    sopsToPrune.add(sop);
//                }
//            }
//        }
//        
//        /*
//         * Prunes the sop tree of search metadata.
//         */
//        sopTree = stb.pruneSOps(sopTree, sopsToPrune);
//        sopsToPrune.clear();
//        
//        /*
//         * Iterate through the sop tree and translate statement patterns into
//         * predicates.
//         */
//        for (SOp sop : sopTree) {
//            final QueryModelNode op = sop.getOperator();
//            if (op instanceof StatementPattern) {
//                final StatementPattern sp = (StatementPattern) op;
//                final Value p = sp.getPredicateVar().getValue();
//                try {
//                    final IPredicate bop;
//                    if (p != null && BD.SEARCH.equals(p)) {
//                        final Set<StatementPattern> metadata = 
//                            searchMetadata.get(sp.getSubjectVar());
//                        bop = toSearchPredicate(sp, metadata);
//                    } else {
//                        bop = toPredicate((StatementPattern) op);
//                    }
//                    sop.setBOp(bop);
//                } catch (UnrecognizedValueException ex) {
//                    /*
//                     * If we encounter a value not in the lexicon, we can
//                     * still continue with the query if the value is in
//                     * either an optional tail or an optional join group (i.e.
//                     * if it appears on the right side of a LeftJoin).  We can
//                     * also continue if the value is in a UNION.
//                     * Otherwise we can stop evaluating right now. 
//                     */
//                    if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
//                        throw new UnrecognizedValueException(ex);
//                    } else {
//                        groupsToPrune.add(sopTree.getGroup(sop.getGroup()));
//                    }
//                }
//            }
//        }
//        
//        /*
//         * Prunes the sop tree of optional join groups containing values
//         * not in the lexicon.
//         */
//        sopTree = stb.pruneGroups(sopTree, groupsToPrune);
//
//        /*
//         * If after pruning groups with unrecognized values we end up with a
//         * UNION with no subqueries, we can safely just return an empty
//         * iteration.
//         */
//        if (SOp2BOpUtility.isEmptyUnion(sopTree.getRoot())) {
//            return new EmptyIteration<BindingSet, QueryEvaluationException>();
//        }
//        
//        /*
//         * If we have a filter in the root group (one that can be safely applied
//         * across the entire query) that we cannot translate into a native
//         * bigdata constraint, we can run it as a FilterIterator after the
//         * query has run natively.
//         */
//        final Collection<Filter> sesameFilters = new LinkedList<Filter>();
//        
//        /*
//         * Iterate through the sop tree and translate Sesame ValueExpr operators
//         * into bigdata IConstraint boperators.
//         */
//        for (SOp sop : sopTree) {
//            final QueryModelNode op = sop.getOperator();
//            if (op instanceof ValueExpr) {
//                /*
//                 * If we have a raw ValueExpr and not a Filter we know it must
//                 * be the condition of a LeftJoin, in which case we cannot
//                 * use the Sesame FilterIterator to safely evaluate it.  A
//                 * UnsupportedOperatorException here must just flow through
//                 * to Sesame evaluation of the entire query.
//                 */
//                final ValueExpr ve = (ValueExpr) op;
//                final IConstraint bop = toConstraint(ve);
//                sop.setBOp(bop);
//            } else if (op instanceof Filter) {
//                final Filter filter = (Filter) op;
//                
//                /*
//                 * If the scope binding names are empty we can definitely
//                 * always fail the filter (since the filter's variables
//                 * cannot be bound).
//                 */
//                if (filter.getBindingNames().isEmpty()) {
//                    final IConstraint bop = new SPARQLConstraint(SparqlTypeErrorBOp.INSTANCE);
//                    sop.setBOp(bop);
//                } else {
//                    final ValueExpr ve = filter.getCondition();
//                    final IConstraint bop = toConstraint(ve);
//                    sop.setBOp(bop);
//                }
//                
////              try {
////                  final ValueExpr ve = filter.getCondition();
////                  final IConstraint bop = toConstraint(ve);
////                  sop.setBOp(bop);
////              } catch (UnsupportedOperatorException ex) {
////                  /*
////                   * If we encounter a sesame filter (ValueExpr) that we
////                   * cannot translate, we can safely wrap the entire query
////                   * with a Sesame filter iterator to capture that
////                   * untranslatable value expression.  If we are not in the
////                   * root group however, we risk applying the filter to the
////                   * wrong context (for example a filter inside an optional
////                   * join group cannot be applied universally to the entire
////                   * solution).  In this case we must punt. 
////                   */
////                  if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
////                      sopsToPrune.add(sop);
////                      sesameFilters.add(filter);
////                  } else {
////                      /*
////                       * Note: DO NOT wrap with a different exception type -
////                       * the caller is looking for this.
////                       */
////                      throw new UnsupportedOperatorException(ex);
////                  }
////              }
//                
//            }
//        }
//        
//        /*
//         * Prunes the sop tree of untranslatable filters.
//         */
//        sopTree = stb.pruneSOps(sopTree, sopsToPrune);
//        
//        /*
//         * Make sure we don't have free text searches searching outside
//         * their named graph scope.
//         */
//        attachNamedGraphsFilterToSearches(sopTree);
//        
//        if (false) {
//            /*
//             * Look for numerical filters that can be rotated inside predicates
//             */
//            final Iterator<SOpGroup> groups = sopTree.groups();
//            while (groups.hasNext()) {
//                final SOpGroup g = groups.next();
//                attachRangeBOps(g);
//            }
//        }
//        
//        if (log.isDebugEnabled()) {
//            log.debug("\n"+sopTree);
//        }
//        
//        /*
//         * Check whether optional join is "well designed" as defined in section
//         * 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge Pérez et al.
//         */
//        checkForBadlyDesignedLeftJoin(sopTree);
//
//        /*
//         * Gather variables required by Sesame outside of the query
//         * evaluation (projection and global sesame filters).
//         */
//        final IVariable[] required = 
//            gatherRequiredVariables(root, sesameFilters);
//        
//        sopTree.setRequiredVars(required);
//        
//        final QueryEngine queryEngine = tripleSource.getSail().getQueryEngine();
//
//        final PipelineOp query;
//        {
//            /*
//             * Note: The ids are assigned using incrementAndGet() so ONE (1) is
//             * the first id that will be assigned when we pass in ZERO (0) as
//             * the initial state of the AtomicInteger.
//             */
//            final AtomicInteger idFactory = new AtomicInteger(0);
//
//            // Convert the step to a bigdata operator tree.
////          query = SOp2BOpUtility.convert(sopTree, idFactory, database,
////                  queryEngine, queryHints);
//            
//            final com.bigdata.rdf.sparql.ast.QueryRoot ast = 
//                    SOp2ASTUtility.convert(sopTree);
//            
//            if (dataset != null) {
//                ast.setDataset(new DatasetNode(dataset));
//            }
//            
//            if (log.isInfoEnabled())
//                log.info("\n"+ast);
//
//            query = AST2BOpUtility.convert(new AST2BOpContext(
//                    ast, idFactory, database, queryEngine, queryHints));
//
//            if (log.isInfoEnabled())
//                log.info("\n"+BOpUtility.toString2(query));
//
//        }
//
//        /*
//         * Begin native bigdata evaluation.
//         */
//        CloseableIteration<BindingSet, QueryEvaluationException> result = doEvaluateNatively(
//                query, bs, queryEngine, required);// , sesameFilters);
//
//        /*
//         * Use the basic filter iterator for any remaining filters which will be
//         * evaluated by Sesame.
//         * 
//         * Note: Some Sesame filters may pre-fetch one or more result(s). This
//         * could potentially cause the IRunningQuery to be asynchronously
//         * terminated by an interrupt. I have lifted the code to wrap the Sesame
//         * filters around the bigdata evaluation out of the code which starts
//         * the IRunningQuery evaluation in order to help clarify such
//         * circumstances as they might relate to [1].
//         * 
//         * [1] https://sourceforge.net/apps/trac/bigdata/ticket/230
//         */
////      if (sesameFilters != null) {
////          for (Filter f : sesameFilters) {
////              if (log.isDebugEnabled()) {
////                  log.debug("attaching sesame filter: " + f);
////              }
////              result = new FilterIterator(f, result, this);
////          }
////      }
//        
////      System.err.println("results");
////      while (result.hasNext()) {
////          System.err.println(result.next());
////      }
//
//        return result;
//
//    }
    
    /**
     * Return an {@link IAsynchronousIterator} that will read a single, empty
     * {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    static private ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Setup the input binding sets which will be fed into the query pipeline.
     */
    static private IAsynchronousIterator<IBindingSet[]> wrapSource(
            final BindingSet bs) {

        final IAsynchronousIterator<IBindingSet[]> source;
        
        if (bs != null) {
        
            /*
             * A single input binding set will be fed into the query pipeline
             * using the supplied bindings.
             */
            
            source = newBindingSetIterator(toBindingSet(bs));
            
        } else {
            
            /*
             * A single empty input binding set will be fed into the query
             * pipeline.
             */
            
            source = newBindingSetIterator(new ListBindingSet());
            
        }

        return source;
        
    }
    
    /**
     * Evaluate a boolean query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The initial solution to kick things off.
     * @param queryEngine
     *            The query engine.
     *            
     * @return <code>true</code> if there are any solutions to the query.
     * 
     * @throws QueryEvaluationException
     */
    static public boolean evaluateBooleanQuery(
            final AbstractTripleStore database, final PipelineOp queryPlan,
            final BindingSet bs, final QueryEngine queryEngine)
            throws QueryEvaluationException {

        IRunningQuery runningQuery = null;
        final IAsynchronousIterator<IBindingSet[]> source = wrapSource(bs); 
        try {

            // Submit query for evaluation.
            runningQuery = queryEngine.eval(queryPlan, source);

            // See if any solutions were produced.
            return runningQuery.iterator().hasNext();

        } catch (Exception e) {

            throw new QueryEvaluationException(e);
            
        } finally {
            
            // Ensure source is closed.
            source.close();
            
            if (runningQuery != null) {
            
                runningQuery.cancel(true/* mayInterruptIfRunning */);
                
                try {
                    
                    runningQuery.get();
                    
                } catch (Exception e) {
            
                    throw new QueryEvaluationException(e);
                    
                }
        
            }

        }

    }

    /**
     * Evaluate a SELECT query.
     * 
     * @param store
     *            The {@link AbstractTripleStore} having the data.
     * @param queryPlan
     *            The query plan.
     * @param bs
     *            The initial solution to kick things off.
     * @param queryEngine
     *            The query engine.
     * @param projected
     *            The variables projected by the query.
     *            
     * @return An object from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
    static public TupleQueryResult evaluateTupleQuery(
            final AbstractTripleStore store, final PipelineOp queryPlan,
            final BindingSet bs, final QueryEngine queryEngine,
            final IVariable[] projected) throws QueryEvaluationException {

        final List<String> projectedSet = new LinkedList<String>();

        for (IVariable<?> var : projected)
            projectedSet.add(var.getName());

        return new TupleQueryResultImpl(projectedSet,
                ASTEvalHelper.doEvaluateNatively(store, queryPlan,
                        new QueryBindingSet(), queryEngine, projected));

    }
    
    /**
     * FIXME What is [required] for? It is probably where we are finally pruning
     * off the variables which are not being projected. That should be done in
     * {@link AST2BOpUtility}. (Yes, it gets passed through to
     * {@link BigdataBindingSetResolverator}, but that will also accept a
     * <code>null</code> and then just materialize all variables it finds, which
     * is what it should be doing if we have already pruned the variables to
     * just the projected variables in the query plan.)
     * <p>
     * There is a twist here. I believe that Sesame may allow the variables
     * given in the initial binding set to flow through even if they were not
     * projected.
     * 
     * TODO Rename as evaluate() (was doEvaluateNatively()).
     */
    static public CloseableIteration<BindingSet, QueryEvaluationException> doEvaluateNatively(
            final AbstractTripleStore database, final PipelineOp queryPlan,
            final BindingSet bs, final QueryEngine queryEngine,
            final IVariable[] required) throws QueryEvaluationException {

        IRunningQuery runningQuery = null;
        final IAsynchronousIterator<IBindingSet[]> source = wrapSource(bs); 
        try {

            // Submit query for evaluation.
            runningQuery = queryEngine.eval(queryPlan, source);

            /*
             * Wrap up the native bigdata query solution iterator as Sesame
             * compatible iteration with materialized RDF Values.
             */
            return iterator(runningQuery, database, required);

        } catch (UnsupportedOperatorException t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            // ensure source is closed on error path.
            source.close();
            /*
             * Note: Do not wrap as a different exception type. The caller is
             * looking for this.
             */
            throw new UnsupportedOperatorException(t);
        } catch (Throwable t) {
            if (runningQuery != null) {
                // ensure query is halted.
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            }
            // ensure source is closed on error path.
            source.close();
            throw new QueryEvaluationException(t);
        }

    }

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
    
    private static CloseableIteration<BindingSet, QueryEvaluationException> 
        iterator(final IRunningQuery runningQuery, final AbstractTripleStore db,
            final IVariable[] required) {
    
        final ICloseableIterator<IBindingSet> it1 = iterator(runningQuery);
        
        // Wrap in an IChunkedOrderedIterator
        final IChunkedOrderedIterator<IBindingSet> it2 = 
            new ChunkedWrappedIterator<IBindingSet>(it1);
        
        // Materialize IVs as RDF Values.
        final CloseableIteration<BindingSet, QueryEvaluationException> it3 =
            // Convert bigdata binding sets to Sesame binding sets.
            new Bigdata2Sesame2BindingSetIterator<QueryEvaluationException>(
                // Materialize IVs as RDF Values.
                new BigdataBindingSetResolverator(db, it2, required).start(
                        db.getExecutorService()));
        
        return it3;
        
    }
    
//    private void attachNamedGraphsFilterToSearches(final SOpTree sopTree) {
//        
//        /*
//         * When in quads mode, we need to go through the free text searches and
//         * make sure that they are properly filtered for the dataset where
//         * needed. Joins will take care of this, so we only need to add a filter
//         * when a search variable does not appear in any other tails that are
//         * non-optional.
//         * 
//         * @todo Bryan seems to think this can be fixed with a DISTINCT JOIN
//         * mechanism in the rule evaluation.
//         */
//        if (database.isQuads() && dataset != null) {
////            for (IPredicate search : searches.keySet()) {
//            for (SOp sop : sopTree) {
//                final QueryModelNode op = sop.getOperator();
//                if (!(op instanceof StatementPattern)) {
//                    continue;
//                }
//                final StatementPattern sp = (StatementPattern) op;
//                final IPredicate pred = (IPredicate) sop.getBOp();
//                if (!(pred.getAccessPathExpander() 
//                        instanceof FreeTextSearchExpander)) {
//                    continue;
//                }
//                final FreeTextSearchExpander expander = (FreeTextSearchExpander) 
//                    pred.getAccessPathExpander();
//                
//                final Set<URI> graphs;
//                switch (sp.getScope()) {
//                case DEFAULT_CONTEXTS: {
//                    /*
//                     * Query against the RDF merge of zero or more source
//                     * graphs.
//                     */
//                    graphs = dataset.getDefaultGraphs();
//                    break;
//                }
//                case NAMED_CONTEXTS: {
//                    /*
//                     * Query against zero or more named graphs.
//                     */
//                    graphs = dataset.getNamedGraphs();
//                    break;
//                }
//                default:
//                    throw new AssertionError();
//                }
//                
//                if (graphs == null) {
//                    continue;
//                }
//                
//                // get ahold of the search variable
//                com.bigdata.bop.Var searchVar = 
//                    (com.bigdata.bop.Var) pred.get(0);
//
//                // start by assuming it needs filtering, guilty until proven
//                // innocent
//                boolean needsFilter = true;
//                // check the other tails one by one
//                for (SOp sop2 : sopTree) {
//                    if (!(sop2.getOperator() instanceof StatementPattern)) {
//                        continue;
//                    }
//                    final IPredicate pred2 = (IPredicate) sop.getBOp();
//                    final IAccessPathExpander expander2 = 
//                        pred.getAccessPathExpander();
//                    // only concerned with non-optional tails that are not
//                    // themselves magic searches
//                    if (expander instanceof FreeTextSearchExpander) {
//                        continue;
//                    }
//                    if (sop2.isRightSideLeftJoin()) {
//                        continue;
//                    }
//                    
//                    // see if the search variable appears in this tail
//                    boolean appears = false;
//                    for (int i = 0; i < pred2.arity(); i++) {
//                        IVariableOrConstant term = pred2.get(i);
//                        if (log.isDebugEnabled()) {
//                            log.debug(term);
//                        }
//                        if (term.equals(searchVar)) {
//                            appears = true;
//                            break;
//                        }
//                    }
//                    // if it appears, we don't need a filter
//                    if (appears) {
//                        needsFilter = false;
//                        break;
//                    }
//                }
//                // if it needs a filter, add it to the expander
//                if (needsFilter) {
//                    if (log.isDebugEnabled()) {
//                        log.debug("needs filter: " + searchVar);
//                    }
//                    expander.addNamedGraphsFilter(graphs);
//                }
//            }
//        }
//        
//    }
//    
//    private void attachRangeBOps(final SOpGroup g) {
//
//        final Map<IVariable,Collection<IValueExpression>> lowerBounds =
//            new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//        final Map<IVariable,Collection<IValueExpression>> upperBounds =
//            new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//        
//        for (SOp sop : g) {
//            final BOp bop = sop.getBOp();
//            if (!(bop instanceof SPARQLConstraint)) {
//                continue;
//            }
//            final SPARQLConstraint c = (SPARQLConstraint) bop;
//            if (!(c.getValueExpression() instanceof CompareBOp)) {
//                continue;
//            }
//            final CompareBOp compare = (CompareBOp) c.getValueExpression();
//            final IValueExpression left = compare.get(0);
//            final IValueExpression right = compare.get(1);
//            final CompareOp op = compare.op();
//            if (left instanceof IVariable) {
//                final IVariable var = (IVariable) left;
//                final IValueExpression ve = right;
//                if (op == CompareOp.GE || op == CompareOp.GT) {
//                    // ve is a lower bound
//                    Collection bounds = lowerBounds.get(var);
//                    if (bounds == null) {
//                        bounds = new LinkedList<IValueExpression>();
//                        lowerBounds.put(var, bounds);
//                    }
//                    bounds.add(ve);
//                } else if (op == CompareOp.LE || op == CompareOp.LT) {
//                    // ve is an upper bound
//                    Collection bounds = upperBounds.get(var);
//                    if (bounds == null) {
//                        bounds = new LinkedList<IValueExpression>();
//                        upperBounds.put(var, bounds);
//                    }
//                    bounds.add(ve);
//                }
//            } 
//            if (right instanceof IVariable) {
//                final IVariable var = (IVariable) right;
//                final IValueExpression ve = left;
//                if (op == CompareOp.LE || op == CompareOp.LT) {
//                    // ve is a lower bound
//                    Collection bounds = lowerBounds.get(var);
//                    if (bounds == null) {
//                        bounds = new LinkedList<IValueExpression>();
//                        lowerBounds.put(var, bounds);
//                    }
//                    bounds.add(ve);
//                } else if (op == CompareOp.GE || op == CompareOp.GT) {
//                    // ve is an upper bound
//                    Collection bounds = upperBounds.get(var);
//                    if (bounds == null) {
//                        bounds = new LinkedList<IValueExpression>();
//                        upperBounds.put(var, bounds);
//                    }
//                    bounds.add(ve);
//                }
//            }
//        }
//        
//        final Map<IVariable,RangeBOp> rangeBOps = 
//            new LinkedHashMap<IVariable,RangeBOp>();
//        
//        for (IVariable v : lowerBounds.keySet()) {
//            if (!upperBounds.containsKey(v))
//                continue;
//            
//            IValueExpression from = null;
//            for (IValueExpression ve : lowerBounds.get(v)) {
//                if (from == null)
//                    from = ve;
//                else
//                    from = new MathBOp(ve, from, MathOp.MAX,this.tripleSource.getDatabase().getNamespace());
//            }
//
//            IValueExpression to = null;
//            for (IValueExpression ve : upperBounds.get(v)) {
//                if (to == null)
//                    to = ve;
//                else
//                    to = new MathBOp(ve, to, MathOp.MIN,this.tripleSource.getDatabase().getNamespace());
//            }
//            
//            final RangeBOp rangeBOp = new RangeBOp(v, from, to); 
//            
//            if (log.isInfoEnabled()) {
//                log.info("found a range bop: " + rangeBOp);
//            }
//            
//            rangeBOps.put(v, rangeBOp);
//        }
//        
//        for (SOp sop : g) {
//            final BOp bop = sop.getBOp();
//            if (!(bop instanceof IPredicate)) {
//                continue;
//            }
//            final IPredicate pred = (IPredicate) bop;
//            final IVariableOrConstant o = pred.get(2);
//            if (o.isVar()) {
//                final IVariable v = (IVariable) o;
//                if (!rangeBOps.containsKey(v)) {
//                    continue;
//                }
//                final RangeBOp rangeBOp = rangeBOps.get(v);
//                final IPredicate rangePred = (IPredicate)
//                    pred.setProperty(SPOPredicate.Annotations.RANGE, rangeBOp);
//                if (log.isInfoEnabled())
//                    log.info("range pred: " + rangePred);
//                sop.setBOp(rangePred);
//            }
//        }
//    }
//
//    /**
//     * Generate a bigdata {@link IPredicate} (tail) for the supplied
//     * StatementPattern.
//     * <p>
//     * As a shortcut, if the StatementPattern contains any bound values that
//     * are not in the database, this method will return null.
//     * 
//     * @param stmtPattern
//     * @return the generated bigdata {@link Predicate} or <code>null</code> if
//     *         the statement pattern contains bound values not in the database.
//     */
//    private IPredicate toPredicate(final StatementPattern stmtPattern) 
//            throws QueryEvaluationException {
//        
//        // create a solution expander for free text search if necessary
//        IAccessPathExpander<ISPO> expander = null;
//        final Value predValue = stmtPattern.getPredicateVar().getValue();
//        if (log.isDebugEnabled()) {
//            log.debug(predValue);
//        }
//        if (predValue != null && BD.SEARCH.equals(predValue)) {
//            final Value objValue = stmtPattern.getObjectVar().getValue();
//            if (log.isDebugEnabled()) {
//                log.debug(objValue);
//            }
//            if (objValue != null && objValue instanceof Literal) {
//                expander = new FreeTextSearchExpander(database,
//                        (Literal) objValue);
//            }
//        }
//        
//        // @todo why is [s] handled differently?
//        // because [s] is the variable in free text searches, no need to test
//        // to see if the free text search expander is in place
//        final IVariableOrConstant<IV> s = toVE(
//                stmtPattern.getSubjectVar());
//        if (s == null) {
//            return null;
//        }
//        
//        final IVariableOrConstant<IV> p;
//        if (expander == null) {
//            p = toVE(stmtPattern.getPredicateVar());
//        } else {
//            p = new Constant(TermId.mockIV(VTE.BNODE));
//        }
//        if (p == null) {
//            return null;
//        }
//        
//        final IVariableOrConstant<IV> o;
//        if (expander == null) {
//            o = toVE(stmtPattern.getObjectVar());
//        } else {
//            o = new Constant(TermId.mockIV(VTE.BNODE));
//        }
//        if (o == null) {
//            return null;
//        }
//        
//        // The annotations for the predicate.
//        final List<NV> anns = new LinkedList<NV>();
//        
//        final IVariableOrConstant<IV> c;
//        if (!database.isQuads()) {
//            /*
//             * Either triple store mode or provenance mode.
//             */
//            final Var var = stmtPattern.getContextVar();
//            if (var == null) {
//                // context position is not used.
//                c = null;
//            } else {
//                final Value val = var.getValue();
//                if (val != null && database.isStatementIdentifiers()) {
//                    /*
//                     * Note: The context position is used as a statement
//                     * identifier (SID). SIDs may be used to retrieve provenance
//                     * statements (statements about statement) using high-level
//                     * query. SIDs are represented as blank nodes and is not
//                     * possible to have them bound in the original query. They
//                     * only become bound during query evaluation.
//                     */
//                    throw new QueryEvaluationException(
//                            "Context position is a statement identifier and may not be bound in the original query: "
//                                    + stmtPattern);
//                }
//                final String name = var.getName();
//                c = com.bigdata.bop.Var.var(name);
//            }
//        } else {
//            /*
//             * Quad store mode.
//             */
//            if (expander != null) {
//                /*
//                 * This code path occurs when we are doing a free text search
//                 * for this access path using the FreeTestSearchExpander. There
//                 * is no need to do any named or default graph expansion work on
//                 * a free text search access path.
//                 */
//                c = null;
//            } else {
//                // the graph variable iff specified by the query.
//                final Var cvar = stmtPattern.getContextVar();
//                // quads mode.
//                anns.add(new NV(Rule2BOpUtility.Annotations.QUADS, true));
//                // attach the Scope.
//                anns.add(new NV(Rule2BOpUtility.Annotations.SCOPE, stmtPattern
//                        .getScope()));
//                if (dataset == null) {
//                    // attach the appropriate expander : @todo drop expanders. 
//                    if (cvar == null) {
//                        /*
//                         * There is no dataset and there is no graph variable,
//                         * so the default graph will be the RDF Merge of ALL
//                         * graphs in the quad store.
//                         * 
//                         * This code path uses an "expander" which strips off
//                         * the context information and filters for the distinct
//                         * (s,p,o) triples to realize the RDF Merge of the
//                         * source graphs for the default graph.
//                         */
//                        c = null;
//                        expander = new DefaultGraphSolutionExpander(null/* ALL */);
//                    } else {
//                        /*
//                         * There is no data set and there is a graph variable,
//                         * so the query will run against all named graphs and
//                         * [cvar] will be to the context of each (s,p,o,c) in
//                         * turn. This handles constructions such as:
//                         * 
//                         * "SELECT * WHERE {graph ?g {?g :p :o } }"
//                         */
//                        expander = new NamedGraphSolutionExpander(null/* ALL */);
//                        c = toVE(cvar);
//                    }
//                } else { // dataset != null
//                    // attach the DataSet.
//                    anns.add(new NV(Rule2BOpUtility.Annotations.DATASET,
//                            new DatasetNode(dataset)));
//                    // attach the appropriate expander : @todo drop expanders. 
//                    switch (stmtPattern.getScope()) {
//                    case DEFAULT_CONTEXTS: {
//                        /*
//                         * Query against the RDF merge of zero or more source
//                         * graphs.
//                         */
//                        expander = new DefaultGraphSolutionExpander(
//                                DataSetSummary.toInternalValues(dataset.getDefaultGraphs()));
//                        /*
//                         * Note: cvar can not become bound since context is
//                         * stripped for the default graph.
//                         */
//                        if (cvar == null)
//                            c = null;
//                        else
//                            c = toVE(cvar);
//                        break;
//                    }
//                    case NAMED_CONTEXTS: {
//                        /*
//                         * Query against zero or more named graphs.
//                         */
//                        expander = new NamedGraphSolutionExpander(
//                                DataSetSummary.toInternalValues(dataset.getNamedGraphs()));
//                        if (cvar == null) {// || !cvar.hasValue()) {
//                            c = null;
//                        } else {
//                            c = toVE(cvar);
//                        }
//                        break;
//                    }
//                    default:
//                        throw new AssertionError();
//                    }
//                }
//            }
//        }
//
//        /*
//         * This applies a filter to the access path to remove any inferred
//         * triples when [includeInferred] is false.
//         * 
//         * @todo We can now stack filters so are we missing out here by not
//         * layering in other filters as well? [In order to rotate additional
//         * constraints onto an access path we would need to either change
//         * IPredicate and AbstractAccessPath to process an IConstraint[] or
//         * write a delegation pattern that let's us wrap one filter inside of
//         * another.]
//         */
//        final IElementFilter<ISPO> filter = 
//            !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE
//                : null;
//
//        // Decide on the correct arity for the predicate.
//        final BOp[] vars;
//        if (!database.isQuads() && !database.isStatementIdentifiers()) {
//            vars = new BOp[] { s, p, o };
//        } else if (c == null) {
//            vars = new BOp[] { s, p, o, com.bigdata.bop.Var.var() };
//        } else {
//            vars = new BOp[] { s, p, o, c };
//        }
//
//        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
//                new String[] { database.getSPORelation().getNamespace() }));//
//        
//        // filter on elements visited by the access path.
//        if (filter != null)
//            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
//                    ElementFilter.newInstance(filter)));
//
//        // free text search expander or named graphs expander
//        if (expander != null)
//            anns.add(new NV(IPredicate.Annotations.ACCESS_PATH_EXPANDER, expander));
//
//        // timestamp
//        anns.add(new NV(Annotations.TIMESTAMP, database
//                .getSPORelation().getTimestamp()));
//
//        /*
//         * Explicitly set the access path / iterator flags.
//         * 
//         * Note: High level query generally permits iterator level parallelism.
//         * We set the PARALLEL flag here so it can be used if a global index
//         * view is chosen for the access path.
//         * 
//         * Note: High level query for SPARQL always uses read-only access paths.
//         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
//         * semantics then you will need to remote the READONLY flag for the
//         * mutable access paths.
//         */
//        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
//                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
//        
//        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
////        return new SPOPredicate(
////                new String[] { database.getSPORelation().getNamespace() },
////                -1, // partitionId
////                s, p, o, c,
////                optional, // optional
////                filter, // filter on elements visited by the access path.
////                expander // free text search expander or named graphs expander
////                );
//        
//    }
//
//    private IPredicate toSearchPredicate(final StatementPattern sp,
//            final Set<StatementPattern> metadata) 
//            throws QueryEvaluationException {
//        
//        final Value predValue = sp.getPredicateVar().getValue();
//        if (log.isDebugEnabled()) {
//            log.debug(predValue);
//        }
//        if (predValue == null || !BD.SEARCH.equals(predValue)) {
//            throw new IllegalArgumentException("not a valid magic search: " + sp);
//        }
//        final Value objValue = sp.getObjectVar().getValue();
//        if (log.isDebugEnabled()) {
//            log.debug(objValue);
//        }
//        if (objValue == null || !(objValue instanceof Literal)) {
//            throw new IllegalArgumentException("not a valid magic search: " + sp);
//        }
//        
//        final Var subjVar = sp.getSubjectVar();
//
//        final IVariableOrConstant<IV> search = 
//            com.bigdata.bop.Var.var(subjVar.getName());
//        
//        IVariableOrConstant<IV> relevance = new Constant(TermId.mockIV(VTE.BNODE));
//        IVariableOrConstant<IV> rank = new Constant(TermId.mockIV(VTE.BNODE));
//        Literal minRank = null;
//        Literal maxRank = null;
//        Literal minRelevance = null;
//        Literal maxRelevance = null;
//        boolean matchAllTerms = false;
//        
//        for (StatementPattern meta : metadata) {
//            if (!meta.getSubjectVar().equals(subjVar)) {
//                throw new IllegalArgumentException("illegal metadata: " + meta);
//            }
//            final Value pVal = meta.getPredicateVar().getValue();
//            final Var oVar = meta.getObjectVar();
//            final Value oVal = oVar.getValue();
//            if (pVal == null) {
//                throw new IllegalArgumentException("illegal metadata: " + meta);
//            }
//            if (BD.RELEVANCE.equals(pVal)) {
//                if (oVar.hasValue()) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                relevance = com.bigdata.bop.Var.var(oVar.getName());
//            } else if (BD.RANK.equals(pVal)) {
//                if (oVar.hasValue()) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                rank = com.bigdata.bop.Var.var(oVar.getName());
//            } else if (BD.MIN_RANK.equals(pVal)) {
//                if (oVal == null || !(oVal instanceof Literal)) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                minRank = (Literal) oVal;
//            } else if (BD.MAX_RANK.equals(pVal)) {
//                if (oVal == null || !(oVal instanceof Literal)) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                maxRank = (Literal) oVal;
//            } else if (BD.MIN_RELEVANCE.equals(pVal)) {
//                if (oVal == null || !(oVal instanceof Literal)) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                minRelevance = (Literal) oVal;
//            } else if (BD.MAX_RELEVANCE.equals(pVal)) {
//                if (oVal == null || !(oVal instanceof Literal)) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                maxRelevance = (Literal) oVal;
//            } else if (BD.MATCH_ALL_TERMS.equals(pVal)) {
//                if (oVal == null || !(oVal instanceof Literal)) {
//                    throw new IllegalArgumentException("illegal metadata: " + meta);
//                }
//                matchAllTerms = ((Literal) oVal).booleanValue();
//            }
//        }
//        
//        final IAccessPathExpander expander = 
//            new FreeTextSearchExpander(database, (Literal) objValue, 
//                minRank, maxRank, minRelevance, maxRelevance, matchAllTerms);
//
//        // Decide on the correct arity for the predicate.
//        final BOp[] vars = new BOp[] {
//            search, // s = searchVar
//            relevance, // p = relevanceVar
//            rank, // o = rankVar
//            new Constant(TermId.mockIV(VTE.BNODE)), // c = reserved
//        };
//        
//        // The annotations for the predicate.
//        final List<NV> anns = new LinkedList<NV>();
//        
//        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
//                new String[] { database.getSPORelation().getNamespace() }));//
//        
//        // free text search expander or named graphs expander
//        if (expander != null)
//            anns.add(new NV(IPredicate.Annotations.ACCESS_PATH_EXPANDER, expander));
//
//        // timestamp
//        anns.add(new NV(Annotations.TIMESTAMP, database
//                .getSPORelation().getTimestamp()));
//
//        /*
//         * Explicitly set the access path / iterator flags.
//         * 
//         * Note: High level query generally permits iterator level parallelism.
//         * We set the PARALLEL flag here so it can be used if a global index
//         * view is chosen for the access path.
//         * 
//         * Note: High level query for SPARQL always uses read-only access paths.
//         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
//         * semantics then you will need to remote the READONLY flag for the
//         * mutable access paths.
//         */
//        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
//                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
//        
//        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
////        return new SPOPredicate(
////                new String[] { database.getSPORelation().getNamespace() },
////                -1, // partitionId
////                search, // s = searchVar
////                relevance, // p = relevanceVar
////                new Constant(DummyIV.INSTANCE), // o = reserved
////                new Constant(DummyIV.INSTANCE), // c = reserved
////                false, // optional
////                null, // filter on elements visited by the access path.
////                expander // free text search expander or named graphs expander
////                );
//        
//    }
//    
//    /**
//     * Takes a ValueExpression from a sesame Filter or LeftJoin and turns it
//     * into a bigdata {@link IConstraint}.
//     */
//    private IConstraint toConstraint(final ValueExpr ve) {
//
//        return new SPARQLConstraint(AST2BOpUtility.toVE(ve));
//        
//    }

}
