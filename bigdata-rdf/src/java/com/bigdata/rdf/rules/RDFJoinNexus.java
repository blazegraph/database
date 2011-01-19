/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.rdf.rules;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.ISortKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.relation.rule.BindingSetSortKeyBuilder;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOSortKeyBuilder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.eval.AbstractJoinNexus;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRangeCountFactory;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.IRuleStatisticsFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * {@link IProgram} execution support for the RDF DB.
 * <p>
 * The rules have potential parallelism when performing closure. Each join has
 * potential parallelism as well for subqueries. We could even define a PARALLEL
 * iterator flag and have parallelism across index partitions for a
 * read-historical iterator since the data service locators are immutable for
 * historical reads.
 * <p>
 * Rule-level parallelism (for fix point closure of a rule set) and join
 * subquery-level parallelism could be distributed to available workers in a
 * cluster. In a similar way, high-level queries could be distributed to workers
 * in a cluster to evaluation. Such distribution would increase the practical
 * parallelism beyond what a single machine could support as long as the total
 * parallelism does not overload the cluster.
 * <p>
 * There is a pragmatic limit on the #of concurrent threads for a single host.
 * When those threads target a blocking queue, then thread contention becomes
 * very high and throughput drops dramatically. We can reduce this problem by
 * allocating a distinct {@link UnsynchronizedArrayBuffer} to each task. The
 * task collects a 'chunk' in the {@link UnsynchronizedArrayBuffer}. When full,
 * the buffer propagates onto a thread-safe buffer of chunks which flushes
 * either on an {@link IMutableRelation} (mutation) or feeding an
 * {@link IAsynchronousIterator} (high-level query). It is chunks themselves
 * that accumulate in this thread-safe buffer, so each add() on that buffer may
 * cause the thread to yield, but the return for yielding is an entire chunk in
 * the buffer, not just a single element.
 * <p>
 * There is one high-level buffer factory corresponding to each of the kinds of
 * {@link ActionEnum}: {@link #newQueryBuffer()};
 * {@link #newInsertBuffer(IMutableRelation)}; and
 * {@link #newDeleteBuffer(IMutableRelation)}. In addition there is one for
 * {@link UnsynchronizedArrayBuffer}s -- this is a buffer that is NOT
 * thread-safe and that is designed to store a single chunk of elements, e.g.,
 * in an array E[N]).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexus extends AbstractJoinNexus implements IJoinNexus {

    protected final static transient Logger log = Logger.getLogger(RDFJoinNexus.class);
    
    private final RDFJoinNexusFactory joinNexusFactory;

    private final boolean justify;
    
    /**
     * when <code>true</code> the backchainer will be enabled for access path
     * reads.
     */
    private final boolean backchain;

    private final IRuleStatisticsFactory ruleStatisticsFactory = new IRuleStatisticsFactory() {

        public RuleStats newInstance(IStep step) {
            
            return new RDFRuleStats(step);
            
        }

        public RuleStats newInstance(IRuleState ruleState) {
         
            return new RDFRuleStats(null, getReadTimestamp(), ruleState);

        }
        
//        /**
//         * Factory will resolve term identifiers in {@link IPredicate}s in the
//         * tail of the {@link IRule} to {@link BigdataValue}s unless the
//         * {@link IIndexManager} is an {@link IBigdataFederation}.
//         * 
//         * @todo translation of term identifiers is disabled. someone is
//         *       interrupting the thread logging the {@link RuleStats}. until i
//         *       can figure out who that is, you will see term identifiers
//         *       rather than {@link BigdataValue}s.
//         */
//        public RuleStats newInstancex(IRuleState ruleState) {
//            
//            return new RDFRuleStats(
//                    (indexManager instanceof IBigdataFederation<?> ? null
//                            : indexManager), //
//                        getReadTimestamp(), //
//                        ruleState//
//                        );
//            
//        }
        
    };
    
    /**
     * Extends {@link RuleStats}s to translate the tail predicates back into
     * RDF by resolving the term identifiers to {@link BigdataValue}s.
     */
    private static class RDFRuleStats extends RuleStats {

        private final IIndexManager indexManager;
        private final long timestamp;
        
        public RDFRuleStats(IStep step) {

            super(step);

            indexManager = null;
            
            timestamp = 0L; // ignored.
            
        }
        
        /**
         * 
         * @param indexManager
         *            When non-<code>null</code>, this is used to resolve
         *            the term identifiers in the {@link IPredicate}s in the
         *            tail of the rule to {@link BigdataValue}s.
         * 
         * @param ruleState
         */
        public RDFRuleStats(final IIndexManager indexManager,
                final long timestamp, final IRuleState ruleState) {

            super(ruleState);

            this.indexManager = indexManager;
            
            this.timestamp = timestamp;
            
        }

        @Override
        @SuppressWarnings("unchecked")
        protected String toString(final IPredicate pred) {

            if (indexManager == null) {

                return pred.toString().replace(", ", " ");
                
            }

            final SPORelation spoRelation = (SPORelation) indexManager
                    .getResourceLocator().locate(pred.getRelationName(0),
                            timestamp);

            final AbstractTripleStore db = spoRelation.getContainer();

            final Object s, p, o;
            try {

                {

                    final IVariableOrConstant<IV> t = pred.get(0);

                    if (t.isVar())
                        s = t.getName();
                    else
                        s = db.toString(t.get());

                }

                {

                    final IVariableOrConstant<IV> t = pred.get(1);

                    if (t.isVar())
                        p = t.getName();
                    else
                        p = db.toString(t.get());

                }

                {

                    final IVariableOrConstant<IV> t = pred.get(2);

                    if (t.isVar())
                        o = t.getName();
                    else
                        o = db.toString(t.get());

                }
            } catch (Throwable t) {
                
                /*
                 * @todo It appears that someone is interrupting the thread in
                 * which the logging data is being generated. You can see this
                 * if you enable translation of term identifiers above in the
                 * factory that produces instances of this class.
                 */
                
                throw new RuntimeException("pred=" + pred + ", timestamp="
                        + timestamp + ", indexManager=" + indexManager
                        + ", db=" + db, t);
            }
           
            return "(" + s + " " + p + " " + o + ")";
            
        }
        
    }
        
	/**
	 * @param joinNexusFactory
	 *            The object used to create this instance and which can be used
	 *            to create other instances as necessary for distributed rule
	 *            execution.
	 * @param indexManager
	 *            The object used to resolve indices, relations, etc.
	 */
	public RDFJoinNexus(final RDFJoinNexusFactory joinNexusFactory,
			final IIndexManager indexManager) {

        super(joinNexusFactory, indexManager);
	    
        this.joinNexusFactory = joinNexusFactory;
        
        this.justify = joinNexusFactory.justify;
        
        this.backchain = joinNexusFactory.backchain;
        
    }

	@Override
    public IRuleStatisticsFactory getRuleStatisticsFactory() {
        
        return ruleStatisticsFactory;
        
    }
    
    /**
     * When {@link #backchain} is <code>true</code> and the tail predicate is
     * reading on the {@link SPORelation}, then the {@link IAccessPath} is
     * wrapped so that the iterator will visit the backchained inferences as
     * well. On the other hand, if {@link IPredicate#getPartitionId()} is
     * defined (not <code>-1</code>) then the returned access path will be for
     * the specified shard using the data service local index manager (
     * {@link #indexManager} MUST be the data service local index manager for
     * this case) and expanders WILL NOT be applied (they require a view of the
     * total relation, not just a shard).
     * 
     * @see InferenceEngine
     * @see BackchainAccessPath
     * 
     * @todo consider encapsulating the {@link IRangeCountFactory} in the
     *       returned access path for non-exact range count requests. this will
     *       make it slightly harder to write the unit tests for the
     *       {@link IEvaluationPlanFactory}
     */
    @Override
	@SuppressWarnings("unchecked")
    public IAccessPath getTailAccessPath(final IRelation relation,
            final IPredicate predicate) {

//        if (predicate.getPartitionId() != -1) {
//
//            /*
//             * Note: This handles a read against a local index partition. For
//             * scale-out, the [indexManager] will be the data service's local
//             * index manager.
//             * 
//             * Note: Expanders ARE NOT applied in this code path. Expanders
//             * require a total view of the relation, which is not available
//             * during scale-out pipeline joins. Likewise, the [backchain]
//             * property will be ignored since it is handled by an expander.
//             * 
//             * @todo If getAccessPathForIndexPartition() is raised into the
//             * IRelation interface, then we can get rid of the cast to the
//             * SPORelation implementation.
//             */
//
////            return ((SPORelation) relation).getAccessPathForIndexPartition(
////                    indexManager, predicate);
//            return relation.getAccessPath(indexManager, relation
//                    .getKeyOrder(predicate), predicate);
//
//        }
//
//        // Find the best access path for the predicate for that relation.
//        IAccessPath accessPath = relation.getAccessPath(predicate);
////
////        if (predicate.getPartitionId() != -1) {
////
////            /*
////             * Note: The expander can not run against a shard since it assumes
////             * access to the full key range of the index. Expanders are
////             * convenient and work well for stand alone indices, but they should
////             * be replaced by rule rewrites for scale-out.
////             */
////
////            return accessPath;
////            
////        }
//        
        final IKeyOrder keyOrder = relation.getKeyOrder(predicate);

        IAccessPath accessPath = relation.getAccessPath(
                indexManager/* localIndexManager */, keyOrder, predicate);

        final IAccessPathExpander expander = predicate.getAccessPathExpander();
//        
//        if (expander != null) {
//            
//            // allow the predicate to wrap the access path
//            accessPath = expander.getAccessPath(accessPath);
//            
//        }

        // @todo raise into SPORelation#getAccessPath/3?
        // @see https://sourceforge.net/apps/trac/bigdata/ticket/231
        if(backchain && relation instanceof SPORelation) {

            if (expander == null || expander.backchain()) {
            
                final SPORelation spoRelation = (SPORelation)relation;
            
                accessPath = new BackchainAccessPath(
                        spoRelation.getContainer(), accessPath,
                        joinNexusFactory.isOwlSameAsUsed ? Boolean.TRUE
                                : Boolean.FALSE);
                
            }
            
        }
        
        // return that access path.
        return accessPath;

    }

//    @SuppressWarnings("unchecked")
//    public boolean bind(final IRule rule, final int index, final Object e,
//            final IBindingSet bindings) {
//
//        // propagate bindings from the visited object into the binding set.
//        copyValues((IElement) e, rule.getTail(index), bindings);
//
//        // verify constraints.
//        return rule.isConsistent(bindings);
//
//    }
//    
//    public boolean bind(final IPredicate<?> pred, final IConstraint constraint,
//            final Object e, final IBindingSet bindings) {
//
//        // propagate bindings from the visited object into the binding set.
//        copyValues((IElement) e, pred, bindings);
//
//        if (constraint != null) {
//
//            // verify constraint.
//            return constraint.accept(bindings);
//        
//        }
//        
//        // no constraint.
//        return true;
//        
//    }
    
//    @SuppressWarnings("unchecked")
//    private void copyValues(final IElement e, final IPredicate<?> pred,
//            final IBindingSet bindingSet) {
//
//        for (int i = 0; i < pred.arity(); i++) {
//
//            final IVariableOrConstant<?> t = pred.get(i);
//
//            if (t.isVar()) {
//
//                final IVariable<?> var = (IVariable<?>) t;
//
//                final Constant<?> newval = new Constant(e.get(i));
//
//                bindingSet.set(var, newval);
//
//            }
//
//        }
//
//    }

    public IConstant fakeBinding(IPredicate pred, Var var) {

        return fakeTermId;

    }

    final private static transient IConstant<IV> fakeTermId = 
        new Constant<IV>(new TermId(VTE.URI, -1L));

    /**
     * FIXME unit tests for DISTINCT with a head and ELEMENT, with bindings and
     * a head, with bindings but no head, and with a head but no bindings
     * (error). See {@link #runQuery(IStep)}
     * 
     * FIXME unit tests for SORT with and without DISTINCT and with the various
     * combinations used in the unit tests for DISTINCT. Note that SORT, unlike
     * DISTINCT, requires that all solutions are materialized before any
     * solutions can be returned to the caller. A lot of optimization can be
     * done for SORT implementations, including merge sort of large blocks (ala
     * map/reduce), using compressed sort keys or word sort keys with 2nd stage
     * disambiguation, etc.
     * 
     * FIXME Add property for sort {ascending,descending,none} to {@link IRule}.
     * The sort order can also be specified in terms of a sequence of variables.
     * The choice of the variable order should be applied here.
     * 
     * FIXME The properties that govern the Unicode collator for the generated
     * sort keys should be configured by the {@link RDFJoinNexusFactory}. In
     * particular, Unicode should be handled however it is handled for the
     * {@link LexiconRelation}.
     */
    public ISortKeyBuilder<IBindingSet> newBindingSetSortKeyBuilder(final IRule rule) {

        final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance();
        
        final int nvars = rule.getVariableCount();
        
        final IVariable[] vars = new IVariable[nvars];
        
        {

            final Iterator<IVariable> itr = rule.getVariables();

            int i = 0;

            while (itr.hasNext()) {

                vars[i++] = itr.next();
                
            }

        }

        // @todo this class has RDF specific stuff in it.
        return new BindingSetSortKeyBuilder(keyBuilder, vars);
        
    }
    
    @Override
    protected ISortKeyBuilder<?> newSortKeyBuilder(final IPredicate<?> head) {

        return new SPOSortKeyBuilder(head.arity());
        
    }

    /**
     * Buffer writes on {@link IMutableRelation#insert(IChunkedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class InsertSPOAndJustificationBuffer<E> extends AbstractSolutionBuffer<E> {
        
        /**
         * @param capacity
         * @param relation
         */
        public InsertSPOAndJustificationBuffer(final int capacity,
                final IMutableRelation<E> relation) {

            super(capacity, relation);
            
        }

        @Override
        protected long flush(final IChunkedOrderedIterator<ISolution<E>> itr) {

            try {

                /*
                 * The mutation count is the #of SPOs written (there is one
                 * justification written per solution generated, but the
                 * mutation count does not reflect duplicate justifications -
                 * only duplicate statements).
                 * 
                 * Note: the optional filter for the ctor was already applied.
                 * If an element/solution was rejected, then it is not in the
                 * buffer and we will never see it during flush().
                 */
                
                long mutationCount = 0;
                
                while (itr.hasNext()) {

                    final ISolution<E>[] chunk = itr.nextChunk();

                    mutationCount += writeChunk(chunk);
                    
                }
                
                return mutationCount;
                
            } finally {

                itr.close();

            }
            
        }
        
        private long writeChunk(final ISolution<E>[] chunk) {

            final int n = chunk.length;
            
            if(log.isDebugEnabled()) 
                log.debug("chunkSize="+n);
            
            final long begin = System.currentTimeMillis();

            final SPO[] a = new SPO[ n ];

            final Justification[] b = new Justification[ n ];

            for (int i = 0; i < chunk.length; i++) {

                if(log.isDebugEnabled()) {
                    
                    log.debug("chunk["+i+"] = "+chunk[i]);
                    
                }
                
                final ISolution<SPO> solution = (ISolution<SPO>) chunk[i];
                
                a[i] = solution.get();
                
                b[i] = new Justification(solution);
                                
            }
            
            final SPORelation r = (SPORelation) (IMutableRelation) getRelation();

            /*
             * Use a thread pool to write out the statement and the
             * justifications concurrently. This drammatically reduces the
             * latency when also writing justifications.
             */

            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);

            /*
             * Note: we reject using the filter before stmts or justifications
             * make it into the buffer so we do not need to apply the filter
             * again here.
             */

            tasks.add(new Callable<Long>(){
                public Long call() {
                    return r.insert(a,a.length,null/*filter*/);
                }
            });
            
            tasks.add(new Callable<Long>(){
                public Long call() {
                    return r
                            .addJustifications(new ChunkedArrayIterator<Justification>(
                                    b.length, b, null/* keyOrder */));
                }
            });
            
            final List<Future<Long>> futures;

            /*
             * @todo The timings for the tasks that we run here are not being
             * reported up to this point.
             */
            final long mutationCount;
            try {

                futures = r.getExecutorService().invokeAll(tasks);

                mutationCount = futures.get(0).get();

                                futures.get(1).get();

            } catch (InterruptedException ex) {

                throw new RuntimeException(ex);

            } catch (ExecutionException ex) {

                throw new RuntimeException(ex);

            }

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Wrote " + mutationCount
                                + " statements and justifications in "
                                + elapsed + "ms");

            return mutationCount;

        }

    }
    
    /**
     * Overridden to handle justifications when using truth maintenance.
     * <p>
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution[]> newInsertBuffer(final IMutableRelation relation) {

        if (getAction() != ActionEnum.Insert)
            throw new IllegalStateException();

        if (log.isDebugEnabled()) {

            log.debug("relation=" + relation);
            
        }
        
        if(justify) {

            /*
             * Buffer knows how to write the computed elements on the statement
             * indices and the computed binding sets on the justifications
             * indices.
             */
            
            return new InsertSPOAndJustificationBuffer(chunkOfChunksCapacity,
                    relation);

        }

        /*
         * Buffer resolves the computed elements and writes them on the
         * statement indices.
         */

        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                chunkOfChunksCapacity, relation);

    }
    
}
