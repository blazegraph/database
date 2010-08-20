/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 20, 2010
 */

package com.bigdata.relation.rule.eval;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bop.ArrayBindingSet;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.io.IStreamSerializer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Base implementation for {@link IJoinNexus}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractJoinNexus implements IJoinNexus {

    private final static transient Logger log = Logger.getLogger(AbstractJoinNexus.class);
    
    private final IJoinNexusFactory joinNexusFactory;
    
    protected final IIndexManager indexManager;
    
    /** Note: cached. */
    protected final IResourceLocator<?> resourceLocator;

    private final ActionEnum action;
    
    private final long writeTimestamp;
    
    protected final long readTimestamp;

    protected final int chunkCapacity;
    protected final int chunkOfChunksCapacity;
    private final boolean forceSerialExecution;
    private final int maxParallelSubqueries;
    private final int fullyBufferedReadThreshold;
    private final long chunkTimeout;

    /**
     * The {@link TimeUnit}s in which the {@link #chunkTimeout} is measured.
     */
    private static final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;

    final public int getChunkOfChunksCapacity() {

        return chunkOfChunksCapacity;
        
    }

    final public int getChunkCapacity() {

        return chunkCapacity;
        
    }
    
    final public int getFullyBufferedReadThreshold() {
        
        return fullyBufferedReadThreshold;
        
    }

    final public String getProperty(final String name, final String defaultValue) {

        // @todo pass namespace in with the IJoinNexusFactory?
        return Configuration.getProperty(indexManager,
                joinNexusFactory.getProperties(), null/* namespace */, name,
                defaultValue);

    }

    final public <T> T getProperty(final String name, final String defaultValue,
            final IValidator<T> validator) {

        // @todo pass namespace in with the IJoinNexusFactory?
        return Configuration.getProperty(indexManager,
                joinNexusFactory.getProperties(), null/* namespace */, name,
                defaultValue, validator);

    }

    protected final int solutionFlags;
    
    protected final IElementFilter<?> filter;

    public IElementFilter<ISolution> getSolutionFilter() {
        
        return filter == null ? null : new SolutionFilter(filter);
        
    }

    /**
     * The factory for rule evaluation plans.
     */
    protected final IEvaluationPlanFactory planFactory;

    /**
     * @todo caching for the same relation and database state shared across join
     *       nexus instances.
     */
    private final IRangeCountFactory rangeCountFactory = new DefaultRangeCountFactory(
            this);

    private final IRuleStatisticsFactory ruleStatisticsFactory = new IRuleStatisticsFactory() {

        public RuleStats newInstance(IStep step) {
            
            return new RuleStats(step);
            
        }

        public RuleStats newInstance(IRuleState ruleState) {
         
            return new RuleStats(ruleState);

        }
        
    };
    
    /**
     * @param joinNexusFactory
     *            The object used to create this instance and which can be used
     *            to create other instances as necessary for distributed rule
     *            execution.
     * @param indexManager
     *            The object used to resolve indices, relations, etc.
     */
    protected AbstractJoinNexus(final IJoinNexusFactory joinNexusFactory,
            final IIndexManager indexManager) {

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.joinNexusFactory = joinNexusFactory;
        
        this.indexManager = indexManager;
        
        this.resourceLocator = indexManager.getResourceLocator();
        
        this.action = joinNexusFactory.getAction();
        
        this.writeTimestamp = joinNexusFactory.getWriteTimestamp();

        this.readTimestamp = joinNexusFactory.getReadTimestamp();
        
        forceSerialExecution = Boolean.parseBoolean(getProperty(
                AbstractResource.Options.FORCE_SERIAL_EXECUTION,
                AbstractResource.Options.DEFAULT_FORCE_SERIAL_EXECUTION));

        maxParallelSubqueries = getProperty(
                AbstractResource.Options.MAX_PARALLEL_SUBQUERIES,
                AbstractResource.Options.DEFAULT_MAX_PARALLEL_SUBQUERIES,
                IntegerValidator.GTE_ZERO);

        chunkOfChunksCapacity = getProperty(
                AbstractResource.Options.CHUNK_OF_CHUNKS_CAPACITY,
                AbstractResource.Options.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY,
                IntegerValidator.GT_ZERO);

        chunkCapacity = getProperty(AbstractResource.Options.CHUNK_CAPACITY,
                AbstractResource.Options.DEFAULT_CHUNK_CAPACITY,
                IntegerValidator.GT_ZERO);

        chunkTimeout = getProperty(AbstractResource.Options.CHUNK_TIMEOUT,
                AbstractResource.Options.DEFAULT_CHUNK_TIMEOUT,
                LongValidator.GTE_ZERO);

        fullyBufferedReadThreshold = getProperty(
                AbstractResource.Options.FULLY_BUFFERED_READ_THRESHOLD,
                AbstractResource.Options.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD,
                IntegerValidator.GT_ZERO);

        this.solutionFlags = joinNexusFactory.getSolutionFlags();

        this.filter = joinNexusFactory.getSolutionFilter();
        
        this.planFactory = joinNexusFactory.getEvaluationPlanFactory();
   
    }

    public IRuleStatisticsFactory getRuleStatisticsFactory() {
        
        return ruleStatisticsFactory;
        
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        
        return joinNexusFactory;
        
    }
    
    public IRangeCountFactory getRangeCountFactory() {
        
        return rangeCountFactory;
        
    }
    
    final public boolean forceSerialExecution() {

        if (log.isInfoEnabled())
            log.info("forceSerialExecution="+forceSerialExecution);

        return forceSerialExecution;
        
    }
    
    final public int getMaxParallelSubqueries() {
        
        return maxParallelSubqueries;
        
    }
    
    final public ActionEnum getAction() {
        
        return action;
        
    }
    
    final public long getWriteTimestamp() {

        return writeTimestamp;

    }

    final public long getReadTimestamp() {
        
        return readTimestamp;
        
    }

    /**
     * The head relation is what we write on for mutation operations and is also
     * responsible for minting new elements from computed {@link ISolution}s.
     * This method depends solely on the name of the head relation and the
     * timestamp of interest for the view.
     */
    public IRelation getHeadRelationView(final IPredicate pred) {
        
//        if (pred == null)
//            throw new IllegalArgumentException();
        
        if (pred.getRelationCount() != 1)
            throw new IllegalArgumentException();
        
        final String relationName = pred.getOnlyRelationName();
        
        final long timestamp = (getAction().isMutation() ? getWriteTimestamp()
                : getReadTimestamp(/*relationName*/));

        final IRelation relation = (IRelation) resourceLocator.locate(
                relationName, timestamp);
        
        if(log.isDebugEnabled()) {
            
            log.debug("predicate: "+pred+", head relation: "+relation);
            
        }
        
        return relation;
        
    }

//    /**
//     * The tail relations are the views from which we read. This method depends
//     * solely on the name(s) of the relation(s) and the timestamp of interest
//     * for the view.
//     * 
//     * @todo we can probably get rid of the cache used by this method now that
//     *       calling this method has been factored out of the join loops.
//     */
//    @SuppressWarnings("unchecked")
//    public IRelation getTailRelationView(final IPredicate pred) {
//
////        if (pred == null)
////            throw new IllegalArgumentException();
//        
//        final int nsources = pred.getRelationCount();
//
//        final IRelation relation;
//        
//        if (nsources == 1) {
//
//            final String relationName = pred.getOnlyRelationName();
//
//            relation = (IRelation) resourceLocator.locate(relationName,
//                    readTimestamp);
//                
//        } else if (nsources == 2) {
//
//            final String relationName0 = pred.getRelationName(0);
//
//            final String relationName1 = pred.getRelationName(1);
//
////            final long timestamp0 = getReadTimestamp(/*relationName0*/);
////
////            final long timestamp1 = getReadTimestamp(/*relationName1*/);
//
//            final IRelation relation0 = (IRelation) resourceLocator.locate(
//                    relationName0, readTimestamp);//timestamp0);
//
//            final IRelation relation1 = (IRelation) resourceLocator.locate(
//                    relationName1, readTimestamp);//timestamp1);
//
//            relation = new RelationFusedView(relation0, relation1).init();
//
//        } else {
//
//            throw new UnsupportedOperationException();
//
//        }
//
//        if(log.isDebugEnabled()) {
//            
//            log.debug("predicate: "+pred+", tail relation: "+relation);
//            
//        }
//        
//        return relation;
//        
//    }

    /**
     * @deprecated by {@link #getTailAccessPath(IRelation, IPredicate)}
     * 
     * @see #getTailAccessPath(IRelation, IPredicate).
     */
    public IAccessPath getTailAccessPath(final IPredicate predicate) {
     
        // Resolve the relation name to the IRelation object.
        final IRelation relation = getTailRelationView(predicate);

        return getTailAccessPath(relation, predicate);

    }

//    /**
//     * When {@link #backchain} is <code>true</code> and the tail predicate is
//     * reading on the {@link SPORelation}, then the {@link IAccessPath} is
//     * wrapped so that the iterator will visit the backchained inferences as
//     * well. On the other hand, if {@link IPredicate#getPartitionId()} is
//     * defined (not <code>-1</code>) then the returned access path will be for
//     * the specified shard using the data service local index manager (
//     * {@link #indexManager} MUST be the data service local index manager for
//     * this case) and expanders WILL NOT be applied (they require a view of the
//     * total relation, not just a shard).
//     * 
//     * @see InferenceEngine
//     * @see BackchainAccessPath
//     * 
//     * @todo consider encapsulating the {@link IRangeCountFactory} in the
//     *       returned access path for non-exact range count requests. this will
//     *       make it slightly harder to write the unit tests for the
//     *       {@link IEvaluationPlanFactory}
//     */
//    public IAccessPath getTailAccessPath(final IRelation relation,
//            final IPredicate predicate) {
//
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
//            return ((SPORelation) relation).getAccessPathForIndexPartition(
//                    indexManager, predicate);
//
//        }
//
////        // Find the best access path for the predicate for that relation.
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
//        final ISolutionExpander expander = predicate.getSolutionExpander();
//        
//        if (expander != null) {
//            
//            // allow the predicate to wrap the access path : @todo caching on AP?
//            accessPath = expander.getAccessPath(accessPath);
//            
//        }
//        
//        if(backchain && relation instanceof SPORelation) {
//
//            if (expander == null || expander.backchain()) {
//            
//                final SPORelation spoRelation = (SPORelation)relation;
//            
//                accessPath = new BackchainAccessPath(
//                        spoRelation.getContainer(), accessPath,
//                        joinNexusFactory.isOwlSameAsUsed ? Boolean.TRUE
//                                : Boolean.FALSE);
//                
//            }
//            
//        }
//        
//        // return that access path.
//        return accessPath;
//
//    }

    public Iterator<PartitionLocator> locatorScan(
            final AbstractScaleOutFederation<?> fed,
            final IPredicate<?> predicate) {

        final long timestamp = getReadTimestamp();

        // Note: assumes that we are NOT using a view of two relations.
        final IRelation<?> relation = (IRelation<?>) fed.getResourceLocator().locate(
                predicate.getOnlyRelationName(), timestamp);

        /*
         * Find the best access path for the predicate for that relation.
         * 
         * Note: All we really want is the [fromKey] and [toKey] for that
         * predicate and index. In general, that information is available from
         * IKeyOrder#getFromKey() and IKeyOrder#getToKey(). However, we also
         * need to know whether quads or triples are being used for RDF and that
         * information is carried by the AbstractTripleStore container or the
         * SPORelation. 
         * 
         * Note: This MUST NOT layer on expander or backchain access path
         * overlays. Those add overhead during construction and the layering
         * also hides the [fromKey] and [toKey].
         */
        @SuppressWarnings("unchecked")
        final AccessPath<?> accessPath = (AccessPath<?>) relation
                .getAccessPath((IPredicate)predicate);

        // Note: assumes scale-out (EDS or JDS).
        final IClientIndex ndx = (IClientIndex) accessPath.getIndex();

        /*
         * Note: could also be formed from relationName + "." +
         * keyOrder.getIndexName(), which is cheaper unless the index metadata
         * is cached.
         */
        final String name = ndx.getIndexMetadata().getName();

        return fed.locatorScan(name, timestamp, accessPath.getFromKey(),
                accessPath.getToKey(), false/* reverse */);

    }
    
    final public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }

    @SuppressWarnings("unchecked")
    final public boolean bind(final IRule rule, final int index,
            final Object e, final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, rule.getTail(index), bindings);

        // verify constraints.
        return rule.isConsistent(bindings);

    }

    final public boolean bind(final IPredicate<?> pred,
            final IConstraint constraint, final Object e,
            final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, pred, bindings);

        if (constraint != null) {

            // verify constraint.
            return constraint.accept(bindings);
        
        }
        
        // no constraint.
        return true;
        
    }
    
    @SuppressWarnings("unchecked")
    final private void copyValues(final IElement e, final IPredicate<?> pred,
            final IBindingSet bindingSet) {

        for (int i = 0; i < pred.arity(); i++) {

            final IVariableOrConstant<?> t = pred.get(i);

            if (t.isVar()) {

                final IVariable<?> var = (IVariable<?>) t;

                final Constant<?> newval = new Constant(e.get(i));

                bindingSet.set(var, newval);

            }

        }

    }

    final public ISolution newSolution(final IRule rule,
            final IBindingSet bindingSet) {

        final Solution solution = new Solution(this, rule, bindingSet);

        if (log.isDebugEnabled()) {

            log.debug(solution.toString());

        }

        return solution;

    }

    final public int solutionFlags() {

        return solutionFlags;

    }

//    /**
//     * FIXME unit tests for DISTINCT with a head and ELEMENT, with bindings and
//     * a head, with bindings but no head, and with a head but no bindings
//     * (error). See {@link #runQuery(IStep)}
//     * 
//     * FIXME unit tests for SORT with and without DISTINCT and with the various
//     * combinations used in the unit tests for DISTINCT. Note that SORT, unlike
//     * DISTINCT, requires that all solutions are materialized before any
//     * solutions can be returned to the caller. A lot of optimization can be
//     * done for SORT implementations, including merge sort of large blocks (ala
//     * map/reduce), using compressed sort keys or word sort keys with 2nd stage
//     * disambiguation, etc.
//     * 
//     * FIXME Add property for sort {ascending,descending,none} to {@link IRule}.
//     * The sort order can also be specified in terms of a sequence of variables.
//     * The choice of the variable order should be applied here.
//     * 
//     * FIXME The properties that govern the Unicode collator for the generated
//     * sort keys should be configured by the {@link RDFJoinNexusFactory}. In
//     * particular, Unicode should be handled however it is handled for the
//     * {@link LexiconRelation}.
//     */
//    public ISortKeyBuilder<IBindingSet> newBindingSetSortKeyBuilder(final IRule rule) {
//
//        final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance();
//        
//        final int nvars = rule.getVariableCount();
//        
//        final IVariable[] vars = new IVariable[nvars];
//        
//        {
//
//            final Iterator<IVariable> itr = rule.getVariables();
//
//            int i = 0;
//
//            while (itr.hasNext()) {
//
//                vars[i++] = itr.next();
//                
//            }
//
//        }
//
//        return new BindingSetSortKeyBuilder(keyBuilder, vars);
//        
//    }
    
    /**
     * FIXME Custom serialization for solution sets, especially since there
     * tends to be a lot of redundancy in the data arising from how bindings are
     * propagated during JOINs.
     * 
     * @todo We can sort the {@link ISolution}s much like we already do for
     *       DISTINCT or intend to do for SORT and use the equivalent of leading
     *       key compression to reduce IO costs (or when they are SORTed we
     *       could leverage that to produce a more compact serialization).
     * 
     * @see SPOSolutionSerializer (needs to be written).
     */
    public IStreamSerializer<ISolution[]> getSolutionSerializer() {
        
        return SerializerUtil.STREAMS;
        
    }

    /**
     * FIXME Custom serialization for binding sets, especially since there tends
     * to be a lot of redundancy in the data arising from how bindings are
     * propagated during JOINs.
     * 
     * @todo We can sort the {@link ISolution}s much like we already do for
     *       DISTINCT or intend to do for SORT and use the equivalent of leading
     *       key compression to reduce IO costs (or when they are SORTed we
     *       could leverage that to produce a more compact serialization).
     * 
     * @see SPOBindingSetSerializer, which has not been finished.
     */
    public IStreamSerializer<IBindingSet[]> getBindingSetSerializer() {
        
        return SerializerUtil.STREAMS;
        
    }

    final public IBindingSet newBindingSet(final IRule rule) {

        final IBindingSet constants = rule.getConstants();

        final int nconstants = constants.size();
        
        final IBindingSet bindingSet = new ArrayBindingSet(rule
                .getVariableCount()
                + nconstants);

        if (nconstants > 0) {
        
            /*
             * Bind constants declared by the rule before returning the binding
             * set to the caller.
             */
            
            final Iterator<Map.Entry<IVariable, IConstant>> itr = constants
                    .iterator();
            
            while(itr.hasNext()) {
                
                final Map.Entry<IVariable,IConstant> entry = itr.next();
                
                bindingSet.set(entry.getKey(), entry.getValue());
                
            }
            
        }
        
        return bindingSet;
        
    }

    final public IRuleTaskFactory getRuleTaskFactory(final boolean parallel,
            final IRule rule) {

        if (rule == null)
            throw new IllegalArgumentException();

        // is there a task factory override?
        IRuleTaskFactory taskFactory = rule.getTaskFactory();

        if (taskFactory == null) {

            // no, use the default factory.
            taskFactory = joinNexusFactory.getDefaultRuleTaskFactory();

        }
        
        /*
         * Note: Now handled by the MutationTask itself.
         */
//        if (getAction().isMutation() && (!parallel || forceSerialExecution())) {
//
//            /*
//             * Tasks for sequential mutation steps are always wrapped to ensure
//             * that the thread-safe buffer is flushed onto the mutable relation
//             * after each rule executes. This is necessary in order for the
//             * results of one rule in a sequential program to be visible to the
//             * next rule in that sequential program.
//             */
//            
//            taskFactory = new RunRuleAndFlushBufferTaskFactory(taskFactory);
//
//        }

        return taskFactory;

    }

    final public IEvaluationPlanFactory getPlanFactory() {
        
        return planFactory;
        
    }
    
    final public IResourceLocator getRelationLocator() {
        
        return resourceLocator;
        
    }

    final public IBuffer<ISolution> newUnsynchronizedBuffer(
            final IBuffer<ISolution[]> targetBuffer, final int chunkCapacity) {

        // MAY be null.
        final IElementFilter<ISolution> filter = getSolutionFilter();
        
        return new UnsynchronizedArrayBuffer<ISolution>(targetBuffer,
                chunkCapacity, filter);
        
    }
    
    /**
     * Note: {@link ISolution} (not relation elements) will be written on the
     * buffer concurrently by different rules so there is no natural order for
     * the elements in the buffer.
     */
    final public IBlockingBuffer<ISolution[]> newQueryBuffer() {

        if (getAction().isMutation())
            throw new IllegalStateException();
        
        return new BlockingBuffer<ISolution[]>(chunkOfChunksCapacity,
                chunkCapacity, chunkTimeout, chunkTimeoutUnit);
        
    }
    
//    /**
//     * Buffer writes on {@link IMutableRelation#insert(IChunkedIterator)} when it is
//     * {@link #flush() flushed}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <E>
//     */
//    public static class InsertSPOAndJustificationBuffer<E> extends AbstractSolutionBuffer<E> {
//        
//        /**
//         * @param capacity
//         * @param relation
//         */
//        public InsertSPOAndJustificationBuffer(final int capacity,
//                final IMutableRelation<E> relation) {
//
//            super(capacity, relation);
//            
//        }
//
//        @Override
//        protected long flush(final IChunkedOrderedIterator<ISolution<E>> itr) {
//
//            try {
//
//                /*
//                 * The mutation count is the #of SPOs written (there is one
//                 * justification written per solution generated, but the
//                 * mutation count does not reflect duplicate justifications -
//                 * only duplicate statements).
//                 * 
//                 * Note: the optional filter for the ctor was already applied.
//                 * If an element/solution was rejected, then it is not in the
//                 * buffer and we will never see it during flush().
//                 */
//                
//                long mutationCount = 0;
//                
//                while (itr.hasNext()) {
//
//                    final ISolution<E>[] chunk = itr.nextChunk();
//
//                    mutationCount += writeChunk(chunk);
//                    
//                }
//                
//                return mutationCount;
//                
//            } finally {
//
//                itr.close();
//
//            }
//            
//        }
//        
//        private long writeChunk(final ISolution<E>[] chunk) {
//
//            final int n = chunk.length;
//            
//            if(log.isDebugEnabled()) 
//                log.debug("chunkSize="+n);
//            
//            final long begin = System.currentTimeMillis();
//
//            final SPO[] a = new SPO[ n ];
//
//            final Justification[] b = new Justification[ n ];
//
//            for(int i=0; i<chunk.length; i++) {
//                
//                if(log.isDebugEnabled()) {
//                    
//                    log.debug("chunk["+i+"] = "+chunk[i]);
//                    
//                }
//                
//                final ISolution<SPO> solution = (ISolution<SPO>) chunk[i];
//                
//                a[i] = solution.get();
//                
//                b[i] = new Justification(solution);
//                                
//            }
//            
//            final SPORelation r = (SPORelation) (IMutableRelation) getRelation();
//
//            /*
//             * Use a thread pool to write out the statement and the
//             * justifications concurrently. This drammatically reduces the
//             * latency when also writing justifications.
//             */
//
//            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);
//
//            /*
//             * Note: we reject using the filter before stmts or justifications
//             * make it into the buffer so we do not need to apply the filter
//             * again here.
//             */
//
//            tasks.add(new Callable<Long>(){
//                public Long call() {
//                    return r.insert(a,a.length,null/*filter*/);
//                }
//            });
//            
//            tasks.add(new Callable<Long>(){
//                public Long call() {
//                    return r
//                            .addJustifications(new ChunkedArrayIterator<Justification>(
//                                    b.length, b, null/* keyOrder */));
//                }
//            });
//            
//            final List<Future<Long>> futures;
//
//            /*
//             * @todo The timings for the tasks that we run here are not being
//             * reported up to this point.
//             */
//            final long mutationCount;
//            try {
//
//                futures = r.getExecutorService().invokeAll(tasks);
//
//                mutationCount = futures.get(0).get();
//
//                                futures.get(1).get();
//
//            } catch (InterruptedException ex) {
//
//                throw new RuntimeException(ex);
//
//            } catch (ExecutionException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//            final long elapsed = System.currentTimeMillis() - begin;
//
//            if (log.isInfoEnabled())
//                log.info("Wrote " + mutationCount
//                                + " statements and justifications in "
//                                + elapsed + "ms");
//
//            return mutationCount;
//
//        }
//
//    }
    
//    /**
//     * Note: {@link #getSolutionFilter()} is applied by
//     * {@link #newUnsynchronizedBuffer(IBuffer, int)} and NOT by the buffer
//     * returned by this method.
//     */
//    @SuppressWarnings("unchecked")
//    public IBuffer<ISolution[]> newInsertBuffer(final IMutableRelation relation) {
//
//        if (getAction() != ActionEnum.Insert)
//            throw new IllegalStateException();
//
//        if (log.isDebugEnabled()) {
//
//            log.debug("relation=" + relation);
//            
//        }
//        
//        if(justify) {
//
//            /*
//             * Buffer knows how to write the computed elements on the statement
//             * indices and the computed binding sets on the justifications
//             * indices.
//             */
//            
//            return new InsertSPOAndJustificationBuffer(chunkOfChunksCapacity,
//                    relation);
//
//        }
//
//        /*
//         * Buffer resolves the computed elements and writes them on the
//         * statement indices.
//         */
//
//        return new AbstractSolutionBuffer.InsertSolutionBuffer(
//                chunkOfChunksCapacity, relation);
//
//    }

    /**
     * Note: {@link #getSolutionFilter()} is applied by
     * {@link #newUnsynchronizedBuffer(IBuffer, int)} and NOT by the buffer
     * returned by this method.
     */
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution[]> newDeleteBuffer(final IMutableRelation relation) {

        if (getAction() != ActionEnum.Delete)
            throw new IllegalStateException();

        if (log.isDebugEnabled()) {

            log.debug("relation=" + relation);

        }

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                chunkOfChunksCapacity, relation);

    }

//    @SuppressWarnings("unchecked")
//    public IChunkedOrderedIterator<ISolution> runQuery(final IStep step)
//            throws Exception {
//
//        if (step == null)
//            throw new IllegalArgumentException();
//
//        if(log.isInfoEnabled())
//            log.info("program="+step.getName());
//
//        if(isEmptyProgram(step)) {
//
//            log.warn("Empty program");
//
//            return (IChunkedOrderedIterator<ISolution>) new EmptyProgramTask(
//                    ActionEnum.Query, step).call();
//
//        }
//
//        final IChunkedOrderedIterator<ISolution> itr = (IChunkedOrderedIterator<ISolution>) runProgram(
//                ActionEnum.Query, step);
//
//        if (step.isRule() && ((IRule) step).getQueryOptions().isDistinct()) {
//
//            /*
//             * Impose a DISTINCT constraint based on the variable bindings
//             * selected by the head of the rule. The DistinctFilter will be
//             * backed by a TemporaryStore if more than one chunk of solutions is
//             * generated. That TemporaryStore will exist on the client where
//             * this method (runQuery) was executed. The TemporaryStore will be
//             * finalized and deleted when it is no longer referenced.
//             */
//
//            final ISortKeyBuilder<ISolution> sortKeyBuilder;
//
//            if (((IRule) step).getHead() != null
//                    && (solutionFlags & ELEMENT) != 0) {
//
//                /*
//                 * Head exists and elements are requested, so impose DISTINCT
//                 * based on the materialized elements.
//                 * 
//                 * FIXME The SPOSortKeyBuilder should be obtained from the head
//                 * relation. Of course there is one sort key for each access
//                 * path, but for the purposes of DISTINCT we want the sort key
//                 * to correspond to the notion of a "primary key" (the
//                 * distinctions that matter) and it does not matter which sort
//                 * order but the SPO sort order probably has the least factor of
//                 * "surprise".
//                 */
//                
//                final int arity = ((IRule)step).getHead().arity();
//                
//                sortKeyBuilder = new DelegateSortKeyBuilder<ISolution, ISPO>(
//                        new SPOSortKeyBuilder(arity)) {
//
//                    protected ISPO resolve(ISolution solution) {
//
//                        return (ISPO) solution.get();
//
//                    }
//
//                };
//
//            } else {
//
//                if ((solutionFlags & BINDINGS) != 0) {
//
//                    /*
//                     * Bindings were requested so impose DISTINCT based on those
//                     * bindings.
//                     */
//                    
//                    sortKeyBuilder = new DelegateSortKeyBuilder<ISolution, IBindingSet>(
//                            newBindingSetSortKeyBuilder((IRule) step)) {
//
//                        protected IBindingSet resolve(ISolution solution) {
//
//                            return solution.getBindingSet();
//
//                        }
//
//                    };
//
//                } else {
//
//                    throw new UnsupportedOperationException(
//                            "You must specify BINDINGS since the rule does not have a head: "
//                                    + step);
//
//                }
//
//            }
//            
//            return new ChunkedConvertingIterator<ISolution, ISolution>(itr,
//                    new DistinctFilter<ISolution>(indexManager) {
//
//                protected byte[] getSortKey(ISolution e) {
//                    
//                    return sortKeyBuilder.getSortKey(e);
//                    
//                }
//                
//            });
//
//        }
//
//        return itr;
//
//    }
    
    final public long runMutation(final IStep step) throws Exception {

        if (step == null)
            throw new IllegalArgumentException();

        if (!action.isMutation())
            throw new IllegalStateException();

        if (step.isRule() && ((IRule) step).getHead() == null) {

            throw new IllegalArgumentException("No head for this rule: " + step);

        }
        
        if(log.isInfoEnabled())
            log.info("action=" + action + ", program=" + step.getName());
        
        if(isEmptyProgram(step)) {

            log.warn("Empty program");

            return (Long) new EmptyProgramTask(action, step).call();

        }
        
        return (Long) runProgram(action, step);

    }
    
    /**
     * Return true iff the <i>step</i> is an empty {@link IProgram}.
     * 
     * @param step
     *            The step.
     */
    final protected boolean isEmptyProgram(final IStep step) {

        if (!step.isRule() && ((IProgram) step).stepCount() == 0) {

            return true;

        }

        return false;

    }

    /**
     * Core impl. This handles the logic required to execute the program either
     * on a target {@link DataService} (highly efficient) or within the client
     * using the {@link IClientIndex} to submit operations to the appropriate
     * {@link DataService}(s) (not very efficient, even w/o RMI).
     * 
     * @return Either an {@link IChunkedOrderedIterator} (query) or {@link Long}
     *         (mutation count).
     */
    final protected Object runProgram(final ActionEnum action, final IStep step)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        final IIndexManager indexManager = getIndexManager();

        if (indexManager instanceof IBigdataFederation<?>) {

            // distributed program execution.
            return runDistributedProgram((IBigdataFederation<?>) indexManager,
                    action, step);

        } else {
            
            // local Journal or TemporaryStore execution.
            return runLocalProgram(action, step);

        }

    }

    /**
     * This variant handles both local indices on a {@link TemporaryStore} or
     * {@link Journal} WITHOUT concurrency controls (fast).
     */
    final protected Object runLocalProgram(final ActionEnum action,
            final IStep step) throws Exception {

        if (log.isInfoEnabled())
            log.info("Running local program: action=" + action + ", program="
                    + step.getName());

        final IProgramTask innerTask = new ProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

    /**
     * Runs a distributed {@link IProgram} (key-range partitioned indices, RMI,
     * and multi-machine).
     */
    final protected Object runDistributedProgram(final IBigdataFederation<?> fed,
            final ActionEnum action, final IStep step) throws Exception {

        if (log.isInfoEnabled()) {

            log.info("Running distributed program: action=" + action
                    + ", program=" + step.getName());

        }

        final IProgramTask innerTask = new ProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

//    /**
//     * This variant is submitted and executes the rules from inside of the
//     * {@link ConcurrencyManager} on the {@link LocalDataServiceImpl} (fast).
//     * <p>
//     * Note: This can only be done if all indices for the relation(s) are (a)
//     * unpartitioned; and (b) located on the SAME {@link DataService}. This is
//     * <code>true</code> for {@link LocalDataServiceFederation}. All other
//     * {@link IBigdataFederation} implementations are scale-out (use key-range
//     * partitioned indices).
//     */
//    protected Object runDataServiceProgram(final DataService dataService,
//            final ActionEnum action, final IStep step)
//            throws InterruptedException, ExecutionException {
//
//        if (log.isInfoEnabled()) {
//
//            log.info("Submitting program to data service: action=" + action
//                    + ", program=" + step.getName() + ", dataService="
//                    + dataService);
//
//        }
//        
//        final IProgramTask innerTask = new ProgramTask(action, step,
//                getJoinNexusFactory());
//
//        return dataService.submit(innerTask).get();
//
//    }

}
