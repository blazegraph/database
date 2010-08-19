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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import com.bigdata.bop.Var;
import com.bigdata.btree.keys.DelegateSortKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.ISortKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.io.IStreamSerializer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.relation.rule.BindingSetSortKeyBuilder;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOSortKeyBuilder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultRangeCountFactory;
import com.bigdata.relation.rule.eval.EmptyProgramTask;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.IProgramTask;
import com.bigdata.relation.rule.eval.IRangeCountFactory;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.IRuleStatisticsFactory;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.ProgramTask;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.relation.rule.eval.Solution;
import com.bigdata.relation.rule.eval.SolutionFilter;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedConvertingIterator;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

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
 * @todo Factor out an abstract base class for general purpose rule execution
 *       over arbitrary relations with foreign key joins.
 * 
 * @todo add an {@link IBindingSet} serializer and drive through with the
 *       {@link IAsynchronousIterator} when wrapped for RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexus implements IJoinNexus {

    protected final static transient Logger log = Logger.getLogger(RDFJoinNexus.class);
    
    protected final static transient boolean DEBUG = log.isDebugEnabled();
    
    private final RDFJoinNexusFactory joinNexusFactory;
    
    private final IIndexManager indexManager;
    
    // Note: cached.
    private final IResourceLocator resourceLocator;

    private final ActionEnum action;
    
    private final long writeTimestamp;
    
    private final long readTimestamp;

    private final boolean justify;
    
    /**
     * when <code>true</code> the backchainer will be enabled for access path
     * reads.
     */
    private final boolean backchain;

    private final int chunkCapacity;
    private final int chunkOfChunksCapacity;
    private final boolean forceSerialExecution;
    private final int maxParallelSubqueries;
    private final int fullyBufferedReadThreshold;
    private final long chunkTimeout;

    /**
     * The {@link TimeUnit}s in which the {@link #chunkTimeout} is measured.
     */
    private static final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;

    public int getChunkOfChunksCapacity() {

        return chunkOfChunksCapacity;
        
    }

    public int getChunkCapacity() {

        return chunkCapacity;
        
    }
    
    public int getFullyBufferedReadThreshold() {
        
        return fullyBufferedReadThreshold;
        
    }

    public String getProperty(final String name, final String defaultValue) {

        // @todo pass namespace in with the RDFJoinNexusFactory?
        return Configuration.getProperty(indexManager,
                joinNexusFactory.properties, null/* namespace */, name,
                defaultValue);

    }

    public <T> T getProperty(final String name, final String defaultValue,
            final IValidator<T> validator) {

        // @todo pass namespace in with the RDFJoinNexusFactory?
        return Configuration.getProperty(indexManager,
                joinNexusFactory.properties, null/* namespace */, name,
                defaultValue, validator);

    }

    private final int solutionFlags;
    
    @SuppressWarnings("unchecked")
	private final IElementFilter filter;

    public IElementFilter<ISolution> getSolutionFilter() {
        
        return filter == null ? null : new SolutionFilter(filter);
        
    }
    
    /**
     * The default factory for rule evaluation.
     */
//    private final IRuleTaskFactory defaultTaskFactory;

    /**
     * The factory for rule evaluation plans.
     */
    private final IEvaluationPlanFactory planFactory;

    /**
     * @todo caching for the same relation and database state shared across join
     *       nexus instances.
     */
    private final IRangeCountFactory rangeCountFactory = new DefaultRangeCountFactory(
            this);

    private final IRuleStatisticsFactory ruleStatisticsFactory = new IRuleStatisticsFactory() {

        public RuleStats newInstance(IStep step) {
            
            return new RDFRuleStats(step);
            
        }

        public RuleStats newInstance(IRuleState ruleState) {
         
            return new RDFRuleStats(null, readTimestamp, ruleState);

        }
        
        /**
         * Factory will resolve term identifiers in {@link IPredicate}s in the
         * tail of the {@link IRule} to {@link BigdataValue}s unless the
         * {@link IIndexManager} is an {@link IBigdataFederation}.
         * 
         * @todo translation of term identifiers is disabled. someone is
         *       interrupting the thread logging the {@link RuleStats}. until i
         *       can figure out who that is, you will see term identifiers
         *       rather than {@link BigdataValue}s.
         */
        public RuleStats newInstancex(IRuleState ruleState) {
            
            return new RDFRuleStats(
                    (indexManager instanceof IBigdataFederation ? null
                            : indexManager), //
                        readTimestamp, //
                        ruleState//
                        );
            
        }
        
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

        @SuppressWarnings("unchecked")
        @Override
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

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.joinNexusFactory = joinNexusFactory;
        
        this.indexManager = indexManager;
        
        this.resourceLocator = indexManager.getResourceLocator();
        
        this.action = joinNexusFactory.action;
        
        this.writeTimestamp = joinNexusFactory.writeTimestamp;

        this.readTimestamp = joinNexusFactory.readTimestamp;

        this.justify = joinNexusFactory.justify;
        
        this.backchain = joinNexusFactory.backchain;
        
//        this.forceSerialExecution = joinNexusFactory.forceSerialExecution;
//        
//        this.maxParallelSubqueries = joinNexusFactory.maxParallelSubqueries;
//        
//        this.chunkOfChunksCapacity = joinNexusFactory.chunkOfChunksCapacity;
//        
//        this.chunkCapacity = joinNexusFactory.chunkCapacity;
//
//        this.chunkTimeout = joinNexusFactory.chunkTimeout;
//        
//        this.fullyBufferedReadThreshold = joinNexusFactory.fullyBufferedReadThreshold;

        forceSerialExecution = Boolean.parseBoolean(getProperty(
                AbstractResource.Options.FORCE_SERIAL_EXECUTION,
                AbstractResource.Options.DEFAULT_FORCE_SERIAL_EXECUTION));

        maxParallelSubqueries = getProperty(
                AbstractResource.Options.MAX_PARALLEL_SUBQUERIES,
                AbstractResource.Options.DEFAULT_MAX_PARALLEL_SUBQUERIES,
                IntegerValidator.GTE_ZERO);

        /*
         * FIXME You can not override the rule execution model yet
         * (nestedSubquery vs pipeline). This is currently a separate field on
         * RDFJoinNexus. However, this will give way to a different approach all
         * together with the rule execution refactor to support things like star
         * joins.
         */
////        final boolean pipelineIsBetter = (indexManager instanceof IBigdataFederation && ((IBigdataFederation) indexManager)
////                .isScaleOut());
//
//        nestedSubquery = Boolean.parseBoolean(getProperty(
//                AbstractResource.Options.NESTED_SUBQUERY, Boolean.FALSE);
////        Boolean
////                        .toString(!pipelineIsBetter)));

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

        this.solutionFlags = joinNexusFactory.solutionFlags;

        this.filter = joinNexusFactory.filter;
        
//        this.defaultTaskFactory = new DefaultRuleTaskFactory();
        
        this.planFactory = joinNexusFactory.planFactory;
   
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        
        return joinNexusFactory;
        
    }

    public IRuleStatisticsFactory getRuleStatisticsFactory() {
        
        return ruleStatisticsFactory;
        
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
    
    public ActionEnum getAction() {
        
        return action;
        
    }
    
    public long getWriteTimestamp() {

        return writeTimestamp;

    }

    final public long getReadTimestamp() {
        
        return readTimestamp;
        
    }
    
//	/**
//	 * Return <code>true</code> if the <i>relationName</i> is on a
//	 * {@link TempTripleStore}
//	 * 
//	 * @todo Rather than parsing the relation name, it would be better to have
//	 *       the temporary store UUIDs explicitly declared.
//	 */
//    protected boolean isTempStore(String relationName) {
//       
//    	/* This is a typical UUID-based temporary store relation name.
//    	 * 
//         *           1         2         3
//         * 01234567890123456789012345678901234567
//         * 81ad63b9-2172-45dc-bd97-03b63dfe0ba0kb.spo
//         */
//        
//        if (relationName.length() > 37) {
//         
//            /*
//             * Could be a relation on a temporary store.
//             */
//            if (       relationName.charAt( 8) == '-' //
//                    && relationName.charAt(13) == '-' //
//                    && relationName.charAt(18) == '-' //
//                    && relationName.charAt(23) == '-' //
//                    && relationName.charAt(38) == '.' //
//            ) {
//                
//                /*
//                 * Pretty certain to be a relation on a temporary store.
//                 */
//                
//                return true;
//                
//            }
//            
//        }
//        
//        return false;
//
//    }

////	/**
////	 * A per-relation reentrant read-write lock allows either concurrent readers
////	 * or an writer on the unisolated view of a relation. When we use this lock
////	 * we also use {@link ITx#UNISOLATED} reads and writes and
////	 * {@link #makeWriteSetsVisible()} is a NOP.
////	 */
////    final private static boolean useReentrantReadWriteLockAndUnisolatedReads = true;
//    
//    public long getReadTimestamp(String relationName) {
//
////		if (useReentrantReadWriteLockAndUnisolatedReads) {
//
////			if (action.isMutation()) {
////				
////                assert readTimestamp == ITx.UNISOLATED : "readTimestamp="+readTimestamp;
////                
////			}
//
//            return readTimestamp;
//
////		} else {
////
////            /*
////             * When the relation is the focusStore choose {@link ITx#UNISOLATED}.
////             * Otherwise choose whatever was specified to the
////             * {@link RDFJoinNexusFactory}. This is because we avoid doing a
////             * commit on the focusStore and instead just its its UNISOLATED
////             * indices. This is more efficient since they are already buffered
////             * and since we can avoid touching disk at all for small data sets.
////             */
////            
////			if (isTempStore(relationName)) {
////
////				return ITx.UNISOLATED;
////
////			}
////
////			if (lastCommitTime != 0L && action.isMutation()) {
////
////				/*
////				 * Note: This advances the read-behind timestamp for a local
////				 * Journal configuration without the ConcurrencyManager (the
////				 * only scenario where we do an explicit commit).
////				 */
////
////				return TimestampUtility.asHistoricalRead(lastCommitTime);
////
////			}
////
////			return readTimestamp;
////
////		}
//        
//    }

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
        
        if(DEBUG) {
            
            log.debug("predicate: "+pred+", head relation: "+relation);
            
        }
        
        return relation;
        
    }

    /**
     * The tail relations are the views from which we read. This method depends
     * solely on the name(s) of the relation(s) and the timestamp of interest
     * for the view.
     * 
     * @todo we can probably get rid of the cache used by this method now that
     *       calling this method has been factored out of the join loops.
     */
    @SuppressWarnings("unchecked")
    public IRelation getTailRelationView(final IPredicate pred) {

//        if (pred == null)
//            throw new IllegalArgumentException();
        
        final int nsources = pred.getRelationCount();

        final IRelation relation;
        
        if (nsources == 1) {

            final String relationName = pred.getOnlyRelationName();

            relation = (IRelation) resourceLocator.locate(relationName,
                    readTimestamp);
                
        } else if (nsources == 2) {

            final String relationName0 = pred.getRelationName(0);

            final String relationName1 = pred.getRelationName(1);

//            final long timestamp0 = getReadTimestamp(/*relationName0*/);
//
//            final long timestamp1 = getReadTimestamp(/*relationName1*/);

            final IRelation relation0 = (IRelation) resourceLocator.locate(
                    relationName0, readTimestamp);//timestamp0);

            final IRelation relation1 = (IRelation) resourceLocator.locate(
                    relationName1, readTimestamp);//timestamp1);

            relation = new RelationFusedView(relation0, relation1).init();

        } else {

            throw new UnsupportedOperationException();

        }

        if(DEBUG) {
            
            log.debug("predicate: "+pred+", tail relation: "+relation);
            
        }
        
        return relation;
        
    }

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
    public IAccessPath getTailAccessPath(final IRelation relation,
            final IPredicate predicate) {

        if (predicate.getPartitionId() != -1) {

            /*
             * Note: This handles a read against a local index partition. For
             * scale-out, the [indexManager] will be the data service's local
             * index manager.
             * 
             * Note: Expanders ARE NOT applied in this code path. Expanders
             * require a total view of the relation, which is not available
             * during scale-out pipeline joins. Likewise, the [backchain]
             * property will be ignored since it is handled by an expander.
             * 
             * @todo If getAccessPathForIndexPartition() is raised into the
             * IRelation interface, then we can get rid of the cast to the
             * SPORelation implementation.
             */

            return ((SPORelation) relation).getAccessPathForIndexPartition(
                    indexManager, predicate);

        }

//        // Find the best access path for the predicate for that relation.
        IAccessPath accessPath = relation.getAccessPath(predicate);
//
//        if (predicate.getPartitionId() != -1) {
//
//            /*
//             * Note: The expander can not run against a shard since it assumes
//             * access to the full key range of the index. Expanders are
//             * convenient and work well for stand alone indices, but they should
//             * be replaced by rule rewrites for scale-out.
//             */
//
//            return accessPath;
//            
//        }
        
        final ISolutionExpander expander = predicate.getSolutionExpander();
        
        if (expander != null) {
            
            // allow the predicate to wrap the access path : @todo caching on AP?
            accessPath = expander.getAccessPath(accessPath);
            
        }
        
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

//    /**
//     * This handles a request for an access path that is restricted to a
//     * specific index partition.
//     * <p>
//     * Note: This path is used with the scale-out JOIN strategy, which
//     * distributes join tasks onto each index partition from which it needs to
//     * read. Those tasks constrain the predicate to only read from the index
//     * partition which is being serviced by that join task.
//     * <p>
//     * Note: Since the relation may materialize the index views for its various
//     * access paths, and since we are restricted to a single index partition and
//     * (presumably) an index manager that only sees the index partitions local
//     * to a specific data service, we create an access path view for an index
//     * partition without forcing the relation to be materialized.
//     * <p>
//     * Note: This method MUST NOT be moved to the {@link SPORelation}!!! The
//     * issue is that the relation is always resolved using the federation's
//     * index manager in order to resolve the metadata for the relation against
//     * the global row store. The {@link RDFJoinNexus} has been specifically
//     * prepared during scale-out joins such that the {@link #indexManager} is
//     * the data service's index manager NOT the federation's index manager. This
//     * gives us access to the local B+Tree views, which are efficient for local
//     * reads. If this method is moved to the {@link SPORelation} then the
//     * relation must be handed the desired index manager since the one it
//     * inherits from its base class will be the federation's index manager, not
//     * the local data service's index manager.
//     * 
//     * FIXME This COULD be moved into the {@link SPORelation} (and hence made
//     * general purpose) if we passed through the local index manager and used
//     * it consistently when obtaining the {@link IAccessPath}.
//     * 
//     * FIXME This is hardcoded for the {@link SPORelation} (it will not handle
//     * joins against other relations).
//     */
//    private IAccessPath getTailAccessPathForIndexPartition(
//            final IRelation relation, final IPredicate predicate) {
//
//        if (indexManager instanceof IBigdataFederation) {
//       
//            /*
//             * This will happen if you fail to re-create the JoinNexus
//             * within the target execution environment.
//             * 
//             * This is disallowed because the predicate specifies an index
//             * partition and expects to have access to the local index
//             * objects for that index partition. However, the index
//             * partition is only available when running inside of the
//             * ConcurrencyManager and when using the IndexManager exposed by
//             * the ConcurrencyManager to its tasks.
//             */
//            throw new IllegalStateException(
//                    "Expecting a local index manager, not: "
//                            + indexManager.getClass().toString());
//
//        }
//        
//        final String namespace = predicate.getOnlyRelationName();
//
//        /*
//         * Find the best access path for that predicate.
//         */
//        final IKeyOrder keyOrder = SPOKeyOrder.getKeyOrder(predicate,
//                ((SPORelation) relation).getKeyArity());
//
//        // The name of the desired index partition.
//        final String name = DataService
//                .getIndexPartitionName(namespace + "."
//                        + keyOrder.getIndexName(), predicate
//                        .getPartitionId());
//        
//        /*
//         * @todo whether or not we need both keys and values depends on the
//         * specific index/predicate.
//         * 
//         * Note: We can specify READ_ONLY here since the tail predicates are
//         * not mutable for rule execution.
//         */
//        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.READONLY;
//
//        final long timestamp = getReadTimestamp();
//        
//        final IIndex ndx = indexManager.getIndex(name, timestamp);
//        
//        return new SPOAccessPath(indexManager, timestamp, predicate,
//                keyOrder, ndx, flags, getChunkOfChunksCapacity(),
//                getChunkCapacity(), getFullyBufferedReadThreshold()).init();
//
//    }
    
    public Iterator<PartitionLocator> locatorScan(
            final AbstractScaleOutFederation fed, final IPredicate predicate) {

        final long timestamp = getReadTimestamp();

        // Note: assumes that we are NOT using a view of two relations.
        final IRelation relation = (IRelation) fed.getResourceLocator().locate(
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
        final AbstractAccessPath accessPath = (AbstractAccessPath) relation
                .getAccessPath(predicate);

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
    
    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }
    
    @SuppressWarnings("unchecked")
    public boolean bind(final IRule rule, final int index, final Object e,
            final IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        copyValues((IElement) e, rule.getTail(index), bindings);

        // verify constraints.
        return rule.isConsistent(bindings);

    }
    
    public boolean bind(final IPredicate<?> pred, final IConstraint constraint,
            final Object e, final IBindingSet bindings) {

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
    private void copyValues(final IElement e, final IPredicate<?> pred,
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
    
//    @SuppressWarnings("unchecked")
//    private void copyValues(final Object e, final IPredicate predicate,
//            final IBindingSet bindingSet) {
//
//        if (e == null)
//            throw new IllegalArgumentException();
//        
//        if (predicate == null)
//            throw new IllegalArgumentException();
//        
//        if (bindingSet == null)
//            throw new IllegalArgumentException();
//        
//        if (e instanceof SPO) {
//        
//            copyValues((SPO) e, (IPredicate<ISPO>) predicate, bindingSet);
//            
//        } else {
//
//            copyValues((MagicTuple) e, 
//                    (IPredicate<IMagicTuple>) predicate, bindingSet);
//            
//        }
//        
//    }
//    
//    @SuppressWarnings("unchecked")
//    private void copyValues(final SPO spo, final IPredicate<ISPO> predicate,
//            final IBindingSet bindingSet) {
//    
//        {
//
//            final IVariableOrConstant<IV> t = predicate.get(0);
//            
//            if(t.isVar()) {
//
//                final IVariable<IV> var = (IVariable<IV>)t;
//                
//                final Constant newval = new Constant<IV>(spo.s);
//
//                bindingSet.set(var, newval);
//                
//            }
//
//        }
//
//        {
//
//            final IVariableOrConstant<IV> t = predicate.get(1);
//            
//            if(t.isVar()) {
//
//                final IVariable<IV> var = (IVariable<IV>)t;
//
//                final Constant newval = new Constant<IV>(spo.p);
//
//                bindingSet.set(var, newval);
//                
//            }
//
//        }
//
//        {
//
//            final IVariableOrConstant<IV> t = predicate.get(2);
//            
//            if(t.isVar()) {
//
//                final IVariable<IV> var = (IVariable<IV>)t;
//
//                final Constant newval = new Constant<IV>(spo.o);
//
//                bindingSet.set(var, newval);
//                
//            }
//
//        }
//        
//        /*if (predicate.arity() == 4)*/ {
//
//            // context position / statement identifier.
//
//            final IVariableOrConstant<IV> t = predicate.get(3);
//            
//            if (t != null && t.isVar()) {
//
//                final IVariable<IV> var = (IVariable<IV>) t;
//
//                final Constant newval = new Constant<IV>(spo.c());
//
//                bindingSet.set(var, newval);
//
//            }
//
//        }
//        
//    }
//
//    @SuppressWarnings("unchecked")
//    private void copyValues(final MagicTuple tuple, 
//            final IPredicate<IMagicTuple> pred, final IBindingSet bindingSet) {
//    
//        for (int i = 0; i < pred.arity(); i++) {
//            
//            final IVariableOrConstant<IV> t = pred.get(i);
//            
//            if(t.isVar()) {
//
//                final IVariable<IV> var = (IVariable<IV>)t;
//                
//                final Constant newval = new Constant<IV>(tuple.getTerm(i));
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

    public ISolution newSolution(final IRule rule, final IBindingSet bindingSet) {

        final Solution solution = new Solution(this, rule, bindingSet);
        
        if(DEBUG) {
            
            log.debug(solution.toString());
            
        }

        return solution;

    }
    
    public int solutionFlags() {
        
        return solutionFlags;
        
    }

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

        return new BindingSetSortKeyBuilder(keyBuilder, vars);
        
    }
    
    /**
     * FIXME Custom serialization for solution sets, especially since there
     * tends to be a lot of redudency in the data arising from how bindings are
     * propagated during JOINs.
     * 
     * @todo We can sort the {@link ISolution}s much like we already do for
     *       DISTINCT or intend to do for SORT and use the equivilent of leading
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
     * to be a lot of redudency in the data arising from how bindings are
     * propagated during JOINs.
     * 
     * @todo We can sort the {@link ISolution}s much like we already do for
     *       DISTINCT or intend to do for SORT and use the equivilent of leading
     *       key compression to reduce IO costs (or when they are SORTed we
     *       could leverage that to produce a more compact serialization).
     * 
     * @see SPOBindingSetSerializer, which has not been finished.
     */
    public IStreamSerializer<IBindingSet[]> getBindingSetSerializer() {
        
        return SerializerUtil.STREAMS;
        
    }

    public IBindingSet newBindingSet(final IRule rule) {

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

    public IRuleTaskFactory getRuleTaskFactory(final boolean parallel,
            final IRule rule) {

        if (rule == null)
            throw new IllegalArgumentException();

        // is there a task factory override?
        IRuleTaskFactory taskFactory = rule.getTaskFactory();

        if (taskFactory == null) {

            // no, use the default factory.
            taskFactory = joinNexusFactory.defaultRuleTaskFactory;

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

    public IEvaluationPlanFactory getPlanFactory() {
        
        return planFactory;
        
    }
    
    public IResourceLocator getRelationLocator() {
        
        return resourceLocator;
//        return indexManager.getResourceLocator();
        
    }

    public IBuffer<ISolution> newUnsynchronizedBuffer(
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
    public IBlockingBuffer<ISolution[]> newQueryBuffer() {

        if (getAction().isMutation())
            throw new IllegalStateException();
        
        return new BlockingBuffer<ISolution[]>(chunkOfChunksCapacity,
                chunkCapacity, chunkTimeout, chunkTimeoutUnit);
        
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
            
            if(DEBUG) 
                log.debug("chunkSize="+n);
            
            final long begin = System.currentTimeMillis();

            final SPO[] a = new SPO[ n ];

            final Justification[] b = new Justification[ n ];

            for(int i=0; i<chunk.length; i++) {
                
                if(DEBUG) {
                    
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
     * Note: {@link #getSolutionFilter()} is applied by
     * {@link #newUnsynchronizedBuffer(IBuffer, int)} and NOT by the buffer
     * returned by this method.
     */
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution[]> newInsertBuffer(final IMutableRelation relation) {

        if (getAction() != ActionEnum.Insert)
            throw new IllegalStateException();

        if (DEBUG) {

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

    /**
     * Note: {@link #getSolutionFilter()} is applied by
     * {@link #newUnsynchronizedBuffer(IBuffer, int)} and NOT by the buffer
     * returned by this method.
     */
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution[]> newDeleteBuffer(final IMutableRelation relation) {

        if (getAction() != ActionEnum.Delete)
            throw new IllegalStateException();

        if (DEBUG) {

            log.debug("relation=" + relation);

        }

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                chunkOfChunksCapacity, relation);

    }

    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<ISolution> runQuery(final IStep step)
            throws Exception {

        if (step == null)
            throw new IllegalArgumentException();

        if(log.isInfoEnabled())
            log.info("program="+step.getName());

        if(isEmptyProgram(step)) {

            log.warn("Empty program");

            return (IChunkedOrderedIterator<ISolution>) new EmptyProgramTask(
                    ActionEnum.Query, step).call();

        }

        final IChunkedOrderedIterator<ISolution> itr = (IChunkedOrderedIterator<ISolution>) runProgram(
                ActionEnum.Query, step);

        if (step.isRule() && ((IRule) step).getQueryOptions().isDistinct()) {

            /*
             * Impose a DISTINCT constraint based on the variable bindings
             * selected by the head of the rule. The DistinctFilter will be
             * backed by a TemporaryStore if more than one chunk of solutions is
             * generated. That TemporaryStore will exist on the client where
             * this method (runQuery) was executed. The TemporaryStore will be
             * finalized and deleted when it is no longer referenced.
             */

            final ISortKeyBuilder<ISolution> sortKeyBuilder;

            if (((IRule) step).getHead() != null
                    && (solutionFlags & ELEMENT) != 0) {

                /*
                 * Head exists and elements are requested, so impose DISTINCT
                 * based on the materialized elements.
                 * 
                 * FIXME The SPOSortKeyBuilder should be obtained from the head
                 * relation. Of course there is one sort key for each access
                 * path, but for the purposes of DISTINCT we want the sort key
                 * to correspond to the notion of a "primary key" (the
                 * distinctions that matter) and it does not matter which sort
                 * order but the SPO sort order probably has the least factor of
                 * "surprise".
                 */
                
                final int arity = ((IRule)step).getHead().arity();
                
                sortKeyBuilder = new DelegateSortKeyBuilder<ISolution, ISPO>(
                        new SPOSortKeyBuilder(arity)) {

                    protected ISPO resolve(ISolution solution) {

                        return (ISPO) solution.get();

                    }

                };

            } else {

                if ((solutionFlags & BINDINGS) != 0) {

                    /*
                     * Bindings were requested so impose DISTINCT based on those
                     * bindings.
                     */
                    
                    sortKeyBuilder = new DelegateSortKeyBuilder<ISolution, IBindingSet>(
                            newBindingSetSortKeyBuilder((IRule) step)) {

                        protected IBindingSet resolve(ISolution solution) {

                            return solution.getBindingSet();

                        }

                    };

                } else {

                    throw new UnsupportedOperationException(
                            "You must specify BINDINGS since the rule does not have a head: "
                                    + step);

                }

            }
            
            return new ChunkedConvertingIterator<ISolution, ISolution>(itr,
                    new DistinctFilter<ISolution>(indexManager) {

                protected byte[] getSortKey(ISolution e) {
                    
                    return sortKeyBuilder.getSortKey(e);
                    
                }
                
            });

        }

        return itr;

    }
    
    public long runMutation(final IStep step) throws Exception {

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
    protected boolean isEmptyProgram(final IStep step) {

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
    protected Object runProgram(final ActionEnum action, final IStep step)
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
    protected Object runLocalProgram(final ActionEnum action, final IStep step)
            throws Exception {

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
    protected Object runDistributedProgram(final IBigdataFederation fed,
            final ActionEnum action, final IStep step) throws Exception {

        if (log.isInfoEnabled()) {

            log.info("Running distributed program: action=" + action
                    + ", program=" + step.getName());

        }

        final IProgramTask innerTask = new ProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

    /**
     * This variant is submitted and executes the rules from inside of the
     * {@link ConcurrencyManager} on the {@link LocalDataServiceImpl} (fast).
     * <p>
     * Note: This can only be done if all indices for the relation(s) are (a)
     * unpartitioned; and (b) located on the SAME {@link DataService}. This is
     * <code>true</code> for {@link LocalDataServiceFederation}. All other
     * {@link IBigdataFederation} implementations are scale-out (use key-range
     * partitioned indices).
     */
    protected Object runDataServiceProgram(final DataService dataService,
            final ActionEnum action, final IStep step)
            throws InterruptedException, ExecutionException {

        if (log.isInfoEnabled()) {

            log.info("Submitting program to data service: action=" + action
                    + ", program=" + step.getName() + ", dataService="
                    + dataService);

        }
        
        final IProgramTask innerTask = new ProgramTask(action, step,
                getJoinNexusFactory());

        return dataService.submit(innerTask).get();

    }

}
