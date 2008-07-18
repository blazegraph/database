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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.ChunkedArrayIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedIterator;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.eval.AbstractSolutionBuffer;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.EmptyProgramTask;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.IProgramTask;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;
import com.bigdata.relation.rule.eval.LocalNestedSubqueryEvaluator;
import com.bigdata.relation.rule.eval.LocalProgramTask;
import com.bigdata.relation.rule.eval.RunRuleAndFlushBufferTaskFactory;
import com.bigdata.relation.rule.eval.Solution;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

/**
 * {@link IProgram} execution support for the RDF DB.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexus implements IJoinNexus {

    protected final static transient Logger log = Logger.getLogger(RDFJoinNexus.class);
    
    private final RDFJoinNexusFactory joinNexusFactory;
    
    private final IIndexManager indexManager;

    private final ActionEnum action;
    
    private final long writeTimestamp;
    
    private final long readTimestamp;
    
    private final boolean justify;

    private final int bufferCapacity;
    
    private final int solutionFlags;
    
    @SuppressWarnings("unchecked")
	private final IElementFilter filter;

    /**
     * The default factory for rule evaluation.
     * 
     * @todo configure for scale-out variants?
     */
    private final IRuleTaskFactory defaultTaskFactory;

	/**
	 * @param joinNexusFactory
	 *            The object used to create this instance and which can be used
	 *            to create other instances as necessary for distributed rule
	 *            execution.
	 * @param indexManager
	 *            The object used to resolve indices, relations, etc.
	 * @param action
	 *            Indicates whether this is a Query, Insert, or Delete
	 *            operation.
	 * @param writeTimestamp
	 *            The timestamp of the relation view(s) using to write on the
	 *            {@link IMutableRelation}s (ignored if you are not execution
	 *            mutation programs).
	 * @param readTimestamp
	 *            The timestamp of the relation view(s) used to read from the
	 *            access paths.
	 * @param bufferCapacity
	 *            The capacity of the buffers used to support chunked iterators
	 *            and efficient ordered writes.
	 * @param solutionFlags
	 *            Flags controlling the behavior of
	 *            {@link #newSolution(IRule, IBindingSet)}.
	 * @param filter
	 *            An optional filter that will be applied to keep matching
	 *            elements out of the {@link IBuffer} for Query or Mutation
	 *            operations.
	 */
	public RDFJoinNexus(RDFJoinNexusFactory joinNexusFactory,
			IIndexManager indexManager) {

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.joinNexusFactory = joinNexusFactory;
        
        this.indexManager = indexManager;
        
        this.action = joinNexusFactory.action;
        
        this.writeTimestamp = joinNexusFactory.writeTimestamp;

        this.readTimestamp = joinNexusFactory.readTimestamp;

        this.justify = joinNexusFactory.justify;
        
        this.bufferCapacity = joinNexusFactory.bufferCapacity;
        
        this.solutionFlags = joinNexusFactory.solutionFlags;

        this.filter = joinNexusFactory.filter;
        
        this.defaultTaskFactory = new IRuleTaskFactory() {

            public IStepTask newTask(IRule rule,IJoinNexus joinNexus, IBuffer<ISolution> buffer) {
                
                return new LocalNestedSubqueryEvaluator(rule, joinNexus, buffer);
                
            }
            
        };
   
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        
        return joinNexusFactory;
        
    }
    
    public ActionEnum getAction() {
        
        return action;
        
    }
    
    public long getWriteTimestamp() {
        
        return writeTimestamp;
        
    }

	/**
	 * Return <code>true</code> if the <i>relationName</i> is on a
	 * {@link TempTripleStore}
	 * 
	 * @todo Rather than parsing the relation name, it would be better to have
	 *       the temporary store UUIDs explicitly declared.
	 */
    protected boolean isTempStore(String relationName) {
       
    	/* This is a typical UUID-based temporary store relation name.
    	 * 
         *           1         2         3
         * 01234567890123456789012345678901234567
         * 81ad63b9-2172-45dc-bd97-03b63dfe0ba0kb.spo
         */
        
        if (relationName.length() > 37) {
         
            /*
             * Could be a relation on a temporary store.
             */
            if (       relationName.charAt( 8) == '-' //
                    && relationName.charAt(13) == '-' //
                    && relationName.charAt(18) == '-' //
                    && relationName.charAt(23) == '-' //
                    && relationName.charAt(38) == '.' //
            ) {
                
                /*
                 * Pretty certain to be a relation on a temporary store.
                 */
                
                return true;
                
            }
            
        }
        
        return false;

    }

//    protected AbstractTripleStore getMutationTarget() {
//    	
//    	return mutationTarget;
//    	
//    }
    
    /**
     * When the relation is the focusStore SPORelation choose
     * {@link ITx#UNISOLATED}. Otherwise choose whatever was specified to the
     * {@link RDFJoinNexusFactory}. This is because we avoid doing a commit on
     * the focusStore and instead just its its UNISOLATED indices. This is more
     * efficient since they are already buffered and since we can avoid touching
     * disk at all for small data sets.
     */
    public long getReadTimestamp(String relationName) {
        
    	if(isTempStore(relationName)) {
    		
    		return ITx.UNISOLATED;
    		
    	}
        
        if (lastCommitTime != 0L && action.isMutation()) {
            
            /*
             * Note: This advances the read-behind timestamp for a local Journal
             * configuration without the ConcurrencyManager (the only scenario
             * where we do an explicit commit).
             * 
             * @issue negative timestamp for historical read.
             */
            
            return -lastCommitTime;
            
        }
        
        return readTimestamp;
        
    }
    
    public IRelation getHeadRelationView(IPredicate pred) {
        
        if (pred == null)
            throw new IllegalArgumentException();
        
        if (pred.getRelationCount() != 1)
            throw new IllegalArgumentException();
        
        final String relationName = pred.getOnlyRelationName();
        
        final long timestamp = (getAction().isMutation() ? getWriteTimestamp()
                : getReadTimestamp(relationName));

        final IRelation relation = (IRelation) getIndexManager()
                .getResourceLocator().locate(relationName, timestamp);
        
        if(log.isDebugEnabled()) {
            
            log.debug("predicate: "+pred+", head relation: "+relation);
            
        }
        
        return relation;
        
    }
    
    public IRelation getTailRelationView(IPredicate pred) {

        if (pred == null)
            throw new IllegalArgumentException();
        
        final int nsources = pred.getRelationCount();

        final IRelation relation;
        
        if (nsources == 1) {

            final String relationName = pred.getOnlyRelationName();

            final long timestamp = getReadTimestamp(relationName);

            relation = (IRelation) getIndexManager().getResourceLocator().locate(
                    relationName, timestamp);

        } else if (nsources == 2) {

            final String relationName0 = pred.getRelationName(0);

            final String relationName1 = pred.getRelationName(1);

            final long timestamp0 = getReadTimestamp(relationName0);

            final long timestamp1 = getReadTimestamp(relationName1);

            final SPORelation relation0 = (SPORelation) getIndexManager()
                    .getResourceLocator().locate(relationName0, timestamp0);

            final SPORelation relation1 = (SPORelation) getIndexManager()
                    .getResourceLocator().locate(relationName1, timestamp1);

            relation = new RelationFusedView<SPO>(relation0, relation1);

        } else {

            throw new UnsupportedOperationException();

        }

        if(log.isDebugEnabled()) {
            
            log.debug("predicate: "+pred+", tail relation: "+relation);
            
        }
        
        return relation;
        
    }
    
    public IIndexManager getIndexManager() {
        
        return indexManager;
        
    }
    
    @SuppressWarnings("unchecked")
    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet ) {

        if (e == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final SPO spo = (SPO) e;
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>)predicate;
        
        {

            final IVariableOrConstant<Long> t = pred.get(0);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.s));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(1);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.p));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(2);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.o));
                
            }

        }
        
    }

    public ISolution newSolution(IRule rule, IBindingSet bindingSet) {

        final Solution solution = new Solution(this, rule, bindingSet);
        
        if(log.isDebugEnabled()) {
            
            log.debug(solution.toString());
            
        }

        return solution;

    }
    
    public int solutionFlags() {
        
        return solutionFlags;
        
    }

    public IBindingSet newBindingSet(IRule rule) {

        return new ArrayBindingSet(rule.getVariableCount());
        
    }

    public IRuleTaskFactory getRuleTaskFactory(boolean parallel, IRule rule) {

        if (rule == null)
            throw new IllegalArgumentException();

        // is there a task factory override?
        IRuleTaskFactory taskFactory = rule.getTaskFactory();

        if (taskFactory == null) {

            // no, use the default factory.
            taskFactory = defaultTaskFactory;

        }

        if (!parallel) {

            /*
             * Tasks for sequential steps are always wrapped to ensure that the
             * buffer is flushed when the task completes.
             */
            taskFactory = new RunRuleAndFlushBufferTaskFactory(taskFactory);

        }

        return taskFactory;

    }

    public IResourceLocator getRelationLocator() {
        
        return indexManager.getResourceLocator();
        
    }
    
    /**
     * Note: {@link ISolution} elements (not {@link SPO}s) will be written on
     * the buffer concurrently by different rules so there is no natural order
     * for the elements in the buffer.
     * <p>
     * Note: the {@link BlockingBuffer} can not deliver a chunk larger than its
     * internal queue capacity.
     */
    @SuppressWarnings("unchecked")
    public IBlockingBuffer<ISolution> newQueryBuffer() {

        if (getAction().isMutation())
            throw new IllegalStateException();
        
        return new BlockingBuffer<ISolution>(bufferCapacity,
                null/* keyOrder */, filter == null ? null
                        : new SolutionFilter(filter));
        
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
        public InsertSPOAndJustificationBuffer(int capacity, IMutableRelation<E> relation,
                IElementFilter<ISolution<E>> filter) {

            super(capacity, relation, filter);
            
        }

        @Override
        protected long flush(IChunkedOrderedIterator<ISolution<E>> itr) {

			/*
			 * FIXME If no concurrency control then acquire write lock here and
			 * release in finally {}.
			 * 
			 * The IAccessPath iterators MUST also acquire a read lock using the
			 * same lock object for the relation. 
			 */
        	
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

            for(int i=0; i<chunk.length; i++) {
                
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
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {

        if (getAction() != ActionEnum.Insert)
            throw new IllegalStateException();

        if(log.isDebugEnabled()) {
            
            log.debug("relation="+relation);
            
        }
        
        if(justify) {

            /*
             * Buffer knows how to write the computed elements on the statement
             * indices and the computed binding sets on the justifications
             * indices.
             */
            
            return new InsertSPOAndJustificationBuffer(
                    bufferCapacity, relation, filter == null ? null
                            : new SolutionFilter(filter));
            
        }
        
        /*
         * Buffer resolves the computed elements and writes them on the
         * statement indices.
         */
        
        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                bufferCapacity, relation, filter == null ? null
                        : new SolutionFilter(filter));

    }
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {

        if (getAction() != ActionEnum.Delete)
            throw new IllegalStateException();


        if(log.isDebugEnabled()) {
            
            log.debug("relation="+relation);
            
        }

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                bufferCapacity, relation, filter == null ? null
                        : new SolutionFilter(filter));

    }

    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<ISolution> runQuery(IStep step)
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

        return (IChunkedOrderedIterator<ISolution>) runProgram(
                ActionEnum.Query, step);

    }

    public long runMutation(IStep step)
            throws Exception {

        if (step == null)
            throw new IllegalArgumentException();
        
        if (!action.isMutation())
            throw new IllegalStateException();
        
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
    protected boolean isEmptyProgram(IStep step) {

        if (!step.isRule() && ((IProgram)step).stepCount() == 0) {

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
     * 
     * FIXME It is possible to locate monolithic indices for a distributed
     * database such that certain JOINs (especially, the {@link SPORelation}
     * self-JOINs required for inference and most query answering) are purely
     * local.
     * <p>
     * Make it easy (a) to deploy under this configuration, (b) to detect when
     * that deployment exists, and (c) then submit the rules to the
     * {@link DataService} on which the {@link SPORelation}'s indices reside.
     */
    protected Object runProgram(ActionEnum action, IStep step)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        final IIndexManager indexManager = getIndexManager();
        
        if(indexManager instanceof IBigdataFederation) {
            
            final IBigdataFederation fed = (IBigdataFederation)indexManager;
            
            if(fed instanceof LocalDataServiceFederation) {
                
                final DataService dataService = ((LocalDataServiceFederation) fed)
                .getDataService();

                // single data service program execution.
                return runDataServiceProgram(dataService, action, step);

            }
            
            // distributed program execution.
            return runDistributedProgram(fed, action, step);
            
        } else {
            
            // local Journal or TemporaryStore execution.
            return runLocalProgram(action, step);

        }

    }

    /**
     * This variant handles both local indices on a {@link TemporaryStore} or
     * {@link Journal} WITHOUT concurrency controls (fast).
     */
    protected Object runLocalProgram(ActionEnum action, IStep step) throws Exception {

        if (log.isInfoEnabled())
            log.info("Running local program: action=" + action + ", program="
                    + step.getName());

        /*
         * If there are uncommitted writes then they need to be made visible
         * before we run the operation so that the read-committed relation views
         * have access to the most recent state. (This is not necessary when
         * using the scale-out API since all client operations are auto-commit
         * unisolated writes).
         */

        makeWriteSetsVisible();
        
        final IProgramTask innerTask = new LocalProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

    /**
     * Runs a distributed {@link IProgram}. This covers both the
     * {@link EmbeddedFederation} (which uses key-range partitioned indices) and
     * {@link AbstractDistributedFederation}s that are truly multi-machine and
     * use RMI.
     * 
     * FIXME This is not optimized for distributed joins. It is actually using
     * the {@link ClientIndexView} and the {@link LocalProgramTask} - this is
     * NOT efficient!!! It needs to be modified to (a) unroll to inner loop of
     * the join; and (b) partition the join such that the outer loop is split
     * across the data services where the index partition resides. This makes
     * the outer loop extremely efficient, distributes the computation over the
     * cluster, and parallelizes the inner loop so that we do at most N queries -
     * one query per index partition spanned by the queries framed for the inner
     * loop by the bindings selected in the outer loop for a given chunk.  A large
     * chunk size for the outer loop is also effective since the reads are local
     * and this reduces the #of times that we will unroll the inner loop.
     */
    protected Object runDistributedProgram(IBigdataFederation fed,
            ActionEnum action, IStep step) throws Exception {

        if (log.isInfoEnabled()) {

            log.info("Running distributed program: action=" + action
                    + ", program=" + step.getName());

        }

        final IProgramTask innerTask = new LocalProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

    /**
     * This variant is submitted and executes the rules from inside of the
     * {@link ConcurrencyManager} on the {@link LocalDataServiceImpl} (fast).
     * <p>
     * Note: This can only be done if all indices for the relation(s) are (a)
     * monolithic; and (b) located on the SAME {@link DataService}. This is
     * <code>true</code> for {@link LocalDataServiceFederation}. All other
     * {@link IBigdataFederation} implementations are scale-out (use key-range
     * partitioned indices).
     */
    protected Object runDataServiceProgram(DataService dataService,
            ActionEnum action, IStep step) throws InterruptedException,
            ExecutionException {

        if (log.isInfoEnabled()) {

            log.info("Submitting program to data service: action=" + action
                    + ", program=" + step.getName() + ", dataService="
                    + dataService);

        }
        
        final IProgramTask innerTask = new LocalProgramTask(action, step,
                getJoinNexusFactory());

        return dataService.submit(innerTask).get();

    }

    public void makeWriteSetsVisible() {
        
//        assert action.isMutation();
        
        assert getWriteTimestamp() == ITx.UNISOLATED;
        
        if (indexManager instanceof Journal) {
        
            /*
             * The RDF KB runs against a LocalTripleStore without concurrency
             * controls.
             * 
             * Commit the Journal before running the operation in order to make
             * sure that the read-committed views are based on the most recently
             * written data (the caller is responsible for flushing any buffers,
             * but this makes sure that the buffered index writes are
             * committed).
             */
            
            // commit.
            ((Journal)getIndexManager()).commit();

            // current read-behind timestamp.
            lastCommitTime = ((IJournal)indexManager).getLastCommitTime();
                
        } else if(indexManager instanceof IJournal) {
            
            /*
             * An IJournal object exposed by the AbstractTask. We don't have to
             * commit anything since the writes were performed under the control
             * of the ConcurrencyManager. However, we can advance the
             * read-behind time to the most recent commit timestamp.
             */
            
            // current read-behind timestamp.
            lastCommitTime = ((IJournal)indexManager).getLastCommitTime();

        } else if(indexManager instanceof TemporaryStore) {
            
            /*
             * This covers the case where the database itself (as opposed to the
             * focusStore) is a TempTripleStore on a TemporaryStore.
             * 
             * Checkpoint the TemporaryStore before running the operation so
             * that the read-committed views of the relations will be up to
             * date.
             * 
             * Note: The value returned by checkpoint() is a checkpoint record
             * address NOT a timestamp. The only index views that are supported
             * for a TemporaryStore are UNISOLATED and READ_COMMITTED.
             */
            
            ((TemporaryStore)getIndexManager()).checkpoint();
            
        } else if(indexManager instanceof IBigdataFederation) {
            
            /*
             * The federation API auto-commits unisolated writes.
             */
            
            // current read-behind timestamp.
            lastCommitTime = ((IBigdataFederation)indexManager).lastCommitTime();
            
        } else {
            
            throw new AssertionError("Not expecting: "
                    + indexManager.getClass());
            
        }
        
    }

    /**
     * Updated each time {@link #makeWriteSetsVisible()} does a commit (which
     * only occurs for the local {@link Journal} scenario) and used instead of
     * {@link ITx#READ_COMMITTED} for the read time for the {@link Journal}.
     */
    private long lastCommitTime = 0L;

}
