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

import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
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
import com.bigdata.relation.rule.eval.ProgramUtility;
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
    
    private final IJoinNexusFactory joinNexusFactory;
    
    private final IIndexManager indexManager;
    
    private final long writeTimestamp;
    
    private final long readTimestamp;
    
    private final int solutionFlags;
    
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
     * @param writeTimestamp
     *            The timestamp of the relation view(s) using to write on the
     *            {@link IMutableRelation}s (ignored if you are not execution
     *            mutation programs).
     * @param readTimestamp
     *            The timestamp of the relation view(s) used to read from the
     *            access paths.
     * @param solutionFlags
     *            Flags controlling the behavior of
     *            {@link #newSolution(IRule, IBindingSet)}.
     * @param filter
     *            An optional filter that will be applied to keep matching
     *            elements out of the {@link IBuffer} for Query or Mutation
     *            operations.
     * 
     * @todo collapse the executor service and index manager?
     */
    public RDFJoinNexus(IJoinNexusFactory joinNexusFactory,
            IIndexManager indexManager, long writeTimestamp,
            long readTimestamp, int solutionFlags, IElementFilter filter) {

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.joinNexusFactory = joinNexusFactory;
        
        this.indexManager = indexManager;
        
        this.writeTimestamp = writeTimestamp;

        this.readTimestamp = readTimestamp;

        this.solutionFlags = solutionFlags;

        this.filter = filter;
        
        this.defaultTaskFactory = new IRuleTaskFactory() {

            public IStepTask newTask(IRule rule,IJoinNexus joinNexus, IBuffer<ISolution> buffer) {
                
                return new LocalNestedSubqueryEvaluator(rule, joinNexus, buffer);
                
            }
            
        };
   
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        
        return joinNexusFactory;
        
    }
    
    public long getWriteTimestamp() {
        
        return writeTimestamp;
        
    }

    public long getReadTimestamp() {
        
        return readTimestamp;
        
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

    /*
     * @todo the element type should probably be unspecified <? extends Object>
     * since we can also materialize stuff from the lexicon relation.
     */
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
    
    @SuppressWarnings("unchecked")
    public IBlockingBuffer<ISolution> newQueryBuffer() {

        /*
         * Note: ISolution elements (not SPOs) will be written on the buffer
         * concurrently by different rules so there is no natural order for the
         * elements in the buffer.
         */
        
        return new BlockingBuffer<ISolution>(DEFAULT_BUFFER_CAPACITY,
                null/* keyOrder */, filter == null ? null
                        : new SolutionFilter(filter));
        
    }
    
    /**
     * The default buffer capacity.
     * 
     * @todo config via the {@link RDFJoinNexusFactory}. Note that the
     *       {@link BlockingBuffer} can not deliver a chunk larger than its
     *       internal queue capacity.
     */
    private final int DEFAULT_BUFFER_CAPACITY = 10000;
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {

        if(log.isDebugEnabled()) {
            
            log.debug("relation="+relation);
            
        }
        
        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation, filter == null ? null
                        : new SolutionFilter(filter));

    }
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {

        if(log.isDebugEnabled()) {
            
            log.debug("relation="+relation);
            
        }

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation, filter == null ? null
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

    public long runMutation(ActionEnum action, IStep step)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();
        
        if (step == null)
            throw new IllegalArgumentException();
        
        if (!action.isMutation())
            throw new IllegalArgumentException();
        
        if(log.isInfoEnabled())
            log.info("action="+action+", program="+step.getName());
        
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
     */
    protected Object runProgram(ActionEnum action, IStep step)
            throws Exception {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        final ProgramUtility util = new ProgramUtility();
        
        final IBigdataFederation fed = util.getFederation(indexManager, step,
                getReadTimestamp());

        if (fed == null) {

            // local Journal or TemporaryStore execution.
            return runLocalProgram(action, step);

        } else if (fed instanceof LocalDataServiceFederation) {

            final DataService dataService = ((LocalDataServiceFederation) fed)
                    .getDataService();

            // single data service program execution.
            return runDataServiceProgram(dataService, action, step);

        } else {

            // distributed program execution.
            return runDistributedProgram(fed, action, step);

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

        final IProgramTask innerTask = new LocalProgramTask(action, step,
                getJoinNexusFactory(), getIndexManager());

        return innerTask.call();

    }

    /**
     * Runs a distributed {@link IProgram}. This covers both the
     * {@link EmbeddedFederation} (which uses key-range partitioned indices) and
     * {@link AbstractDistributedFederation}s that are truely multi-machine and
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

}
