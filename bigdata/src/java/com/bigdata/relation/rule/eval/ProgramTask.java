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
 * Created on Jun 24, 2008
 */

package com.bigdata.relation.rule.eval;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.ChunkConsumerIterator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Task for executing a program when all of the indices for the relation are
 * co-located on the same {@link DataService}.
 * 
 * @todo Named result sets. This would provide a means to run a IRuleTask and
 *       cache the output for further evaluation as a named result set. The
 *       backing store should be a temporary resource. for scale-out it needs to
 *       be visible to the federation (since the rule executing against that
 *       data may be distributed across the federation based on the access path
 *       for the SPORelation) so it would have to be registered on some data
 *       service (any) in the federation and dropped in a finally {} clause.
 *       <p>
 *       When the sets are large then they may need a backing store, e.g.,
 *       BigdataSet<Long> (specialized so that it does not store anything under
 *       the key since we can decode the Long from the key - do utility versions
 *       BigdataLongSet(), but the same code can serve float, double, and int as
 *       well. Avoid override for duplicate keys to reduce IO.
 * 
 * @todo it should be possible to have a different action associated with each
 *       rule in the program, and to have a different target relation for the
 *       head of each rule on which we will write (mutation). Different query or
 *       mutation count results could be handled by an extension with
 *       "nextResultSet" style semantics. However, for now, all rules MUST write
 *       on the same buffer. Query results will therefore be multiplexed as will
 *       mutations counts.
 * 
 * @todo foreign key joins: it should be possible to handle different relation
 *       classes in the same rules, e.g., RDF and non-RDF relations. Or even the
 *       SPO and lexicon relation for the RDF DB -- the latter will be useful
 *       for materializing externalized statements efficiently.
 * 
 * @todo could make the return type a generic for {@link AbstractStepTask} and
 *       make this class a concrete implementation of that one.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProgramTask implements IProgramTask,
        IDataServiceAwareProcedure, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7047397038429305180L;

    protected static final transient Logger log = Logger.getLogger(ProgramTask.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    private final ActionEnum action;
    
    private final IStep step;

    /**
     * Serializable field specified when the {@link ProgramTask} will be
     * submitted (via RMI or not) to a {@link DataService}. A new
     * {@link IJoinNexus} is instantiated in the execution context on the
     * {@link DataService} from this field.
     */
    private final IJoinNexusFactory joinNexusFactory;
    
    /**
     * Note: NOT serialized! The {@link IIndexManager} will be set by
     * {@link #setDataService(DataService)} if this object submitted using
     * {@link DataService#submit(Callable)}.
     */
    private transient IIndexManager indexManager;
    
    /**
     * Note: NOT serialized!
     */
    private transient DataService dataService;

    public void setDataService(DataService dataService) {

        if (dataService == null)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("Running on data service: dataService="+dataService);
        
        this.dataService = dataService;
        
        this.indexManager = dataService.getFederation();

    }

    /**
     * Variant when the task will be submitted using
     * {@link IDataService#submit(Callable)} (efficient since all indices will
     * be local, but the indices must not be partitioned and must all exist on
     * the target {@link DataService}).
     * <p>
     * Note: the caller MUST submit the {@link ProgramTask} using
     * {@link DataService#submit(Callable)} in which case {@link #dataService}
     * field will be set (after the ctor) by the {@link DataService} itself. The
     * {@link DataService} will be used to identify an {@link ExecutorService}
     * and the {@link IJoinNexusFactory} will be used to establish access to
     * indices, relations, etc. in the context of the {@link AbstractTask} - see
     * {@link AbstractStepTask#submit()}.
     * 
     * @param action
     * @param step
     * @param joinNexus
     */
    public ProgramTask(ActionEnum action, IStep step,
            IJoinNexusFactory joinNexusFactory) {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();

        this.action = action;

        this.step = step;

        this.joinNexusFactory = joinNexusFactory;
        
        this.indexManager = null; 

    }

    /**
     * Variant when the task will be executed directly by the caller.
     * 
     * @param action
     * @param step
     * @param joinNexusFactory
     * @param indexManager
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     */
    public ProgramTask(ActionEnum action, IStep step,
            IJoinNexusFactory joinNexusFactory, IIndexManager indexManager) {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.action = action;

        this.step = step;

        this.joinNexusFactory = joinNexusFactory;

        this.indexManager = indexManager;

    }

    /**
     * Execute the program.
     * 
     * @throws Exception
     */
    public Object call() throws Exception {

        if (DEBUG) {

            log.debug("begin: program=" + step.getName() + ", action="
                            + action);
            
        }

        try {

            final ProgramUtility util = new ProgramUtility();
            
            if (action.isMutation()) {

                final RuleStats totals;

                if (!step.isRule() && ((IProgram) step).isClosure()) {

                    /*
                     * Compute closure of a flat set of rules.
                     */
                    
                    totals = executeClosure((IProgram) step);

                } else if (util.isClosureProgram(step)) {

                    /*
                     * Compute closure of a program that embedded closure
                     * operations.
                     */
                    
                    totals = executeProgramWithEmbeddedClosure((IProgram)step);

                } else {

                    /*
                     * Execute a mutation operation that does not use closure.
                     */
                    
                    totals = executeMutation(step);
                    
                }

                if(INFO) {
                    
                    log.info("totals: \n" + totals);
                    
                }
                
                return totals.mutationCount.get();
                
            } else {

                if ((!step.isRule() && ((IProgram) step).isClosure())
                        || util.isClosureProgram(step)) {

                    /*
                     * The step is either a closure program or embeds a closure
                     * program.
                     */
                    
                    throw new UnsupportedOperationException(
                            "Closure only allowed for mutation.");

                }

                /*
                 * Execute a query.
                 */
                final IAsynchronousIterator<ISolution[]> ret = executeQuery(step);
                
                if (indexManager instanceof AbstractDistributedFederation) {

                    /*
                     * Note: The distributed federation (JDS) uses RMI and
                     * requires either a "thick" iterator (fully buffered) or a
                     * proxy object.
                     * 
                     * Note: There is no natural order for high-level query
                     * since results can be produced by parallel threads for
                     * join subqueries. The only way to have a natural order is
                     * for a sort to be imposed on the result. And the results
                     * are often Solutions, not just Elements, so they lack any
                     * sense of natural order.
                     */
                    
                    return ((AbstractDistributedFederation) indexManager)
                            .getProxy(ret, joinNexusFactory.newInstance(
                                    indexManager).getSolutionSerializer(), null/* keyOrder */);

                }

                return new ChunkConsumerIterator<ISolution>(ret);

            }

        } finally {

            if (DEBUG)
                log.debug("bye");

        }

    }

    /**
     * Execute the {@link IStep} as a query.
     * 
     * @param step
     *            The {@link IStep}.
     * 
     * @return The {@link IChunkedOrderedIterator} that will drain the
     *         {@link ISolution}s generated by the {@link IStep}. Execution
     *         will be cancelled if the iterator is
     *         {@link ICloseableIterator#close() closed}. If execution results
     *         in an error, then the iterator will throw a
     *         {@link RuntimeException} whose cause is the error.
     * 
     * @throws RuntimeException
     */
    protected IAsynchronousIterator<ISolution[]> executeQuery(final IStep step) {
        
        if (step == null)
            throw new IllegalArgumentException();

        if (DEBUG)
            log.debug("program=" + step.getName());
        
        // buffer shared by all rules run in this query.
        final IBlockingBuffer<ISolution[]> buffer = joinNexusFactory.newInstance(
                indexManager).newQueryBuffer();

        // the task to execute.
        final QueryTask queryTask = new QueryTask(step, joinNexusFactory,
                buffer, indexManager, dataService);

        Future<RuleStats> future = null;
        
        try {

            /*
             * Note: We do NOT get() this Future. This task will run
             * asynchronously.
             * 
             * The Future is cancelled IF (hopefully WHEN) the iterator is
             * closed.
             * 
             * If the task itself throws an error, then it will use
             * buffer#abort(cause) to notify the buffer of the cause (it will be
             * passed along to the iterator) and to close the buffer (the
             * iterator will notice that the buffer has been closed as well as
             * that the cause was set on the buffer).
             * 
             * @todo if the #of results is small and they are available with
             * little latency then return the results inline using a fully
             * buffered iterator.
             */

            // run the task.
            future = queryTask.submit();
            
            // set the future on the BlockingBuffer.
            buffer.setFuture(future);

            if (DEBUG)
                log.debug("Returning iterator reading on async query task");

            // return the async iterator.
            return buffer.iterator();

        } catch (Throwable ex) {

            try {

                log.error(ex, ex);
                
                throw new RuntimeException(ex);
                
            } finally {

                buffer.close();

                if (future != null) {

                    future.cancel(true/* mayInterruptIfRunning */);

                }

            }
            
        }
    
    }

    /**
     * Run a mutation {@link IStep}. The {@link IStep} may consist of many sub-{@link IStep}s.
     * <p>
     * Note: If you specify {@link ITx#READ_COMMITTED} for mutation operations
     * when using a federation then concurrent split/join/move can cause the
     * operation to fail. It is safer to use read-consistent semantics by
     * specifying {@link IIndexStore#getLastCommitTime()} instead.
     * 
     * @param step
     *            The {@link IStep}.
     * 
     * @return Metadata about the program execution, including the required
     *         {@link RuleStats#mutationCount}.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected RuleStats executeMutation(final IStep step)
            throws InterruptedException, ExecutionException {

        if (step == null)
            throw new IllegalArgumentException();

        if (!action.isMutation())
            throw new IllegalArgumentException();
        
//        /*
//         * Advance the read-consistent timestamp so that any writes from
//         * the previous rules or the last round are now visible.
//         */
//        joinNexusFactory.setReadTimestamp(TimestampUtility
//                .asHistoricalRead(indexManager.getLastCommitTime()));

        final MutationTask mutationTask = new MutationTask(action, joinNexusFactory,
                step, indexManager, dataService );

        if (DEBUG)
            log.debug("begin: action=" + action + ", program=" + step.getName()
                    + ", task=" + mutationTask);
        
        /*
         * Submit task and await completion, returning the result.
         * 
         * Note: The task is responsible for computing the aggregate mutation
         * count.
         */
        return mutationTask.submit().get();
        
    }

    /**
     * Computes the closure of a set of {@link IRule}s until the relation(s) on
     * which they are writing reach a "fixed point".
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * in turn (either sequentially or in parallel, depending on the program).
     * Solutions computed for each rule in each round written onto the relation
     * for the head of that rule. The process halts when no new solutions are
     * computed in a given round.
     * <p>
     * Note: When we are running the program on a {@link ConcurrencyManager},
     * each round of the closure is submitted as a single {@link AbstractTask}.
     * This allows us to do historical reads during the round and to update the
     * read-behind timestamp before each round. During the round, the program
     * will write on buffers that are flushed at the end of the round. Those
     * buffers will use unisolated writes onto the appropriate relations.
     * 
     * <h2>mutation counts</h2>
     * 
     * In order to detect the fixed point we MUST know whether or not any
     * mutations were made to the relation during the round. The design does NOT
     * rely on the relation count before and after the round since it would have
     * to use an _exact_ range count for the relation (otherwise it is possible
     * that a deleted tuple would be overwritten by a computed entailment but
     * that the count would not change). However, the exact range count is
     * relatively expensive which is why the design insists on getting back the
     * #of elements actually written on the index from each rule in each round.
     * If no rules in a given round caused an element to be written, then we are
     * at the fixed point.
     * <p>
     * Note: This assumes that you are following the {@link IMutableRelation}
     * contract -- you MUST NOT overwrite tuples with the same key and value, or
     * at least you must not report such "do nothing" overwrites in the mutation
     * count!!!
     * 
     * @param action
     *            The action (must be a mutation operation).
     * @param program
     *            The program to be executed.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected RuleStats executeClosure(IProgram program)
            throws InterruptedException, ExecutionException {

        if (program == null)
            throw new IllegalArgumentException();

        if(!program.isClosure())
            throw new IllegalArgumentException();
        
        final long begin = System.currentTimeMillis();

        final RuleStats totals = joinNexusFactory.newInstance(indexManager)
                .getRuleStatisticsFactory().newInstance(program);
        
        int round = 1;
        
        long mutationCount = 0L;
        while (true) {

            // mutationCount before this round.
            final long mutationCount0 = totals.mutationCount.get();
            
            if (DEBUG)
                log.debug("round=" + round + ", mutationCount(before)="
                        + mutationCount0);

//            if (round > 1) {
//
//                /*
//                 * Advance the read-consistent timestamp so that any writes from
//                 * the previous rules or the last round are now visible.
//                 */
//
//                joinNexusFactory.setReadTimestamp(TimestampUtility
//                        .asHistoricalRead(indexManager.getLastCommitTime()));
//                
//            }
            
            // execute the program.
            final RuleStats tmp = executeMutation(program);

            /*
             * This is the #of mutations from executing this round.
             * 
             * Note: each round has its own mutation buffer so this is just the
             * #of mutations in the round. This is because executeMutation()
             * builds a new execution context for each round.
             */
            final long mutationDelta = tmp.mutationCount.get();
            
            // Total mutation count so far.
            final long mutationCount1 = mutationCount = mutationCount0
                    + tmp.mutationCount.get();

            // set the round identifier.
            tmp.closureRound = round;
            
            // Aggregate the rule statistics, but not mutationCount.
            totals.add(tmp);

            if (DEBUG) {

                log.debug("round# " + round + ", mutationCount(before="
                        + mutationCount0 + ", after=" + mutationCount1
                        + ", delta=" + mutationDelta + "):" + totals);

            }

            if (mutationDelta == 0L)
                break;

            round++;

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (!totals.mutationCount.compareAndSet(0L, mutationCount)) {
            
            throw new AssertionError("mutationCount=" + totals.mutationCount);
            
        }

        if (INFO) {

            log.info("\nComputed fixed point: program=" + program.getName()
                    + ", rounds=" + round + ", elapsed=" + elapsed + "ms");

        }

        return totals;

    }

    /**
     * Execute an {@link IProgram} containing one or more sub-{@link IProgram}
     * that are closure operations. The top-level program must not be a closure
     * operation. All steps above the closure operations will be run in a
     * sequence. The closure operations themselves will be executed using
     * {@link #executeClosure(IProgram)}.
     * <p>
     * Note: Any program that embeds a closure operation must be sequential
     * (this is enforced by the Program class).
     * 
     * Note: Programs that use closure operations are constrained to either (a)
     * a fix point of a (normally parallel) program consisting solely of
     * {@link IRule}s; or (b) a sequential program containing some steps that
     * are the fix point of a (normally parallel) program consisting solely of
     * {@link IRule}s.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * @todo this will not correctly handle programs use closure in a
     *       sub-sub-program.
     * 
     * @throws IllegalArgumentException
     *             if <i>program</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if <i>program</i> is <em>itself</em> a closure operation.
     * @throws IllegalStateException
     *             unless the {@link ActionEnum} is a mutation operation.
     */
    protected RuleStats executeProgramWithEmbeddedClosure(IProgram program)
            throws InterruptedException, ExecutionException {

        if (program == null)
            throw new IllegalArgumentException();
        
        if (program.isClosure())
            throw new IllegalArgumentException();
        
        if(!action.isMutation()) {
            throw new IllegalStateException();
        }
        
        if (INFO)
            log.info("program embeds closure operations");
        
        final RuleStats totals = joinNexusFactory.newInstance(indexManager)
                .getRuleStatisticsFactory().newInstance(program);

        final Iterator<? extends IStep> itr = (program).steps();
        
        long mutationCount = 0L;
        while(itr.hasNext()) {
            
            final IStep step = itr.next();
            
            final RuleStats stats;
            
            if (!step.isRule() && ((IProgram) step).isClosure()) {
                
                // A closure step.
                stats = executeClosure((IProgram) step);
                
            } else {
                
                // A non-closure step.
                stats = executeMutation(step);
                
            }
            
            totals.add(stats);

            /*
             * Note: both a executeClosure() and executeMutation() will run with
             * their own buffers so flush() reporting is not carried forward
             * beyond those methods. Hence we have to aggregate the
             * mutationCount ourselves for each step that we run.
             */
            mutationCount += stats.mutationCount.get();
            
        }

        // transfer the final mutation count onto the total.
        if (!totals.mutationCount.compareAndSet(0L, mutationCount)) {
            
            throw new AssertionError("mutationCount=" + totals.mutationCount);
            
        }
        
        return totals;

    }
    
}
