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
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IClosableIterator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.DataService.IDataServiceAwareProcedure;

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
 * @todo performance comparisons with the old rule execution logic.
 * 
 * @todo it should be possible to have a different action associated with each
 *       rule in the program, and to have a different target relation for the
 *       head of each rule on which we will write (mutation). Different query or
 *       mutation count results could be handled by an extension with
 *       "nextResultSet" style semantics. However, for now, all rules MUST write
 *       on the same buffer. Query results will therefore be multiplexed as will
 *       mutations counts.
 * 
 * @todo it should be possible to handle different relation classes in the same
 *       rules, e.g., RDF and non-RDF relations. Or even the SPO and lexicon
 *       relation for the RDF DB -- the latter will be useful for materializing
 *       extenalized statements efficiently.
 * 
 * @todo could make the return type a generic for {@link AbstractStepTask} and
 *       make this class a concrete implementation of that one.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalProgramTask implements IProgramTask,
        IDataServiceAwareProcedure, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7047397038429305180L;

    protected static final transient Logger log = Logger.getLogger(LocalProgramTask.class);
    
    private final ActionEnum action;
    
    private final IStep step;

    /**
     * Serializable field specified when the {@link LocalProgramTask} will be
     * submitted (via RMI or not) to a {@link DataService}. A new
     * {@link IJoinNexus} is instantiated in the execution context on the
     * {@link DataService} from this field.
     */
    private final IJoinNexusFactory joinNexusFactory;
    
//    /**
//     * Note: NOT serialized!
//     */
//    * <p>
//    * Note: NOT <code>final</code> since this field must be patched when we
//    * create an {@link AbstractTask} in order to resolve the named indices
//    * using the {@link AbstractTask}.
//    private transient IJoinNexus joinNexus;
    
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

        if (log.isInfoEnabled())
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
     * Note: the caller MUST submit using DataService#submit() in which case
     * {@link #dataService} will be set after the ctor is done by the
     * {@link DataService} itself. The {@link DataService} will be used to
     * identify an {@link ExecutorService} and the {@link IJoinNexusFactory}
     * will be used to establish access to indices, relations, etc. that first
     * resolves against the {@link AbstractTask} - see
     * {@link AbstractStepTask#submit()}.
     * 
     * @param action
     * @param step
     * @param joinNexus
     */
    public LocalProgramTask(ActionEnum action, IStep step,
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
    public LocalProgramTask(ActionEnum action, IStep step, IJoinNexusFactory joinNexusFactory, IIndexManager indexManager) {

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

        if(log.isDebugEnabled()) {

            log.debug("begin: program="+step.getName()+", action="+action);
            
        }

        try {

            final ProgramUtility util = new ProgramUtility();

            if (util.isClosureProgram(step)) {

                log.info("program uses closure operations");
                
                if (!action.isMutation()) {

                    throw new UnsupportedOperationException(
                            "closure requires mutation");

                }

                /*
                 * There is a closure operation either at the top-level or in a
                 * direct sub-program. We run all steps above the closure in a
                 * sequence.
                 * 
                 * Note: The closure itself will consist of a flat set of rules.
                 * 
                 * Note: When we are running the program on a concurrency
                 * manager, each round of the closure is submitted as a single
                 * task. This allows us to do historical reads during the round
                 * (more efficient than read-committed) and to update the
                 * read-behind timestamp before each round. During the round,
                 * the program will write on buffers that are flushed at the end
                 * of the round. Those buffers will use unisolated writes onto
                 * the appropriate relations.
                 * 
                 * Note: Any program that embeds a closure operation must be
                 * sequential (this is enforced by the Program class).
                 * 
                 * Note: Programs that use closure operations are constrained to
                 * either (a) a fix point of a (normally parallel) program
                 * consisting solely of {@link IRule}s; or (b) a sequential
                 * program containing some steps that are the fix point of a
                 * (normally parallel) program consisting solely of
                 * {@link IRule}s.
                 * 
                 * @todo this will not correctly handle programs use closure in
                 * a sub-sub-program.
                 */
                
                final RuleStats totals = new RuleStats(step);
                
                final Iterator<? extends IStep> itr = ((IProgram)step).steps();
                
                while(itr.hasNext()) {
                    
                    final IStep step = itr.next();
                    
                    if (!step.isRule() && ((IProgram) step).isClosure()) {
                        
                        final RuleStats stats = executeClosure((IProgram) step);
                        
                        totals.add( stats );
                        
                    } else {
                        
                        // A non-closure step.
                        
                        final RuleStats stats = executeMutation(step);
                        
                        totals.add(stats);
                        
                    }
                    
                }
                
                return totals.mutationCount.get();
                
            } else if (action.isMutation()) {

                return executeMutation(step).mutationCount.get();

            } else {

                return executeQuery(step);

            }

        } finally {

            log.debug("bye");

        }

    }

//    /**
//     * Run a task.  It will be submitted to the {@link #executorService} iff
//     * it is defined and otherwise the {@link #dataService} must be defined
//     * and it will be submitted to the {@link ConcurrencyManager} for the
//     * {@link DataService}.
//     */
//    protected Future<RuleStats> submit(AbstractStepTask task) {
//        
//        if (executorService == null) {
//
//            if (dataService == null) {
//
//                throw new IllegalStateException();
//
//            }
//
//            /*
//             * This condition occurs when this Callable is sent to the
//             * DataService using IDataService#submit(Callable). In order to gain
//             * access to the named indices for the relation, we have to wrap up
//             * this Callable as an AbstractTask that declares the appropriate
//             * timestamp and resources and set [service] to an ExecutorService
//             * running on the DataService itself. The AbstractTask will then be
//             * submitted to the ConcurrencyManager for execution. It will
//             * delegate back to #call(). Since [service] will be non-null, the
//             * other code branch will be followed.
//             * 
//             * Note: The [dataService] reference is set by the DataService
//             * itself when it executes this Callable and notices the
//             * IDataServiceAwareProcedure interface.
//             * 
//             * Note: The [service] field is NOT serializable and MUST NOT be set
//             * if this task is to execute on the DataService.
//             */
//
//            log.info("Will run on data service's concurrency manager.");
//
//            return task.submit();
//
//        }
//
//        return executorService.submit(task);
//
//    }
    
    /**
     * Execute the {@link IProgram} as a query.
     * 
     * @param program
     * 
     * @return The {@link IChunkedOrderedIterator} that will drain the
     *         {@link ISolution}s generated by the {@link IProgram}. The
     *         program execution will be cancelled if the iterator is
     *         {@link IClosableIterator#close() closed}. If the program
     *         execution results in an error, then the iterator will throw a
     *         {@link RuntimeException} whose cause is the program error.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected IChunkedOrderedIterator<ISolution> executeQuery(final IStep step)
            throws Exception {

        if (step == null)
            throw new IllegalArgumentException();

        if (log.isDebugEnabled())
            log.debug("program=" + step.getName());
        
        // buffer shared by all rules run in this query.
        final IBlockingBuffer<ISolution> buffer = joinNexusFactory.newInstance(
                indexManager).newQueryBuffer();
        
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

            final QueryTask queryTask = new QueryTask(step, joinNexusFactory,
                    buffer, indexManager, dataService);

            future = queryTask.submit();
            
            buffer.setFuture(future);

            if (log.isDebugEnabled())
                log.debug("Returning iterator reading on async query task");

            /*
             * FIXME The distributed federation (JDS) requires a proxy object.
             * 
             * When using RMI the return iterator for a QUERY needs be a proxy
             * for the remote iterator running on the data service that is
             * actually executing that proxy (this is only true for remote data
             * services).
             * 
             * When the proxy iterator is closed by the client the close needs
             * to make it back to the data service where it must cancel the
             * Future(s) writing on the buffer.
             * 
             * @todo add factory to IJoinNexus so that this can be overriden for
             * the JDS
             * 
             * @todo when returning a proxy for a Future whose get() returns the
             * iterator reading from the query buffer, the proxy should note
             * whether or not the iterator is exhausted each time it fetches the
             * next chunk and should only fetch chunks - if the caller want to
             * use the element at a time aspect of the iterator it should still
             * fetch a chunk and then step through the elements until the next
             * chunk is required.
             */

//            return new ClosableIteratorFuture<ISolution, RuleStats>(buffer,
//                    future);
            
            return buffer.iterator();

        } catch (Exception ex) {

            log.error(ex, ex);

            buffer.close();
            
            if (future != null) {

                future.cancel(true/* mayInterruptIfRunning */);

            }

            throw ex;
            
        }
    
    }

    /**
     * Run a mutation program.
     * 
     * @param step
     *            The program.
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
        
        final MutationTask mutationTask = new MutationTask(action, joinNexusFactory,
                step, indexManager, dataService );

        if (log.isDebugEnabled())
            log.debug("begin: action=" + action + ", program=" + step.getName()
                    + ", task=" + mutationTask);
        
        // submit to the service and await completion, returning the result.
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
     * contract -- you MUST NOT overwriting tuples with the same key and value,
     * or at least you must not report such "do nothing" overwrites in the
     * mutation count!!!
     * 
     * @param action
     *            The action (must be a mutation operation).
     * @param program
     *            The program to be executed.
     * 
     * @todo While there is nothing place to detect {@link IProgram} whose
     *       closure will not terminate, it requires either a custom
     *       {@link IStepTask} or added expressivity to produce an
     *       {@link IProgram} whose closure is non-terminating.
     *       <p>
     *       If the caller has a Future for this {@link Callable} then they can
     *       always cancel the {@link Future} or submit it with a timeout.
     *       <p>
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected RuleStats executeClosure(IProgram program)
            throws InterruptedException, ExecutionException {

        if (program == null)
            throw new IllegalArgumentException();

        if(!action.isMutation()) {

            /*
             * FIXME closure for query is valid, but you have to compute the
             * closure over a temporary relation for the result set and then
             * return the iterator that reads from an arbitrary access path for
             * that relation.
             * 
             * The class relation of relation depends on the class of the
             * relation for the head of each rule and there needs to be one
             * temporary relation for each distinct relation for the heads of
             * the rule.
             * 
             * The temporary relation only requires a single index, which ever
             * is most efficient, since it only needs to enforce distinct.
             * 
             * The temporary relation(s) are dropped when the iterator is
             * closed.
             * 
             * The temporary relation(s) can be backed by a TemporaryRawStore
             * for local execution. For distributed execution there will have to
             * be remote access for writing on that temporary relation, so it
             * could be a scale-out index or the data service can be generalized
             * to expose indices on temporary stores. Regardless, writes on the
             * temporary relations need to be buffered and go through the
             * IMutableRelation interface for the relation.
             * 
             * In fact, this is exactly what truth maintenance is doing. It is
             * also handling additional wrinkles such as mapping the rules over
             * the temporary store and the database. What it is not doing as
             * well is generalizing the notion of a temporary relation into the
             * basic program execution support. However, the caller does have
             * more control over the life cycle of that temporary relation and
             * that additional control is probably necessary if we are
             * interested in more than just a single iterator pass over the
             * temporary relation.
             */
            
            throw new UnsupportedOperationException(
                    "Closure only allowed for mutation.");
            
        }

        if(!program.isClosure())
            throw new IllegalArgumentException();
        
        final long begin = System.currentTimeMillis();

        final RuleStats totals = new RuleStats(program);
        
        int round = 0;

        while (true) {

            final long mutationCount0 = totals.mutationCount.get();
            
            if (log.isDebugEnabled())
                log.debug("round=" + round);

            final RuleStats tmp = executeMutation(program);

            /*
             * Post-round mutation counter.
             * 
             * Note: We MUST flush the buffer(s) before obtaining this counter -
             * otherwise there may be solutions in the buffer(s) that have not
             * been flushed to the mutable relation which would lead to
             * undercounting the #of mutations in this round.
             */
            final long mutationCount1 = totals.mutationCount.get();

            final long mutationDelta = mutationCount1 - mutationCount0;
            
            // aggregate the rule statistics.
            totals.add(tmp);

            if (log.isDebugEnabled()) {

                log.debug("round# " + round + ", mutationCount(delta="
                        + mutationDelta + ", before=" + mutationCount0
                        + ", after=" + mutationCount1 + "):" + totals);

            }

            if (mutationDelta == 0L)
                break;
            
            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled()) {

            log.info("\nComputed fixed point: program="
                            + program.getName() + ", rounds=" + (round + 1)
                            + ", elapsed=" + elapsed + "ms");
                        
        }

        return totals;

    }

}
