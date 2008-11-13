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
 * Created on Jul 2, 2008
 */

package com.bigdata.relation.rule.eval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.FlushBufferTask;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.DataService;

/**
 * A task that executes a mutation operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutationTask extends AbstractStepTask {

    protected MutationTask(ActionEnum action,
            IJoinNexusFactory joinNexusFactory, IStep step,
            IIndexManager indexManager, DataService dataService) {

        super(action, joinNexusFactory, step, indexManager, dataService);
        
    }

    /**
     * Run the task.
     * <p>
     * Note: We can create the individual tasks that we need to execute now that
     * we are in the correct execution context.
     * <P>
     * Note: The mutation tasks write on {@link IBuffer}s and those buffers
     * flush to indices in the {@link IMutableRelation}s. We have to defer the
     * creation of those buffers until we are in the execution context and have
     * access to the correct indices. In turn, this means that we can not create
     * the tasks that we are going to execute until we have those buffers on
     * hand. Hence everything gets deferred until we are in the correct
     * execution context and have the actual {@link IIndexManager} with which
     * the tasks will execute.
     */
    public RuleStats call() throws Exception {
        
        /*
         * Create the IJoinNexus that will be used to evaluate the operation now
         * that we are in the execution context and have the correct
         * IIndexManager object.
         */
        final IJoinNexus joinNexus = joinNexusFactory.newInstance(indexManager);

        /*
         * Note: This assumes that we are using the same write timestamp for
         * each relation....  True for now, but consider if two transactions
         * were being written on in conjunction.
         */
        final Map<String, IRelation> relations = getWriteRelations(
                indexManager, step, joinNexus.getWriteTimestamp());

        assert !relations.isEmpty();

        final Map<String, IBuffer<ISolution[]>> buffers = getMutationBuffers(
                joinNexus, relations);

        assert !buffers.isEmpty();

        final List<Callable<RuleStats>> tasks = newMutationTasks(step,
                joinNexus, buffers);

        assert !tasks.isEmpty();

        final RuleStats totals;

        if (tasks.size() == 1) {

            totals = runOne(joinNexus, step, tasks.get(0));

        } else if (!joinNexus.forceSerialExecution() && !step.isRule()
                && ((IProgram) step).isParallel()) {

            totals = runParallel(joinNexus, step, tasks);

            /*
             * Note: buffers MUST be flushed!!!
             */
            flushBuffers(joinNexus, buffers);
            
        } else {

            /*
             * Note: flushes buffer after each step.
             * 
             * Note: strictly speaking, a parallel program where
             * [forceSerialExecution] is specified does not need to flush the
             * buffer after each step since parallel programs do not have
             * sequential dependencies between the rules in the rule set.
             * 
             * @todo not flushing the buffer when [forceSerialExecution] is
             * specified _AND_ the program is parallel would improve
             * performance. Since this is relatively common when computing
             * closure it makes sense to implement this tweak.
             * 
             * @todo replace [forceSerialExecution] with [maxRuleParallelism] or
             * add the latter and give the former the semantics of a debug
             * switch?
             */
            totals = runSequential(joinNexus, step, tasks);

        }

        /*
         * Note: This gets the mutationCount onto [totals]. If a given buffer is
         * empty (either because nothing was written onto it or because it was
         * already flushed above) then this will have very little overhead
         * beyond getting the current mutationCount back from flush(). You
         * SHOULD NOT rely on this to flush the buffers since it does not
         * parallelize that operation and flushing large buffers can be a high
         * latency operation!
         */

        getMutationCountFromBuffers(totals, buffers);

        if(INFO) {
            
            log.info(totals);
            
        }
        
        return totals;
        
    }

    /**
     * Flush the buffer(s) and aggregate the mutation count from each buffer.
     * This is the actual mutation count for the step(s) executed by the
     * {@link MutationTask} (no double-counting).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void flushBuffers(IJoinNexus joinNexus, 
            Map<String, IBuffer<ISolution[]>> buffers)
            throws InterruptedException, ExecutionException {

        if (joinNexus == null)
            throw new IllegalArgumentException();
        
        if (buffers == null)
            throw new IllegalArgumentException();

        final int n = buffers.size();

        if (n == 0) {

            if (INFO)
                log.info("No buffers.");

            return;
            
        }
        
        if (n == 1) {

            /*
             * One buffer, so flush it in this thread.
             */
            
            final IBuffer<ISolution[]> buffer = buffers.values().iterator().next();

            if (INFO)
                log.info("Flushing one buffer: size="+buffer.size());

            buffer.flush();

        } else {

            /*
             * Multiple buffers, each writing on a different relation. Create a
             * task per buffer and submit those tasks to the service to flush
             * them in parallel.
             */
            
            if (INFO)
                log.info("Flushing " + n +" buffers.");

            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>( n );
            
            final Iterator<IBuffer<ISolution[]>> itr = buffers.values().iterator();
            
            while(itr.hasNext()) {
                
                final IBuffer<ISolution[]> buffer = itr.next();
                
                tasks.add(new FlushBufferTask(buffer));
                
            }

            final List<Future<Long>> futures = indexManager
                    .getExecutorService().invokeAll(tasks);

            for (Future<Long> f : futures) {

                // verify task executed Ok.
                f.get();
                
//                mutationCount += f.get();

                // totals.mutationCount.addAndGet(mutationCount);

            }
            
        }

        // make the write sets visible.
        joinNexus.makeWriteSetsVisible();
        
    }
    
    /**
     * This just reads off and aggregates the mutationCount from each buffer as
     * reported by {@link IBuffer#flush()}. This is the actual mutation count
     * for the step(s) executed by the {@link MutationTask} (no
     * double-counting).
     * <p>
     * Note: The buffers SHOULD already have been flushed as this does NOT
     * parallelise the writes on the {@link IMutableRelation}s. See
     * {@link #flushBuffers(IJoinNexus, RuleStats, Map)}, which does
     * parallelize those writes.
     * 
     * @return The mutation count, which was also set as a side-effect on
     *         <i>totals</i>.
     */
    protected long getMutationCountFromBuffers(RuleStats totals,
            Map<String, IBuffer<ISolution[]>> buffers) {
        
        if (totals == null)
            throw new IllegalArgumentException();
        
        if (buffers == null)
            throw new IllegalArgumentException();

        /*
         * Aggregate the mutationCount from each buffer.
         */

        long mutationCount = 0L;

        final Iterator<IBuffer<ISolution[]>> itr = buffers.values().iterator();

        while (itr.hasNext()) {

            final IBuffer<ISolution[]> buffer = itr.next();

            mutationCount += buffer.flush();
            
        }

        /*
         * Note: For a distributed "pipeline" join, the JoinTask(s) for the last
         * join dimension each create their own buffers onto which they write
         * their solution. This is done in order to prevent all data from
         * flowing through the join master. This means that the buffer that is
         * being flushed above was never written on, so the [mutationCount] as
         * computed above will be zero for a _distributed_ pipeline join.
         * 
         * @todo while this avoids an assertion error for the pipeline join, I
         * need to explore more carefully how each join implementation reports
         * the mutation count and how it is aggregated throughput the program
         * execution.
         */
        
//        if (mutationCount > 0) {
        
            /*
             * Atomic set of the total mutation count for all buffers on which
             * the set of step(s) were writing [but only if the task did not
             * update the mutationCount itself].
             */

//            if (!
                    totals.mutationCount.compareAndSet(0L, mutationCount);
//                    ) {
//
//                throw new AssertionError("Already set: mutationCount="
//                        + mutationCount + ", task=" + this);
//
//            }
            
//        }
        
        return mutationCount;
        
    }
    
    /**
     * Builds a set of tasks for the program.
     * 
     * @param buffer
     * 
     * @return
     */
    protected List<Callable<RuleStats>> newMutationTasks(IStep step,
            IJoinNexus joinNexus, Map<String, IBuffer<ISolution[]>> buffers) {

        if (DEBUG)
            log.debug("program=" + step.getName());

        final List<Callable<RuleStats>> tasks;

        if (step.isRule()) {

            if (step.isRule() && ((IRule) step).getHead() == null) {

                throw new IllegalArgumentException("No head for this rule: " + step);

            }

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) step;

            final IBuffer<ISolution[]> buffer = buffers.get(rule.getHead().getOnlyRelationName());
            
            final Callable<RuleStats> task = joinNexus.getRuleTaskFactory(
                    false/* parallel */, rule).newTask(rule, joinNexus, buffer);
   
            tasks.add(task);

        } else {

            final IProgram program = (IProgram)step;
            
            final boolean parallel = program.isParallel();

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<IStep> itr = program.steps();

            while (itr.hasNext()) {

                // @todo handle sub-programs.
                final IRule rule = (IRule) itr.next();

                if (rule.getHead() == null) {

                    throw new IllegalArgumentException("No head for this rule: " + rule);

                }

                final IBuffer<ISolution[]> buffer = buffers.get(rule.getHead()
                        .getOnlyRelationName());

                final IStepTask task = joinNexus.getRuleTaskFactory(parallel,
                        rule).newTask(rule, joinNexus, buffer);

                if (!parallel || joinNexus.forceSerialExecution()) {

                    /*
                     * Tasks for sequential mutation steps are always wrapped to
                     * ensure that the thread-safe buffer is flushed onto the
                     * mutable relation after each rule executes. This is
                     * necessary in order for the results of one rule in a
                     * sequential program to be visible to the next rule in that
                     * sequential program.
                     */

                    tasks.add(new RunRuleAndFlushBufferTask(task, buffer));

                } else {

                    /*
                     * Add the task.
                     */

                    tasks.add(task);

                }
                
            }

        }

        if (DEBUG) {

            log.debug("Created " + tasks.size() + " mutation tasks: action="
                    + action);

        }

        return tasks;

    }
    
}
