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

    /**
     * @param buffers
     *            The buffers on which the tasks will write. There must be one
     *            buffer for each distinct relation in the heads of the various
     *            rules from which the tasks were created.
     */
    protected MutationTask(ActionEnum action,
            IJoinNexusFactory joinNexusFactory, IStep step,
            IIndexManager indexManager, DataService dataService) {

        super(action, joinNexusFactory, step, indexManager, dataService);
        
    }

    /**
     * Run the task.
     */
    public RuleStats call() throws Exception {
        
        /*
         * Create the IJoinNexus that will be used to evaluate the Query now
         * that we are in the execution context and have the correct
         * IIndexManager object.
         */
        final IJoinNexus joinNexus = joinNexusFactory.newInstance(indexManager);

        /*
         * Create the individual tasks that we need to execute now that we are
         * in the correct execution context.
         * 
         * Note: The mutation tasks write on buffers and those buffers flush to
         * indices in the mutable relations. We have to defer the creation of
         * those buffers until we are in the execution context and have access
         * to the correct indices. In turn, this means that we can not create
         * the tasks that we are going to execute until we have those buffers on
         * hand. Hence everything gets deferred until we are in the correct
         * execution context and have the actual IIndexManager with which the
         * tasks will execute.
         */

        /*
         * Note: This assumes that we are using the same write timestamp for
         * each relation....  True for now, but consider if two transactions
         * were being written on in conjunction.
         */
        final Map<String, IRelation> relations = getWriteRelations(
                indexManager, step, joinNexus.getWriteTimestamp());

        assert !relations.isEmpty();

        final Map<String, IBuffer<ISolution>> buffers = getMutationBuffers(
                joinNexus, relations);

        assert !buffers.isEmpty();

        final List<Callable<RuleStats>> tasks = newMutationTasks(step,
                joinNexus, buffers);

        assert !tasks.isEmpty();

        final RuleStats totals;

        if(tasks.size()==1) {
            
            totals = runOne(step, tasks.get(0));
            
        } else if (!joinNexus.forceSerialExecution() && !step.isRule()
                && ((IProgram) step).isParallel()) {

            totals = runParallel(step, tasks);

            flushBuffers(joinNexus, totals, buffers);

        } else {

            // Note: flushes buffer after each step.
            totals = runSequential(step, tasks);

        }

        if (log.isDebugEnabled())
            log.debug("done: program=" + step.getName());

        return totals;
        
    }

    /**
     * Flush the buffer(s).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void flushBuffers(IJoinNexus joinNexus, RuleStats totals,
            Map<String, IBuffer<ISolution>> buffers)
            throws InterruptedException, ExecutionException {

        if (joinNexus == null)
            throw new IllegalArgumentException();
        
        if (totals == null)
            throw new IllegalArgumentException();
        
        if (buffers == null)
            throw new IllegalArgumentException();

        final int n = buffers.size();

        if (n == 0) {

            if (log.isInfoEnabled())
                log.info("No buffers.");

            return;
            
        }
        
        if (n == 1) {

            /*
             * One buffer, so flush it in this thread.
             */
            
            final IBuffer<ISolution> buffer = buffers.values().iterator().next();

            if (log.isInfoEnabled())
                log.info("Flushing one buffer: size="+buffer.size());

            final long mutationCount = buffer.flush();

            totals.mutationCount.addAndGet(mutationCount);

        } else {

            /*
             * Multiple buffers, each writing on a different relation. Create a
             * task per buffer and submit those tasks to the service to flush
             * them in parallel.
             */
            
            if (log.isInfoEnabled())
                log.info("Flushing " + n +" buffers.");

            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>( n );
            
            final Iterator<IBuffer<ISolution>> itr = buffers.values().iterator();
            
            while(itr.hasNext()) {
                
                final IBuffer<ISolution> buffer = itr.next();
                
                tasks.add(new FlushBufferTask(buffer));
                
            }
            
            final List<Future<Long>> futures = indexManager.getExecutorService().invokeAll(tasks);
            
            for( Future<Long> f : futures ) {
                
                final long mutationCount = f.get(); 
                
                totals.mutationCount.addAndGet(mutationCount);
                
            }
            
        }

        // make the write sets visible.
        joinNexus.makeWriteSetsVisible();
        
        if(log.isInfoEnabled()) {
            
            log.info(totals);
            
        }
        
    }
    
    /**
     * Builds a set of tasks for the program.
     * 
     * @param buffer
     * 
     * @return
     */
    protected List<Callable<RuleStats>> newMutationTasks(IStep step,
            IJoinNexus joinNexus, Map<String, IBuffer<ISolution>> buffers) {

        if (log.isDebugEnabled())
            log.debug("program=" + step.getName());

        final List<Callable<RuleStats>> tasks;

        if (step.isRule()) {

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) step;

            final IBuffer<ISolution> sharedBuffer = buffers.get(rule.getHead().getOnlyRelationName());
            
//            final IBuffer<ISolution> localBuffer = new UnsynchronizedArrayBuffer<ISolution>(
//                    1000, sharedBuffer);
            
            final Callable<RuleStats> task = joinNexus.getRuleTaskFactory(false/*parallel*/,
                    rule).newTask(rule, joinNexus, 
//                            localBuffer
                            sharedBuffer
                            );
   
            tasks.add(task);

        } else {

            final IProgram program = (IProgram)step;
            
            final boolean parallel = program.isParallel();

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<IStep> itr = program.steps();

            while (itr.hasNext()) {

                // @todo handle sub-programs.
                final IRule rule = (IRule) itr.next();

                final IBuffer<ISolution> buffer = buffers.get(rule.getHead().getOnlyRelationName());
                
                final Callable<RuleStats> task = joinNexus.getRuleTaskFactory(parallel, rule)
                        .newTask(rule, joinNexus, buffer);

                tasks.add(task);

            }

        }

        if (log.isDebugEnabled()) {

            log.debug("Created " + tasks.size() + " mutation tasks: action="
                    + action);

        }

        return tasks;

    }
    
}
