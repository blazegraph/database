package com.bigdata.relation.rule.eval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.DataService;

/**
 * A task that executes a query operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryTask extends AbstractStepTask {

    /**
     * 
     */
    private static final long serialVersionUID = -1795376592525891934L;
    
    /**
     * The {@link IBlockingBuffer} on which the {@link ISolution}s will be
     * written.
     */
    private final IBlockingBuffer<ISolution[]> buffer;

    /**
     * 
     * @param buffer
     *            The {@link IBlockingBuffer} on which the {@link ISolution}s
     *            will be written.
     */
    public QueryTask(IStep step, IJoinNexusFactory joinNexusFactory,
            IBlockingBuffer<ISolution[]> buffer, IIndexManager indexManager,
            DataService dataService) {

        super(ActionEnum.Query, joinNexusFactory, step, indexManager,
                dataService);
        
        if (buffer == null)
            throw new IllegalArgumentException();
        
        this.buffer = buffer;
        
    }
    
    /**
     * Run the task (invoked once we are in the target execution context).
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
         */
        
        final List<Callable<RuleStats>> tasks = newQueryTasks(step, joinNexus,
                buffer);

        try {

            // run those tasks and wait for them to complete.
            final RuleStats totals = runTasks(joinNexus, tasks);

            /*
             * Nothing more will be written on the buffer so we close it. The
             * iterator will drain anything in the queue from the buffer and
             * then hasNext() will report false.
             */

            if (log.isDebugEnabled()) {

                log.debug("done - closing the blocking buffer");

            }

            // producer is done : close the BlockingBuffer.
            buffer.close();

            RuleLog.log(totals);
            
            return totals;

        } catch (Throwable t) {
            
            try {
                
                log.error("Problem running query: " + t, t);
                
            } catch (Throwable ignored) {
                
                // ignored : logging system problem.
                
            }

            /*
             * Note: This will close the buffer. It will also cause the iterator
             * to throw the [cause] from hasNext() (or next(), which invokes
             * hasNext()).
             */

            buffer.abort(t/* cause */);
        
            throw new RuntimeException(t);
            
        }
        
    }

    /**
     * Run the task(s) and wait for them to complete.
     * 
     * @return The {@link RuleStats}
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected RuleStats runTasks(final IJoinNexus joinNexus,
            final List<Callable<RuleStats>> tasks) throws InterruptedException,
            ExecutionException {
        
        assert tasks != null;
        assert !tasks.isEmpty();

        final RuleStats totals;

        if (tasks.size() == 1) {

            totals = runOne(joinNexus, step, tasks.get(0));

        } else if (!joinNexus.forceSerialExecution() && !step.isRule()
                && ((IProgram) step).isParallel()) {

            totals = runParallel(joinNexus, step, tasks);

        } else {

            totals = runSequential(joinNexus, step, tasks);

        }

        return totals;

    }
    
    /**
     * Builds a set of tasks for the program. Each task is assigned its own
     * {@link UnsynchronizedArrayBuffer}. Each task will flush that
     * {@link UnsynchronizedArrayBuffer} onto the given {@link IBuffer} when it
     * completes.
     * 
     * @param step
     *            The {@link IStep}.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     * @param buffer
     *            The thread-safe buffer onto which the individual tasks emit
     *            chunks.
     * 
     * @return The list of tasks to run.
     */
    protected List<Callable<RuleStats>> newQueryTasks(IStep step,
            IJoinNexus joinNexus, IBlockingBuffer<ISolution[]> buffer) {

        if (log.isDebugEnabled())
            log.debug("step=" + step.getName());

        final List<Callable<RuleStats>> tasks;

        if (step.isRule()) {

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) step;

            final Callable<RuleStats> task = joinNexus.getRuleTaskFactory(
                    false/* parallel */, rule).newTask(rule, joinNexus,
                    buffer);
            
            tasks.add(task);

        } else {

            final IProgram program = (IProgram)step;
            
            final boolean parallel = program.isParallel();

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<? extends IStep> itr = program.steps();

            while (itr.hasNext()) {

                // FIXME RULE_REFACTOR handle sub-programs.
                final IRule rule = (IRule) itr.next();

                final Callable<RuleStats> task = joinNexus.getRuleTaskFactory(
                        parallel, rule).newTask(rule, joinNexus, buffer);

                tasks.add(task);

            }

        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+tasks.size()+" query tasks");
            
        }
        
        return tasks;

    }

}
