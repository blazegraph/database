package com.bigdata.relation.rule.eval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IClosableIterator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.DataService;

/**
 * Asynchronous task writes {@link ISolution}s for an {@link IRule} or
 * {@link IProgram} onto an {@link IBlockingBuffer}. When there are no more
 * solutions, the buffer will be {@link IBlockingBuffer#close()}ed and the
 * iterator will report that is has been exhausted once it finishes draining the
 * {@link IBlockingBuffer}'s internal queue.
 * <p>
 * Note: If the iterator is {@link IClosableIterator#close()}ed then it MUST
 * cause the backing {@link IBlockingBuffer} to also be closed (you have to wrap
 * up the iterator before returning it to the client).
 * <p>
 * Note: This task can not be submitted to a remote service since the
 * {@link IBlockingBuffer} is a purely local object. Instead, an instance of
 * this task must be created on the remote service and a proxy must be created
 * for the iterator returned by the {@link IBlockingBuffer}. That proxy is then
 * returned to the client.
 * 
 * @see ClosableIteratorFuture
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryTask extends AbstractStepTask {
    
    private final IBlockingBuffer<ISolution> buffer;

    /**
     * 
     * @param buffer
     *            The buffer on which the {@link ISolution}s will be written.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     */
    public QueryTask(IStep step, IJoinNexus joinNexus,
//            List<Callable<RuleStats>> tasks,
            IRuleTaskFactory defaultTaskFactory,
            IBlockingBuffer<ISolution> buffer,
            ExecutorService executorService, DataService dataService) {

        super(ActionEnum.Query, joinNexus, step, defaultTaskFactory, executorService, dataService);
        
        if (buffer == null)
            throw new IllegalArgumentException();
        
        this.buffer = buffer;
        
    }
    
    /**
     * Run the task.
     * 
     * @throws IllegalStateException
     *             if the {@link #executorService} is <code>null</code>.
     */
    public RuleStats call() throws Exception {

        if (executorService == null) {

            /*
             * See the base class.
             */
            
            throw new IllegalStateException();
            
        }
        
        final List<Callable<RuleStats>> tasks = newQueryTasks(step, joinNexus,
                buffer);

        assert tasks != null;
        assert !tasks.isEmpty();
        
        try {

            final RuleStats totals;

            if (!step.isRule() && ((IProgram)step).isParallel()) {

                totals = runParallel(executorService, step, tasks);

            } else {

                totals = runSequential(executorService, step, tasks);

            }

            /*
             * Nothing more will be written on the buffer so we close it.
             * The iterator will drain anything in the queue from the buffer
             * and then hasNext() will report false.
             */
            
            if(log.isDebugEnabled()) {
                
                log.debug("done - closing the blocking buffer");
                
            }
            
            buffer.close();
            
            if(log.isInfoEnabled()) {
                
                log.info(totals);
                
            }
            
            return totals;

        } catch (Throwable t) {
            
            log.error("Problem running query: "+t, t);

            /*
             * Note: This will close the buffer. It will also cause the
             * iterator to throw the [cause] from hasNext() (or next(),
             * which invokes hasNext()).
             */
            
            buffer.abort(t/*cause*/);
        
            throw new RuntimeException(t);
            
        }
        
    }

    /**
     * Builds a set of tasks for the program.
     * 
     * @param buffer
     * 
     * @return
     */
    protected List<Callable<RuleStats>> newQueryTasks(IStep step,
            IJoinNexus joinNexus, IBlockingBuffer<ISolution> buffer) {

        if (log.isDebugEnabled())
            log.debug("program=" + step.getName());

        final List<Callable<RuleStats>> tasks;

        if (step.isRule()) {

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) step;

            final Callable<RuleStats> task = getTaskFactory(false/* parallel */,
                    rule).newTask(rule, joinNexus, buffer);
            
            tasks.add(task);

        } else {

            final IProgram program = (IProgram)step;
            
            final boolean parallel = program.isParallel();

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<? extends IStep> itr = program.steps();

            while (itr.hasNext()) {

                // @todo handle sub-programs.
                final IRule rule = (IRule) itr.next();

                final Callable<RuleStats> task = getTaskFactory(parallel, rule)
                        .newTask(rule, joinNexus, buffer);

                tasks.add(task);

            }

        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+tasks.size()+" query tasks");
            
        }
        
        return tasks;

    }

}
