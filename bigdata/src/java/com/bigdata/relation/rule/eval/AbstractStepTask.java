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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.relation.DefaultRelationLocator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.DataService.IDataServiceAwareProcedure;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractStepTask implements IStepTask, IDataServiceAwareProcedure, Cloneable {

    protected static final transient Logger log = Logger.getLogger(AbstractStepTask.class);
    
    protected final ActionEnum action;
    protected /*final*/ IJoinNexus joinNexus;
    protected final IStep step;
    protected final IRuleTaskFactory defaultTaskFactory;
//    protected final List<Callable<RuleStats>> tasks;
    protected/* final */ExecutorService executorService;
    protected DataService dataService;
    
    public void setDataService(DataService dataService) {

        if (dataService == null)
            throw new IllegalArgumentException();
        
        log.info("Running on data service: dataService="+dataService);
        
        this.dataService = dataService;

    }

    /**
     * Base class handles submit either to the caller's {@link ExecutorService}
     * or to the {@link ConcurrencyManager} IFF the task was submitted to a
     * {@link DataService}.
     * <p>
     * Note: The {@link DataService} will notice the
     * {@link IDataServiceAwareProcedure} interface and set a reference to
     * itself using {@link #setDataService(DataService)}. {@link #submit()}
     * notices this case and causes <i>this</i> task to be {@link #clone()},
     * the {@link ExecutorService} set, and it is then submitted to the
     * {@link ConcurrencyManager} for the {@link DataService}.
     * 
     * @param step
     *            The rule or program.
     * @param joinNexus
     *            Various goodies.
     * @param defaultTaskFactory
     *            For the tasks to be executed.
     * @param executorService
     *            MAY be null. When <code>null</code> the task MUST be
     *            executing on a {@link DataService}.
     * @param dataService
     *            non-<code>null</code> iff the caller is already running on
     *            a {@link DataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>action</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>joinNexus</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>step</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>tasks</i> is <code>null</code>.
     */
    protected AbstractStepTask(ActionEnum action, IJoinNexus joinNexus,
            IStep step, 
            IRuleTaskFactory defaultTaskFactory,
            //List<Callable<RuleStats>> tasks,
            ExecutorService executorService,DataService dataService) {

        if (action == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();
        
        if (step == null)
            throw new IllegalArgumentException();

        if (defaultTaskFactory == null)
            throw new IllegalArgumentException();
        
//        if (tasks == null)
//            throw new IllegalArgumentException();

        this.action = action;
        
        this.joinNexus = joinNexus;
        
        this.step = step;
        
        this.defaultTaskFactory = defaultTaskFactory;
        
//        this.tasks = tasks;
        
        this.executorService = executorService;
        
        this.dataService = dataService;
        
    }

    public String toString() {
        
        return "{" + getClass().getSimpleName() + ", action=" + action
                + ", step=" + step.getName() + ", executorService="
                + executorService + ", dataService=" + dataService + "}"; 
        
    }
    
    /**
     * Run program steps in parallel.
     * 
     * @param service
     * @param tasks
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo adapt the {@link ClientIndexView} code so that we notice all
     *       errors, log them all, and report them all in a single thrown
     *       exception. note that we may be running asynchronously inside of a
     *       task after the caller has an iterator that is draining the buffer.
     *       when an error occurs in that context the buffer should be flagged
     *       to indicate an exception and closed and the iterator should report
     *       the exception to the client.
     *       <p>
     *       Do the same thing for running a program as a sequence.
     */
    protected RuleStats runParallel(ExecutorService service, IStep program,
            List<Callable<RuleStats>> tasks) throws InterruptedException,
            ExecutionException {
    
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+tasks.size());
        
        final RuleStats totals = new RuleStats(program);
        
        // submit tasks and await their completion.
        final List<Future<RuleStats>> futures = service.invokeAll(tasks);
    
        // verify no problems with tasks.
        for (Future<RuleStats> f : futures) {
    
            final RuleStats tmp = f.get();
            
            totals.add(tmp);
    
        }
    
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+tasks.size()+" - done");
    
        return totals;
    
    }

    /**
     * Run program steps in sequence.
     * 
     * @param service
     * 
     * @param tasks
     * 
     * @return 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected RuleStats runSequential(ExecutorService service, IStep program, List<Callable<RuleStats>> tasks)
            throws InterruptedException, ExecutionException {
    
        final int ntasks = tasks.size();
        
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+ntasks);
    
        final RuleStats totals = new RuleStats(program);
        
        final Iterator<Callable<RuleStats>> itr = tasks.iterator();
    
        int n = 0;
        
        while (itr.hasNext()) {
    
            final Callable<RuleStats> task = itr.next();
    
            /* Submit and wait for the future.
             * 
             * Note: tasks that are run in a sequential program are required
             * to flush the buffer so that all solutions are available for the
             * next step of the program.  This is critical for programs that
             * have dependencies between their steps.
             */
            final RuleStats tmp = service.submit(task).get();
    
            totals.add( tmp );
    
            n++;
            
            if(log.isDebugEnabled()) {
                
                log.debug("program="+program.getName()+", finished "+n+" of "+ntasks+" seqential tasks.");
                
            }
            
        }
    
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+ntasks+" - done");
    
        return totals;
    
    }

    /**
     * Run <i>this</i> task.
     * <p>
     * The task will be submitted to the {@link #executorService} iff it is
     * defined and otherwise the {@link #dataService} must be defined and a copy
     * of this task will be submitted to the {@link ConcurrencyManager} for that
     * {@link DataService}.
     * <p>
     * This condition occurs when this Callable is sent to the DataService using
     * IDataService#submit(Callable). In order to gain access to the named
     * indices for the relation, we have to wrap up this Callable as an
     * AbstractTask that declares the appropriate timestamp and resources and
     * set [service] to an ExecutorService running on the DataService itself.
     * The AbstractTask will then be submitted to the ConcurrencyManager for
     * execution. It will delegate back to #call(). Since [service] will be
     * non-null, the other code branch will be followed.
     * 
     * Note: The {@link #dataService} reference is set by the
     * {@link DataService} itself when it executes this {@link Callable} and
     * notices the {@link IDataServiceAwareProcedure} interface. It is then
     * preserved by {@link #clone()}.
     * 
     * Note: The {@link #executorService} field is NOT serializable and MUST NOT
     * be set if this task is to execute on the DataService.
     * 
     * @throws IllegalStateException
     *             if {@link #dataService} is <code>null</code>
     * @throws IllegalStateException
     *             if {@link #executorService} is <code>non-null</code>
     */
    public Future<RuleStats> submit() {

        if (executorService != null) {

            if(log.isInfoEnabled()) {

                log.info("running on executorService: " + this);
                
            }
            
            return executorService.submit(this);

        }

        if (dataService == null)
            throw new IllegalStateException();

        final IConcurrencyManager concurrencyManager = dataService
                .getConcurrencyManager();

        final ProgramUtility util = new ProgramUtility(joinNexus);

        final boolean isClosure = util.isClosureProgram(step);

        if (isClosure) {

            /*
             * Note: The steps above the closure should have been flattened out
             * by the caller and run directly so that we never reach this point
             * with a closure operation.
             */

            throw new UnsupportedOperationException();

        }

        /*
         * The index names must be gathered from each relation on which the task
         * will write so that they can be declared.
         * 
         * Note: We can't just pick and choose using the access paths since we
         * do not know how the propagation of bindings will effect access path
         * selection so we need a lock on all of the indices before the task can
         * run (at least, before it can run if it is a writer - no locks are
         * required for query).
         * 
         * 1. Find the distinct relations that are used by the rules.
         * 
         * 2. Collect the names of the indices maintained by those relations.
         * 
         * 3. @todo verify that those indices are all local to this data
         * service.
         * 
         * 4. Declare the indices since the task will need an exclusive lock on
         * them (mutation) or at least the ability to read from those indices
         * (query).
         */

        // choose timestamp based on more recent view required.
        final long timestamp = action.isMutation() ? joinNexus
                .getWriteTimestamp() : joinNexus.getReadTimestamp();

        if(log.isInfoEnabled()) {
         
            log.info("timestamp="+timestamp+", task="+this);
            
        }
                
        // Note: These relations are ONLY used to get the index names.
        final Map<IRelationName, IRelation> tmpRelations = util.getRelations(step,
                timestamp);

        // Collect names of the required indices.
        final Set<String> indexNames = util.getIndexNames(tmpRelations.values());

        // Declare all index names.
        final String[] resource = indexNames.toArray(new String[] {});

        if (log.isInfoEnabled()) {

            log.info("resource=" + Arrays.toString(resource));

        }

        /*
         * Setup the AbstractTask.
         */

        final ExecutorService executorService = dataService.getFederation()
                .getThreadPool();

        /*
         * Create the inner task.
         * 
         * Note: This sets the service which breaks the recursion. The [service]
         * is the thread pool that will be used to execute the rules with
         * parallelism once the innerTask runs.
         * 
         * Note: This also sets the data service since the DataService won't see
         * the [innerTask] but the innerTask still needs the [dataService]
         * reference.
         * 
         * Note: The [timestamp] was choosen above. The writeTimestamp iff this
         * is a mutation operation and the [readTimestamp] otherwise.
         */
        final AbstractStepTask innerTask = this.clone();

        innerTask.executorService = executorService;

        final AbstractTask task = new AbstractTask(concurrencyManager,
                timestamp, resource) {

            @Override
            protected Object doTask() throws Exception {

                log.info("Execution the inner task: "+this);

                final IJoinNexus tmp = innerTask.joinNexus;

                /*
                 * FIXME This will loose any local resources, e.g., on a
                 * TemporaryStore. Those resources need to be explicitly
                 * declared. They are available for LDS (only) when running in
                 * the DataService. Resolve this with
                 * IJoinNexus#addLocalResource() and
                 * IJoinNexus#getLocalResources(). Those need to be searched
                 * before the DefaultRelationLocator. When we override that
                 * locator here to use the isolation level of the task, we again
                 * need to setup those resources so that they are searched
                 * before the DefaultRelationLocator.
                 */
                final IRelationLocator relationLocator = new DefaultRelationLocator(
                        executorService, getJournal());

                innerTask.joinNexus = new DelegateJoinNexus(tmp) {

                    /**
                     * Overridden to resolve the indices using the AbstractTask.
                     */
                    public IRelationLocator getRelationLocator() {

                        return relationLocator;

                    }

                };

                return innerTask.call();

            }

        };

        if(log.isInfoEnabled()) {

            log.info("running on concurrencyManager: " + this);
            
        }
        
        /*
         * Run on the concurrency manager.
         */
        final Future<RuleStats> future = (Future<RuleStats>) concurrencyManager
                .submit(task);

        return future;

    }

    /**
     * Strengthens the return type and masquerades the
     * {@link CloneNotSupportedException}.
     */
    public AbstractStepTask clone() {

        try {

            return (AbstractStepTask) super.clone();

        } catch (CloneNotSupportedException ex) {

            throw new RuntimeException(ex);

        }

    }

    
    /**
     * Return the effective {@link IRuleTaskFactory} for the rule. When the rule
     * is a step of a sequential program, then the returned {@link IStepTask}
     * will automatically flush the buffer after the rule executes.
     * 
     * @param parallel
     *            <code>true</code> unless the rule is a step is a sequential
     *            {@link IProgram}. Note that a sequential step MUST flush its
     *            buffer since steps are run in sequence precisely because they
     *            have a dependency!
     * @param rule
     *            A rule that is a step in some program. If the program is just
     *            a rule then the value of <i>parallel</i> does not matter. The
     *            buffer will is cleared when it flushed so a re-flushed is
     *            always a NOP.
     * 
     * @return The {@link IStepTask} to execute for that rule.
     * 
     * @see RunRuleAndFlushBufferTaskFactory
     * @see RunRuleAndFlushBufferTask
     */
    protected IRuleTaskFactory getTaskFactory(boolean parallel, IRule rule) {
        
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

}
