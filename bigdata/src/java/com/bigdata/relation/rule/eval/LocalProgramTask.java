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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.DelegateChunkedIterator;
import com.bigdata.relation.accesspath.FlushBufferTask;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IClosableIterator;
import com.bigdata.relation.rdf.SPORelationFactory;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.DataService.IDataServiceAwareProcedure;

/**
 * Task for executing a program when all of the indices for the relation are
 * co-located on the same {@link DataService}.
 * 
 * @todo Add an option to force sequential execution and flush the buffer after
 *       rule so that the mutation and solution counts are exact rule by rule?
 *       This will presumably be much slower, but it might be interesting for
 *       seeing how many solutions were being computed by each rule.
 * 
 * @todo Make sure that we can run the "fast" closure method - it uses some very
 *       special rules.
 * 
 * @todo Make sure that we can run the distinct term scan, the match rule, and
 *       other somewhat customized rules or pseudo-access paths.
 * 
 * @todo performance comparisons with the old rule execution logic. *
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
 * FIXME When using RMI the iterator returned by the JOIN for a QUERY needs be a
 * proxy for the remote iterator running on the data service that is actually
 * executing that proxy (this is only true for scale-out JOINs). When the proxy
 * iterator is closed by the client the close needs to make it back to the data
 * service where it must cancel the Future(s) writing on the buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalProgramTask implements IProgramTask, IDataServiceAwareProcedure,Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7047397038429305180L;

    protected static final Logger log = Logger.getLogger(LocalProgramTask.class);
    
    private ActionEnum action;
    
    private IProgram program;
    
    /**
     * Note: NOT serialized!
     */
    private transient IJoinNexus joinNexus;
    
    /**
     * Note: NOT serialized!
     */
    private transient ExecutorService service;
    
    /**
     * Note: NOT serialized!
     */
    private transient DataService dataService;

    /**
     * The default factory for rule evaluation.
     */
    private transient IRuleTaskFactory defaultTaskFactory = new IRuleTaskFactory() {

        public IRuleTask newTask(IRule rule,IJoinNexus joinNexus, IBuffer<ISolution> buffer) {
            
            return new LocalNestedSubqueryEvaluator(rule,joinNexus,buffer);
            
        }
        
    };
    
    public void setDataService(DataService dataService) {

        if (dataService == null)
            throw new IllegalArgumentException();
        
        log.info("Running on data service: dataService="+dataService);
        
        this.dataService = dataService;

    }

    /**
     * De-serialization ctor.
     */
    public LocalProgramTask() {

    }

    /**
     * Variant when the task will be submitted using
     * {@link IDataService#submit(Callable)} (efficient since all indices will
     * be local, but the indices must not be partitioned and must all exist on
     * the target {@link DataService}).
     * 
     * @param action
     * @param program
     * @param joinNexus
     */
    public LocalProgramTask(ActionEnum action, IProgram program,
            IJoinNexus joinNexus) {

        this(action, program, joinNexus, null);

    }

    /**
     * Variant when the task will be executed directly by the caller (not
     * efficient).
     * 
     * @param action
     * @param program
     * @param joinNexus
     * @param service
     *            The service that will be used to parallelize the task
     *            execution. If <code>null</code>, then the caller MUST be
     *            submit using {@link IDataService#submit(Callable)}.
     * 
     * @see IBigdataFederation#getThreadPool()
     */
    public LocalProgramTask(ActionEnum action, IProgram program,
            IJoinNexus joinNexus, ExecutorService service) {

        if (action == null)
            throw new IllegalArgumentException();

        if (program == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        this.action = action;
        
        this.program = program;

        this.joinNexus = joinNexus;
        
        /*
         * If null, then the caller MUST be submit using DataService#submit().
         */
        this.service = service; 
        
    }

    /**
     * Execute the program.
     * <p>
     * First, locate the distinct relations on which the rules will write. The
     * rules will all write on a buffer. The type of buffer is choosen based on
     * Query vs Insert or Delete (mutation). If we are reading from the rule,
     * then it writes on a {@link IBlockingBuffer} and we return the
     * {@link IChunkedOrderedIterator} that drains that {@link IBlockingBuffer}.
     * If we are writing on a relation (insert or delete) then we use an
     * {@link AbstractArrayBuffer} that flushes onto the appropriate method on
     * the {@link IMutableRelation}.
     * 
     * @throws Exception
     */
    public Object call() throws Exception {

        if(log.isDebugEnabled()) {

            log.debug("begin: program="+program.getName()+", action="+action);
            
        }

        if (dataService != null && service == null) {

            /*
             * This condition occurs when this Callable is sent to the
             * DataService using IDataService#submit(Callable). In order to gain
             * access to the named indices for the relation, we have to wrap up
             * this Callable as an AbstractTask that declares the appropriate
             * timestamp and resources and set [service] to an ExecutorService
             * running on the DataService itself. The AbstractTask will then be
             * submitted to the ConcurrencyManager for execution. It will
             * delegate back to #call(). Since [service] will be non-null, the
             * other code branch will be followed.
             * 
             * Note: The [dataService] reference is set by the DataService
             * itself when it executes this Callable and notices the
             * IDataServiceAwareProcedure interface.
             * 
             * Note: The [service] field is NOT serializable and MUST NOT be set
             * if this task is to execute on the DataService.
             */
            
            log.info("Will run on data service's concurrency manager.");
            
            return submitToConcurrencyManager().get();
            
        }
        
        final ExecutorService service = getService();

        try {

            if (action.isMutation()) {

                if (program.isClosure()) {

                    return fixPoint(program, joinNexus, service);

                }

                return executeMutation(program, service).mutationCount.get();

            } else {

                return executeQuery(program, service);

            }

        } finally {

            log.debug("bye");

        }

    }

    /**
     * Wrap up this {@link Callable} as an {@link AbstractTask} that is
     * submitted to the {@link ConcurrencyManager} so that it can executed with
     * all necessary concurrency controls.
     * 
     * @throws IllegalStateException
     *             if {@link #dataService} is <code>null</code>
     * @throws IllegalStateException
     *             if {@link #service} is <code>non-null</code>
     * 
     * @todo fixed point computations on the data service will need to be
     *       handled by sub-tasks so that we can update the commit point from
     *       which we are reading.
     *       <p>
     *       I think that we need to constraint programs to only fix point an
     *       entire program in order for this to work since the top-level task
     *       can then easily submit the necessary {@link AbstractTask}s.
     *       <p>
     *       However, the "fast closure" program does use fix point for some of
     *       its steps so program execution will have to be more general to
     *       handle that. (If there are sub-steps of the program that need to be
     *       brought to a fixed point then the program above that level needs to
     *       be a sequence of {@link AbstractTask}s submitted by a controller
     *       task).
     */
    protected Future<Object> submitToConcurrencyManager() {

        if (dataService == null)
            throw new IllegalStateException();
        
        if (service != null)
            throw new IllegalStateException();
        
        final IConcurrencyManager concurrencyManager = dataService.getConcurrencyManager();
        
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
        
        final ProgramUtility util = new ProgramUtility(joinNexus);
        
        final Map<IRelationName, IRelation> relations = util.getRelations(program);

        final Set<String> indexNames = util.getIndexNames(relations.values());

        final String[] resource = indexNames.toArray(new String[]{});

        if (log.isInfoEnabled()) {

            log.info("resource=" + Arrays.toString(resource));
            
        }

        /*
         * Setup the AbstractTask.
         */
        
        final ExecutorService service = dataService.getFederation().getThreadPool();
        
        final LocalProgramTask innerTask = new LocalProgramTask(action,
                program, joinNexus);

        /*
         * Set the service (breaks recursion). This is the thread pool that will
         * be used to execute the rules with parallelism once the innerTask
         * runs.
         */
        innerTask.service = service;

        /*
         * Set this since the data service won't see the [innerTask] but the
         * task still needs the [dataService] reference.
         */
        innerTask.setDataService(dataService);
        
        final AbstractTask task = new AbstractTask(concurrencyManager,
                joinNexus.getTimestamp(), resource) {

            @Override
            protected Object doTask() throws Exception {
                
                log.info("Execution the inner task");

                final IJoinNexus tmp = innerTask.joinNexus;

                /*
                 * FIXME hardwired to SPORelationFactory. This should use the
                 * relation factory for the source relation locator.
                 * 
                 * We lack any metadata to tell us how to interpret a relation's
                 * namespace as the Class of the relation implementation. We
                 * need that relation Class in order to know what indices to
                 * fetch. This issue is trivially resolved as long as the
                 * relations are homogenous, e.g., SPORelation. But it is NOT
                 * resolvable without additional information once those
                 * relations are heterogeneous, e.g., SPORelation plus
                 * RDFLexicon or SPORelation + SparseRowStore, etc. In fact, the
                 * SPORelation and the RDFLexicon have the _same_ [namespace].
                 */
                final IRelationLocator relationLocator = new DefaultRelationLocator(
                        service, getJournal(), new SPORelationFactory());
                
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

        log.info("Submitting task to the data service.");

        /*
         * Run on the concurrency manager.
         */
        final Future<Object> future = concurrencyManager.submit( task );
        
        return future;

    }
    
    /**
     * Return the {@link ExecutorService}.
     * 
     * @throws IllegalStateException
     *             if <i>service</i> is not set
     */
    protected ExecutorService getService() {
        
        if (service == null) {

            throw new IllegalStateException();
            
        }

        return service;

    }
    
    
    /**
     * Create the appropriate buffers to absorb writes by the rules in the
     * program that target an {@link IMutableRelation}.
     * 
     * @return the map from relation identifier to the corresponding buffer.
     * 
     * @throws IllegalStateException
     *             if the program is being executed as an
     *             {@link ActionEnum#Query}.
     * @throws RuntimeException
     *             If a rule requires mutation for a relation (it will write on
     *             the relation) and the corresponding entry in the map does not
     *             implement {@link IMutableRelation}.
     */
    protected Map<IRelationName, IBuffer<ISolution>> getMutationBuffers(
            Map<IRelationName, IRelation> relations) {

        if (action == ActionEnum.Query) {

            throw new IllegalStateException();
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("");
            
        }

        final Map<IRelationName, IBuffer<ISolution>> c = new HashMap<IRelationName, IBuffer<ISolution>>(
                relations.size());

        final Iterator<Map.Entry<IRelationName, IRelation>> itr = relations
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<IRelationName, IRelation> entry = itr.next();

            final IRelationName relationName = entry.getKey();

            final IRelation relation = entry.getValue();

            final IBuffer<ISolution> buffer;

            switch (action) {
            
            case Insert:
                
                buffer = joinNexus.newInsertBuffer((IMutableRelation)relation);
                
                break;
                
            case Delete:
                
                buffer = joinNexus.newDeleteBuffer((IMutableRelation)relation);
                
                break;
                
            default:
                
                throw new AssertionError("action=" + action);
            
            }

            c.put(relationName, buffer);
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+c.size()+" mutation buffers: action="+action);
            
        }

        return c;
        
    }
    
    /**
     * Execute the {@link IProgram} as a query.
     * 
     * @param service
     *            The service that will be used to run (and parallelize) the
     *            program.
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
    protected IChunkedOrderedIterator<ISolution> executeQuery(
            final IProgram program, final ExecutorService service)
            throws InterruptedException, ExecutionException {

        if (program == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();
        
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName());
        
        final IBlockingBuffer<ISolution> buffer = joinNexus.newQueryBuffer();

        final List<Callable<RuleStats>> tasks = newQueryTasks(program, joinNexus,
                buffer);

        assert tasks != null;
        assert !tasks.isEmpty();
        
        /*
         * Note: We do NOT get() this Future. This task will run asynchronously.
         * 
         * The Future is cancelled IF (hopefully WHEN) the iterator is closed.
         * 
         * If the task itself throws an error, then it will use
         * buffer#abort(cause) to notify the buffer of the cause (it will be
         * passed along to the iterator) and to close the buffer (the iterator
         * will notice that the buffer has been closed as well as that the cause
         * was set on the buffer).
         */
        
        final Future<RuleStats> future = service.submit(new QueryTask(service,
                tasks, buffer));
        
        /*
         * @todo if the #of results is small and they are available with little
         * latency then return the results inline using a fully buffered
         * iterator.
         */
        
        if (log.isDebugEnabled())
            log.debug("Returning iterator reading on async query task");
        
        return new DelegateChunkedIterator<ISolution>(buffer.iterator()) {
            
            /**
             * If the iterator is closed then cancel the Future that is running
             * the program.
             * 
             * @todo refactor this into a helper class?
             */
            public void close() {

                if(!future.isDone()) {

                    /*
                     * If the query is still running and we close the iterator
                     * then the query will block once the iterator fills up and
                     * it will fail to progress. To avoid this, and to have the
                     * query terminate eagerly if the client closes the
                     * iterator, we cancel the future if it is not yet done.
                     */
                    
                    if(log.isDebugEnabled()) {
                        
                        log.debug("will cancel future: "+future);
                        
                    }
                    
                    future.cancel(true/*mayInterruptIfRunning*/);
                    
                    if(log.isDebugEnabled()) {
                        
                        log.debug("did cancel future: "+future);
                        
                    }

                }

                // pass close() onto the delegate.
                super.close();
                
            }
            
        };

    }

    /**
     * Asynchronous tasks writes {@link ISolution}s on an
     * {@link IBlockingBuffer}. The client will be given an iterator that
     * drains from the buffer. When there are no more solutions, the buffer will
     * be {@link IBlockingBuffer#close()}ed and the iterator will report that
     * is has been exhausted once it finishes draining the buffer's queue.
     * <p>
     * Note: If the client closes the iterator, then the iterator will cause the
     * backing buffer to also be closed. This is handled by
     * {@link LocalProgramTask#executeQuery(IProgram, ExecutorService)} which
     * wraps the iterator before returning it to the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class QueryTask implements Callable<RuleStats> {
        
        private final ExecutorService service;
        private final List<Callable<RuleStats>>tasks;
        private final IBlockingBuffer<ISolution> buffer;

        public QueryTask(ExecutorService service,
                List<Callable<RuleStats>> tasks, IBlockingBuffer<ISolution> buffer) {

            if (service == null)
                throw new IllegalArgumentException();

            if (tasks == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();
            
            this.service = service;
            
            this.tasks = tasks;
            
            this.buffer = buffer;
            
        }
        
        public RuleStats call() throws Exception {

            try {

                final RuleStats totals;

                if (program.isParallel()) {

                    totals = runParallel(service, program, tasks);

                } else {

                    totals = runSequential(service, program, tasks);

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
        
    }
    
    protected RuleStats executeMutation(final IProgram program, final ExecutorService service)
            throws InterruptedException, ExecutionException {

        if (program == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        if (log.isDebugEnabled())
            log.debug("begin: program=" + program.getName());
        
        final ProgramUtility util = new ProgramUtility(joinNexus);
        
        final Map<IRelationName, IRelation> relations = util.getRelations(program);

        assert !relations.isEmpty();
        
        final Map<IRelationName, IBuffer<ISolution>> buffers = getMutationBuffers(relations);

        assert !buffers.isEmpty();
        
        final List<Callable<RuleStats>> tasks = newMutationTasks(program,
                joinNexus, buffers);

        assert !tasks.isEmpty();
        
        final RuleStats totals;
        
        if (program.isParallel()) {

            totals = runParallel(service, program, tasks);

        } else {

            totals = runSequential(service, program, tasks);

        }
        
        flushBuffers(totals, service, buffers);

        if (log.isDebugEnabled())
            log.debug("done: program=" + program.getName());

        return totals;
        
    }

    /**
     * Flush the buffer(s).
     * 
     * @throws InterruptedException
     * @throws ExecutionException 
     */
    protected void flushBuffers(RuleStats totals, ExecutorService service,
            Map<IRelationName, IBuffer<ISolution>> buffers)
            throws InterruptedException, ExecutionException {

        if (totals == null)
            throw new IllegalArgumentException();
        
        if (service == null)
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
            
            log.info("Flushing one buffer");
            
            final long mutationCount = buffers.values().iterator().next()
                    .flush();

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
            
            final List<Future<Long>> futures = service.invokeAll(tasks);
            
            for( Future<Long> f : futures ) {
                
                final long mutationCount = f.get(); 
                
                totals.mutationCount.addAndGet(mutationCount);
                
            }
            
        }

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
    protected List<Callable<RuleStats>> newQueryTasks(IProgram program,
            IJoinNexus joinNexus, IBlockingBuffer<ISolution> buffer) {

        if (log.isDebugEnabled())
            log.debug("program=" + program.getName());

        final List<Callable<RuleStats>> tasks;

        if (program.isRule()) {

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) program;

            // is there a task factory override?
            IRuleTaskFactory taskFactory = rule.getTaskFactory();
            
            if (taskFactory == null) {
             
                // no, use the default factory.
                taskFactory = defaultTaskFactory;
            
            }
            
            final Callable<RuleStats> task = taskFactory.newTask(rule, joinNexus, buffer);
            
            tasks.add(task);

        } else {

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<IProgram> itr = program.steps();

            while (itr.hasNext()) {

                // @todo handle sub-programs.
                final IRule rule = (IRule) itr.next();

                final IRuleTask task = new LocalNestedSubqueryEvaluator(
                        rule, joinNexus, buffer);

                tasks.add(task);

            }

        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+tasks.size()+" query tasks");
            
        }
        
        return tasks;

    }

    /**
     * Builds a set of tasks for the program.
     * 
     * @param buffer
     * 
     * @return
     */
    protected List<Callable<RuleStats>> newMutationTasks(IProgram program,
            IJoinNexus joinNexus, Map<IRelationName, IBuffer<ISolution>> buffers) {

        if (log.isDebugEnabled())
            log.debug("program=" + program.getName());

        final List<Callable<RuleStats>> tasks;

        if (program.isRule()) {

            tasks = new ArrayList<Callable<RuleStats>>(1);

            final IRule rule = (IRule) program;

            final IBuffer<ISolution> buffer = buffers.get(rule.getHead().getRelationName());
            
            assert buffer != null;
            
            // is there a task factory override?
            IRuleTaskFactory taskFactory = rule.getTaskFactory();
            
            if (taskFactory == null) {
             
                // no, use the default factory.
                taskFactory = defaultTaskFactory;
            
            }
            
            final Callable<RuleStats> task = taskFactory.newTask(rule, joinNexus, buffer);
   
            tasks.add(task);

        } else {

            tasks = new ArrayList<Callable<RuleStats>>(program.stepCount());

            final Iterator<IProgram> itr = program.steps();

            while (itr.hasNext()) {

                // @todo handle sub-programs.
                final IRule rule = (IRule) itr.next();

                final IRuleTask task = new LocalNestedSubqueryEvaluator(
                        rule, joinNexus, buffers.get(rule.getHead()
                                .getRelationName()));

                tasks.add(task);

            }

        }

        if (log.isDebugEnabled()) {

            log.debug("Created " + tasks.size() + " mutation tasks: action="
                    + action);

        }

        return tasks;

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
    protected RuleStats runParallel(ExecutorService service, IProgram program,
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
    protected RuleStats runSequential(ExecutorService service,
            IProgram program, List<Callable<RuleStats>> tasks)
            throws InterruptedException, ExecutionException {

        final int ntasks = tasks.size();
        
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+ntasks);

        final RuleStats totals = new RuleStats(program);
        
        final Iterator<Callable<RuleStats>> itr = tasks.iterator();

        int n = 0;
        
        while (itr.hasNext()) {

            final Callable<RuleStats> task = itr.next();

            // submit and wait for the future.
            final RuleStats tmp = service.submit(task).get();

            /*
             * FIXME a sequential step should always flush its buffers since
             * steps are run in sequence precisely because they have a
             * dependency!  That flush could be a parameter when the task
             * was created which would take it out of line and ensure that
             * we wait for the flush to complete.
             */
            
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
     * Computes the "fixed point" of a program.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * in turn (either sequentially or in parallel, depending on the program).
     * Solutions computed for each rule in each round written onto the relation
     * for the head of that rule. The process halts when no new solutions are
     * computed in a given round.
     * <p>
     * <h2>mutation counts</h2>
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
     * @param program
     *            The program to be executed.
     * @param service
     *            The service that will be used to execute the rules.
     * 
     * @todo Does it make sense for the sub-programs of a closure to themselves
     *       allow closure or should they be required to be a simple set of
     *       rules to be executed it parallel?
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected RuleStats fixPoint(IProgram program, IJoinNexus joinNexus,
            ExecutorService service) throws InterruptedException,
            ExecutionException {

        if(!action.isMutation()) {
            
            throw new UnsupportedOperationException(
                    "Closure only allowed for mutation.");
            
        }

        final long begin = System.currentTimeMillis();

        /*
         * @todo After each round we need to be reading from the post-update
         * state the relation(s) on which the rules are writing. (It is an error
         * if the rules are not writing on at least one relation.) This assumes
         * that all of the rules are writing on the relation specified for the
         * head of the first rule.
         * 
         * @todo While this will work auto-magically if the entailments are
         * written onto the using UNISOLATED while the rules use READ_COMMITTED
         * reads (and they will because the data service always using
         * READ_COMMITTED for an UNISOLATED task that is READ_ONLY), we will get
         * better performance if we use a fixed read-behind commit timestamp for
         * each round since the same index instances will be reused (the
         * read-committed view is always read from the disk!)
         */
        final RuleStats totals = new RuleStats(program);
        
        int round = 0;

        while (true) {

            final long mutationCount0 = totals.mutationCount.get();
            
            if (log.isDebugEnabled())
                log.debug("round=" + round);

            final RuleStats tmp = executeMutation(program, service);

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

            if (log.isInfoEnabled()) {

                log.info("round# " + round + ", mutationCount(delta="
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
