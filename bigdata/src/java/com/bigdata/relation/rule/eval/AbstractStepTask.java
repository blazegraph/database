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
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationIdentifier;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.DataService.IDataServiceAwareProcedure;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractStepTask implements IStepTask, IDataServiceAwareProcedure, Cloneable {

    protected static final transient Logger log = Logger.getLogger(AbstractStepTask.class);
    
    protected final ActionEnum action;
    protected final IJoinNexusFactory joinNexusFactory;
    protected /*final*/ IIndexManager indexManager;
    protected final IStep step;
//    protected/* final */ExecutorService executorService;
    protected DataService dataService;
    
    public void setDataService(DataService dataService) {

        if (dataService == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("Running on data service: dataService="+dataService);
        
        this.dataService = dataService;

//        this.executorService = dataService.getFederation().getExecutorService();
        
    }

    /**
     * Base class handles submit either to the caller's {@link ExecutorService}
     * or to the {@link ConcurrencyManager} IFF the task was submitted to a
     * {@link DataService}.
     * <p>
     * Note: The {@link DataService} will notice the
     * {@link IDataServiceAwareProcedure} interface and set a reference to
     * itself using {@link #setDataService(DataService)}. {@link #submit()}
     * notices this case and causes <i>this</i> task to be {@link #clone()}ed,
     * the {@link ExecutorService} set on the clone, and the clone is then
     * submitted to the {@link ConcurrencyManager} for the {@link DataService}.
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
    protected AbstractStepTask(ActionEnum action,
            IJoinNexusFactory joinNexusFactory, IStep step,
            IIndexManager indexManager, DataService dataService) {

        if (action == null)
            throw new IllegalArgumentException();

        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        
        if (step == null)
            throw new IllegalArgumentException();

        this.action = action;
        
        this.joinNexusFactory = joinNexusFactory;
        
        this.step = step;
        
        this.indexManager = indexManager; // @todo MAY be null?
        
        this.dataService = dataService;
        
    }

    public String toString() {
        
        return "{" + getClass().getSimpleName() + ", action=" + action
                + ", step=" + step.getName() + ", joinNexusFactory="
                + joinNexusFactory + ", indexManager=" + indexManager
                + ", dataService=" + dataService + "}"; 
        
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
    protected RuleStats runParallel(IStep program,
            List<Callable<RuleStats>> tasks) throws InterruptedException,
            ExecutionException {
    
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+tasks.size());
        
        if (indexManager == null)
            throw new IllegalStateException();
        
        final RuleStats totals = new RuleStats(program);
        
        final ExecutorService service = indexManager.getExecutorService();
        
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
    protected RuleStats runSequential(IStep program,
            List<Callable<RuleStats>> tasks) throws InterruptedException,
            ExecutionException {
    
        final int ntasks = tasks.size();
        
        if (log.isDebugEnabled())
            log.debug("program=" + program.getName()+", #tasks="+ntasks);
    
        if (indexManager == null)
            throw new IllegalStateException();
        
        final ExecutorService service = indexManager.getExecutorService();
        
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
     * If we are executing on a {@link DataService} then {@link #dataService}
     * will have been set automatically and the task will be submitted to the
     * {@link ConcurrencyManager} for that {@link DataService}.
     * <p>
     * This condition occurs when this {@link Callable} is sent to the
     * {@link DataService} using {@link DataService#submit(Callable)}. In order
     * to gain access to the named indices for the relation, we have to wrap up
     * this {@link Callable} as an {@link AbstractTask} that declares the
     * appropriate timestamp and resources. The {@link AbstractTask} is then
     * submitted to the {@link ConcurrencyManager} for execution. Once the
     * {@link AbstractTask} is actually running, the inner task
     * <em>overrides</em> the {@link #indexManager} to be
     * {@link AbstractTask#getJournal()}. This provides access to the indices,
     * relations, etc. appropriate for the isolation level granted to the task
     * by the {@link ConcurrencyManager} - without this step the
     * {@link AbstractTask} will wind up using an {@link IClientIndex} view and
     * lose the benefits of access to unisolated indices.
     */
    public Future<RuleStats> submit() {

        if (dataService == null) {

            if(log.isInfoEnabled()) {

                log.info("running w/o concurrency control: " + this);
                
            }
            
            return indexManager.getExecutorService().submit(this);

        }

        return submitToConcurrencyManager();
        
    }
    
    private Future<RuleStats> submitToConcurrencyManager() {
        
        if (dataService == null)
            throw new IllegalStateException();

        final ProgramUtility util = new ProgramUtility();

        {
            
            final boolean isClosure = util.isClosureProgram(step);

            if (isClosure) {

                /*
                 * Note: The steps above the closure should have been flattened
                 * out by the caller and run directly so that we never reach
                 * this point with a closure operation.
                 */

                throw new UnsupportedOperationException();

            }

        }
        
        if(log.isInfoEnabled()) {

            log.info("running w/ concurrency control: " + this);
            
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
         * 3. Declare the indices since the task will need an exclusive lock on
         * them (mutation) or at least the ability to read from those indices
         * (query).
         * 
         * Note: if an index is not found on the live journal then it will be
         * resolved against the federation (if running in a federation). This
         * means that the task will run with the live index objects when they
         * are local and with IClientIndex objects when the index is remote.
         * 
         * Note; In general, mixtures of live and remote index objects do not
         * occur since indices are either partitioned (a federation) or
         * monolithic (a Journal).
         * 
         * Note: You CAN place indices onto specific data services running on a
         * set of machines and set [enableOverflow := false] such that the
         * indices never become partitioned. In that case you can have optimized
         * joins for some relations on one data service and for other relations
         * on another data service. E.g., locating the statement indices for the
         * triple store on one data service, the lexicon on another, and a repo
         * on a third. This will give very good performance for Query and Truth
         * Maintenance since the JOINs will be mostly executing against live
         * index objects.
         */

        final long timestamp;
        {
         
            // flyweight instance.
            IJoinNexus joinNexus = joinNexusFactory.newInstance(indexManager);
            
            // choose timestamp based on more recent view required.
            timestamp = action.isMutation() ? joinNexus.getWriteTimestamp()
                    : joinNexus.getReadTimestamp();
            
        }

        if(log.isInfoEnabled()) {
         
            log.info("timestamp="+timestamp+", task="+this);
            
        }

        // Declare all index names to which we want access.
        final String[] resource;
        {
        
            // Note: These relations are ONLY used to get the index names.
            final Map<IRelationIdentifier, IRelation> tmpRelations = util
                    .getRelations(indexManager, step, timestamp);

            // Collect names of the required indices.
            final Set<String> indexNames = util.getIndexNames(tmpRelations
                    .values());

            // The set of indices that the task will declare.
            resource = indexNames.toArray(new String[] {});

            if (log.isInfoEnabled()) {

                log.info("resource=" + Arrays.toString(resource));

            }

        }

        /*
         * Create the inner task. A clone is used to prevent possible side
         * effects on the original task.
         * 
         * Note: The [timestamp] was choosen above. The writeTimestamp iff this
         * is a mutation operation and the [readTimestamp] otherwise.
         */
        final AbstractStepTask innerTask = this.clone();

        final IConcurrencyManager concurrencyManager = dataService
                .getConcurrencyManager();

        final AbstractTask task = new AbstractTask(concurrencyManager,
                timestamp, resource) {

            @Override
            protected Object doTask() throws Exception {

                if (log.isInfoEnabled())
                    log.info("Executing inner task: " + this
//                            + ", indexManager=" + getJournal()
                            );

                /*
                 * Override to use the IJournal exposed by the AbstractTask.
                 * This IJournal imposes the correct isolation control and
                 * allows access to the unisolated indices (if you have declared
                 * them and are running an UNISOLATED AbstractTask).
                 */
                innerTask.indexManager = getJournal();
                
////                final IJoinNexus tmp = innerTask.joinNexus;
//
////                final IResourceLocator resourceLocator = new DefaultResourceLocator(
////                        executorService, //
////                        getJournal(),// 
////                        getJournal().getResourceLocator()//
////                        );
////
//                innerTask.joinNexus = new DelegateJoinNexus(tmp) {
//
//                    /**
//                     * Overridden to resolve the indices using the
//                     * {@link IJournal} exposed to the task by the
//                     * {@link AbstractTask}.
//                     * <p>
//                     * Note: Without this, the joinNexus in effect will use the
//                     * locator for the caller, which was probably resolving
//                     * using the {@link IBigdataFederation}'s locator.
//                     */
//                    public IResourceLocator getRelationLocator() {
//
////                        return resourceLocator;
//
////                        /* Note: This issue appears to be resolved.
////                         *
////                         * FIXME I am not clear yet why this construction works
////                         * but the simpler one below does not. Track this down.
////                         * 
////                         * One question is whether both locators really need to
////                         * specify a delegate. I would think that it should be
////                         * one or the other and that if both do it that we will
////                         * search one delegate twice, but clearly the delegates
////                         * are not the same or these would be interchangable
////                         * constructs. it would be much nicer if we don't have
////                         * to override this at all since that would make running
////                         * inside of the concurrency controls that much more
////                         * transparent.
////                         */
//                        
//                      return getJournal().getResourceLocator();
//                      
//                    }
//
//                };

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


}
