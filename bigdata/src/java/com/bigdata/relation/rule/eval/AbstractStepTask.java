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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.bigdata.journal.ITx;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
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
     * @param program
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
    
        if (log.isInfoEnabled())
            log.info("program=" + program.getName()+", #tasks="+tasks.size());
        
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
    
        if (log.isInfoEnabled())
            log.info("program=" + program.getName()+", #tasks="+tasks.size()+" - done");
    
        return totals;
    
    }

    /**
     * Run program steps in sequence.
     * 
     * @param program
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
        
        if (log.isInfoEnabled())
            log.info("program=" + program.getName()+", #tasks="+ntasks);
    
        if (indexManager == null)
            throw new IllegalStateException();
        
        final ExecutorService service = indexManager.getExecutorService();
        
        final RuleStats totals = new RuleStats(program);
        
        final Iterator<Callable<RuleStats>> itr = tasks.iterator();
    
        int n = 0;
        
        while (itr.hasNext()) {
    
            final Callable<RuleStats> task = itr.next();
    
            /*
             * Submit and wait for the future.
             * 
             * Note: tasks that are run in a sequential program are required to
             * flush the buffer so that all solutions are available for the next
             * step of the program. This is critical for programs that have
             * dependencies between their steps.
             * 
             * Note: This is handled by the task factory.
             */
            final RuleStats tmp = service.submit(task).get();
    
            totals.add(tmp);

            n++;

            if (log.isDebugEnabled()) {

                log.debug("program=" + program.getName() + ", finished " + n
                        + " of " + ntasks + " seqential tasks.");

            }

        }

        if (log.isInfoEnabled())
            log.info("program=" + program.getName() + ", #tasks=" + ntasks
                    + " - done");
    
        return totals;
    
    }

    /**
     * Run a single step (sequence of one).
     * <p>
     * Note: use {@link #runOne(IStep, Callable)} rather than either
     * {@link #runParallel(IStep, List)} or {@link #runSequential(IStep, List)}
     * when there is only one task to execute in order to avoid an unnecessary
     * layering of the {@link RuleStats} (this is due to a coupling between the
     * {@link RuleStats} reporting structure and the control structure for
     * executing the tasks).
     * 
     * @param program
     * @param tasks
     * 
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected RuleStats runOne(IStep program,
            Callable<RuleStats> task) throws InterruptedException,
            ExecutionException {
    
        if (log.isInfoEnabled())
            log.info("program=" + program.getName());
    
        if (indexManager == null)
            throw new IllegalStateException();
        
        final ExecutorService service = indexManager.getExecutorService();
        
        /*
         * Submit and wait for the future.
         * 
         * Note: tasks that are run in a sequential (or as a single task)
         * program are required to flush the buffer so that all solutions are
         * available for the next step of the program. This is critical for
         * programs that have dependencies between their steps.
         * 
         * Note: This is handled by the task factory.
         */
        final RuleStats stats = service.submit(task).get();

        if (log.isInfoEnabled())
            log.info("program=" + program.getName() + " - done");
    
        return stats;
    
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

            if (util.isClosureProgram(step)) {

                /*
                 * If this is not a rule, and it is not a closure of a flat rule
                 * set, and there is a buried closure operation inside of the
                 * program then we have a problem since the steps above the
                 * closure should have been flattened out by the caller and run
                 * directly such that we never reach this point with a closure
                 * operation.
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

//        final long timestamp;
//        {
//         
//            // flyweight instance.
//            IJoinNexus joinNexus = joinNexusFactory.newInstance(indexManager);
//            
//            // choose timestamp based on more recent view required.
//            timestamp = action.isMutation() ? joinNexus.getWriteTimestamp()
//                    : joinNexus.getReadTimestamp();
//            
//        }
//
//        if(log.isInfoEnabled()) {
//         
//            log.info("timestamp="+timestamp+", task="+this);
//            
//        }

        /*
         * The set of indices that we need to declare for the task.
         */
        final Set<String> indexNames = new HashSet<String>();
        
        if(action.isMutation()) {
         
            /*
             * Obtain the name of each index for which we want write access.
             * These are the indices for the relations named in the head of each
             * rule.
             * 
             * Note: We are not actually issuing any tasks here, just
             * materializing relation views so that we can obtain the names of
             * the indices required for those views in order to declare them to
             * the ConcurrencyManager. (In fact, we will defer the choice of the
             * views on which we write until execution time since we will run
             * the mutation operation inside of the ConcurrencyManager.) Hence
             * the timestamp associated with the request does not really matter.
             */
            final Map<String, IRelation> tmpRelations = getWriteRelations(
                indexManager, step, ITx.UNISOLATED);
            
            // Collect names of the required indices.
            final Set<String> writeIndexNames = getIndexNames(tmpRelations
                    .values());
            
            indexNames.addAll(writeIndexNames);

        }

        {

            /*
             * Obtain the name of each index for which we want read access.
             * These are the indices for the relation view(s) named in the tails
             * of each rule.
             * 
             * Note: We are not actually issuing any tasks here, just
             * materializing relation views so that we can obtain the names of
             * the indices required for those views. UNISOLATED is always safe
             * in this context, even for a relation on a temporary where data
             * has been written but no commits performed.
             */
            final Map<String, IRelation> tmpRelations = getReadRelations(
                    indexManager, step, ITx.UNISOLATED);
                
            // Collect names of the required indices.
            final Set<String> readIndexNames = getIndexNames(tmpRelations
                    .values());
                
            indexNames.addAll(readIndexNames);
            
        }
        
        final String[] resource;
        {
            
             // The set of indices that the task will declare.
            resource = indexNames.toArray(new String[] {});

            if (log.isInfoEnabled()) {

                log.info("resource=" + Arrays.toString(resource));

            }

        }

        /*
         * Choose the timestamp for the AbstractTask. The most interesting
         * choice is whether or not the task is UNISOLATED (an unisolated task
         * will obtain exclusive locks on the live indices declared by the
         * task).
         * 
         * A mutation task runs with the writeTimestamp.
         * 
         * A query task runs as READ_COMMITTED.
         * 
         * @todo handle transactions in this context.
         */
        final long timestamp;
        {
            
            if (action.isMutation()) {

                timestamp = joinNexusFactory.newInstance(indexManager)
                        .getWriteTimestamp();

            } else {

                timestamp = ITx.READ_COMMITTED;
                
            }

            if (log.isInfoEnabled()) {

                log.info("timestamp=" + timestamp + ", task=" + this);

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
                    log.info("Executing inner task: " + this);

                /*
                 * Override to use the IJournal exposed by the AbstractTask.
                 * This IJournal imposes the correct isolation control and
                 * allows access to the unisolated indices (if you have declared
                 * them and are running an UNISOLATED AbstractTask).
                 */
                innerTask.indexManager = getJournal();
                
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
     * The set of distinct relations identified by the head of each rule in the
     * program.
     */
    protected Set<String> getWriteRelationNames(IStep step) {

        Set<String> c = new HashSet<String>();
        
        getWriteRelationNames(step, c);

        if(log.isDebugEnabled()) {
            
            log.debug("Found "+c.size()+" relations, program="+step.getName());
            
        }

        return c;
        
    }
    
    private void getWriteRelationNames(IStep p, Set<String> c) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            c.add(r.getHead().getOnlyRelationName());

        } else {
            
            final Iterator<IStep> itr = ((IProgram)p).steps();

            while (itr.hasNext()) {

                getWriteRelationNames(itr.next(), c);

            }

        }

    }
    
    /**
     * Locate the distinct relation identifiers corresponding to the head of
     * each rule and resolve them to their relations.
     * 
     * @param timestamp
     *            The timestamp associated with the relation views on which the
     *            rule(s) will write.
     * 
     * @throws RuntimeException
     *             if any relation can not be resolved.
     */
    protected Map<String, IRelation> getWriteRelations(
            IIndexManager indexManager, IStep step, long timestamp) {

        if (step == null)
            throw new IllegalArgumentException();

        final Map<String, IRelation> c = new HashMap<String, IRelation>();

        getWriteRelations(indexManager, step, c, timestamp);

        if (log.isDebugEnabled()) {

            log.debug("Located " + c.size()
                    + " relations in the head(s), program=" + step.getName());

        }

        return c;

    }

    @SuppressWarnings("unchecked")
    private void getWriteRelations(IIndexManager indexManager, IStep p,
            Map<String, IRelation> c, long timestamp) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            final String relationIdentifier = r.getHead().getOnlyRelationName();

            if (!c.containsKey(relationIdentifier)) {

                final IRelation relation = (IRelation) indexManager
                        .getResourceLocator().locate(relationIdentifier,
                                timestamp);

                c.put(relationIdentifier, relation);

            }

        } else {
            
            final Iterator<IStep> itr = ((IProgram)p).steps();

            while (itr.hasNext()) {

                getWriteRelations(indexManager, itr.next(), c, timestamp);

            }

        }

    }
    
    /**
     * Locate the distinct relation identifiers corresponding to the tail(s) of
     * each rule and resolve them to their relations. Note that a tail predicate
     * can read on a fused view of more than one relation.
     * 
     * @throws RuntimeException
     *             if any relation can not be resolved.
     */
    protected Map<String, IRelation> getReadRelations(IIndexManager indexManager,
            IStep step, final long timestamp) {

        if (step == null)
            throw new IllegalArgumentException();

        final Map<String, IRelation> c = new HashMap<String, IRelation>();

        getReadRelations(indexManager, step, c, timestamp);

        if (log.isDebugEnabled()) {

            log.debug("Located " + c.size()
                    + " relations in the tail(s), program=" + step.getName());

        }

        return c;

    }

    @SuppressWarnings("unchecked")
    private void getReadRelations(IIndexManager indexManager, IStep p,
            Map<String, IRelation> c, long timestamp) {

        if (p.isRule()) {

            final IRule r = (IRule) p;

            final Iterator<IPredicate> itr = r.getTail();

            while (itr.hasNext()) {

                final IPredicate pred = itr.next();

                final int relationCount = pred.getRelationCount();

                for (int i = 0; i < relationCount; i++) {

                    final String relationName = pred.getRelationName(i);

                    if (!c.containsKey(relationName)) {

                        final IRelation relation = (IRelation) indexManager
                                .getResourceLocator().locate(relationName,
                                        timestamp);

                        c.put(relationName, relation);

                    }

                }
                
            }

        } else {
            
            final Iterator<IStep> itr = ((IProgram)p).steps();

            while (itr.hasNext()) {

                getReadRelations(indexManager, itr.next(), c, timestamp);

            }

        }

    }
    
    /**
     * Create the appropriate buffers to absorb writes by the rules in the
     * program that target an {@link IMutableRelation}.
     * 
     * @return the map from relation identifier to the corresponding buffer.
     * 
     * @throws IllegalStateException
     *             if the program is being executed as mutation.
     * @throws RuntimeException
     *             If a rule requires mutation for a relation (it will write on
     *             the relation) and the corresponding entry in the map does not
     *             implement {@link IMutableRelation}.
     */
    protected Map<String, IBuffer<ISolution>> getMutationBuffers(
            IJoinNexus joinNexus, Map<String, IRelation> relations) {

        if (!action.isMutation()) {

            throw new IllegalStateException();
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("");
            
        }

        final Map<String, IBuffer<ISolution>> c = new HashMap<String, IBuffer<ISolution>>(
                relations.size());

        final Iterator<Map.Entry<String, IRelation>> itr = relations
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, IRelation> entry = itr.next();

            final String relationIdentifier = entry.getKey();

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

            c.put(relationIdentifier, buffer);
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("Created "+c.size()+" mutation buffers: action="+action);
            
        }

        return c;
        
    }
    
    /**
     * Returns the names of the indices maintained by the relations.
     * 
     * @param c
     *            A collection of {@link IRelation}s.
     * 
     * @return The names of the indices maintained by those relations.
     */
    @SuppressWarnings("unchecked")
    protected Set<String> getIndexNames(Collection<IRelation> c) {

        if (c == null)
            throw new IllegalArgumentException();

        if (c.isEmpty())
            return Collections.EMPTY_SET;

        final Set<String> set = new HashSet<String>();
        
        final Iterator<IRelation> itr = c.iterator();
        
        while(itr.hasNext()) {
            
            final IRelation relation = itr.next();
            
            set.addAll(relation.getIndexNames());
            
        }

        return set;
        
    }

}
