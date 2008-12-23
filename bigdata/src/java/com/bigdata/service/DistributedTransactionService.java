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
 * Created on Dec 18, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.RunState;
import com.bigdata.resources.StoreManager;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Implementation for an {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class DistributedTransactionService extends
        AbstractTransactionService {

    /**
     * Options understood by this service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AbstractTransactionService.Options {

    }

    /**
     * A map of the distributed transactions that are currently committing.
     * 
     * @todo config for initial capacity and concurrency?
     */
    private final ConcurrentHashMap<Long/* tx */, TxState/* state */> commitList = new ConcurrentHashMap<Long, TxState>();

    /**
     * The {@link LockManager} used to impose a partial ordering on tx commits.
     */
    private final LockManager<String> lockManager = new LockManager<String>(
            0/* maxConcurrencyIsIgnored */, true/* predeclareLocks */);

    /**
     * @param properties
     */
    public DistributedTransactionService(final Properties properties) {

        super(properties);

        final AbstractFederation fed = (AbstractFederation) getFederation();

        // @todo config options
        fed.addScheduledTask(new NotifyReleaseTimeTask(), 60/* initialDelay */,
                60/* delay */, TimeUnit.SECONDS);
        
    }

    @Override
    protected void abortImpl(final TxState state) {

        assert state.lock.isHeldByCurrentThread();

        assert state.isActive();

        if(state.isReadOnly()) {
    
            state.setRunState(RunState.Aborted);
            
            return;
            
        }

        final UUID[] uuids = state.getDataServiceUUIDs();

        final IDataService[] services = getDataServices(uuids);
        
        // @todo could be executed in parallel.
        for (IDataService dataService : services) {

            try {

                dataService.abort(state.tx);

            } catch (Throwable t) {

                /*
                 * Collect all causes and always log an error if any data
                 * service abort fails.
                 * 
                 * Note: If an exception is thrown here the transaction will be
                 * aborted regardless. Howwever, the data service which threw
                 * the exception may still have local state on hand for the tx.
                 */
                
                log.error(t, t);

            }

        }
        
    }

    /**
     * Transaction commits for a distributed database MUST be executed in a
     * partial order so that they do not deadlock when acquiring the necessary
     * locks on the named indices on the local data services. Commits could be
     * serialized, but that restricts the possible throughput. The maximum
     * possible concurrency is achieved by ordering the commits using the set of
     * named index (partitions) on which the transaction has written. A partial
     * ordering could also be established based on the {@link IDataService}s,
     * or the scale-out index names, but both of those orderings would limit the
     * possible concurrency.
     * <p>
     * The partial ordering is imposed on commit requests using a
     * {@link LockManager}. A commit must obtain a lock on all of the necessary
     * resources before proceeding. If there is an existing commit using some of
     * those resources, then any concurrent commit requiring any of those
     * resources will block. The {@link LockManager} is configured to require
     * that commits pre-declare their locks. Deadlocks are NOT possible when the
     * locks are pre-declared.
     * 
     * @todo Single phase commits currently contend for the resource locks.
     *       However, they could also executed without acquiring those locks
     *       since they will be serialized locally by the {@link IDataService}
     *       on which they are executing.
     */
    @Override
    protected long commitImpl(final TxState state) throws Exception {
        
        if (state.isReadOnly() || state.getDataServiceCount() == 0) {
            
            /*
             * Note: We do not maintain any transaction state on the client for
             * read-only transactionss.
             * 
             * Note: If the transaction was never started on any data service so
             * we do not need to notify any data service. In effect, the tx was
             * started but never used.
             */

            state.setRunState(RunState.Committed);
            
            return 0L;
            
        }
     
        // the set of resources across all data services on which the tx wrote.
        final String resource[] = state.getResources();
        
        // declare resource(s) to lock (exclusive locks are used).
        lockManager.addResource(resource);

        // delegate will handle lock acquisition and invoke doTask().
        final LockManagerTask<String, Long> delegate = new LockManagerTask<String, Long>(
                lockManager, resource, new TxCommitTask(state));

        /*
         * This queues the commit request until it holds the necessary locks and
         * then commits the tx.
         */

        return delegate.call();
        
    }

    /**
     * Task runs the commit for the transaction once the necessary resource
     * locks have been required.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class TxCommitTask implements Callable<Long> {

        private final TxState state;

        public TxCommitTask(final TxState state) {

            if (state == null)
                throw new IllegalArgumentException();
            
            if(!state.lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            
            this.state = state;

        }

        /**
         * This method will be invoked by the {@link LockManagerTask} once it
         * holds all of the necessary resource locks. This is how we impose a
         * partial order. Deadlocks can not arise because we predeclare the
         * locks required for the commit and {@link LockManager} can guarentee
         * no deadlocks in that case by sorting the requested resources and
         * acquiring the locks in the sorted order.
         */
        public Long call() throws Exception {

            if (state.isDistributedTx()) {

                return distributedCommit(state);

            } else {

                return singlePhaseCommit(state);

            }

        }

    }
    
    /**
     * Prepare and commit a read-write transactions that has written on a single
     * data service.
     */
    protected long singlePhaseCommit(final TxState state) throws Exception {

        assert state.lock.isHeldByCurrentThread();

        final UUID[] uuids = state.getDataServiceUUIDs();

        if (uuids.length != 1)
            throw new AssertionError();

        final UUID serviceUUID = uuids[0];

        final IDataService dataService = getFederation().getDataService(
                serviceUUID);

        try {

            final long commitTime = dataService.singlePhaseCommit(state.tx);

            state.setRunState(RunState.Committed);

            return commitTime;

        } catch (Throwable t) {

            state.setRunState(RunState.Aborted);

            throw new RuntimeException(t);

        }

    }
    
    /**
     * Resolve UUIDs to services (arrays are correlated).
     * 
     * @param uuids
     *            The (meta)data service UUIDs.
     * 
     * @return The (meta)data service proxies.
     */
    protected IDataService[] getDataServices(final UUID[] uuids) {
        
        final IDataService[] services = new IDataService[uuids.length];

        final IBigdataFederation fed = getFederation();
        
        int i = 0;
        
        // UUID of the metadata service (if forced to discover it).
        UUID mdsUUID = null;

        for (UUID uuid : uuids) {

            IDataService service = fed.getDataService(uuid);

            if (service == null) {

                if (mdsUUID == null) {
                
                    try {
                    
                        mdsUUID = fed.getMetadataService().getServiceUUID();
                        
                    } catch (IOException ex) {
                        
                        throw new RuntimeException(ex);
                    
                    }
                    
                }
                
                if (uuid == mdsUUID) {

                    /*
                     * @todo getDataServices(int maxCount) DOES NOT return MDS
                     * UUIDs because we don't want people storing application
                     * data there, but getDataService(UUID) should probably work
                     * for the MDS UUID also since once you have the UUID you
                     * want the service.
                     */

                    service = fed.getMetadataService();
                }
                
            }

            if (service == null) {

                throw new RuntimeException("Could not discover service: uuid="
                        + uuid);

            }

            services[i++] = service;

        }
        
        return services;

    }

    /**
     * Prepare and commit a read-write transaction that has written on more than
     * one data service.
     * <p>
     * Note: read-write transactions that have written on multiple journals must
     * use a distributed (2-/3-phase) commit protocol. As part of the commit
     * protocol, we obtain an exclusive write lock on each journal on which the
     * transaction has written. This is necessary in order for the transaction
     * as a whole to be assigned a single commit time. Latency is critical in
     * this commit protocol since the journals participating in the commit will
     * be unable to perform any unisolated operations until the transaction
     * either commits or aborts.
     * <p>
     * Note: There is an assumption that the revisionTime does not need to be
     * the commitTime. This allows us to get all the heavy work done before we
     * reach the "prepared" barrier, which means that the commit phase should be
     * very fast. The assumption is that validation of different transactions
     * writing on the same unisolated indices is in fact serialized. The
     * transaction services MUST enforce that assumption by serializing
     * distributed commits (at least those which touch the same index partitions
     * (necessary constraint), the same indices (sufficient constraint) or the
     * same {@link IDataService}s (sufficient constraint)). If it did not
     * serialize distributed commits then <strong>deadlocks</strong> could
     * arise where two distributed commits were each seeking the exclusive write
     * lock on resources, one of which was already held by the other commit.
     * 
     * @throws Exception
     *             if anything goes wrong.
     * 
     * @return The commit time for the transaction.
     */
    protected long distributedCommit(final TxState state) throws Exception {

        if(!state.lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        // The UUIDs of the participating (meta)dataServices.
        final UUID[] uuids = state.getDataServiceUUIDs();

        // The corresponding data services.
        final IDataService[] services = getDataServices(uuids);
        
        // choose the revision timestamp.
        final long revisionTime = nextTimestamp();

        commitList.put(state.tx, state);

        try {
        
            /*
             * Submit a task that will run issue the prepare(tx,rev) messages to
             * each participating data service and await its future.
             */
            
            final long commitTime = getFederation()
                    .getExecutorService()
                    .submit(
                            new RunCommittersTask(services, state, revisionTime))
                    .get();

            return commitTime;
            
        } finally {

            commitList.remove(state.tx);
            
        }
        
    }

    /**
     * Task handles the distributed read-write transaction commit protocol. 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class RunCommittersTask implements Callable<Long> {
        
        final IDataService[] services;

        final TxState state;

        final long revisionTime;

        /**
         * The #of participating {@link IDataService}s.
         */
        final int nservices;

        public RunCommittersTask(final IDataService[] services,
                final TxState state, final long revisionTime) {

            this.services = services;

            this.state = state;

            this.revisionTime = revisionTime;

            this.nservices = services.length;
            
        }
        
        /**
         * Setups up the {@link TxState#preparedBarrier} and the
         * {@link TxState#committedBarrier} and then runs the
         * {@link DistributedCommitterTask} tasks.
         * <p>
         * Post-conditions: {@link TxState#isComplete()} will be true. The
         * transaction will either have been aborted or committed on all
         * {@link IDataService}s.
         * 
         * @return The assigned commit time.
         * 
         * @todo Allow interrupt of the data service committers if any task
         *       fails during prepare() rather than having to wait for all of
         *       those tasks to join at the {@link TxState#preparedBarrier}.
         *       This is only an optimization.
         */
        public Long call() throws Exception {

            setupPreparedBarrier();
            
            setupCommittedBarrier();
            
            /*
             * The futures for the tasks used to invoke prepare(tx,rev) on each
             * dataService.
             */
            final List<Future<Void>> futures;
            {

                final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                        nservices);

                for (IDataService dataService : services) {

                    tasks.add(new DistributedCommitterTask(dataService));

                }

                state.lock.lock();
                try {
                
                    /*
                     * Await all futures, returning once they are all done.
                     */
                    
                    futures = getFederation().getExecutorService().invokeAll(
                            tasks);

                    // tx must be complete (either committed or aborted). 
                    assert state.isComplete() : "tx=" + state;
                    
                } catch(Throwable t) {
                    
                    /*
                     * If we can not invoke all tasks then abort
                     */

                    log.error(t.getLocalizedMessage(), t);
                    
                    state.setRunState(RunState.Aborted);
                    
                    /*
                     * reset the barriers in case anyone is waiting.
                     */
                    
                    state.preparedBarrier.reset();
                    
                    state.committedBarrier.reset();

                    throw new RuntimeException(t);
                    
                } finally {
                    
                    state.lock.unlock();
                    
                }

            }

            List<Throwable> causes = null;
            for (Future f : futures) {

                try {

                    f.get();

                } catch (Throwable t) {

                    if (causes == null) {

                        causes = new LinkedList<Throwable>();

                    }

                    causes.add(t);

                    log.error(t.getLocalizedMessage(), t);

                }

            }

            if (causes != null) {

                final int nfailed = causes.size();

                state.lock.lock();
                try {

                    state.setRunState(RunState.Aborted);
                    
                    /*
                     * reset the barriers in case anyone is waiting (should
                     * not be possible).
                     */
                    
                    state.preparedBarrier.reset();
                    
                    state.committedBarrier.reset();

                } finally {

                    state.lock.unlock();

                }

                throw new ExecutionExceptions("Committer(s) failed: n=" + nservices
                        + ", nfailed=" + nfailed, causes);

            }

            // Success - commit is finished.
            state.lock.lock();
            try {

                state.setRunState(RunState.Committed);
                
                return state.getCommitTime();
                
            } finally {
                
                state.lock.unlock();
                
            }
            
        }

        /**
         * Sets up the {@link TxState#preparedBarrier}. When the barrier action
         * runs it will change {@link RunState} to {@link RunState#Prepared} and
         * assign a <em>commitTime</em> to the transaction. When the barrier
         * breaks, the assigned <i>commitTime</i> will be reported back to the
         * {@link IDataService}s waiting in
         * {@link ITransactionService#prepared(long, UUID)} as the return value
         * for that method.
         */
        private void setupPreparedBarrier() {

            state.preparedBarrier = new CyclicBarrier(nservices,

            new Runnable() {

                public void run() {

                    state.lock.lock();

                    try {

                        state.setRunState( RunState.Prepared );

                        state.setCommitTime(nextTimestamp());

                    } finally {

                        state.lock.unlock();

                    }

                }

            });

        }

        /**
         * Sets up the {@link TxState#committedBarrier}. When the barrier
         * action runs it will change the {@link RunState} to
         * {@link RunState#Committed}.
         */
        protected void setupCommittedBarrier() {

            state.committedBarrier = new CyclicBarrier(nservices,

            new Runnable() {

                public void run() {

                    state.lock.lock();

                    try {

                        state.setRunState(RunState.Committed);

                    } finally {

                        state.lock.unlock();

                    }

                }

            });

        }
        
        /**
         * Task issues {@link ITxCommitProtocol#prepare(long, long)} to an
         * {@link IDataService} participating in a distributed commit.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        protected class DistributedCommitterTask implements Callable<Void> {

            final IDataService service;
            
            public DistributedCommitterTask(final IDataService service) {

                this.service = service;
                
            }

            public Void call() throws Exception {

                try {

                    service.prepare(state.tx, revisionTime);

                } catch (Throwable e) {

                    /*
                     * If an exception is thrown, then make sure that the tx
                     * is in the [Abort] state.
                     */
                    
                    try {
                        log.error(e.getLocalizedMessage(), e);
                    } catch (Throwable t) {
                        // ignored
                    }
                    
                    state.lock.lock();

                    try {

                        state.setRunState(RunState.Aborted);

                    } finally {

                        state.lock.unlock();

                    }
                    
                    throw new RuntimeException(e);

                }

                return null;

            }

        }

    }

    /**
     * Waits at "prepared" barrier. When the barrier breaks, examing the
     * {@link TxState}. If the transaction is aborted, then throw an
     * {@link InterruptedException}. Otherwise return the commitTime assigned
     * to the transaction.
     * 
     * @throws InterruptedException
     *             if the barrier is reset while the caller is waiting.
     */
    public long prepared(final long tx, final UUID dataService)
            throws IOException, InterruptedException, BrokenBarrierException {

        final TxState state = commitList.get(tx);
        
        if (state == null) {

            /*
             * Transaction is not committing.
             */
            
            throw new IllegalStateException();
            
        }
        
        state.lock.lock();
        
        try {
        
            if(!state.isStartedOn(dataService)) {
                
                throw new IllegalArgumentException();
                
            }
            
            // wait at the 'prepared' barrier.
            state.preparedBarrier.await();

            if (state.isAborted())
                throw new InterruptedException();

            return state.getCommitTime();
            
        } finally {
            
            state.lock.unlock();
            
        }
        
    }
    
    /**
     * Wait at "committed" barrier. When the barrier breaks, examing the
     * {@link TxState}. If the transaction is aborted, then return
     * <code>false</code>. Otherwise return true.
     * <p>
     * Note: The {@link TxState} will be aborted if any of the committers throws
     * an exception of their {@link ITxCommitProtocol#prepare(long, long)}
     * method.
     */
    public boolean committed(final long tx, final UUID dataService)
            throws IOException, InterruptedException, BrokenBarrierException {

        final TxState state = commitList.get(tx);
        
        if (state == null) {

            /*
             * Transaction is not committing.
             */
            
            throw new IllegalStateException();
            
        }
        
        state.lock.lock();
        
        try {
        
            if(!state.isStartedOn(dataService)) {
                
                throw new IllegalArgumentException();
                
            }
            
            // wait at the 'committed' barrier.
            state.committedBarrier.await();

            if (state.isAborted())
                return false;

            return true;
            
        } finally {
            
            state.lock.unlock();
            
        }
                
    }

    final public void notifyCommit(final long commitTime) {
        
        synchronized(lastCommitTimeLock) {
            
            /*
             * Note: commit time notifications can be overlap such that they
             * appear out of sequence with respect to their values. This is Ok.
             * We just ignore any older commit times. However we do need to be
             * synchronized here such that the commit time notices themselves
             * are serialized so that we do not miss any.
             */
            
            if (lastCommitTime < commitTime) {

                lastCommitTime = commitTime;
                
            }
            
        }
        
    }
    
    final public long lastCommitTime() {
        
        return lastCommitTime;
        
    }
    
    /**
     * The last (known) commit time.
     * 
     * @todo must be restart safe. can be obtained by querying data services or
     *       written in a local file. (the last timestamp issued must also be
     *       restart safe.)
     */
    private volatile long lastCommitTime = 0L;
    private final Object lastCommitTimeLock = new Object();

    /**
     * Set the new release time. A scheduled task will periodically notify the
     * discovered {@link IDataService}s of the new release time. Sometime after
     * notification an {@link IDataService} may choose to release resources up
     * to the new release time.
     * 
     * @param releaseTime
     *            The new release time (must strictly advance).
     * 
     * @throws IllegalArgumentException
     *             if <i>releaseTime</i> would be decreased.
     * 
     * @todo must also notify the metadata service once it is partitioned.
     * 
     * @todo Advance this value each time the transaction with the smallest
     *       value for [abs(tx)] completes. This needs to be integrated with how
     *       we assign transaction identifiers.
     * 
     * FIXME This should be a protected method since applications should not be
     * doing this themselves.
     * 
     * FIXME Enable the check against the releaseTime going backward once
     * closure is reworked for the triple store to use read-only transactions.
     */
    synchronized public void setReleaseTime(final long releaseTime) {
        
//        if (releaseTime < this.releaseTime) {
//            
//            throw new IllegalStateException();
//            
//        }
        
        this.releaseTime = releaseTime;
        
    }
    /**
     * 
     * @todo The release time on startup should be set to
     *       {@link #nextTimestamp()} - minReleaseAge.
     *       <p>
     *       Move the minReleaseAge configuration property here from the
     *       {@link StoreManager} and have the {@link StoreManager} refuse to
     *       release history until it has been notified of a releaseTime. This
     *       will centralize the value for the minimum amount of history that
     *       will be preserved across the federation.
     *       <p>
     *       If minReleaseTime is increased, then the release time can be
     *       changed to match, but only by NOT advancing it until we are
     *       retaining enough history.
     *       <p>
     *       If minReleaseTime is decreased, then we can immediately release
     *       more history (or at least as soon as the task runs to notify the
     *       discovered data services of the new release time).
     */
    private long releaseTime = 0L;

    /**
     * Task periodically notifies the discovered {@link IDataService}s of the
     * new release time.
     * <p>
     * Note: Running a concurrent instance of this could cause release times to
     * be distributed that do not strictly advance. If you need to do this,
     * e.g., in order to immediately update the release time, then also
     * introduce a lock for this task on the {@link AbstractTransactionService}
     * so that instances of the task must run in sequence.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class NotifyReleaseTimeTask implements Runnable {
        
        /**
         * Notifies all {@link IDataService}s of the current release time. The
         * current release time is discovered when this method runs and the same
         * value is sent to each discovered {@link IDataService}.
         * <p>
         * Note: If this method throws an exception then the task will no longer
         * be scheduled!
         * 
         * @todo We could monitor data service joins (for jini) and immediately
         *       notify newly joined data services of the current release time.
         */
        public void run() {

            /*
             * Choose a consistent value for the notices that we will generate.
             * 
             * FIXME When migrating [minReleaseAge] here the logic MUST use
             * 
             * Math.min(currentTime-minReleaseAge,releaseTime)
             * 
             * This allows us to update the releaseTime on the tx service each
             * time the oldest tx completes while respecting the global
             * constraint on the minReleaseAge.  The code for this is already
             * in the StoreManager.
             */
            final long releaseTime = DistributedTransactionService.this.releaseTime;

            final IBigdataFederation fed = getFederation();

            final UUID[] a = fed.getDataServiceUUIDs(0/* maxCount */);

            for (UUID serviceUUID : a) {

                try {

                    fed.getDataService(serviceUUID).setReleaseTime(releaseTime);

                } catch (IOException ex) {

                    log.error("Could not notify service: " + serviceUUID, ex);

                }

            }

        }

    }

    /**
     * Return the {@link CounterSet}.
     * 
     * @todo define interface declaring the counters reported here.
     * 
     * @todo add a run state counter.
     * 
     * @todo add counter for #of tx started, completed, committed, and aborted,
     *       the #of read-only and the #of read-write tx started and active, and
     *       the min(abs(tx)) (earliest) active tx).
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(getActiveCount());
                }
            });

            countersRoot.addCounter("lastCommitTime", new Instrument<Long>() {
                protected void sample() {
                    setValue(lastCommitTime());
                }
            });
            
            countersRoot.addCounter("releaseTime", new Instrument<Long>() {
                protected void sample() {
                    setValue(releaseTime);
                }
            });

            /*
             * The lock manager imposing a partial ordering on transaction
             * commits.
             */
            countersRoot.makePath("Lock Manager").attach(
                    lockManager.getCounters());

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;

}
