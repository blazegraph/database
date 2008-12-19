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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.ValidationError;
import com.bigdata.resources.StoreManager;

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
    protected void abortImpl(final long tx) {

        try {

            final TxState t = activeTx.get(tx);

            if (t == null)
                throw new IllegalStateException();
            
            assert t.lock.isHeldByCurrentThread();
            
            assert t.isActive();

            /*
             * @todo won't work for read-only since wroteOn is not notified. if
             * necessary change to startedOn(tx,dataServiceUUID) so that we can
             * release the local state on the ds's txmgr.
             */
            final UUID[] uuids = t.getDataServiceUUIDs();

            for (UUID uuid : uuids) {

                try {

                    final IDataService dataService = getFederation()
                            .getDataService(uuid);

                    dataService.abort(t.tx);

                } catch (Throwable t2) {

                    /*
                     * @todo collect all causes and always throw an error if any
                     * ds abort fails.
                     */
                    log.error(t2, t2);

                }
                
            }

        } catch (Throwable t) {

            throw new RuntimeException(t);

        }

    }

    @Override
    protected void commitImpl(final long tx, final long commitTime)
            throws ValidationError {

        final TxState t = activeTx.get(tx);
        
        if (t == null)
            throw new IllegalStateException();
        
        // @todo unless we always need to commit/abort on each touched ds.
        if (!t.isReadOnly() && t.isEmptyWriteSet()) {
            
            // NOP
            return;
            
        }
        
        if (t.isDistributed()) {

            twoPhaseCommit(t, commitTime);

        } else {

            singlePhaseCommit(t, commitTime);

        }

    }

    /**
     * Prepare and commit a read-write transactions that has written on a single
     * data service.
     */
    protected void singlePhaseCommit(final TxState tx, final long commitTime) 
        throws ValidationError {

        assert tx.lock.isHeldByCurrentThread();

        final UUID[] uuids = tx.getDataServiceUUIDs();

        if (uuids.length != 1)
            throw new AssertionError();

        final UUID serviceUUID = uuids[0];

        final IDataService dataService = getFederation().getDataService(
                serviceUUID);

        try {

            // note: prepare + commit.
            dataService.commit(tx.tx, commitTime);

        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }
    
    /**
     * Prepare and commit a read-write transaction that has written on more than
     * one data service.
     * <p>
     * Note: read-write transactions that have written on multiple journals must
     * use a 2-/3-phase commit protocol. Latency is critical in multi-phase
     * commits since the journals will be unable to perform unisolated writes
     * until the transaction either commits or aborts.
     */
    protected void twoPhaseCommit(final TxState tx, final long commitTime) {

        assert tx.lock.isHeldByCurrentThread();

        final UUID[] uuids = tx.getDataServiceUUIDs();

        /*
         * @todo issue prepare messages concurrently to reduce latency,
         * collecting all exceptions and reporting them all back to the
         * client. If the exceptions are just validation errors then we can
         * summarize since the client does not need to know about the
         * details, just that the tx could be be validated and hence was
         * aborted.
         * 
         * @todo resolve data services once (not one for prepare and once
         * for commit).
         * 
         * @todo if any prepare messages fail, then ALL data services must
         * abort (since all services have write sets that need to be
         * discarded).
         * 
         * @todo prepare might well flush any writes not related to the tx
         * by gaining an exclusive write service lock, forcing a commit of
         * any running tasks, and then preparing and continuing to hold the
         * lock until a commit message is received or a timeout occurs.
         */
        try {

            for (UUID uuid : uuids) {

                final IDataService dataService = getFederation()
                        .getDataService(uuid);

                /*
                 * @todo ITx#prepare() appears to already want the
                 * commitTime. reconcile this.
                 */
                dataService.prepare(tx.tx);

            }

        } catch (Throwable t) {

            /*
             * Caller will abort.
             */

            if (t instanceof ValidationError)
                throw (ValidationError) t;

            throw new RuntimeException(t);

        }

        /*
         * @todo issue commit messages concurrently to reduce latency
         * 
         * @todo if any commit messages fail, then we have a problem since
         * the data may be restart safe on some of the journals. A three
         * phase commit would prevent further commits by the journal until
         * all journals had successfully committed and would rollback the
         * journals to the prior commit points (touching the root block to
         * do this) if any journal failed to commit.
         */

        try {

            for (UUID uuid : uuids) {

                final IDataService dataService = getFederation()
                        .getDataService(uuid);

                dataService.commit(tx.tx, commitTime);

            }

        } catch (Throwable t) {

            /*
             * caller will abort.
             */

            throw new RuntimeException(t);

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
     * 
     * FIXME make sure that these counters are reported to the LBS through the
     * client by a delegation pattern for the service.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(activeTx.size());
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

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;

}
