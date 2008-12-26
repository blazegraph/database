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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.RunState;
import com.bigdata.util.concurrent.ExecutionExceptions;
import com.ibm.icu.impl.ByteBuffer;

/**
 * Implementation for an {@link IBigdataFederation} supporting both single-phase
 * commits (for transactions that execute on a single {@link IDataService}) and
 * distributed commits.
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

        /**
         * The directory in which the persistent state of this service will be
         * stored.
         */
        String DATA_DIR = DistributedTransactionService.class.getName()
                + ".dataDir";

        /**
         * The interval in milliseconds between writing a snapshot of the index
         * of accessible commit points into the {@link #DATA_DIR} ({@value #DEFAULT_SHAPSHOT_INTERVAL}).
         * <p>
         * Two snapshots are retained of the commit time index so that those
         * historical commit times required for reading on committed states of
         * the database GT the <i>releaseTime</i> may be on hand after a
         * service restart. Two snapshots are maintained, with the older
         * snapshot being overwritten each time. A snapshot is written every N
         * milliseconds, where N is configured using this property, and also
         * when the service is shutdown.
         * <p>
         * This MAY be ZERO (0L) to disable snapshots - a feature that is used
         * by the {@link EmbeddedFederation} when run in a diskless mode.
         */
        String SHAPSHOT_INTERVAL = DistributedTransactionService.class
                .getName()
                + ".snapshotInterval";

        /** 5 minutes (in millseconds). */
        String DEFAULT_SHAPSHOT_INTERVAL = ""
                + (5 * 60 * 1000); 
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
     * A {@link BTree} containing a log of the historical commit points.
     * <p>
     * The main things that it gives us are (a) the half-open ranges within
     * which we can allocate read-historical transactions; and (b) the last
     * commit time on record. It seems that creating an image of the log every N
     * seconds should be sufficient.
     * <p>
     * Note: Read and write operations on this index MUST be synchronized on the
     * index object.
     */
    protected final CommitTimeIndex commitTimeIndex;

    /**
     * True iff the service does not write any state on the disk.
     */
    private final boolean isTransient;
    
    /**
     * The data directory -or- <code>null</code> iff the service is transient.
     */
    protected final File dataDir;

    /**
     * The interval in milliseconds between logging an image of the
     * {@link #commitTimeIndex}.
     * 
     * @see Options#COMMIT_TIME_INDEX_SHAPSHOT_INTERVAL
     */
    private final long snapshotInterval;
    
    /**
     * The last (known) commit time.
     */
    private volatile long lastCommitTime = 0L;

    /**
     * @param properties
     */
    public DistributedTransactionService(final Properties properties) {

        super(properties);

        if (properties.getProperty(Options.DATA_DIR) == null) {

            throw new RuntimeException("Required property: " + Options.DATA_DIR);

        }
        
        snapshotInterval = LongValidator.GTE_ZERO.parse(
                Options.SHAPSHOT_INTERVAL, properties.getProperty(
                        Options.SHAPSHOT_INTERVAL,
                        Options.DEFAULT_SHAPSHOT_INTERVAL));

        if (INFO)
            log.info(Options.SHAPSHOT_INTERVAL + "=" + snapshotInterval);

        isTransient = snapshotInterval == 0;

        if (isTransient) {
            
            dataDir = null;
            
        } else {
            
            dataDir = new File(properties.getProperty(Options.DATA_DIR));

            if (INFO)
                log.info(Options.DATA_DIR + "=" + dataDir);
            
        }
        
        // Create transient BTree for the commit time log.
        commitTimeIndex = CommitTimeIndex.createTransient();

        setup();
        
        if (INFO)
            log.info("lastCommitTime=" + lastCommitTime + ", #commitTimes="
                    + commitTimeIndex.getEntryCount());
        
    }

    /**
     * Either creates the data directory or reads the {@link #commitTimeIndex}
     * from files in an existing data directory.
     */
    private void setup() {

        if(isTransient) {
            
            // nothing committed yet.
            lastCommitTime = 0L;

            return;

        }
        
        if (!dataDir.exists()) {

            /*
             * New service if its data directory does not exist.
             */

            if (!dataDir.mkdirs()) {

                throw new RuntimeException("Could not create: " + dataDir);

            }

            // nothing committed yet.
            lastCommitTime = 0L;

            return;

        }

        {

            // the files on which the images should have been written.
            final File file0 = new File(dataDir, BASENAME + "0" + EXT);

            final File file1 = new File(dataDir, BASENAME + "1" + EXT);

            if (!file0.exists() && !file1.exists()) {

                log.warn("No commit time logs - assuming new service: dataDir="
                        + dataDir);

                // nothing committed yet.
                lastCommitTime = 0L;

                return;

            }
            // timestamps on those files (zero if the file does not exist)
            final long time0 = file0.lastModified();
            final long time1 = file1.lastModified();

            // true iff file0 is more recent.
            final boolean isFile0 = (time0 != 0L && time1 != 0L) //
                ? (time0 > time1 ? true: false)// Note: both files exist.
                : (time0 != 0L ? true: false)// Note: only one file exists
                ;

            final File file = isFile0 ? file0 : file1;

//            System.err.println("file0: "+file0.lastModified());
//            System.err.println("file1: "+file1.lastModified());
//            System.err.println("isFile0="+isFile0);

            /*
             * Note: On restart the value of this counter is set to either
             * ONE(1) or TWO(1) depending on which snapshot file is more
             * current.
             * 
             * It is ONE(1) if we read file0 since the counter would be ONE(1)
             * after we write file0 for the first time.
             * 
             * It is TWO(2) if we read file1 since the counter would be TWO(2)
             * after we write file1 for the first time.
             */
            snapshotCount = isFile0 ? 1 : 2;

            try {

                // read most recent image.
                final int entryCount = SnapshotHelper.read(commitTimeIndex,
                        file);

                log.warn("Read snapshot: entryCount=" + entryCount + ", file="
                        + file);
                
            } catch (IOException ex) {

                throw new RuntimeException("Could not read file: " + file, ex);

            }

        }

        if (commitTimeIndex.getEntryCount() == 0) {

            // nothing in the commit time log.
            lastCommitTime = 0;

        } else {

            // the last commit time in the log. @todo write unit test to
            // verify on restart.
            lastCommitTime = commitTimeIndex.decodeKey(commitTimeIndex
                    .keyAt(commitTimeIndex.getEntryCount() - 1));

        }

    }

    /**
     * Basename for the files written in the {@link #dataDir} containing images
     * of the {@link #commitTimeIndex}.
     */
    static protected final String BASENAME = "commitTime";
    
    /**
     * Extension for the files written in the {@link #dataDir} containing
     * snapshots of the {@link #commitTimeIndex}.
     */
    static protected final String EXT = ".snapshot";
    
    /**
     * #of times we have written a snapshot of the {@link #commitTimeIndex}.
     */
    private long snapshotCount = 0L;
    
    /**
     * Runs the {@link SnapshotTask} once.
     */
    public void snapshot() {
        
        new SnapshotTask().run();

    }
    
    /**
     * A task that writes a snapshot of the commit time index onto a pair of
     * alternating files. This is in the spirit of the Challis algorithm, but
     * the approach is less rigerous.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SnapshotTask implements Runnable {
      
        /**
         * Note: Anything thrown out of this method will cause the task to no
         * longer be scheduled!
         */
        public void run() {
            
            if(isTransient) {

                // snapshot not supported for transient service.
                throw new RuntimeException("Service is transient");
                
            }
            
            lock.lock();

            try {

                final long begin = System.currentTimeMillis();

                // either 0 or 1.
                final int i = (int) snapshotCount % 2;

                final File file = new File(dataDir, BASENAME + i + EXT);

                final int entryCount;
                synchronized (commitTimeIndex) {

                    entryCount = SnapshotHelper.write(commitTimeIndex, file);

                }

                // increment counter iff successful.
                snapshotCount++;

                final long elapsed = System.currentTimeMillis() - begin;

                log.warn("snapshot: snapshotCount=" + snapshotCount
                        + ", entryCount=" + entryCount + ", file=" + file
                        + ", elapsed=" + elapsed);

            } catch (Throwable t) {

                log.error(t.getMessage(), t);

                return;

            } finally {

                lock.unlock();

            }

        }
        
    };
    
    /**
     * A helper class for reading and writing snapshots of the commit time
     * index. The image contains the commit timestamps in order.
     * <p>
     * Note: The caller must prevent concurrent changes to the index.
     * 
     * @todo write counters into the files since the system clock could be
     *       messed with on before a restart but the counters will always be
     *       valid. we would then either read both and choose one, or have a
     *       method to report the header with the earlier counter.
     * 
     * @todo Checksum the commit time log file? this is easily done either using
     *       a {@link ByteBuffer} or using {@link Adler32}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SnapshotHelper {

        static public int read(CommitTimeIndex ndx, File file)
                throws IOException {

            final FileInputStream is = new FileInputStream(file);

            try {

                final BufferedInputStream bis = new BufferedInputStream(is);

                final DataInputStream dis = new DataInputStream(bis);

                return SnapshotHelper.read(ndx, dis);

            } finally {

                is.close();

            }

        }
        
        static public int read(CommitTimeIndex ndx, DataInputStream is)
                throws IOException {

            final int n = is.readInt();

            for (int i = 0; i < n; i++) {

                ndx.add(is.readLong());

            }

            return n;
            
        }

        static public int write(CommitTimeIndex ndx, File file)
                throws IOException {

            final FileOutputStream os = new FileOutputStream(file);

            try {

                final BufferedOutputStream bos = new BufferedOutputStream(os);

                final DataOutputStream dos = new DataOutputStream(bos);

                // write the image on the file.
                final int entryCount = SnapshotHelper.write(ndx, dos);

                dos.flush();
                
                bos.flush();

                return entryCount;
                
            } finally {

                os.close();

            }

        }
        
        static public int write(CommitTimeIndex ndx, DataOutputStream os)
                throws IOException {

            final int entryCount = ndx.getEntryCount();
            
            os.writeInt(entryCount);
            
            final ITupleIterator itr = ndx.rangeIterator();

            int n = 0;
            
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final long commitTime = ndx.decodeKey(tuple.getKey());

                os.writeLong(commitTime);

                n++;
                
            }
            
            if (n != entryCount) {
                
                /*
                 * Note: probable error is the caller not preventing concurrent
                 * modification.
                 */
                
                throw new AssertionError();
                
            }
            
            return entryCount;
            
        }
        
    }
    
    public DistributedTransactionService start() {

        /*
         * Note: lock makes operation _mostly_ atomic even though the base class
         * changes the runState. For example, new transactions can not start
         * without this lock.
         */
        lock.lock();

        try {

            super.start();

            addScheduledTasks();
            
            return this;

        } finally {

            lock.unlock();

        }
        
    }
    
    /**
     * Adds the scheduled tasks.
     */
    protected void addScheduledTasks() {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        final AbstractFederation fed = (AbstractFederation) getFederation();

        // @todo config options (verify units).
        notifyFuture = fed.addScheduledTask(new NotifyReleaseTimeTask(),
                60/* initialDelay */, 60/* delay */, TimeUnit.SECONDS);

        if (snapshotInterval != 0L) {

            // start the snapshot task.

            writeFuture = fed.addScheduledTask(
                    new SnapshotTask(),
                    snapshotInterval/* initialDelay */,
                    snapshotInterval/* delay */,
                    TimeUnit.MILLISECONDS);

        }

    }
    
    private ScheduledFuture notifyFuture = null;
    private ScheduledFuture writeFuture = null;
    
    public void shutdown() {
        
        lock.lock();
        try {

            switch (getRunState()) {
            case Shutdown:
            case ShutdownNow:
            case Halted:
                return;
            }

            /*
             * First make sure that all tx are terminated - this is important
             * otherwise we will write the commit time index image before we
             * have the last commit times on hand.
             */
            super.shutdown();

            /*
             * No need to interrupt this task. It will complete soon enough.
             * However, we do want to cancel it so it will stop running.
             */
            if (notifyFuture != null)
                notifyFuture.cancel(false/* mayInterruptIfRunning */);

            /*
             * Cancel this task, but DO NOT interrupt it to avoid a partial
             * write if there is a write in progress. If there is a write in
             * progress, then we will wind up writing it again immediately since
             * we do that below. This is Ok. We will just have a current image
             * and a nearly current image.
             */
            if (writeFuture != null)
                writeFuture.cancel(false/* mayInterruptIfRunning */);

            if (snapshotInterval != 0L) {
             
                // write a final image during shutdown.
                new SnapshotTask().run();
                
            }

        } finally {

            lock.unlock();

        }

    }

    public void shutdownNow() {

        lock.lock();
        try {
            
            switch (getRunState()) {
            case ShutdownNow:
            case Halted:
                return;
            }

            /*
             * First make sure that all tx are terminated - this is important
             * otherwise we will write the commit time index image before we
             * have the last commit times on hand.
             */
            super.shutdownNow();

            /*
             * Cancel and interrupt if running.
             */
            if (notifyFuture != null)
                notifyFuture.cancel(true/* mayInterruptIfRunning */);

            /*
             * Cancel this task and interrupt if running. Interrupting this will
             * leave a partial snapshot on the disk, but we do not advance the
             * counter unless the snapshot is successful so we will overwrite
             * that partial snapshot below when we write a final snapshot.
             */
            if (writeFuture != null)
                writeFuture.cancel(true/* mayInterruptIfRunning */);

            if (snapshotInterval != 0L) {

                // write a final snapshot during shutdown.
                snapshot();
                
            }

        } finally {

            lock.unlock();
            
        }

    }
    
    public void destroy() {

        lock.lock();

        try {

            super.destroy();

            if (!isTransient) {

                // delete the commit time index log files.
                new File(dataDir, BASENAME + "0" + EXT).delete();
                new File(dataDir, BASENAME + "1" + EXT).delete();

                // delete the data directory (works iff it is empty).
                dataDir.delete();

            }

        } finally {

            lock.unlock();
            
        }
        
    }

    /**
     * Extended to truncate the head of the {@link #commitTimeIndex} such only
     * the commit times requires for reading on timestamps GTE to the new
     * releaseTime are retained.
     */
    protected void setReleaseTime(long releaseTime) {
        
        super.setReleaseTime(releaseTime);

        /*
         * Truncate the head of the commit time index since we will no longer
         * grant transactions whose start time is LTE the new releaseTime.
         */
        
        // Note: Use the current value.
        releaseTime = getReleaseTime();
        
        if (releaseTime > 0) {
        
            synchronized (commitTimeIndex) {

                /*
                 * The exclusive upper bound is the timestamp of the earliest
                 * commit point on which we can read with this [releaseTime].
                 */
                final long toKey = commitTimeIndex.find(releaseTime + 1);

                final ITupleIterator itr = commitTimeIndex.rangeIterator(0L,
                        toKey, 0/* capacity */, IRangeQuery.KEYS
                                | IRangeQuery.CURSOR, null/* filter */);

                while (itr.hasNext()) {

                    itr.next();

                    // remove the tuple from the index.
                    itr.remove();

                }

            }
            
        }
        
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
     * @todo Single phase commits currently contend for the named resource locks
     *       (locks on the index names). However, they could also executed
     *       without acquiring those locks since they will be serialized locally
     *       by the {@link IDataService} on which they are executing.
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
     
        /*
         * The LockManagerTask will handle lock acquisition for the named
         * resources and then invoke our task to perform the commit.
         */
        final LockManagerTask<String, Long> delegate = new LockManagerTask<String, Long>(
                lockManager, state.getResources(), new TxCommitTask(state));

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

    protected long findCommitTime(final long timestamp) {
        
        synchronized(commitTimeIndex) {
            
            return commitTimeIndex.find(timestamp);
            
        }
        
    }

    protected long findNextCommitTime(long commitTime) {
        
        synchronized(commitTimeIndex) {
            
            return commitTimeIndex.findNext(commitTime);
            
        }

    }
    
    /**
     * @todo Is it a problem if the commit notices do not arrive in sequence?
     *       Because they will not. Unisolated operations will participate in
     *       group commits using timestamps obtained from the transaction
     *       service, but those commit operations will not be serialize and
     *       their reporting of the timestamps for the commits will likewise not
     *       be serialized.
     *       <p>
     *       The danger is that we could assign a read-historical transaction
     *       start time based on the {@link #commitTimeIndex} and then have
     *       commit timestamps arrive that are within the interval in which we
     *       made the assignment. Essentially, our interval was too large and
     *       the assigned start time may have been on either side of a
     *       concurrent commit. However, this can only occur for unisolated
     *       operations (non-transactional commits). The selected timestamp will
     *       always be coherent with respect to transaction commits since those
     *       are coordinated and use a shared commit time.
     *       <p>
     *       This issue can only arise when requesting historical reads for
     *       timestamps that are "close" to the most recent commit point since
     *       the latency involve would otherwise not effect the assignment of
     *       transaction start times. However, it can occur either when
     *       specifying the symbolic constant {@link ITx#READ_COMMITTED} to
     *       {@link #newTx(long)} or when specifying the exact commitTime
     *       reported by a transaction commit.
     *       <p>
     *       Simply stated, there is NO protection against concurrently
     *       unisolated operations committing. If such operations are used on
     *       the same indices as transactions, then it IS possible that the
     *       application will be unable to read from exactly the post-commit
     *       state of the transaction for a brief period (10s of milliseconds)
     *       until the unisolated commit notices have been propagated to the
     *       {@link DistributedTransactionService}. This issue will only occur
     *       when there is also a lot of contention for reading on the desired
     *       timestamp since otherwise the commitTime itself may be used as a
     *       transaction start time.
     */
    final public void notifyCommit(final long commitTime) {

        synchronized(commitTimeIndex) {

            /*
             * Add all commit times
             */
            commitTimeIndex.add(commitTime);
            
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
    
    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }
    
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
     * @todo must also notify the metadata service once it is partitioned.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class NotifyReleaseTimeTask implements Runnable {
        
        /**
         * Notifies all {@link IDataService}s of the current release time.
         * <p>
         * Note: An {@link IDataService} WILL NOT release its most current
         * commit point, regardless of the releaseTime that is sent to that
         * service.
         * <p>
         * Note: If this method throws an exception then the task will no longer
         * be scheduled!
         * 
         * @todo We could monitor data service joins (for jini) and immediately
         *       notify newly joined data services of the current release time.
         */
        public void run() {

            final long releaseTime = getReleaseTime();

            final IBigdataFederation fed = getFederation();

            final UUID[] a = fed.getDataServiceUUIDs(0/* maxCount */);

            // @todo notify in parallel?
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
     * Adds counters for the {@link LockManager}.
     */
    synchronized public CounterSet getCounters() {

        if (countersRoot == null) {
            
            /*
             * Setup basic counters.
             */
            super.getCounters();

            /*
             * The lock manager imposing a partial ordering on transaction
             * commits.
             */
            countersRoot.makePath("Lock Manager").attach(
                    ((DistributedTransactionService) this).lockManager
                            .getCounters());

            countersRoot.addCounter("snapshotCount",
                    new Instrument<Long>() {
                        protected void sample() {
                            setValue(snapshotCount);
                        }
                    });

            /**
             * The #of distributed transaction commits that are currently in
             * progress.
             */
            countersRoot.addCounter("distributedCommitsInProgressCount",
                    new Instrument<Integer>() {
                        protected void sample() {
                            setValue(commitList.size());
                        }
                    });

            /**
             * The #of commit times that are currently accessible.
             */
            countersRoot.addCounter("commitTimesCount",
                    new Instrument<Integer>() {
                        protected void sample() {
                            synchronized (commitTimeIndex) {
                                setValue(commitTimeIndex.getEntryCount());
                            }
                        }
                    });

            countersRoot.addCounter("dataDir", new Instrument<String>() {
                protected void sample() {
                    setValue(dataDir.toString());
                }
            });

        }

        return countersRoot;

    }

}
