/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Feb 10, 2010
 */

package com.bigdata.io;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.io.WriteCache.RecordMetadata;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.ha.QuorumManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A {@link WriteCacheService} is provisioned with some number of
 * {@link WriteCache} buffers and a writer thread. Caller's populate
 * {@link WriteCache} instances. When they are full, they are transferred to a
 * queue which is drained by the {@link WriteCacheService}. Hooks are provided
 * to wait until the current write set has been written (e.g., at a commit point
 * when the cached writes must be written through to the backing channel).
 * 
 * @see WriteCache
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME This is an alternative implementation of the {@link WriteCacheService}
 * in which we are working towards clearer use of the lock and condition patterns,
 * away from loops and sleeping patterns, and trying to close any holes which might
 * exist in try/finally patterns.
 * 
 * @todo Build out write cache service for WORM (serialize writes on the cache
 *       but the caller does not wait for the IO and readers are non-blocking).
 *       A pool of write cache instances should be used. Readers should check
 *       the current write cache and also each non-recycled write cache with
 *       writes on it in the pool. Write caches remain available to readers
 *       until they need to be recycled as the current write cache (the one
 *       servicing new writes). The write cache services needs to maintain a
 *       dirty list of write cache instances. A single thread will handle writes
 *       onto the disk. When the caller calls flush() on the write cache service
 *       it flush() the current write cache (if dirty) to the dirty list and
 *       then wait until the specific write cache instances now on the dirty
 *       list have been serviced (new writes MAY continue asynchronously).
 * 
 * @todo Build out write cache service for RW. The salient differences here is
 *       gathered writes on the store. Note that writers do not have more
 *       concurrency since that bit is still synchronized inside of
 *       {@link WriteCache#write(long, ByteBuffer)} and the {@link RWStore}
 *       serializes its allocation requests. Also, the gathering writes can
 *       combine and order records from the dirty write cache list for better
 *       efficiency. However, if it does this during flush(), then it should not
 *       combine records from write caches which are inside of the write cache
 *       set on which the flush() is waiting in order to ensure that flush() is
 *       service in a timely manner.
 * 
 * @todo The WORM (and RW) stores need to also establish a read-write lock to
 *       prevent changes in the file extent from causing corrupt data for
 *       concurrent read or write operations on the file. See
 *       {@link #write(long, ByteBuffer)} for my notes on this.
 * 
 * @todo I wonder if we can take this a step at a time without branching? The
 *       main danger point is when we allow readers (and a single write thread)
 *       to run concurrently on the store. We just need to MUTEX those
 *       conditions with file extension, and a read-write lock is exactly the
 *       tool for that job. We also need to explore whether or not (and if so,
 *       how) to queue disk reads for servicing. I would like to take a metrics
 *       based approach to that once we have concurrent readers. I expect that
 *       performance could be very good on a server grade IO bus such as the
 *       cluster machines. The SAS should already handle the reordering of
 *       concurrent reads. However, it is clear that the SATA (non-SCSI) bus is
 *       not as good at this, so maybe handling in s/w makes sense for non-SCSI
 *       disks?
 * 
 * @todo test @ nbuffers=1 and nbuffers=2 which are the most stressful
 *       conditions.
 * 
 * @todo There needs to be a unit test which verifies overwrite of a record in
 *       the {@link WriteCache}. It is possible for this to occur with the
 *       {@link RWStore} (highly unlikely, but possible).
 * 
 * @todo When integrating with the {@link RWStore} or the
 *       {@link DiskOnlyStrategy} we may need to discard the cache on abort in
 *       order to prevent read-through of records which were written on to the
 *       cache and may even have been written through onto the disk, but which
 *       did not make it into the commit group. Is this a problem or not?
 */
abstract public class WriteCacheService2 implements IWriteCache {

    protected static final Logger log = Logger
            .getLogger(WriteCacheService.class);

    /**
     * <code>true</code> until the service is shutdown (actually, until a
     * request is made to shutdown the service).
     */
    final private AtomicBoolean open = new AtomicBoolean(true);

    /**
     * A single threaded service which writes dirty {@link WriteCache}s onto the
     * backing store.
     */
    final private ExecutorService localWriteService;

    /**
     * The {@link Future} of the task running on the {@link #localWriteService}.
     */
    final private Future<Void> localWriteFuture;

    /**
     * A single threaded service which writes dirty {@link WriteCache}s onto the
     * downstream service in the quorum.
     */
    final private ExecutorService remoteWriteService;

//    /**
//     * The deferredList is used when WriteCaches should be added to the dirtyList
//     * but there is an ongoing {@link #flush(boolean)}. In this case the WriteCache is added to the deferredList
//     * and when the flush is complete, any members are transferred in order to the
//     * dirtyList.
//     */
//    final protected BlockingQueue<WriteCache> deferredDirtyList;
//
//    final private Latch deferredLatch = new Latch();

    /**
     * A list of clean buffers. By clean, we mean not needing to be written.
     * Once a dirty write cache has been flushed, it is placed onto the
     * {@link #cleanList}. Clean buffers can be taken at any time for us as the
     * current buffer.
     */
    final private BlockingQueue<WriteCache> cleanList;

//    /**
//     * This latch tracks the number of operations acting on the {@link #current}
//     * buffer. It is incremented by {@link #acquire()} and decremented by
//     * {@link #release()}. The {@link #current} buffer can not be changed until
//     * this latch reaches zero.
//     */
//    final private Latch latch = new Latch();

    /**
     * The read lock allows concurrent {@link #acquire()}s while the write lock
     * prevents {@link #acquire()} when we must either reset the
     * {@link #current} cache buffer or change the {@link #current} reference.
     * E.g., {@link #flush(boolean, long, TimeUnit)}.
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * A list of dirty buffers. Writes from these may be combined, but not
     * across {@link #flush(boolean)}.
     */
    final private BlockingQueue<WriteCache> dirtyList;

    /**
     * Condition signaled by the {@link WriteTask} when the {@link #dirtyList}
     * becomes empty (but not until the last buffer drained from that list has
     * been written through to the backing channel).
     * {@link #flush(boolean, long, TimeUnit)} acquires this lock and uses it to
     * await the {@link #dirtyListEmpty} {@link Condition}.
     */
    final private ReentrantLock dirtyListLock = new ReentrantLock();

    /**
     * Lock used to put cache buffers onto the {@link #dirtyList}. This lock is
     * required in order for {@link #flush(boolean, long, TimeUnit)} to have
     * atomic semantics, otherwise new cache buffers could be added to the dirty
     * list. This lock is distinct from the {@link #lock} because we do not want
     * to yield that lock when awaiting the {@link #dirtyListEmpty} condition.
     * 
     * @see #dirtyListLock.
     */
    final private Condition dirtyListEmpty = dirtyListLock.newCondition();

    /**
     * Condition signaled whenever the dirty list becomes non-empty. This is
     * only awaited by {@link WriteTask#call()} so only signalAll() is not
     * necessary.
     */
    final private Condition dirtyListNotEmpty = dirtyListLock.newCondition();

    /**
     * The current buffer. Modification of this value and reset of the current
     * {@link WriteCache} are protected by the write lock of {@link #lock()}.
     */
    final private AtomicReference<WriteCache> current = new AtomicReference<WriteCache>();

    /**
     * The capacity of the cache buffers. This is assumed to be the same for
     * each buffer.
     */
    final private int capacity;

    /**
     * Object knows how to (re-)open the backing channel.
     */
    final private IReopenChannel<? extends Channel> opener;

    /**
     * The object which notices the quorum change events.
     */
    final protected QuorumManager quorumManager;
    
    /**
     * A map from the offset of the record on the backing file to the cache
     * buffer on which that record was written.
     */
    final private ConcurrentMap<Long/* offset */, WriteCache> recordMap;

    /**
     * Allocates N buffers from the {@link DirectBufferPool}.
     * 
     * @param nbuffers
     *            The #of buffers to allocate.
     * @param opener
     *            The object which knows how to (re-)open the channel to which
     *            cached writes are flushed.
     * 
     * @throws InterruptedException
     */
    public WriteCacheService2(final int nbuffers,
            final IReopenChannel<? extends Channel> opener,
            final QuorumManager quorumManager)
            throws InterruptedException {

        if (nbuffers <= 0)
            throw new IllegalArgumentException();

        if (opener == null)
            throw new IllegalArgumentException();

        if (quorumManager == null)
            throw new IllegalArgumentException();

        this.opener = opener;

        this.quorumManager = quorumManager;
        
        dirtyList = new LinkedBlockingQueue<WriteCache>();

//        deferredDirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingQueue<WriteCache>();
        
        for (int i = 0; i < nbuffers - 1; i++) {

            cleanList.add(newWriteCache(null/* buf */, opener));

        }

        current.set(newWriteCache(null/* buf */, opener));

        // assume capacity is the same for each buffer instance.
        capacity = current.get().capacity();

        // set initial capacity based on an assumption of 1024k buffers.
        recordMap = new ConcurrentHashMap<Long, WriteCache>(nbuffers
                * (capacity / 1024));

        // start service to write on the backing channel.
        localWriteService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        if (quorumManager.replicationFactor() > 1) {
            // service used to write on the downstream node in the quorum.
            remoteWriteService = Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                            .getName()));
        } else {
            remoteWriteService = null;
        }
        
        // run the write task
        localWriteFuture = localWriteService.submit(new WriteTask());

    }

	/**
     * The task responsible for writing dirty buffers onto the backing channel.
     * 
     * There should be only one of these
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class WriteTask implements Callable<Void> {

        public Void call() throws Exception {

            while (true) {
                dirtyListLock.lockInterruptibly();
                try {
                    /*
                     * FIXME an error during cache.flush() will cause the
                     * [cache] buffer to be dropped. it needs to be put back
                     * onto the dirtyList in the correct position, which
                     * suggests using a LinkedDeque.
                     * 
                     * If there is an error in this thread then it needs to be
                     * propagated to the threads write()ing on the cache or
                     * awaiting flush() and from there back to the caller and an
                     * abort(). abort() will have to reconfigure the
                     * WriteCacheService, perhaps using a new instance. HA
                     * failover will have to do similar things. The way I
                     * normally do this is a volatile [halt:boolean] and
                     * [firstCause]. We do not need to bother the readers, but
                     * the reprovisioning of the write cache service must hold
                     * the writeLock so as to occur when there are no
                     * outstanding reads executing against the write cache
                     * service.
                     */
                    while (dirtyList.isEmpty()) {
                        dirtyListNotEmpty.await();
                    }
                    final WriteCache cache = dirtyList.take();
                    if (dirtyList.isEmpty()) {
                        /*
                         * Will signal Condition when we release the
                         * dirtyListLock. We only need signal(), rather than
                         * signalAll(), because it is flush() who waits for this
                         * signal and flush() already holds the WriteLock so
                         * there can not be concurrent invocations of flush().
                         */
                        dirtyListEmpty.signal();
                    }

                    Future<?> remoteWriteFuture = null;

                    if (remoteWriteService != null) {
                        // Start downstream IOs (network IO).
                        remoteWriteService.submit(cache
                                .getDownstreamWriteRunnable(quorumManager));
                    }

                    // Do the local IOs.
                    cache.flush(false/* force */);

                    if (remoteWriteFuture != null) {
                        // Wait for the downstream IOs to finish.
                        remoteWriteFuture.get();
                    }

                    cleanList.add(cache);

                } catch (InterruptedException t) {
                    /*
                     * This task can only be interrupted by a thread with its
                     * Future, so this interrupt is a clear signal that the
                     * write cache service is closing down.
                     */
                    return null;
                } catch (Throwable t) {
                    /*
                     * Anything else is an error, but we will not halt
                     * processing.
                     */
                    log.error(t, t);
                } finally {
                    dirtyListLock.unlock();
                }

            }// while(true)

        }

    }

    /**
     * Factory for {@link WriteCache} implementations.
     * 
     * @param buf
     *            The backing buffer (optional).
     * @param opener
     *            The object which knows how to re-open the backing channel
     *            (required).
     * 
     * @return A {@link WriteCache} wrapping that buffer and able to write on
     *         that channel.
     * 
     * @throws InterruptedException
     */
    abstract protected WriteCache newWriteCache(ByteBuffer buf,
            IReopenChannel<? extends Channel> opener)
            throws InterruptedException;

    /**
     * {@inheritDoc}
     * <p>
     * This implementation calls {@link IWriteCache#reset()} on all
     * {@link #dirtyList} objects and moves those buffers to the
     * {@link #availList}.
     */
    public void reset() throws InterruptedException {

        final WriteLock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            final List<WriteCache> c = new LinkedList<WriteCache>();

            dirtyList.drainTo(c);

            for (WriteCache t1 : dirtyList) {

                t1.resetWith(recordMap);

                cleanList.put(t1);

            }
            
        } finally {

            writeLock.unlock();

        }

    }

//    /**
//     * Set when closing. This field is volatile so its state is noticed by the
//     * {@link WriteTask} without explicit synchronization.
//     */
//    private volatile boolean m_closing = false;
    
    public void close() throws InterruptedException {

        if (open.compareAndSet(true/* expect */, false/* update */)) {

            try {
//            	m_closing = true;
            	
                // Interrupt the write task.
                localWriteFuture.cancel(true/* mayInterruptIfRunning */);

                // Immediate shutdown of the write service.
                localWriteService.shutdownNow();

                if (remoteWriteService != null) {

                    // Immediate shutdown of the write service.
                    remoteWriteService.shutdownNow();

                }

            } finally {

                /*
                 * Ensure that the buffers are closed in a timely manner.
                 */
                final WriteLock writeLock = lock.writeLock();

                writeLock.lock(); // Note: not interruptible in close()
                
                try {

                    final List<WriteCache> c = new LinkedList<WriteCache>();

                    dirtyList.drainTo(c);

                    cleanList.drainTo(c);

                    final WriteCache t = current.getAndSet(null);

                    if (t != null) {

                        c.add(t);

                    }

                    for (WriteCache t1 : c) {

                        t1.close();

                    }

                } finally {

                    writeLock.unlock();

                }

//                m_closing = false;

            }

        }

    }

    /**
     * Ensures that {@link #close()} is eventually invoked so the buffers can be
     * returned to the {@link DirectBufferPool}.
     * 
     * @throws Throwable
     */
    protected void finalized() throws Throwable {
        close();
    }

    /**
     * @throws IllegalStateException
     *             if the service is closed.
     * @throws IllegalStateException
     *             if the write task has failed.
     */
    protected void assertOpen() {

        if (!open.get())
            throw new IllegalStateException();

        if (localWriteFuture.isDone()) {

            /*
             * If the write task terminates abnormally then throw the exception
             * out here.
             */

            try {

                localWriteFuture.get();

            } catch (Throwable t) {

                throw new IllegalStateException(t);

            }

        }

    }
    
    /**
     * Return the current buffer. The caller may read or write on the buffer.
     * Once they are done, the caller MUST call {@link #release()}.
     * 
     * @return The buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link WriteCacheService} is closed.
     */
    private WriteCache acquire() throws InterruptedException,
            IllegalStateException {

        final ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();
        
        try {

            assertOpen();

//            latch.inc();

            /*
             * Note: acquire() does not block since it holds the ReadLock.
             * Methods which change [current] MUST hold the WriteLock across
             * that operation to ensure that [current] is always non-null since
             * acquire() will not block once it acquires the ReadLock.
             */
            final WriteCache tmp = current.get();

            if (tmp == null) {

//                latch.dec();

                throw new RuntimeException();

            }

            return tmp;

        } catch(Throwable t){

            /*
             * Note: release the lock only on the error path.
             */

            readLock.unlock();

            throw new RuntimeException(t); 
            
        }

    }

    /**
     * Release the latch on an acquired buffer.
     */
    private void release() {

        /*
         * Note: This is releasing the ReadLock which was left open by
         * acquire().
         */
        lock.readLock().unlock();

    }

    /**
     * Flush the current write set through to the backing channel.
     * 
     * @throws InterruptedException
     */
    public void flush(final boolean force) throws InterruptedException {

        try {

            if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {

                throw new RuntimeException();

            }

        } catch (TimeoutException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * flush() is a blocking method. At most one flush() operation may run at a
     * time. The {@link #current} buffer is moved to the {@link #dirtyList}
     * while holding the {@link WriteLock} and flush() then waits until the
     * dirtyList becomes empty, at which point all dirty records have been
     * written through to the backing file.
     * 
     * @see WriteTask
     * @see #dirtyList
     * @see #dirtyListEmpty
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit units) throws TimeoutException, InterruptedException {

        final WriteLock writeLock = lock.writeLock();
        writeLock.lockInterruptibly(); // @todo adjust remaining.
        try {
            /*
             * @todo also, monitor [halt] and report [firstCause]. To notice
             * [halt] while waiting on the [dirtyListEmpty] Condition, we need
             * to signal that condition when an error occurs in the WriteTask
             * (or somehow cause it to wake up with an InterruptedException).
             */
            final WriteCache tmp = current.get();
            dirtyListLock.lockInterruptibly(); // @todo w/ timeout and adjust for time remaining.
            try {
                /*
                 * Note: [tmp] may be empty, but there is basically zero cost in
                 * WriteTask to process and empty buffer and, done this way, the
                 * code is much less complex here.
                 */
                dirtyList.add(tmp);
                dirtyListNotEmpty.signal();
                while (!dirtyList.isEmpty()) {
                    // @todo adjust for remaining.
                    if (!dirtyListEmpty.await(timeout, units)) {
                        throw new TimeoutException();
                    }
                }
            } finally {
                dirtyListLock.unlock();
            }
            // Replace [current] with a clean cache buffer.
            final WriteCache nxt = cleanList.take();
            nxt.resetWith(recordMap);
            current.set(nxt);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean write(long offset, ByteBuffer data)
            throws IllegalStateException, InterruptedException {

        return writeChk(offset, data, 0);

    }

    /**
     * Write the record onto the cache. If the record is too large for the cache
     * buffers, then it is written synchronously onto the backing channel.
     * Otherwise it is written onto a cache buffer which is lazily flushed onto
     * the backing channel. Cache buffers are written in order once they are
     * full. This method does not impose synchronization on writes which fit the
     * capacity of a cache buffer.
     * <p>
     * When integrating with the {@link RWStore} or the {@link DiskOnlyStrategy}
     * there needs to be a read/write lock such that file extension is mutually
     * exclusive with file read/write operations (due to a Sun bug). The caller
     * can override {@link #newWriteCache(ByteBuffer, IReopenChannel)} to
     * acquire the necessary lock (the read lock of a {@link ReadWriteLock}).
     * This is even true when the record is too large for the cache since we
     * delegate the write to a temporary {@link WriteCache} wrapping the
     * caller's buffer.
     * 
     * @return <code>true</code> since the record is always accepted by the
     *         {@link WriteCacheService} (unless an exception is thrown).
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     * 
     * @todo The WORM serializes invocations on this method because it must put
     *       each record at a specific offset into the user extent of the file.
     *       However, the RW store does not do this. Therefore, for the RW store
     *       only, we could use a queue with lost cost access and scan for best
     *       fit packing into the write cache buffer. When a new buffer is set
     *       as [current], we could pack the larger records in the queue onto
     *       that buffer first. This might provide better throughput for the RW
     *       store but would require an override of this method specific to that
     *       implementation.
     */
    public boolean writeChk(final long offset, final ByteBuffer data,
            final int chk) throws InterruptedException, IllegalStateException {

    	if (log.isInfoEnabled()) {
    		log.info("offset: " + offset + ", length: " + data.limit());
    	}
    	
        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        if (!open.get())
            throw new IllegalStateException();

        if (nbytes > capacity) {

            /*
             * Write the record onto the file at that offset. This operation is
             * synchronous (to protect the ByteBuffer from concurrent
             * modification by the caller). It will block until the record has
             * been written.
             * 
             * Note: Code below will take an empty buffer and put this record
             * onto it, so we do not have starvation even if the caller's record
             * would completely fill a cache buffer.
             * 
             * FIXME Should this block if we are waiting for the dirty list to
             * become empty (e.g., a flush)? This might be necessary to gain a
             * strong guarantee of the state of the write pipeline. An
             * alternative would be to drop the caller's buffer onto the
             * dirtyList and await some buffer specific condition until that
             * buffer has been written through.
             */
            try {

                // A singleton map for that record.
                final Map<Long, RecordMetadata> recordMap = Collections
                        .singletonMap(offset, new RecordMetadata(offset,
                                0/* bufferOffset */, nbytes));

                /*
                 * Wrap the caller's record as a ByteBuffer.
                 * 
                 * Note: The ByteBuffer MIGHT NOT be direct!
                 */
                final WriteCache tmp = newWriteCache(data, opener);
                
                // FIXME write pipeline for this buffer as well.

                // Write the record on the channel using write cache factory.
                tmp.writeOnChannel(data, recordMap, Long.MAX_VALUE/* nanos */);

                return true;

            } catch (Throwable e) {

                throw new RuntimeException(e);

            }

        }

        /*
         * The record can fit into a cache instance, so try and acquire one and
         * write the record onto it.
         */
        {

            final WriteCache cache = acquire();

            try {

                // write on the cache.
                if (cache.writeChk(offset, data, chk)) {

                    /*
                     * Note: We MUST use put() here rather than putIfAbsent()
                     * and the return value MAY be non-null. This condition
                     * arises when there exists a record on a clean buffer which
                     * was part of an aborted write set. Such records may be
                     * rewritten following the abort. Since the record was
                     * already laid down on the backing channel, there is no
                     * point clearing it from the clean write cache buffers,
                     * which would require us to track all records in a given
                     * write set (not very scalable).
                     * 
                     * Note: put() SHOULD return non-null further down in this
                     * method since we will always be looking at a new buffer in
                     * those code paths.
                     */
                    if (recordMap.put(offset, cache) != null) {
                        throw new AssertionError(
                                "Record already in cache: offset=" + offset);
                    }

                    return true;

                }

            } finally {

                release();

            }

        }

        /*
         * The record did not fit into the current buffer but it is small enough
         * to fit into an empty buffer. Grab the write lock and then try again.
         * If it still does not fit, then put the current buffer onto the dirty
         * list and take a buffer from the clean list and then write the record
         * onto that buffer while we are holding the lock. This last step must
         * succeed since the buffer will be empty and the record can fit into an
         * empty buffer.
         */
        {

            final Lock writeLock = lock.writeLock();

            writeLock.lockInterruptibly();

            try {

                /*
                 * While holding the write lock, see if the record can fit into
                 * the current buffer. Note that the buffer we acquire here MAY
                 * be a different buffer since a concurrent write could have
                 * already switched us to a new buffer. In that case, the record
                 * might fit into the new buffer.
                 */

                // Acquire a buffer. Maybe the same one, maybe different.
                WriteCache cache = acquire();

                try {

                    // While holding the write lock, see if the record fits.
                    if (cache.writeChk(offset, data, chk)) {

                        /*
                         * It fits: someone already changed to a new cache,
                         * which is fine.
                         */
                        if (recordMap.put(offset, cache) != null) {
                            // The record should not already be in the cache.
                            throw new AssertionError(
                                    "Record already in cache: offset=" + offset);
                        }

                        return true;

                    }

                    /*
                     * There is not enough room in the current buffer for this
                     * record, so put the buffer onto the dirty list. Then take
                     * a new buffer from the clean list (block), reset the
                     * buffer to clear the old writes, and set it as current. At
                     * that point, the record should always fit.
                     * 
                     * Note: When we take a cache instances from the cleanList
                     * we need to remove any entries in our recordMap which are
                     * in its record map.
                     * 
                     * Note: We move the current buffer to the dirty list before
                     * we take a buffer from the clean list. This is absolutely
                     * necessary since the code will otherwise deadlock if there
                     * is only one buffer.
                     * 
                     * Note: Do NOT yield the WriteLock here. That would make it
                     * possible for another thread to acquire() the current
                     * buffer, which has already been placed onto the dirtyList
                     * by this thread!!!
                     */

                    /*
                     * Move the current buffer to the dirty list.
                     * 
                     * Note: The lock here is required to give flush() atomic
                     * semantics with regard to the set of dirty write buffers
                     * when flush() gained the writeLock [in fact, we only need
                     * the dirtyListLock for the dirtyListEmpty Condition].
                     */
                    dirtyListLock.lockInterruptibly();
                    try {
                        dirtyList.add(cache);
                        dirtyListNotEmpty.signal();
                    } finally {
                        dirtyListLock.unlock();
                    }

                    // Take the buffer from the cleanList (blocks).
                    final WriteCache newBuffer = cleanList.take();

                    // Clear the state on the new buffer and remove from cacheService map
                    newBuffer.resetWith(recordMap);

                    // Set it as the new buffer.
                    current.set(cache = newBuffer);

                    // Try to write on the new buffer.
                    if (cache.writeChk(offset, data, chk)) {

                        // This must be the only occurrence of this record.
                        if (recordMap.put(offset, cache) != null) {
                            throw new AssertionError(
                                    "Record already in cache: offset=" + offset);
                        }

                        return true;

                    }

                    /*
                     * Should never happen.
                     */
                    throw new IllegalStateException("Unable to write into current WriteCache");

                } finally {

                    release();

                }

            } finally {
                writeLock.unlock();
            }

        }

    }
    
    /**
     * This is a non-blocking query of all write cache buffers (current, clean
     * and dirty).
     * <p>
     * This implementation DOES NOT throw an {@link IllegalStateException} for
     * an asynchronous close.
     */
    public ByteBuffer read(final long offset) throws InterruptedException {

        if (!open.get()) {

            // Not open.
            return null;
            
        }

        final Long off = Long.valueOf(offset);

        final WriteCache cache = recordMap.get(off);

        if (cache == null) {

            // No match.
            return null;

        }

        /*
         * Ask the cache buffer if it has the record still. It will not if the
         * cache buffer has been concurrently reset.
         */

        return cache.read(off);

    }

}
