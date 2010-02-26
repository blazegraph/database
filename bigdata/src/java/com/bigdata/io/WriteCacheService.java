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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
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

import org.apache.log4j.Logger;

import com.bigdata.io.WriteCache.RecordMetadata;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.Latch;

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
abstract public class WriteCacheService implements IWriteCache {

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
    final private ExecutorService writeService;

    /**
     * The {@link Future} of the task running on the {@link #writeService}.
     */
    final private Future<Void> writeFuture;

    /**
     * A list of dirty buffers. Writes from these may be combined, but not
     * across {@link #flush(boolean)}.
     */
    final protected BlockingQueue<WriteCache> dirtyList;

    /**
     * The deferredList is used when WriteCaches should be added to the dirtyList
     * but there is an ongoing {@link #flush(boolean)}. In this case the WriteCache is added to the deferredList
     * and when the flush is complete, any members are transferred in order to the
     * dirtyList.
     */
    final protected BlockingQueue<WriteCache> deferredDirtyList;

    final private Latch deferredLatch = new Latch();
    
    /**
     * A list of clean buffers. By clean, we mean not needing to be written.
     * Once a dirty write cache has been flushed, it is placed onto the
     * {@link #cleanList}. Once a buffer has been placed on the list a
     * low validation task will validate the buffer and move to the {@link #availList}.
     */
    final protected BlockingQueue<WriteCache> cleanList;

    /**
     * A list of validated buffers. Clean buffers are moved from the {@link #cleanList} to the
     * {@link #availList} once validated by read back. When the {@link #current} write cache buffer needs to
     * be replaced, one of the buffers from the {@link #availList} is recycled. If none are available
     * then a new buffer is created.
     */
    final protected BlockingQueue<WriteCache> availList;

    /**
     * The current buffer.
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
     * A map from the offset of the record on the backing file to the cache
     * buffer on which that record was written.
     */
    protected final ConcurrentMap<Long/* offset */, WriteCache> recordMap;

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
    public WriteCacheService(final int nbuffers,
            final IReopenChannel<? extends Channel> opener)
            throws InterruptedException {

        if (nbuffers <= 0)
            throw new IllegalArgumentException();

        if (opener == null)
            throw new IllegalArgumentException();

        this.opener = opener;

        dirtyList = new LinkedBlockingQueue<WriteCache>();
        deferredDirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingQueue<WriteCache>();
        availList = new LinkedBlockingQueue<WriteCache>();

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
        writeService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        // run the write task
        writeFuture = writeService.submit(new WriteTask());

    }

    /**
     * The task responsible for writing dirty buffers onto the backing channel.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class WriteTask implements Callable<Void> {

        public Void call() throws Exception {

            while (true) {

                try {

                    lock.readLock().lockInterruptibly(); // allows shutdown1

                    try {

                        if (dirtyList.isEmpty()) {
                        	continue;
                        }
                        
                        final WriteCache cache = dirtyList.take();
                        
                        cache.flush(false/* force */);

                        cleanList.add(cache);
                        
                        if (dirtyList.isEmpty()) {
                        	try {
                        		dllock.lock();
                                dirtyListEmpty.signalAll();
                        		
                        	} finally {
                        		dllock.unlock();
                        	}
                         }
                            
                    } finally {

                        lock.readLock().unlock();
                        
                    }
                    
                } catch (Throwable t) {

                    log.error(t, t);

                }

            }

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
     * call reset on all dirtList objects and move to cleanList
     * 
     * @throws InterruptedException 
     */
    public void resetAll() throws InterruptedException {
        final Lock readLock = lock.readLock();

        readLock.lockInterruptibly();

        try {

            for (WriteCache t1 : dirtyList) {

                t1.resetWith(recordMap);

            }

        	dirtyList.drainTo(cleanList);

        } finally {

            readLock.unlock();

        }
    }
    
    public void close() throws InterruptedException {

        if (open.compareAndSet(true/* expect */, false/* update */)) {

            try {

                // Interrupt the write task.
                writeFuture.cancel(true/* mayInterruptIfRunning */);

                // Immediate shutdown of the write service.
                writeService.shutdownNow();

            } finally {

                /*
                 * Ensure that the buffers are closed in a timely manner.
                 */

                final List<WriteCache> c = new LinkedList<WriteCache>();

                dirtyList.drainTo(c);

                final WriteCache t = current.getAndSet(null);

                if (t != null) {

                    c.add(t);

                }

                for (WriteCache t1 : c) {

                    t1.close();

                }

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
     * This latch tracks the number of operations acting on the {@link #current}
     * buffer. It is incremented by {@link #acquire()} and decremented by
     * {@link #release()}. The {@link #current} buffer can not be changed until
     * this latch reaches zero.
     */
    final private Latch latch = new Latch();

    /**
     * The read lock allows concurrent {@link #acquire()}s while the write lock
     * prevents {@link #acquire()} during critical sections such as
     * {@link #flush(boolean, long, TimeUnit)}.
     * <p>
     * Note: To avoid lock ordering problems, acquire the read lock before you
     * increment the latch and acquire the write lock before you await the
     * latch.
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Condition signaled by the {@link WriteTask} when the {@link #dirtyList}
     * becomes empty (but not until the last buffer drained from that list has
     * been written through to the backing channel).
     */
    final private ReentrantLock dllock = new ReentrantLock();
    final private Condition dirtyListEmpty = dllock.newCondition();
    
    /**
     * @throws IllegalStateException
     *             if the service is closed.
     * @throws IllegalStateException
     *             if the write task has failed.
     */
    protected void assertOpen() {

        if (!open.get())
            throw new IllegalStateException();

        if (writeFuture.isDone()) {

            /*
             * If the write task terminates abnormally then throw the exception
             * out here.
             */

            try {

                writeFuture.get();

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

        final Lock readLock = lock.readLock();

        readLock.lockInterruptibly();

        try {

            assertOpen();

            latch.inc();

            final WriteCache tmp = current.get();

            if (tmp == null) {

                latch.dec();

                throw new RuntimeException();

            }

            return tmp;

        } finally {

            readLock.unlock();

        }

    }

    /**
     * Release the latch on an acquired buffer.
     */
    private void release() {

        latch.dec();

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
     * Flush the current write set through to the backing channel.
     * 
     * @throws InterruptedException
     * 
     * @see #dirtyListEmpty
     * 
     *      FIXME This needs to create a definition of the write set by
     *      acquiring the write lock, moving the {@link #current} buffer to the
     *      {@link #dirtyList} and then waiting until the set of buffers on the
     *      dirty list as of that moment have been written to the backing
     *      channel. This will involve coordination with {@link WriteTask},
     *      which needs to notify us when buffers are written up. So the write
     *      task needs to acquire the read lock and we need a {@link Condition}
     *      for waking up here and checking if our write set has become empty.
     *      <p>
     *      IF we guarantee that the buffers are written out in order THEN we
     *      have satisfy this with a counter. We just note the current counter
     *      value while holding a lock and wait until the counter is incremented
     *      by the #of write blocks on the dirty list.
     *      <p>
     *      A somewhat safer alternative is to refuse to accept new buffers on
     *      the dirty list until the flush has been satisfied. This way, we just
     *      wait until the dirty list is empty, which is a simple
     *      {@link Condition}. The only drawback is that the cache will stop
     *      buffering writes as soon as the current buffer is full.
     *      <p>
     *      If we allow at most one outstanding flush, then we could introduce
     *      another list of deferred dirty buffers. During a flush, full buffers
     *      are transferred to the deferred list rather than the dirty list.
     *      This lets us cache ahead as long as we have free buffers. Once the
     *      flush is satisfied we need to transfer the buffers on the deferred
     *      list to the dirty list (in order) so that they can be written onto
     *      the channel.
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit units) throws TimeoutException, InterruptedException {
    	synchronized (this) {
        final Lock writeLock = lock.writeLock();
        boolean isLocked = false;
        writeLock.lockInterruptibly();
        isLocked = true;
        try {
        	deferredLatch.inc();
        	
            final WriteCache tmp = current.get();
            if (!tmp.isEmpty()) {
	            final WriteCache nxt = cleanList.take();
	            nxt.resetWith(recordMap);
	            current.set(nxt); // 
	            if (tmp == null) {
	            	throw new RuntimeException();
	            }
	            dirtyList.add(tmp);
            }
            
            // wait for dirtyList empty with signal from WriteTask
            
            try {
                writeLock.unlock();
                isLocked = false;
	            dllock.lockInterruptibly();
	            if (!dirtyList.isEmpty())
	            	dirtyListEmpty.await(timeout, units);
            } finally {
            	dllock.unlock();
            }
            
            // now check for deferredDirty, not allowing anything else to write to it!
            
            writeLock.lockInterruptibly();
            isLocked = true;
            if (log.isInfoEnabled())
            	log.info("deferredDirtyList.isEmpty: " + deferredDirtyList.isEmpty());
            deferredDirtyList.drainTo(dirtyList);
            
            if (true) {
            	((FileChannel) opener.reopenChannel()).force(force);
            }
            
                    	
            return true;
    	} catch (IOException e) {
			throw new RuntimeException(e); // force reopen
		} finally {
        	deferredLatch.dec();
        	if (isLocked)
        		writeLock.unlock();
    	}
    	}
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
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     */
    public boolean write(final long offset, final ByteBuffer data)
            throws InterruptedException, IllegalStateException {

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

        if (nbytes > capacity) {

            /*
             * Write the record onto the file at that offset. This operation is
             * synchronous. It will block until the record has been written.
             * 
             * @todo this should probably block if we are waiting for the dirty
             * list to become empty (e.g., a flush). i can't see why this would
             * matter much, but it seems better to give the disk over to the
             * flush rather than having a concurrent write here.
             */
            try {

                // A singleton map for that record.
                final Map<Long, RecordMetadata> recordMap = Collections
                        .singletonMap(offset, new RecordMetadata(offset,
                                0/* bufferOffset */, nbytes));

                // Write the record on the channel using write cache factory.
                newWriteCache(data, opener).writeOnChannel(data, recordMap,
                        Long.MAX_VALUE/* nanos */);
                
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
                if (cache.write(offset, data)) {

                    // Done.
                    recordMap.put(offset, cache);

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
                    if (cache.write(offset, data)) {

                        // It fits: someone already changed to a new cache.
                        recordMap.put(offset, cache);

                        return true;

                    }

                    /*
                     * There is not enough room in the current buffer for this
                     * record, so put the buffer onto the dirty list. Then take
                     * a new buffer from the clean list, reset the buffer to
                     * clear if
                     * 
                     * Note: When we take a cache instances from the cleanList
                     * we need to remove any entries in our recordMap which are
                     * in its record map.
                     * 
                     * Note: We move the current buffer to the dirty list before
                     * we take a buffer from the clean list. This is absolutely
                     * necessary since the code will otherwise deadlock if there
                     * is only one buffer.
                     */

                    final long permits = latch.get();

                    assert permits == 1 : "There are " + permits
                            + " outstanding permits (should be just one).";

                    // Move the current buffer to the dirty list.
                    if (deferredLatch.get() == 0)
                    	dirtyList.add(cache);
                    else 
                    	deferredDirtyList.add(cache);

                    // Take the first clean buffer (may block).
                    final WriteCache newBuffer = cleanList.take();

                    // Clear entries from our record map before reusing.                   
//                    {
//
//                        final Iterator<Map.Entry<Long, WriteCache>> itr = recordMap
//                                .entrySet().iterator();
//
//                        while (itr.hasNext()) {
//
//                            final Map.Entry<Long, WriteCache> e = itr.next();
//
//                            if (e.getValue() == newBuffer) {
//
//                                itr.remove();
//
//                            }
//
//                        }
//
//                    }

                    // Clear the state on the new buffer and remove from cacheService map
                    newBuffer.resetWith(recordMap);

                    // Set it as the new buffer.
                    current.set(cache = newBuffer);

                    // Try to write on the new buffer.
                    if (cache.write(offset, data)) {

                        // This should be the first record in the new cache.
                        recordMap.put(offset, cache);

                        return true;

                    }

                    /*
                     * Should never happen.
                     */
                    throw new RuntimeException();

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
     */
    public ByteBuffer read(final long offset) throws InterruptedException,
            IllegalStateException {

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
