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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bigdata.btree.IndexSegmentBuilder;
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
 *       concurrent read or write operations on the file.
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
 */
abstract public class WriteCacheService implements IWriteCache {

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
     * A list of dirty buffers. Writes from these may be combined, but not
     * across {@link #flush(boolean)}.
     */
    final BlockingQueue<WriteCache> dirtyList;

    /**
     * A list of clean buffers. By clean, we mean not needing to be written.
     * Once a dirty write cache has been flushed, it is placed onto the
     * {@link #cleanList}. When the {@link #current} write cache buffer needs to
     * be replaced, one of the buffers from the {@link #cleanList} is recycled.
     */
    final BlockingQueue<WriteCache> cleanList;
    
    /**
     * The current buffer. 
     */
    final AtomicReference<WriteCache> current = new AtomicReference<WriteCache>();

    /**
     * Allocates N buffers from the {@link DirectBufferPool}.
     * 
     * @param nbuffers
     *            The #of buffers to allocate.
     *            
     * @throws InterruptedException
     */
    public WriteCacheService(final int nbuffers) throws InterruptedException {

        writeService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        dirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingQueue<WriteCache>();

        for (int i = 0; i < nbuffers - 1; i++) {

            cleanList.add(newWriteCache());

        }
        
        current.set(newWriteCache());
        
    }

    abstract protected WriteCache newWriteCache() throws InterruptedException;

    public boolean isOpen() {
        return open.get();
    }

    public void close() throws InterruptedException {

        if (open.compareAndSet(true/* expect */, false/* update */)) {

            try {

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
     * @throws IllegalStateException
     *             if the service is closed.
     */
    protected void assertOpen() {
        if (!open.get())
            throw new IllegalStateException();
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
     * 
     * @todo could dynamically grow the #of buffers here.
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
     * @todo This needs to create a definition of the write set by moving the
     *       {@link #current} buffer to the {@link #dirtyList} and then waiting
     *       until the set of buffers on the dirty list as of that moment have
     *       been written to the backing channel. This will involve coordination
     *       with {@link #writeOnChannel(WriteCache, ByteBuffer, Map, long)} and
     *       the use of the write lock.
     *       <p>
     *       If a buffer is available on the {@link #cleanList} then it should
     *       be taken and set on {@link #current}. Otherwise, the first buffer
     *       to get written out by
     *       {@link #writeOnChannel(WriteCache, ByteBuffer, Map, long)} must be
     *       set on {@link #current}. The write lock must be held until
     *       {@link #current} is non-null. It can be released after that so
     *       threads waiting on {@link #acquire()} to read/write/update can
     *       proceed.
     *       <p>
     *       Speaking of update, update can not be permitted for a buffer which
     *       is being flush()ed. write() always writes on the {@link #current}
     *       buffer, but {@link #update(long, int, ByteBuffer)} can write on a
     *       dirty buffer UNLESS it is actively being written out. Thus we need
     *       one more collection of buffers not available for update but which
     *       are still dirty.
     *       <p>
     *       When an update comes through for a record on a buffer which can not
     *       be updated in place, then we should copy the old record from the
     *       buffer where it exists into the {@link #current} buffer and update
     *       it there. This will save us a disk read.
     *       <p>
     *       Even better, {@link #update(long, int, ByteBuffer)} could be
     *       completely unnecessary, even for the {@link RWStore}. It was
     *       developed to support double-linked leaves in the
     *       {@link IndexSegmentBuilder} and we are doing that differently. For
     *       the {@link RWStore}, we actually write a new record and schedule
     *       the old one for delete.
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit units) throws TimeoutException, InterruptedException {

        throw new UnsupportedOperationException();
        
    }

    public boolean write(long addr, ByteBuffer data)
            throws InterruptedException, IllegalStateException {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @todo This must search {@link #current} and also the {@link #dirtyList}.
     *       That search should be fast, so maybe maintain an addrMap here whose
     *       values are {@link WriteCache} references. The entries for a given
     *       {@link WriteCache} must be cleared from the map before it can be
     *       made available again.
     */
    public ByteBuffer read(long addr) throws InterruptedException,
            IllegalStateException {
        throw new UnsupportedOperationException();
    }

}
