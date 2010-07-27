package com.bigdata.io;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.TransientBufferStrategy;
import com.bigdata.rawstore.Bytes;

/**
 * An instance of this class manages a JVM-wide pool of direct (aka native)
 * {@link ByteBuffer}s. Methods are provided to acquire a {@link ByteBuffer}
 * from the pool and to release a {@link ByteBuffer} back to the pool.
 * <p>
 * Note: There is a bug in the release of large temporary direct
 * {@link ByteBuffer}s which motivates this class. For regular journals that
 * overflow, the write cache is allocated once and handed off from journal to
 * journal. However, there is no such opportunity for temporary stores.
 * Unfortunately it is NOT an option to simply disable the write cache for
 * temporary stores since NIO will allocate (and fail to release) an "temporary"
 * direct buffer for the operation which transfers the data from the
 * {@link TransientBufferStrategy} to disk. Therefore the data is copied into a
 * temporary buffer allocated from this pool and then the buffer is either
 * handed off to the {@link DiskOnlyStrategy} for use as its write cache (in
 * which case the {@link TemporaryRawStore} holds a reference to the buffer and
 * releases it back to those pool when it is finalized) or the buffer is
 * immediately released back to this pool.
 * <p>
 * Note: Precisely because of the bug which motivates this class, we DO NOT
 * release buffers back to the JVM. This means that the size of a
 * {@link DirectBufferPool} can only increase, but at least you get to (re-)use
 * the memory that you have allocated rather than leaking it to the native heap.
 * 
 * @see http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8f
 *      ab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo It should be possible to define a method which attempts to release
 *       direct buffers back to the JVM. The JVM can eventually GC them after a
 *       full GC, but often not in time to avoid an OOM error on the Java heap.
 */
public class DirectBufferPool {

    protected static final Logger log = Logger
            .getLogger(DirectBufferPool.class);

    /**
     * The name of the buffer pool.
     */
    final private String name;
    
    /**
     * Note: This is NOT a weak reference collection since the JVM will leak
     * native memory.
     */
    final private BlockingQueue<ByteBuffer> pool;

    /**
     * Used to recognize {@link ByteBuffer}s allocated by this pool so that
     * we can refuse offered buffers that were allocated elsewhere (a
     * paranoia feature which could be dropped).
     * <p>
     * Note: YOU CAN NOT use a hash-based collection here. hashCode() and
     * equals() for a {@link ByteBuffer} are very heavy operations that are
     * dependent on the data actually in the buffer at the time the
     * operation is evaluated!
     * <p>
     * Note: if you set [allocated := null] in the ctor then tests of the
     * allocated list are disabled.
     */
    final private List<ByteBuffer> allocated;

    /**
     * The number {@link ByteBuffer}s allocated (must use {@link #lock} for
     * updates or reads to be atomic).
     */
    private int size = 0;

    /**
     * The maximum #of {@link ByteBuffer}s that will be allocated. 
     */
    private final int poolCapacity;

    /**
     * The capacity of the {@link ByteBuffer}s managed by this pool.
     */
    private final int bufferCapacity;

    /**
     * Lock used to serialize access to the other fields on this class.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition used to await a buffer release.
     */
    private final Condition bufferRelease = lock.newCondition();

    /**
     * The name of this buffer pool instance.
     */
    public String getName() {
        return name;
    }
    
    /**
     * The capacity of the buffer as specified to the ctor.
     */
    public int getPoolCapacity() {

        return poolCapacity;

    }

    /**
     * The approximate #of {@link ByteBuffer}s currently managed by this
     * pool.
     */
    public int getPoolSize() {

        lock.lock();

        try {

            return size;

        } finally {

            lock.unlock();

        }

    }

    /**
     * The capacity of the {@link ByteBuffer}s managed by this pool as
     * specified to the ctor.
     */
    public int getBufferCapacity() {

        return bufferCapacity;

    }

    /**
     * Options for provisioning the <em>static</em> instance of the
     * {@link DirectBufferPool}.
     * <p>
     * Note: Since the {@link DirectBufferPool#INSTANCE} is static all of these
     * options MUST be specified on the command line using <code>-D</code> to
     * define the relevant property.
     * <p>
     * Note: The default configuration will never block but is not bounded in
     * how many buffers it will allocate.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * The capacity of the {@link DirectBufferPool} is the maximum #of
         * direct {@link ByteBuffer} instances that may reside in the pool
         * (default {@value #DEFAULT_POOL_CAPACITY}).
         * <p>
         * Note: Placing a limit on the pool size could cause threads to
         * deadlock awaiting a direct buffer from the pool. For this reason is
         * it good practice to use
         * {@link DirectBufferPool#acquire(long, TimeUnit)} with a timeout.
         */
        String POOL_CAPACITY = DirectBufferPool.class.getName()
                + ".poolCapacity";

        /**
         * The default pool capacity (no limit).
         */
        String DEFAULT_POOL_CAPACITY = "" + Integer.MAX_VALUE;

        /**
         * The capacity in bytes of the direct {@link ByteBuffer} instances
         * allocated and managed by the {@link DirectBufferPool} ({@link #DEFAULT_BUFFER_CAPACITY}).
         */
        String BUFFER_CAPACITY = DirectBufferPool.class.getName()
                + ".bufferCapacity";

        /**
         * The default capacity of the allocated buffers.
         */
        String DEFAULT_BUFFER_CAPACITY = "" + Bytes.megabyte32 * 1;
        
    }

    /**
     * A JVM-wide pool of direct {@link ByteBuffer}s used for a variety of
     * purposes with a default {@link Options#BUFFER_CAPACITY} of
     * <code>1 MB</code>.
     * <p>
     * Note: {@link #acquire()} requests will block once the pool capacity has
     * been reached until a buffer is {@link #release(ByteBuffer)}ed.
     */
    public final static DirectBufferPool INSTANCE;

    /**
     * A JVM-wide pool of direct {@link ByteBuffer}s with a default
     * {@link Options#BUFFER_CAPACITY} of <code>10 MB</code>. The main use case
     * for the 10M buffers are multi-block IOs for the {@link IndexSegment}s.
     */
    public final static DirectBufferPool INSTANCE_10M;

    /**
     * An unbounded list of all {@link DirectBufferPool} instances.
     */
    static private List<DirectBufferPool> pools = Collections
            .synchronizedList(new LinkedList<DirectBufferPool>()); 
    
    static {
        
        final int poolCapacity = Integer.parseInt(System.getProperty(
                Options.POOL_CAPACITY, Options.DEFAULT_POOL_CAPACITY));

        if(log.isInfoEnabled())
            log.info(Options.POOL_CAPACITY + "=" + poolCapacity);

        final int bufferCapacity = Integer.parseInt(System.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));

        if (log.isInfoEnabled())
            log.info(Options.BUFFER_CAPACITY + "=" + bufferCapacity);

        INSTANCE = new DirectBufferPool(//
                "default",//
                poolCapacity,//
                bufferCapacity//
                );

        INSTANCE_10M = new DirectBufferPool(//
                "10M",//
                Integer.MAX_VALUE, // poolCapacity
                10 * Bytes.megabyte32 // bufferCapacity
                );
            
            /*
             * This configuration will block if there is a concurrent demand for
             * more than [poolCapacity] buffers.
             * 
             * This is a pretty reasonable configuration for deployment. It will
             * serialize (or timeout) concurrent operations requiring more than
             * [poolCapacity] buffers in aggregate demand and each buffer is
             * modestly large so that the write cache will have good
             * performance. The total native memory demand for temporary stores
             * is capped at [poolCapacity * bufferCapacity] bytes, which is
             * again a reasonable value.
             */
//            100, // poolCapacity
//            1 * Bytes.megabyte32 // bufferCapacity
            
            /*
             * This configuration may be useful for stress testing.
             */
//            1, // poolCapacity
//            Bytes.kilobyte32

    }
    
    /**
     * Create a direct {@link ByteBuffer} pool.
     * <p>
     * Note: When the <i>poolSize</i> is bounded then {@link #acquire()} MAY
     * block. This can introduce deadlocks into the application. You can use a
     * timeout to limit the sensitivity to deadlocks or you can use an unbounded
     * pool and accept that {@link OutOfMemoryError}s will arise if there is
     * too much concurrent demand for the buffers supplied by this pool.
     * 
     * @param poolCapacity
     *            The maximum capacity of the pool. Use
     *            {@link Integer#MAX_VALUE} to have a pool with an unbounded
     *            number of buffers.
     * @param bufferCapacity
     *            The capacity of the {@link ByteBuffer}s managed by this pool.
     * 
     * @see #INSTANCE
     */
    protected DirectBufferPool(final String name, final int poolCapacity,
            final int bufferCapacity) {

        if (name == null)
            throw new IllegalArgumentException();
        
        if (poolCapacity <= 0)
            throw new IllegalArgumentException();

        if (bufferCapacity <= 0)
            throw new IllegalArgumentException();

        this.name = name;
        
        this.poolCapacity = poolCapacity;

        this.bufferCapacity = bufferCapacity;

        this.allocated = null; // Note: disables assertion
        //            this.allocated = new LinkedList<ByteBuffer>();

        this.pool = new LinkedBlockingQueue<ByteBuffer>(poolCapacity);

        pools.add(this);
        
    }

    /**
     * Return a direct {@link ByteBuffer}. The capacity of the buffer is
     * determined by the configuration of this pool. The position will be equal
     * to zero, the limit will be equal to the capacity, and the mark will not
     * be set.
     * <p>
     * Note: This method will block if there are no free buffers in the pool and
     * the pool was configured with a maximum capacity. In addition it MAY block
     * if there is not enough free memory to fulfill the request. It WILL log an
     * error if it blocks. While blocking is not very safe, using a heap
     * ByteBuffer is not very safe either since Java NIO will allocate a
     * temporary direct {@link ByteBuffer} for IOs and that can both run out of
     * memory and leak memory.
     * 
     * @return A direct {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     *             if the caller's {@link Thread} is interrupted awaiting a
     *             buffer.
     * @throws TimeoutException
     */
    public ByteBuffer acquire() throws InterruptedException {

        try {
            
            return acquire(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            
        } catch (TimeoutException e) {
            
            // The TimeoutException should not be thrown.
            throw new AssertionError(e);
            
        }

    }

    /**
     * Return a direct {@link ByteBuffer}. The capacity of the buffer is
     * determined by the configuration of this pool. The position will be equal
     * to zero, the limit will be equal to the capacity, and the mark will not
     * be set.
     * <p>
     * Note: This method will block if there are no free buffers in the pool and
     * the pool was configured with a maximum capacity. In addition it MAY block
     * if there is not enough free memory to fulfill the request. It WILL log an
     * error if it blocks. While blocking is not very safe, using a heap
     * ByteBuffer is not very safe either since Java NIO will allocate a
     * temporary direct {@link ByteBuffer} for IOs and that can both run out of
     * memory and leak memory.
     * 
     * @param timeout
     * @param unit
     * 
     * @return A direct {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public ByteBuffer acquire(final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException {

        if(log.isInfoEnabled())
            log.info("");

        lock.lock();

        try {

            if (pool.isEmpty()) {

                allocate(timeout, unit);

            }

            // the head of the pool must exist.
            final ByteBuffer b = pool.take();

            assertOurBuffer(b);

            // limit -> capacity; pos-> 0; mark cleared.
            b.clear();

            return b;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Release a direct {@link ByteBuffer} allocated by this pool back to
     * the pool.
     * 
     * @param b
     *            The buffer.
     *            
     * @throws InterruptedException
     */
    public void release(final ByteBuffer b) throws InterruptedException {

        if (!release(b, Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {

            throw new AssertionError();
            
        }

    }

    public boolean release(final ByteBuffer b, long timeout, TimeUnit units)
            throws InterruptedException {

        if(log.isInfoEnabled())
            log.info("");

        if (b == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {

            assertOurBuffer(b);

            // add to the pool.
            if(!pool.offer(b, timeout, units))
                return false;

            /*
             * Signal ONE thread that there is a buffer available.
             * 
             * Note: There is the potential for this signal to be missed if
             * the thread waiting in [allocate] allows itself to be
             * interrupted once the signal has arrived and before it has
             * returned to the caller. Once the buffer is in the caller's
             * hands it is up to the caller to ensure that it is released
             * back to the pool, e.g., in finalize().
             * 
             * Note: Another way to handle this is to signalAll() and then
             * have the thread check to see if the pool is empty and restart
             * its wait (after subtracting out the time already elapsed). This
             * is doubtless more robust.
             */
            bufferRelease.signal();

            return true;
            
        } finally {

            lock.unlock();

        }

    }

    /**
     * Attempts to allocate another direct {@link ByteBuffer}. If
     * successful then it will add the buffer to the {@link #pool}.
     * 
     * @throws InterruptedException 
     * @throws TimeoutException 
     * 
     * @throws
     */
    private void allocate(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {

        assert lock.isHeldByCurrentThread();

        if(log.isInfoEnabled())
            log.info("");

        try {

            if (size >= poolCapacity) {

                /*
                 * Wait for a free buffer since the pool is at its capacity.
                 */

                log.error("Pool is at capacity - waiting for a free buffer");

                awaitFreeBuffer(timeout, unit);

            }

            // allocate a buffer
            final ByteBuffer b = ByteBuffer.allocateDirect(bufferCapacity);

            // update the pool size.
            size++;

            // add to the set of known buffers
            if (allocated != null) {

                allocated.add(b);

            }

            // add to the pool.
            pool.add(b);

            /*
             * There is now a buffer in the pool and the caller will get it
             * since they hold the lock.
             */

            return;

        } catch (OutOfMemoryError err) {


            log.error("Not enough native memory - will await a free buffer: "
                    + err, err);

            awaitFreeBuffer(timeout, unit);

        }

    }

    private void awaitFreeBuffer(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {

        // await a buffer to appear in the pool.
        if (!bufferRelease.await(timeout, unit)) {

            throw new TimeoutException();

        }

        /*
         * There is now a buffer in the pool and the caller will get it
         * since they hold the lock.
         * 
         * Note: From here until the caller gets the buffer either (a)
         * do NOT allow the current thread to be interrupted. Failure to
         * adhere to this advice can result in a buffer remaining the
         * pool but no thread awaiting that released buffer noticing it
         * there!
         */

        assert !pool.isEmpty();

        return;

    }

    /**
     * Note: There is really no reason why we could not accept "donated" direct
     * {@link ByteBuffer}s as long as they conform with the constraints on the
     * {@link DirectBufferPool}.
     * 
     * @param b
     */
    private void assertOurBuffer(ByteBuffer b) {

        assert lock.isHeldByCurrentThread();

        if (b == null)
            throw new IllegalArgumentException("null reference");

        if (b.capacity() != bufferCapacity)
            throw new IllegalArgumentException("wrong capacity");

        if(!b.isDirect())
            throw new IllegalArgumentException("not direct");
        
        if (allocated == null) {

            // test is disabled.

            return;

        }

        for (ByteBuffer x : allocated) {

            if (x == b)
                return;

        }

        throw new IllegalArgumentException("Buffer not allocated by this pool.");

    }

    /**
     * Return the {@link CounterSet} for the {@link DirectBufferPool}.
     * 
     * @return The counters.
     */
    static public CounterSet getCounters() {

        final CounterSet tmp = new CounterSet();

        // #of buffer pools.
        int bufferPoolCount = 0;
        // #of buffers currently allocated across all buffer pools.
        int bufferInUseCount = 0;
        // #of bytes currently allocated across all buffer pools.
        final AtomicLong totalBytesUsed = new AtomicLong(0L);
        // For each buffer pool.
        for (DirectBufferPool p : pools) {
            
            final CounterSet c = tmp.makePath(p.getName());
            
            final int poolSize = p.getPoolSize();
            
            final int poolCapacity = p.getPoolCapacity();
            
            final int bufferCapacity = p.getBufferCapacity();
            
            final long bytesUsed = poolSize * bufferCapacity;
            
            bufferPoolCount++;
            bufferInUseCount += poolSize;
            totalBytesUsed.addAndGet(bytesUsed);

            c.addCounter("poolCapacity", new OneShotInstrument<Integer>(
                    poolCapacity));

            c.addCounter("bufferCapacity", new OneShotInstrument<Integer>(
                    bufferCapacity));

            c.addCounter("poolSize", new Instrument<Integer>() {
                public void sample() {
                    setValue(poolSize);
                }
            });

            /*
             * #of bytes allocated and held by the DirectBufferPool.
             */
            c.addCounter("bytesUsed", new Instrument<Long>() {
                public void sample() {
                    setValue(bytesUsed);
                }
            });
        
        } // next DirectBufferPool

        /*
         * Totals.
         */
        
        tmp.addCounter("bufferPoolCount", new OneShotInstrument<Integer>(
                bufferPoolCount));

        tmp.addCounter("bufferInUseCount", new OneShotInstrument<Integer>(
                bufferInUseCount));

        tmp.addCounter("totalBytesUsed", new Instrument<Long>() {
            public void sample() {
                setValue(totalBytesUsed.get());
            }
        });

        return tmp;

    }
    
}
