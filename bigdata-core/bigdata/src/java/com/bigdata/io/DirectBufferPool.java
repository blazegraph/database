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

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.TransientBufferStrategy;
import com.bigdata.util.Bytes;

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
 * handed off to the {@link IBufferStrategy} for use as its write cache (in
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

    private static final Logger log = Logger
            .getLogger(DirectBufferPool.class);

    /**
     * Object tracking state for allocated buffer instances. This is used to
     * reject double-release of a buffer back to the pool, which is critical.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class BufferState implements IBufferAccess {

        /**
         * The buffer instance. This is guarded by the monitor of the buffer
         * state object (changes to this field are made while holding the
         * monitor of the {@link BufferState} object).  However, the field is
         * marked as <code>volatile</code> so we can peek at it without holding
         * the monitor's lock, e.g., in {@link #toString()}
         */
        private volatile ByteBuffer buf;

        /**
         * The stack trace where the {@link ByteBuffer} was acquired IFF DEBUG
         * and otherwise <code>null</code>.
         */
        private final Throwable allocationStack;

        /**
         * The stack trace where the {@link ByteBuffer} was released IFF DEBUG
         * and otherwise <code>null</code>. This is guarded by the monitor of
         * the {@link BufferState} object.
         */
        private Throwable releaseStack;

        /**
         * This is set to <code>true</code> iff the buffer is released by 
         * {@link #finalize()}.  In this case, we do not treat an invocation
         * of {@link #release(long, TimeUnit)} <i>after</i> the finalizer has
         * run as a double-release. (Yes, this situation can arise...).
         */
        private boolean releasedByFinalizer = false;
        
        BufferState(final ByteBuffer buf) {

            if (buf == null)
                throw new IllegalArgumentException();
            
            this.buf = buf;
            
            this.allocationStack = (DEBUG ? new RuntimeException("Allocation")
                    : null);
            
        }
        
        /**
         * The hash code depends only on the object id (NOT the buffer's data).
         * <p>
         * Note: {@link ByteBuffer#hashCode()} is a very heavy operator whose
         * result depends on the data actually in the buffer at the time the
         * operation is evaluated!
         */
        public int hashCode() {

            return super.hashCode();
            
        }

        /**
         * Equality depends only on a reference checks.
         * <p>
         * Note: {@link ByteBuffer#equals(Object)} is very heavy operator whose
         * result depends on the data actually in the buffer at the time the
         * operation is evaluated!
         */
        public boolean equals(final Object o) {
            if (this == o) {
                // Same BufferState, must be the same buffer.
                return true;
            }
            if (!(o instanceof BufferState)) {
                return false;
            }
            if (this.buf == ((BufferState) o).buf) {
                /*
                 * We have two distinct BufferState references for the same
                 * ByteBuffer reference. This is an error. There should be a
                 * one-to-one correspondence.
                 */
                throw new AssertionError();
            }
            return false;
        }

        public String toString() {
            final ByteBuffer tmp = this.buf;
            return super.toString() + "{buf"
                    + (tmp == null ? "N/A" : "buf.capacity=" + buf.capacity())
                    + "}";
        }
        
        // Implement IDirectBuffer methods
        public ByteBuffer buffer() {
            synchronized (this) {
                if (buf == null)
                    throw new IllegalStateException();
			    return buf;
			}
		}

		public void release() throws InterruptedException {
			release(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		}

        public void release(final long timeout, final TimeUnit units)
                throws InterruptedException {

            /*
             * Note: There is a data race between finalizers whose call graphs
             * wind up requesting the release of the buffer. This occurs when a
             * buffer user, such as a Journal, is being finalized. At that time,
             * the IBufferAccess object will also be finalizable. Java does not
             * define the ordering of those finalizer invocations so either the
             * Journal may release() the IBufferState explicitly first or the
             * IBufferState#finalized() method may run first. In either case we
             * go through a synchronized(this) block, return immediately if the
             * [buf] reference has already been cleared and otherwise return the
             * buffer to the pool and clear the [buf] reference to null.
             */
            synchronized (this) {
                if (buf == null) {
                    if(releasedByFinalizer) {
                        /*
                         * This situation can arise.  Just return quietly.
                         */
                        return;
                    }
                    if (DEBUG) {
                        log.error("Double release: AllocationTrace",
                                allocationStack);
                        if (releaseStack == null)
                            log
                                    .error("Double release: FirstReleaseStack NOT available");
                        else
                            log.error("Double release: FirstReleaseStack: "
                                    + releaseStack, releaseStack);
                        log.error("Double release: DoubleReleaseStack",
                                new RuntimeException("DoubleReleaseStack"));
                    }
                    return;
                }
                DirectBufferPool.this.release(buf, timeout, units);
                buf = null;
                if (DEBUG) {
                    /*
                     * The stack frame where the ByteBuffer was released.
                     */
                    releaseStack = new RuntimeException("ReleaseTrace");
                }
            } // synchronized(this)
            
		}// release(timeout,unit)

        /*
         * Note: It is apparent that the JVM can order things such that the
         * finalize() method can be called before an invocation of release() on
         * the BufferState object. Presumably this arises when a set of
         * references are all due to be finalized because none of them are more
         * than weakly reachable from a GC root. At that point, the JVM is faced
         * with the (impossible) task of ordering their finalizer() invocations.
         * Since object references are (in general) an undirected graph, it
         * seems that Java will invoke the finalizers on those references in
         * some undefined (and perhaps not definable) order. This can lead to a
         * "double-release" situation where the first release was the JVM
         * invoking the finalizer and the second release was a different
         * finalizer invoking release() on this BufferState object.
         * 
         * Given this state of affairs, the "right" thing to do is write the
         * finalizer defensively for a concurrent environment. It should
         * atomically release the buffer back to the pool and clear the buffer
         * reference. We should then ignore the double-release request rather
         * than throwing out an IllegalStateException.
         * 
         * This appears to be the right thing to do if the application holds a
         * hard reference to the BufferState object until it has release()ed the
         * buffer. If the application fails to hold that hard reference, then
         * putting the ByteBuffer back on the pool will cause concurrent data
         * modification problems within the ByteBuffer. Applications MUST verify
         * that they correctly hold that hard reference!
         */
		protected void finalize() throws Throwable {
            /*
             * Ultra paranoid block designed to ensure that we do not double
             * release the ByteBuffer to the owning pool via the action of
             * another finalized.
             */
		    final ByteBuffer buf;
		    final int nacquired;
		    synchronized(this) {
                buf = this.buf;
                this.buf = null; // NB: Ignore FindBugs warning on this line!!!
                nacquired = DirectBufferPool.this.acquired;
                if (buf != null) {
                    releasedByFinalizer = true;
                    if (releaseStack == null) {
                        releaseStack = new RuntimeException(
                                "ReleasedInFinalizer");
                    }
                }
            }
            if (buf == null)
                return;
            if (DEBUG) {
                /*
                 * Note: This code path WILL NOT return the buffer to the pool.
                 * This is deliberate. When DEBUG is true we do not permit a
                 * buffer which was not correctly release to be reused.
                 * 
                 * Note: A common cause of this is that the caller is holding
                 * onto the acquired ByteBuffer object rather than the
                 * IBufferAccess object. This permits the IBufferAccess
                 * reference to be finalized. When the IBufferAccess object is
                 * finalized, it will attempt to release the buffer (except in
                 * DEBUG mode). However, if the caller is holding onto the
                 * ByteBuffer then this is an error which can rapidly lead to
                 * corrupt data through concurrent modification (what happens is
                 * that the ByteBuffer is handed out to another thread in
                 * response to another acquire() and we now have two threads
                 * using the same ByteBuffer, each of which believes that they
                 * "own" the reference).
                 */
                leaked.increment();
                final long nleaked = leaked.get();
                log.error("Buffer release on finalize (nacquired=" + nacquired
                        + ",nleaked=" + nleaked + "): AllocationStack",
                        allocationStack);
            } else {
//                log.error("Buffer release on finalize."); // NB: NOT an error.
                /*
                 * TODO We do not currently set this.buf = buf if we are
                 * interrupted in release(buf) here, so this is not acid. But
                 * maybe we should accept the memory leak on that code path?
                 */
                DirectBufferPool.this.release(buf);
            }
        }
        
    }
    
    /**
     * The name of the buffer pool.
     */
    final private String name;

    /**
     * A pool of direct {@link ByteBuffer}s which may be acquired.
     * <p>
     * Note: This is NOT a weak reference collection since the JVM will leak
     * native memory.
     */
    final private BlockingQueue<ByteBuffer> pool;

    /**
     * The number {@link ByteBuffer}s allocated (must use {@link #lock} for
     * updates or reads to be atomic). This counter is incremented each time a
     * buffer is allocated. Since we do not free buffers when they are released
     * (to prevent an effective JVM memory leak) this counter is never
     * decremented.
     */
    private int size = 0;

    /**
     * The #of {@link ByteBuffer}s which are currently acquired (must use
     * {@link #lock} for updates or reads to be atomic). This counter is
     * incremented when a buffer is acquired and decremented when a buffer
     * is released.
     */
    private int acquired = 0;

    /**
     * The #of buffers leaked out of {@link BufferState#finalize()} when
     * {@link #DEBUG} is <code>true</code>.
     */
    private final CAT leaked = new CAT();
    
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
     * Package private counter of the total #of acquired buffers in all pools.
     * This is used to check for memory leaks in the test suites. The value is
     * reset before/after each test.
     */
    static final CAT totalAcquireCount = new CAT();
    static final CAT totalReleaseCount = new CAT();
    
    /**
     * The name of this buffer pool instance.
     */
    public String getName() {
        return name;
    }

    /**
     * The #of {@link ByteBuffer}s which are currently acquired. This counter is
     * incremented when a buffer is acquired and decremented when a buffer is
     * released.
     */
    public int getAcquiredBufferCount() {
        
        lock.lock();

        try {

            return acquired;

        } finally {

            lock.unlock();

        }
        
    }
    
    /**
     * The maximum capacity in buffers of the {@link DirectBufferPool} as
     * specified to the constructor.
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
     * The capacity in bytes of the {@link ByteBuffer}s managed by this pool as
     * specified to the constructor.
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

        /**
         * Option to use conservative assumptions about buffer release and to
         * report allocation stack traces for undesired events (double-release,
         * never released, etc).
         */
        String DEBUG = DirectBufferPool.class.getName() + ".debug";

        String DEFAULT_DEBUG = "false";
        
    }

    /**
     * @see Options#DEBUG
     */
    private final static boolean DEBUG;
    
    /**
     * A JVM-wide pool of direct {@link ByteBuffer}s used for a variety of
     * purposes with a default {@link Options#BUFFER_CAPACITY} of
     * <code>1 MB</code>.
     * <p>
     * Note: {@link #acquire()} requests will block once the pool capacity has
     * been reached until a buffer is {@link #release(ByteBuffer)}ed.
     */
    public final static DirectBufferPool INSTANCE;

//    /**
//     * A JVM-wide pool of direct {@link ByteBuffer}s with a default
//     * {@link Options#BUFFER_CAPACITY} of <code>10 MB</code>. The main use case
//     * for the 10M buffers are multi-block IOs for the {@link IndexSegment}s.
//     */
//    public final static DirectBufferPool INSTANCE_10M;

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

        DEBUG = Boolean.valueOf(System.getProperty(Options.DEBUG,
                Options.DEFAULT_DEBUG));
        
        INSTANCE = new DirectBufferPool(//
                "default",//
                poolCapacity,//
                bufferCapacity//
                );
        
//        INSTANCE_10M = new DirectBufferPool(//
//                "10M",//
//                Integer.MAX_VALUE, // poolCapacity
//                10 * Bytes.megabyte32 // bufferCapacity
//                );
            
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

        this.pool = new LinkedBlockingQueue<ByteBuffer>(poolCapacity);
        
        pools.add(this);
        
    }

    /**
     * Return an {@link IBufferAccess} wrapping a direct {@link ByteBuffer}. The
     * capacity of the buffer is determined by the configuration of this pool.
     * The position will be equal to zero, the limit will be equal to the
     * capacity, and the mark will not be set.
     * <p>
     * Note: This method will block if there are no free buffers in the pool and
     * the pool was configured with a maximum capacity. It WILL log an error if
     * it blocks. While blocking is not very safe, using a heap (vs direct)
     * {@link ByteBuffer} is not very safe either since Java NIO will allocate a
     * temporary direct {@link ByteBuffer} for IOs and that can both run out of
     * memory and leak memory.
     * 
     * @return A direct {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     *             if the caller's {@link Thread} is interrupted awaiting a
     *             buffer.
     * @throws OutOfMemoryError
     *             if there is not enough free memory to fulfill the request.
     */
    public IBufferAccess acquire() throws InterruptedException {

        try {
            
            return acquire(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            
        } catch (TimeoutException e) {
            
            // The TimeoutException should not be thrown.
            throw new AssertionError(e);
           
        }

    }

    /**
     * Return an {@link IBufferAccess} wrapping a direct {@link ByteBuffer}. The
     * capacity of the buffer is determined by the configuration of this pool.
     * The position will be equal to zero, the limit will be equal to the
     * capacity, and the mark will not be set.
     * <p>
     * Note: This method will block if there are no free buffers in the pool and
     * the pool was configured with a maximum capacity. In addition it MAY block
     * if there is not enough free memory to fulfill the request. It WILL log an
     * error if it blocks. While blocking is not very safe, using a heap (vs
     * direct) {@link ByteBuffer} is not very safe either since Java NIO will
     * allocate a temporary direct {@link ByteBuffer} for IOs and that can both
     * run out of memory and leak memory.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The units for that timeout.
     * 
     * @return A direct {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     *             if the caller's {@link Thread} is interrupted awaiting a
     *             buffer.
     * @throws TimeoutException
     */
    public IBufferAccess acquire(final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException {

//        if(log.isInfoEnabled())
//            log.info("");

        lock.lock();

        try {

            if (pool.isEmpty()) {

                allocate(timeout, unit);

            }

            // the head of the pool must exist.
            final ByteBuffer buf = pool.take();

            acquired++;
            totalAcquireCount.increment();

            // limit -> capacity; pos-> 0; mark cleared.
            buf.clear();

            if (log.isTraceEnabled()) {
                final Throwable t = new RuntimeException(
                        "Stack trace of buffer acquisition");
                log.trace(t, t);
            }
            
            return new BufferState(buf);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Release a direct {@link ByteBuffer} allocated by this pool back to the
     * pool.
     * 
     * @param b
     *            The buffer.
     * 
     * @throws IllegalArgumentException
     *             if the buffer is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the buffer does not belong to this pool.
     * @throws IllegalArgumentException
     *             if the buffer has already been released.
     * @throws InterruptedException
     */
    final private void release(final ByteBuffer b) throws InterruptedException {

        if (!release(b, Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {

            throw new AssertionError();
            
        }

    }

    /**
     * Release a direct {@link ByteBuffer} allocated by this pool back to the
     * pool.
     * 
     * @param b
     *            The buffer.
     * 
     * @throws IllegalArgumentException
     *             if the buffer is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the buffer does not belong to this pool.
     * @throws IllegalArgumentException
     *             if the buffer has already been released.
     * @throws InterruptedException
     */
    final private boolean release(final ByteBuffer b, final long timeout,
            final TimeUnit units) throws InterruptedException {

//        if(log.isInfoEnabled())
//            log.info("");

        if (b == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {
            // add to the pool.
            if(!pool.offer(b, timeout, units))
                return false;

            acquired--;
            totalReleaseCount.increment();
            
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

            if (log.isTraceEnabled()) {
                final Throwable t = new RuntimeException(
                        "Stack trace of buffer release");
                log.trace(t, t);
            }

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

        if(log.isDebugEnabled())
            log.debug("");

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


            // add to the pool.
            pool.add(b);
            
            /*
             * There is now a buffer in the pool and the caller will get it
             * since they hold the lock.
             */

            return;

        } catch (OutOfMemoryError err) {

            /*
             * Note: It is dangerous wait if the JVM is out of memory since this
             * could deadlock even when there is an unlimited capacity on the
             * pool. It is much safer to throw out an exception.
             */

//            log.error("Not enough native memory - will await a free buffer: "
//                    + err, err);
//
//            awaitFreeBuffer(timeout, unit);
            throw err;
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
     * Return the {@link CounterSet} for the {@link DirectBufferPool}.
     * <dl>
     * <dt>poolSize</dt>
     * <dd>The approximate number of direct {@link ByteBuffer}s currently
     * managed by the pool.</dd>
     * <dt>poolCapacity</dt>
     * <dd>The maximum number of direct {@link ByteBuffer}s that may be
     * allocated by the pool.</dd>
     * <dt>bufferCapacity</dt>
     * <dd>The capacity in bytes of each direct {@link ByteBuffer} associated
     * with a given pool.</dd>
     * <dt>acquired</dt>
     * <dd>The number of direct {@link ByteBuffer}s currently acquired by the
     * application.</dd>
     * <dt>bytesUsed</dt>
     * <dd>The number of bytes managed by the pool.</dd>
     * </dl>
     * 
     * @return The counters.
     */
    static public CounterSet getCounters() {

        final CounterSet tmp = new CounterSet();

        // #of buffer pools.
        int bufferPoolCount = 0;
        // #of buffers currently allocated across all buffer pools.
        int bufferInUseCount = 0;
        // #of buffers currently acquired across all buffer pools.
        int totalAcquired = 0;
        // #of bytes currently allocated across all buffer pools.
        final AtomicLong totalBytesUsed = new AtomicLong(0L);
        // For each buffer pool.
        for (DirectBufferPool p : pools) {
            
            final CounterSet c = tmp.makePath(p.getName());
            
            final int poolSize = p.getPoolSize();
            
            final int poolCapacity = p.getPoolCapacity();
            
            final int bufferCapacity = p.getBufferCapacity();

            final int acquired = p.getAcquiredBufferCount();

            final long nleaked = p.leaked.get();
            
            final long bytesUsed = poolSize * bufferCapacity;
            
            bufferPoolCount++;
            bufferInUseCount += poolSize;
            totalAcquired += acquired;
            totalBytesUsed.addAndGet(bytesUsed);

            c.addCounter("poolCapacity", new OneShotInstrument<Integer>(
                    poolCapacity));

            c.addCounter("bufferCapacity", new OneShotInstrument<Integer>(
                    bufferCapacity));

            c.addCounter("acquired", new OneShotInstrument<Integer>(acquired));

            c.addCounter("leaked", new OneShotInstrument<Long>(nleaked));
        
            c.addCounter("poolSize", new OneShotInstrument<Integer>(poolSize));

            /*
             * #of bytes allocated and held by the DirectBufferPool.
             */
            c.addCounter("bytesUsed", new OneShotInstrument<Long>(bytesUsed));
        
        } // next DirectBufferPool

        /*
         * Totals.
         */

        tmp.addCounter("totalAcquired", new OneShotInstrument<Integer>(
                totalAcquired));

        tmp.addCounter("bufferPoolCount", new OneShotInstrument<Integer>(
                bufferPoolCount));

        tmp.addCounter("bufferInUseCount", new OneShotInstrument<Integer>(
                bufferInUseCount));

        tmp.addCounter("totalBytesUsed", new OneShotInstrument<Long>(
                totalBytesUsed.get()));

        return tmp;

    }
    
}
