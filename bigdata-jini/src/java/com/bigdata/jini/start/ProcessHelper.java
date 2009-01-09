package com.bigdata.jini.start;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * Helper object for a running {@link Process} that DOES NOT require any input.
 * The output of the process will be logged, but not otherwise processed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProcessHelper {

    protected static final Logger log = Logger.getLogger(ProcessHelper.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * A useful name for the process.
     */
    public final String name;
    
    final IServiceListener listener;

    /**
     * The {@link Process}.
     */
    private final Process process;
    
    private final ReentrantLock lock = new ReentrantLock();
    
    private final Condition dead = lock.newCondition();
    
    private final AtomicInteger exitValue = new AtomicInteger(-1);

    public String toString() {
        
        final int exitValue = this.exitValue.get();
        
        return getClass().getSimpleName() + "{name=" + name
                + (exitValue != -1 ? ", exitValue=" + exitValue : "") + "}";
        
    }
    
//    /**
//     * Return <code>true</code> iff the process is still executing.
//     */
//    public boolean isRunning() {
//
//        lock.lock();
//
//        try {
//
//            return exitValue.get() != -1;
//
//        } finally {
//
//            lock.unlock();
//
//        }
//
//    }
    
    /**
     * Await the exit value and return it when it becomes available.
     * 
     * @return The exit value.
     * 
     * @throws InterruptedException
     *             if the caller's thread is interrupted while awaiting the
     *             process exit value.
     */
    public int exitValue() throws InterruptedException {

        try {

            return exitValue(Long.MAX_VALUE, TimeUnit.SECONDS);
            
        } catch(TimeoutException ex) {
            
            // Note: SHOULD NOT throw a TimeoutException!
            throw new AssertionError();
            
        }
        
    }
        
    /**
     * Waits up to timeout units for the {@link Process} to terminate and then
     * returns its exit value.
     * 
     * @param timeout
     * @param unit
     * @return The exit value.
     * 
     * @throws TimeoutException
     *             if the {@link Process} is still running when the specified
     *             timeout elapsed.
     * @throws InterruptedException
     *             if the caller's thread was interrupted awaiting the exit
     *             value.
     */
    public int exitValue(long timeout, final TimeUnit unit)
            throws TimeoutException, InterruptedException {

        final long begin = System.nanoTime();

        timeout = unit.toNanos(timeout);

        lock.lock();

        try {

            /*
             * Note: We will always execute the loop at least once and will
             * return the exitValue if it is already available.
             */
            while (true) {

                // Check to see if the exitValue has been assigned.
                final int exitValue = this.exitValue.get();

                if (exitValue != -1) {

                    // the exitValue has been assigned.
                    return exitValue;

                }

                final long elapsed = System.nanoTime() - begin;

                timeout -= elapsed;

                if (timeout <= 0)
                    throw new TimeoutException();

                dead.await(timeout, TimeUnit.NANOSECONDS);
                
            }
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Destroy the process.
     * <p>
     * Note: {@link Process#destroy()} is non-blocking, but this method blocks
     * until the process is terminated.
     * <p>
     * Note: processes with child processes (including any bigdata services
     * since they start children to report OS performance counters) MUST exit
     * normally (at least under windows) or the parent process will not be able
     * to exit. Therefore it is very important to extend this method and send
     * proper notice to the process requesting that it terminate itself.
     */
    synchronized public void destroy() {

        log.warn(this);

        try {
            process.destroy();
        } catch (Throwable t) {
            log.warn(this, t);
        }

        try {

            log.warn("Waiting on exitValue: " + this);

            final int exitValue = exitValue();

            log.warn("Process is dead: " + this + ", exitValue=" + exitValue);

            // fall through

        } catch (InterruptedException e) {

            log.warn(this, e);

            // fall through.
            
        }

        listener.remove(this);

    }
    
    /**
     * Only accept reference tests for equality.
     */
    public boolean equals(Object o) {

        if (this == o)
            return true;

        return false;
        
    }
    
    /**
     * Starts the {@link Process}, starts a {@link Thread} to consume its
     * output, and registers the {@link Process} with the
     * {@link IServiceListener}.
     * 
     * @param name
     *            A useful name for the process.
     * @param builder
     *            The object used to start the {@link Process}.
     * @param running
     *            A {@link Queue} of the running {@link Process}es.
     * 
     * @throws IOException
     */
    public ProcessHelper(final String name, final ProcessBuilder builder,
            final IServiceListener listener) throws IOException {

        if (name == null)
            throw new IllegalArgumentException();

        if (builder == null)
            throw new IllegalArgumentException();

        if (listener == null)
            throw new IllegalArgumentException();

        this.name = name;

        // save the listener reference.
        this.listener = listener;
        
        /*
         * Merge stdout and stderr so that we only need one thread to drain the
         * output of the process.
         */
        builder.redirectErrorStream(true);

        // start the process (it may take a bit to be really running).
        this.process = builder.start();

        // add to queue of running (or at any rate, started) processes.
        listener.add(ProcessHelper.this);
        
        final Thread thread = new Thread(name) {

            public void run() {

                try {

                    consumeOutput();

                } finally {

                    // no longer running.
                    lock.lock();
                    try {

                        // ensure process is destroyed.
                        process.destroy();

                        // wait for the exit value from the process.
                        exitValue.set(process.exitValue());

                        // signal so that anyone waiting will awaken.
                        dead.signalAll();

                        // remove the element from queue.
                        listener.remove(ProcessHelper.this);

                        // log event.
                        log.warn("Process destroyed: " + name);

                    } finally {

                        lock.unlock();

                    }

                }

            }

        };

        /*
         * Note: the service starter can fail to exit properly if this is not a
         * daemon thread.
         */
        thread.setDaemon(true);

        thread.start();

        if (INFO)
            log.info("Process starting: name=" + name + ", cmd="
                    + builder.command() + ", env=" + builder.environment());

    }

    /**
     * Consumes the output of the process, writing each line onto a
     * {@link Logger}.
     */
    protected void consumeOutput() {

        try {

            final BufferedReader is = new BufferedReader(new InputStreamReader(
                    process.getInputStream()));

            String s;

            /*
             * Note: when the process is killed, readLine() will return
             * since the stream will be closed.
             */
            while ((s = is.readLine()) != null) {

                log.warn(s);

            }

        } catch (IOException ex) {

            log.error(ex, ex);

        }

    }

    /**
     * Interrupts the caller's {@link Thread} if the process dies within the
     * specified timeout.
     * <p>
     * This thread can still be running after call() has returned and it can
     * cause a spurious interrupt. To avoid that you MUST cancel the thread
     * monitoring for the process death using the returned {@link Future}.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The timeout unit.
     * 
     * @return The {@link Future} for the {@link Thread} awaiting the task
     *         death.
     * 
     * @todo isRunning() does not appear to be atomic with respect to the
     *       interrupt of the caller. In fact, it seems like you have to use
     *       {@link #exitValue(long, TimeUnit)} with a small timeout to verify
     *       that the process is in fact dead. That is weird. I am even forcing
     *       the wait for the exit value in this method but to no avail.
     */
    public Future interruptWhenProcessDies(final long timeout,
            final TimeUnit unit) {

        final long nanos = unit.toNanos(timeout);

        final Thread callersThread = Thread.currentThread();

        final Condition done = lock.newCondition();

        final Thread t = new Thread() {

            public void run() {

                lock.lock();

                try {

                    if (dead.await(nanos, TimeUnit.NANOSECONDS)) {

                        // force a wait until the exitValue has been set.
                        final int exitValue = exitValue();
                        
                        if (INFO)
                            log.info("Process is dead: name=" + name
                                    + ", exitValue=" + exitValue);
                        
                        // The process is dead, so interrupt the caller.
                        callersThread.interrupt();

                        // done
                        return;
                        
                    }

                    // timeout.
                    return;

                } catch (InterruptedException e) {

                    // halt.
                    return;

                } finally {

                    done.signalAll();
                    
                    lock.unlock();

                }

            }

        };

        t.setDaemon(true);

        t.start();
        
        return new Future() {

            private volatile boolean cancelled = false;
            
            public boolean cancel(boolean mayInterruptIfRunning) {
                if (t.isAlive() && mayInterruptIfRunning) {
                    t.interrupt();
                    return true;
                }
                cancelled = true;
                return false;
            }

            public Object get() throws InterruptedException, ExecutionException {
                try {
                    return get(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // TimeoutException should not be thrown.
                    throw new AssertionError();
                }
            }

            public Object get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException,
                    TimeoutException {
                lock.lock();
                try {
                    if (t.isAlive()) {
                        done.await(timeout, unit);
                    }
                } finally {
                    lock.unlock();
                }
                return null;
            }

            public boolean isCancelled() {
                return cancelled;
            }

            public boolean isDone() {
                return !t.isAlive();
            }
            
        };

    }

}
