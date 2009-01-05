package com.bigdata.jini.start;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Queue;
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
    
    /**
     * The {@link Process}.
     */
    private final Process process;
    
    private final ReentrantLock lock = new ReentrantLock();
    
    private final Condition dead = lock.newCondition();
    
    private final AtomicInteger exitValue = new AtomicInteger(-1);
    
    /**
     * Return <code>true</code> iff the process is still executing.
     */
    public boolean isRunning() {

        return exitValue.get() != -1;

    }
    
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
     */
    public void destroy() {
        
        log.warn("name=" + name);

        process.destroy();
        
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
     * output, and registers the {@link Process} in the
     * {@link ServicesManager#runningProcesses} collection.
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
            final Queue<ProcessHelper> running) throws IOException {

        if (name == null)
            throw new IllegalArgumentException();

        if (builder == null)
            throw new IllegalArgumentException();

        if (running == null)
            throw new IllegalArgumentException();

        this.name = name;

        if (INFO)
            log.info("process: name=" + name + ", cmd="
                    + builder.command());

        this.process = builder.start();

        final Thread thread = new Thread(name) {

            public void run() {

                // add to queue.
                running.add(ProcessHelper.this);
                
                try {

                    consumeOutput();

                } finally {

                    try {

                        // ensure process is destroyed.
                        process.destroy();

                    } finally {

                        // no longer running.
                        lock.lock();
                        try {

                            // set the exit value from the process.
                            exitValue.set(process.exitValue());
                            
                            // signal so that anyone waiting will awaken.
                            dead.signalAll();
                            
                            // remove the element from queue.
                            running.remove(ProcessHelper.this);

                            // log event.
                            log.warn("Process destroyed: " + name);

                        } finally {
                        
                            lock.unlock();
                            
                        }

                    }

                }

            }

        };

        thread.setDaemon(false);

        thread.start();

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

}
