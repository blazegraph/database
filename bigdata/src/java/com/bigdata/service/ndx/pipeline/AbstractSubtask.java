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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Abstract implementation of a subtask for the {@link AbstractMasterTask}
 * handles the protocol for startup and termination of the subtask. A concrete
 * implementation must handle the chunks of elements being drained from the
 * subtask's {@link #buffer} via {@link #nextChunk(Object[])}.
 * 
 * @param <HS>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the subtask.
 * @param <M>
 *            The generic type of the master task implementation class.
 * @param <E>
 *            The generic type of the elements in the chunks stored in the
 *            {@link BlockingBuffer}.
 * @param <L>
 *            The generic type of the key used to lookup a subtask in the
 *            internal map (must be unique and must implement hashCode() and
 *            equals() per their contracts).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractSubtask<//
HS extends AbstractSubtaskStats,//
M extends AbstractMasterTask<? extends AbstractMasterStats<L, HS>, E, ? extends AbstractSubtask, L>,//
E,//
L>//
        implements Callable<HS> {

    protected static transient final Logger log = Logger
            .getLogger(AbstractSubtask.class);

    /**
     * The master.
     */
    protected final M master;

    /**
     * The unique key for the subtask.
     */
    protected final L locator;
    
    /**
     * The buffer on which the {@link #master} is writing.
     */
    protected final BlockingBuffer<E[]> buffer;

    /**
     * The iterator draining the {@link #buffer}.
     * <p>
     * Note: DO NOT close this iterator from within {@link #call()} as that
     * would cause this task to interrupt itself!
     */
    protected final IAsynchronousIterator<E[]> src;

    /**
     * The statistics used by this task.
     */
    protected final HS stats;

    /**
     * The default timeout in nanoseconds before closing an idle output sink.
     */
    public static final long DEFAULT_IDLE_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(2000);
    
    /**
     * The timeout in nanoseconds before closing an idle output sink.
     * 
     * @todo config
     */
    private final long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    
    /**
     * The timeout in nanoseconds that we will wait for a chunk to become
     * available. This is NOT the same as the chunk combiner timeout. It should
     * generally be a small multiple of the chunk combiner timeout so that
     * chunks may be readily combined while polling.
     * 
     * @todo config
     */
    private final long pollTimeout = TimeUnit.MILLISECONDS.toNanos(50);

    public AbstractSubtask(final M master, final L locator,
            final BlockingBuffer<E[]> buffer) {

        if (master == null)
            throw new IllegalArgumentException();

        if (locator == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.master = master;
        
        this.locator = locator;

        this.buffer = buffer;

        this.src = buffer.iterator();

        this.stats = (HS) master.stats.getSubtaskStats(locator);

    }

    public HS call() throws Exception {

        try {

            /*
             * Poll the iterator with a timeout to avoid deadlock with
             * awaitAll().
             * 
             * Note: In order to ensure termination the subtask MUST poll
             * the iterator with a timeout so that a subtask which was
             * created in response to a StaleLocatorException during
             * master.awaitAll() can close its own blocking buffer IF: (a)
             * the top-level blocking buffer is closed; and (b) the
             * subtask's blocking buffer is empty. This operation needs to
             * be coordinated using the master's [lock], as does any
             * operation which writes on the subtask's buffer. Otherwise we
             * can wait forever for a subtask to complete. The subtask uses
             * the [subtask] Condition signal the master when it is
             * finished.
             */
            final Thread t = Thread.currentThread();
            long lastChunkNanos = System.nanoTime();
            while (true) {

                master.halted();

                if (t.isInterrupted()) {

                    throw master.halt(new InterruptedException(toString()));

                }

                // nothing available w/in timeout?
                if (!src.hasNext(pollTimeout, TimeUnit.NANOSECONDS)) {

                    // are we done? should we close our buffer?
                    master.lock.lockInterruptibly();
                    try {
                        if (!buffer.isOpen() && !src.hasNext()) {
                            // We are done.
                            if (log.isInfoEnabled())
                                log.info("No more data: " + this);
                            break;
                        }
                        final long elapsedSinceLastChunk = System.nanoTime()
                                - lastChunkNanos;
                        final boolean idle = elapsedSinceLastChunk > idleTimeout;
                        if (idle || master.src.isExhausted()) {
                            if (buffer.isEmpty()) {
                                /*
                                 * Close out buffer. Since the buffer is empty
                                 * the iterator will be exhausted and the
                                 * subtask will quit the next time through the
                                 * loop.
                                 * 
                                 * Note: This can happen either if the master
                                 * is closed or if idle too long.
                                 */
                                buffer.close();
                                if (log.isInfoEnabled())
                                    log.info("Closed buffer: idle=" + idle
                                            + " : " + this);
                                if(!src.hasNext()) {
                                    if (log.isInfoEnabled())
                                        log.info("No more data: " + this);
                                    break;
                                }
                            }
                        }
                        // poll the itr again.
                        continue;
                    } finally {
                        master.lock.unlock();
                    }

                }

                // update timestamp of the last chunk read.
                lastChunkNanos = System.nanoTime();
                
                if (nextChunk(src.next())) {

                    if (log.isInfoEnabled())
                        log.info("Eager termination.");

                    // Done (eager termination).
                    break;

                }

            }

            // normal completion.
            master.removeOutputBuffer(locator, this);

            if (log.isInfoEnabled())
                log.info("Done: " + locator);

            // done.
            return stats;

        } catch (Throwable t) {

            if (log.isInfoEnabled()) {
                // show stack trace @ INFO
                log.warn(this, t);
            } else {
                // else only abbreviated warning.
                log.warn(this + " : " + t);
            }
            
            /*
             * Halt processing.
             * 
             * Note: This is responsible for propagating any errors such that
             * the master halts in a timely manner. This is necessary since no
             * one is checking the Future for the sink tasks (except when we
             * wait for them to complete before we reopen an output buffer).
             */
            master.halt(t);

            throw new RuntimeException(t);

        } finally {

            master.lock.lock();
            try {
                master.subtask.signalAll();
            } finally {
                master.lock.unlock();
            }

        }

    }

    /**
     * Process a chunk from the buffer.
     * 
     * @return <code>true</code> iff the task should exit immediately.
     */
    abstract protected boolean nextChunk(E[] chunk) throws Exception;

}
