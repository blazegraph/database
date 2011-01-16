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
 * Created on Apr 15, 2009
 */

package com.bigdata.util.concurrent;

import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for tasks whose processing may be halted asynchronously.
 * While this bears some resemblance to {@link Future}, this class is used in
 * contexts where the process to be halted does not map nicely into a
 * {@link Callable} or {@link Runnable} and hence can not be modeled using a
 * {@link FutureTask}.
 * <p>
 * This class embeds certain knowledge about which exceptions may be observed
 * during normal termination of asynchronous processes using I/O, thread pools,
 * and {@link IBlockingBuffer}s. See
 * {@link #isNormalTerminationCause(Throwable)} for a list of the
 * {@link Throwable} causes which are treated as normal termination.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: AbstractHaltableProcess.java 2265 2009-10-26 12:51:06Z
 *          thompsonbry $
 */
public class Haltable<V> implements IHaltable<V> {

    private final transient static Logger log = Logger
            .getLogger(Haltable.class);

//    /**
//     * Exception used to indicate a {@link #cancel(boolean) cancelled}
//     * computation.
//     */
//    private static final Throwable CANCELLED = new InterruptedException("CANCELLED");

    /**
     * Lock guarding the {@link #halted} condition and the various non-volatile,
     * non-atomic fields.
     */
    final private Lock lock = new ReentrantLock();

    /**
     * {@link Condition} signaled when the process is halted
     */
    final private Condition halted = lock.newCondition();

    /**
     * The result of the computation. This is guarded by the {@link #lock} .
     */
    private V result = null;

    /**
     * The first cause as set by {@link #halt(Throwable)}.
     */
    private volatile Throwable firstCause = null;

    /**
     * Flag is set <code>true</code> if the process was halted by a
     * {@link Throwable} not included in the set of normal termination causes.
     */
    private volatile boolean error = false;

    /**
     * Set to <code>true</code> iff the process should halt.
     */
    private volatile boolean halt = false;

    /**
     * Set to <code>true</code> iff the process was {@link #cancel(boolean)
     * cancelled}.
     */
    private volatile boolean cancelled = false;
    
    /**
     * Halt (normal termination).
     */
    final public void halt(final V v) {
        lock.lock();
        try {
            if (halt) {
                // Already halted.
                return;
            }
            try {
                // signal *all* listeners.
                halted.signalAll();
                result = v; // save result.
            } finally {
                halt = true; // volatile write.
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Halt (exception thrown). <strong>The caller is responsible for throwing
     * the cause out of their own context.</strong>
     * 
     * @param cause
     *            The cause (required).
     * 
     * @return The argument.
     */
    final public <T extends Throwable> T halt(final T cause) {
        final boolean didHalt;
        lock.lock();
        try {
            if (didHalt = !halt) {
            	/*
            	 * This is the first cause.
            	 */
            	// note the first cause (and handle an illegal null if found).
                firstCause = (cause != null ? cause
                        : new IllegalArgumentException());
                // note if abnormal termination (firstCause only)
                error = !isNormalTerminationCause(firstCause);
                try {
                    // signal *all* listeners.
                    halted.signalAll();
                } finally {
                    halt = true; // volatile write.
                }
            }
        } finally {
            lock.unlock();
        }
        try {
            /*
             * Log exception (can potentially log all causes, not just the first
             * cause).
             */
            logCause(didHalt, cause);
        } catch (Throwable ex) {
            // error in logging system - ignore.
        }
        return cause;
    }

	/**
	 * Return unless processing has been halted. The method should be invoked
	 * from within the execution of the process itself so that it may notice
	 * asynchronous termination. It will throw out the wrapped first cause if
	 * the process is halted.
	 * <p>
	 * Note: External processes waiting on the {@link Future} interface should
	 * use {@link #isDone()} which does not have the semantics of asserting that
	 * the process should still be running.
	 * 
	 * @throws RuntimeException
	 *             wrapping the {@link #firstCause} iff processing has been
	 *             halted.
	 */
    final public void halted() {

        if (halt) {
            if (firstCause == null) {
                /*
                 * Note: this is an error since there is an expectation by the
                 * process when it invokes halted() that the process is still
                 * running (since it invoked halted() it must be running). Since
                 * it is running,
                 */
                throw new RuntimeException();
                
            }
            throw new RuntimeException(firstCause);
        }

    }

    final public boolean cancel(final boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            
            final Throwable t = new InterruptedException();

            halt(t);

            if (firstCause == t) {
                // iff this was the firstCause.
                cancelled = true;
                return true;
            }

            return false;
            
        } finally {
            
            lock.unlock();
            
        }
    }

    final public V get() throws InterruptedException, ExecutionException {
        lock.lock();
        try {
            while (!halt) {
                halted.await();
            }
            if(cancelled) {
                final CancellationException t = new CancellationException();
                t.initCause(firstCause);
                throw t;
            }
            if (error)
                throw new ExecutionException(firstCause);
            return result;
        } finally {
            lock.unlock();
        }
    }

    final public V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final long begin = System.nanoTime();
        long nanos = unit.toNanos(timeout);
        if (lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
            try {
                // subtract out the elapsed time
                nanos -= (System.nanoTime() - begin);
                while (!halt) {
                    if (nanos > 0)
                        nanos = halted.awaitNanos(nanos);
                    else
                        throw new TimeoutException();
                }
                if (error)
                    throw new ExecutionException(firstCause);
                // normal completion.
                return result;
            } finally {
                lock.unlock();
            }
        }
        // timeout awaiting the lock.
        throw new TimeoutException();
    }

    /**
     * Return <code>true</code> if the process is done.
     */
    final public boolean isDone() {

        return halt;

    }

    /**
     * Return <code>true</code> if the process was halted by a {@link Throwable}
     * not included in the set of known normal termination causes.
     */
    final public boolean isError() {

        // Note: lock required for atomic visibility for [halt AND error].
        lock.lock();
        try {
            return halt && error;
        } finally {
            lock.unlock();
        }

    }

    public boolean isCancelled() {

        // Note: lock required for atomic visibility for [halt AND cancelled].
        lock.lock();
        try {
            return halt && cancelled;
        } finally {
            lock.unlock();
        }

    }

	final public Throwable getCause() {

		lock.lock();
		try {
		
			if (!halt)
				return null;

			if (!error)
				return null;

			return firstCause;

		} finally {
		
			lock.unlock();
			
		}

    }

    /**
     * Return the first {@link Throwable cause} regardless of whether it is
     * indicative of normal termination and <code>null</code> iff no cause has
     * been set.
     */
    final public Throwable getAsThrownCause() {

        return firstCause;

    }

	/**
	 * Return <code>true</code> if the {@link Throwable} is a known normal
	 * termination cause for the process. The method inspects the stack trace,
	 * examining both the outer and {@link InnerCause}s. The following causes
	 * are interpreted as normal termination:
	 * <dl>
	 * <dt>{@link InterruptedException}</dt>
	 * <dd>The process was terminated by an interrupt. Interrupts are typically
	 * used to terminate asynchronous processes when their production limit has
	 * been satisfied or the consumer otherwise chooses to
	 * {@link IAsynchronousIterator#close()} the iterator through which they are
	 * consuming results from the process.</dd>
	 * <dt>{@link CancellationException}</dt>
	 * <dd>A process has been canceled using its {@link Future}.</dd>
	 * <dt>{@link ClosedByInterruptException}</dt>
	 * <dd>A process was interrupted during an IO operation.</dd>
	 * <dt>{@link BufferClosedException}</dt>
	 * <dd>The {@link IBlockingBuffer} on which the process was writing was
	 * asynchronously closed.</dd>
	 * <dt>{@link RejectedExecutionException}</dt>
	 * <dd>A process was not executed because the pool against which it was
	 * submitted had been shutdown (this of course implies that the work queue
	 * was unbounded as a bounded pool will throw this exception if the work
	 * queue is full).</dd>
	 * </dl>
	 * 
	 * @param cause
	 *            The {@link Throwable}.
	 * 
	 * @return <code>true</code> if the {@link Throwable} indicates normal
	 *         termination.
	 * 
	 * @see #getCause()
	 */
    protected boolean isNormalTerminationCause(final Throwable cause) {
        if (InnerCause.isInnerCause(cause, InterruptedException.class))
            return true;
        if (InnerCause.isInnerCause(cause, CancellationException.class))
            return true;
        if (InnerCause.isInnerCause(cause, ClosedByInterruptException.class))
            return true;
        if (InnerCause.isInnerCause(cause, BufferClosedException.class))
            return true;
        if (InnerCause.isInnerCause(cause, RejectedExecutionException.class))
            return true;
        return false;
    }

    /**
     * This logs all unexpected causes @ WARN (anything not reported as normal
     * termination by {@link #isNormalTerminationCause(Throwable)}), not just
     * the first cause. All exceptions are logged @ TRACE.
     */
    protected void logCause(final boolean isFirstCause, final Throwable cause) {
        if (log.isEnabledFor(Level.WARN)) {
            if (error) {
                log.warn(this + " : isFirstCause=" + isFirstCause + " : "
                        + cause, cause);
            } 
        } else if(log.isTraceEnabled()) {
            log.trace(this + " : isFirstCause=" + isFirstCause + " : " + cause,
                    cause);
        }
    }

}
