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
 * Created on Oct 19, 2010
 */

package com.bigdata.relation.accesspath;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * Class allows new sources to be attached dynamically. If the existing sources
 * are drained then the iterator will {@link #close()} itself so that new
 * sources can no longer be attached.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MultiSourceSequentialAsynchronousIterator<E> implements
        IMultiSourceAsynchronousIterator<E> {

    private final static Logger log = Logger.getLogger(MultiSourceSequentialAsynchronousIterator.class);
    
    private final ReentrantLock lock = new ReentrantLock();

    private final Queue<IAsynchronousIterator<E>> sources = new LinkedBlockingQueue<IAsynchronousIterator<E>>();

    /**
     * The current inner iterator. When <code>null</code> the outer iterator has
     * been closed and will not deliver any more results and will not accept any
     * new sources.
     * <p>
     * Note: This can be asynchronously closed if the application invokes
     * {@link #close()}. Methods which test on this can not assume that it will
     * be non-<code>null</code> the next time they check unless they are holding
     * the {@link #lock}. Methods which do not obtain the lock can offer a
     * weaker atomicity by copying the reference to a local variable and then
     * testing that variable.
     */
    private volatile IAsynchronousIterator<E> current;

    public MultiSourceSequentialAsynchronousIterator(final IAsynchronousIterator<E> src) {
        current = src;
    }
    
    public void close() {
        lock.lock();
        try {
            /*
             * Ensure that all sources are eventually closed.
             */
            // close the current source (if any).
            final IAsynchronousIterator<E> current = this.current;
            this.current = null;
            if (current != null) {
                if (log.isInfoEnabled())
                    log.info("Closing source: " + current);
                current.close();
            }
            // Close any sources still in the queue.
            for(IAsynchronousIterator<E> t : sources) {
                if (log.isInfoEnabled())
                    log.info("Closing source: " + t);
                t.close();
            }
            // Clear the queue.
            sources.clear();
        } finally {
            lock.unlock();
        }
    }

    public boolean add(final IAsynchronousIterator<E> src) {
        if (src == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (current == null)
                return false;
            sources.add(src);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * If the current source is not exhausted, then return it immediately.
     * Otherwise, return the next source which is not exhausted. If no such
     * sources are available, then {@link #close()} the iterator. The decision
     * to accept another source or to close the iterator is made atomic by the
     * use of the {@link #lock} in this method and in {@link #close()}.
     * 
     * @return The next source -or- <code>null</code> if there are no sources
     *         available.
     */
    private IAsynchronousIterator<E> nextSource() {
        final IAsynchronousIterator<E> tmp = current;
        if (tmp == null)
            return null;
        if (!tmp.isExhausted())
            return current; // Note: MAY be asynchronously cleared!
        // current is known to be [null].
        lock.lock();
        try {
            /* Close iterator which has been consumed.
             * 
             */
            if (log.isInfoEnabled())
                log.info("Closing source: " + current);
            current.close();
            // remove the head of the queue (non-blocking)
            while ((current = sources.poll()) != null) {
                if (!current.isExhausted()) {
                    return current;
                } else {
                    // Note: should already be closed since exhausted.
                    current.close();
                }
            }
            // no more sources with data, close while holding lock.
            close();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public boolean hasNext() {
        while (true) {
            final IAsynchronousIterator<E> tmp = nextSource();
            if (tmp == null)
                return false;
            if (tmp.hasNext())
                return true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @todo Due to the inherent non-atomicity of the while(hasNext()) next()
     *       idiom, it is possible for {@link #hasNext()} to report true and for
     *       {@link #next()} to throw {@link NoSuchElementException} if the
     *       iterator has been concurrently closed.
     */
    public E next() {
        while (true) {
            final IAsynchronousIterator<E> tmp = nextSource();
            if (tmp == null)
                throw new NoSuchElementException();
            if (tmp.hasNext())
                return tmp.next();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This will report <code>true</code> iff all iterators currently
     * attached have been consumed, at which point {@link #current} becomes
     * <code>null</code> and no more iterators may be attached, hence the high
     * level iterator is provably exhausted.
     */
    public boolean isExhausted() {
        return nextSource() == null;
    }

    public boolean hasNext(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;
        while (remaining > 0) {
            final IAsynchronousIterator<E> tmp = nextSource();
            if (tmp == null)
                return false;
            if (tmp.hasNext(remaining, TimeUnit.NANOSECONDS))
                return true;
            remaining = nanos - (System.nanoTime() - begin);
        }
        // timeout.
        return false;
    }

    public E next(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;
        while (true) {
            final IAsynchronousIterator<E> tmp = nextSource();
            if (tmp == null)
                return null;
            if (tmp.hasNext(remaining, TimeUnit.NANOSECONDS))
                return tmp.next();
            remaining = nanos - (System.nanoTime() - begin);
        }
    }

}
