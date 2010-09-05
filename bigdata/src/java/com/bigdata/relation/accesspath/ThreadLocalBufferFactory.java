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
 * Created on Sep 1, 2010
 */

package com.bigdata.relation.accesspath;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.relation.rule.eval.pipeline.JoinTask;
import com.bigdata.util.concurrent.Haltable;

/**
 * A factory pattern for per-thread objects whose life cycle is tied to some
 * container. . The pool can be torn down when the container is torn down, which
 * prevents its thread-local references from escaping.
 * <p>
 * Note: This implementation uses a true thread local buffers managed by a
 * {@link ConcurrentHashMap}. This approach has approximately 3x higher
 * concurrency than striped locks. The advantage of striped locks is that you
 * can directly manage the #of buffers when when the threads using those buffers
 * is unbounded. However, doing so could lead to deadlock since two threads can
 * be hashed onto the same buffer object.
 * 
 * @author thompsonbry@users.sourceforge.net
 * @version $Id: ThreadLocalBufferFactory.java 3500 2010-09-03 00:27:45Z
 *          thompsonbry $
 * @param <T>
 *            The generic type of the thread-local object.
 */
abstract public class ThreadLocalBufferFactory<T extends IBuffer<E>, E> {

    static private final Logger log = Logger
            .getLogger(ThreadLocalBufferFactory.class);
    
    /**
     * The thread-local queues.
     */
    private final ConcurrentHashMap<Thread, T> map;

    /**
     * A list of all objects visible to the caller. This is used to ensure that
     * any objects allocated by the factory are visited.
     * 
     * <p>
     * Note: Since the collection is not thread-safe, synchronization is
     * required when adding to the collection and when visiting the elements of
     * the collection.
     */
    private final LinkedList<T> list = new LinkedList<T>();

    protected ThreadLocalBufferFactory() {

        this(16/* initialCapacity */, .75f/* loadFactor */, 16/* concurrencyLevel */);

    }

    protected ThreadLocalBufferFactory(final int initialCapacity,
            final float loadFactor, final int concurrencyLevel) {

        map = new ConcurrentHashMap<Thread, T>(initialCapacity, loadFactor,
                concurrencyLevel);

    }

    /**
     * Return the #of thread-local objects.
     */
    final public int size() {

        return map.size();

    }

    /**
     * Add the element to the thread-local buffer.
     * 
     * @param e
     *            An element.
     * 
     * @throws IllegalStateException
     *             if the factory is asynchronously closed.
     */
    public void add(final E e) {

        get().add(e);

    }

    /**
     * Return a thread-local buffer
     * 
     * @return The thread-local buffer.
     * 
     * @throws RuntimeException
     *             if the join is halted.
     */
    final public T get() {
        final Thread t = Thread.currentThread();
        T tmp = map.get(t);
        if (tmp == null) {
            if (map.put(t, tmp = initialValue()) != null) {
                /*
                 * Note: Since the key is the thread it is not possible for
                 * there to be a concurrent put of an entry under the same key
                 * so we do not have to use putIfAbsent().
                 */
                throw new AssertionError();
            }
            // Add to list.
            synchronized (list) {
                list.add(tmp);
            }
        }
        halted();
        return tmp;
    }

    /**
     * Flush each of the unsynchronized buffers onto their backing synchronized
     * buffer.
     * 
     * @throws RuntimeException
     *             if the join is halted.
     */
    public void flush() {
        synchronized (list) {
            int n = 0;
            long m = 0L;
            for (T b : list) {
                halted();
                // #of elements to be flushed.
                final int size = b.size();
                // flush, returning total #of elements written onto this
                // buffer.
                final long counter = b.flush();
                m += counter;
                n++;
                if (log.isDebugEnabled())
                    log.debug("Flushed buffer: size=" + size + ", counter="
                            + counter);
            }
            if (log.isInfoEnabled())
                log.info("Flushed " + n + " unsynchronized buffers totalling "
                        + m + " elements");
        }
    }

    /**
     * Reset each of the synchronized buffers, discarding their buffered writes.
     * <p>
     * Note: This method is used during error processing, therefore it DOES NOT
     * check {@link JoinTask#halt}.
     */
    public void reset() {
        synchronized (list) {
            int n = 0;
            for (T b : list) {
                // #of elements in the buffer before reset().
                final int size = b.size();
                // reset the buffer.
                b.reset();
                if (log.isDebugEnabled())
                    log.debug("Reset buffer: size=" + size);
            }
            if (log.isInfoEnabled())
                log.info("Reset " + n + " unsynchronized buffers");
        }
    }

    /**
     * Create and return a new object.
     */
    abstract protected T initialValue();

    /**
     * Test to see if the process has been halted.
     * 
     * @see Haltable#halted()
     */
    abstract protected void halted();

}
