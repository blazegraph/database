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
 * Created on Feb 2, 2010
 */

package com.bigdata.util.concurrent;

import java.lang.ref.WeakReference;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.util.InnerCause;

/**
 * Pattern using a {@link FutureTask} to force synchronization only on tasks
 * waiting for the same computation.  This is based on Java Concurrency in
 * Practice, page 108.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Memoizer<A, V> implements Computable<A, V> {

    /**
     * Cache accumulates results.
     * 
     * @todo Because the cache is unbounded, it will just take on more and more
     *       memory as new results are computed. This could be handled by
     *       imposing a capacity constraint on the cache, by the use of
     *       {@link WeakReference}s if they can be made to stand for the
     *       specific computation to be performed, or by the use of a timeout on
     *       the cache entries.
     */
    protected final ConcurrentMap<A, Future<V>> cache = new ConcurrentHashMap<A, Future<V>>();

    /**
     * The method which computes a result (V) from an argument (A).
     */
    private final Computable<A, V> c;

    public Memoizer(final Computable<A, V> c) {
        this.c = c;
    }

    public V compute(final A arg) throws InterruptedException {
        while (true) {
            Future<V> f = cache.get(arg);
            boolean willRun = false;
            if (f == null) {
                final Callable<V> eval = new Callable<V>() {
                    public V call() throws InterruptedException {
                        return c.compute(arg);
                    }
                };
                final FutureTask<V> ft = new FutureTask<V>(eval);
                f = cache.putIfAbsent(arg, ft);
                if (f == null) {
                    willRun = true; // Note: MUST set before running!
                    f = ft;
                    ft.run(); // call to c.compute() happens here.
                }
            }
            try {
                return f.get();
            } catch (CancellationException e) {
                // remove cancelled task iff still our task.
                cache.remove(arg, f);
            } catch (ExecutionException e) {
                if (!willRun
                        && InnerCause.isInnerCause(e,
                                InterruptedException.class)) {
                    /*
                     * Since the task was executed by another thread (ft.run()),
                     * remove the interrupted task and retry.
                     * 
                     * Note: Basically, what has happened is that the thread
                     * which got to cache.putIfAbsent() first ran the Computable
                     * and was interrupted while doing so, so that thread needs
                     * to propagate the InterruptedException back to its caller.
                     * However, other threads which concurrently request the
                     * same computation MUST NOT see the InterruptedException
                     * since they were not actually interrupted. Therefore, we
                     * yank out the FutureTask and retry for any thread which
                     * did not run the task itself.
                     */ 
                    cache.remove(arg, f);
                    // Retry.
                    continue;
                }
                throw launderThrowable(e.getCause());
            }
        }
    }

    static private RuntimeException launderThrowable(final Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else
            throw new IllegalStateException("Not unchecked", t);
    }

}
