package com.bigdata.concurrent;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Bundles the resources identifying the required locks with the task to be
 * executed once it holds those locks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <R>
 *            The generic type of the declared lock objects.
 * @param <T>
 *            The generic type of the {@link Callable}'s outcome.
 */
public class LockCallableImpl<R extends Comparable, T> implements
        LockCallable<R, T> {

    final R[] resource;

    final Callable<T> task;

    final TimeUnit unit;
    
    final long timeout;
    
    final int maxtries;
    
    public LockCallableImpl(final R[] resource, final Callable<T> task) {
        
        this(resource, task, TimeUnit.SECONDS, Long.MAX_VALUE, 1/* maxtries */);
        
    }

    public LockCallableImpl(final R[] resource, final Callable<T> task,
            final TimeUnit unit, final long timeout, final int maxtries) {

        if (resource == null)
            throw new IllegalArgumentException();

        for (R r : resource)
            if (r == null)
                throw new IllegalArgumentException();

        if (task == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        if (maxtries <= 0)
            throw new IllegalArgumentException();
        
        this.resource = resource;

        this.task = task;

        this.unit = unit;

        this.timeout = timeout;

        this.maxtries = maxtries;

    }

    public R[] getResource() {

        return resource;

    }

    public T call() throws Exception {

        return task.call();

    }

    public String toString() {

        return task.toString() + "{locks=" + Arrays.toString(resource) + "}";

    }

}
