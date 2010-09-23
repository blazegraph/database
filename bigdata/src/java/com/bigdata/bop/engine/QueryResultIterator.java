package com.bigdata.bop.engine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Delegate pattern cancels the {@link RunningQuery} when the iterator is
 * {@link #close() closed} and signals normal completion of the query once the
 * iterator is exhausted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class QueryResultIterator<E> implements IAsynchronousIterator<E> {

    private final RunningQuery runningQuery;

    private final IAsynchronousIterator<E> src;
    
    private final AtomicBoolean open = new AtomicBoolean(true);

    public QueryResultIterator(final RunningQuery runningQuery,
            final IAsynchronousIterator<E> src) {

        if (runningQuery == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.runningQuery = runningQuery;

        this.src = src;

    }

    public void close() {
        if (open.compareAndSet(true/* expect */, false/* update */)) {
            try {
                runningQuery.cancel(true/* mayInterruptIfRunning */);
            } finally {
                src.close();
            }
        }
    }

    private void normalCompletion() {
        if (open.compareAndSet(true/* expect */, false/* update */)) {
            /*
             * Note: DO NOT halt the query here!!!! That will cause it to not
             * accept any more messages. Just close the source iterator.
             */
//            try {
//                runningQuery.halt();
//            } finally {
                src.close();
//            }
        }
    }
    
    public boolean isExhausted() {
//        return src.isExhausted();
        if (src.isExhausted()) {
            normalCompletion();
            return true;
        }
        return false;
    }

    public boolean hasNext() {
//        return src.hasNext();
        if (!src.hasNext()) {
            normalCompletion();
            return false;
        }
        return true;
    }

    public boolean hasNext(long timeout, TimeUnit unit)
            throws InterruptedException {
        return src.hasNext(timeout, unit);
    }

    public E next(long timeout, TimeUnit unit) throws InterruptedException {
        return src.next(timeout, unit);
    }

    public E next() {
        return src.next();
    }

    public void remove() {
        src.remove();
    }

}
