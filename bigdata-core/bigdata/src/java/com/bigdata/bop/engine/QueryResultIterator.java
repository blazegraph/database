package com.bigdata.bop.engine;

import java.util.concurrent.atomic.AtomicBoolean;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Delegate pattern cancels the {@link IRunningQuery} when the iterator is
 * {@link #close() closed} and signals normal completion of the query once the
 * iterator is exhausted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class QueryResultIterator<E> implements ICloseableIterator<E> {

    private final IRunningQuery runningQuery;

    private final ICloseableIterator<E> src;
    
    private final AtomicBoolean open = new AtomicBoolean(true);

    public QueryResultIterator(final IRunningQuery runningQuery,
            final ICloseableIterator<E> src) {

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
            src.close();
//            try {
//                runningQuery.halt();
//            } finally {
//                src.close();
//            }
        }
    }
    
    public boolean hasNext() {
//        return src.hasNext();
        if (!src.hasNext()) {
            normalCompletion();
            return false;
        }
        return true;
    }

    public E next() {
        return src.next();
    }

    public void remove() {
//        src.remove();
        throw new UnsupportedOperationException();
    }

}
