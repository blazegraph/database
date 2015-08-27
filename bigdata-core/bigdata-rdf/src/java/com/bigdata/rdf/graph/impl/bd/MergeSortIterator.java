package com.bigdata.rdf.graph.impl.bd;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * An N-way merge sort of N source iterators visiting {@link Value}s (which are
 * actually {@link IV}s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class MergeSortIterator implements Iterator<Value> {

    // private final static Logger log =
    // Logger.getLogger(MergeSortIterator.class);

    /**
     * The #of source iterators.
     */
    private final int n;

    /**
     * The source iterators in the order given to the ctor.
     */
    private final Iterator<Value>[] sourceIterator;

    /**
     * The current value from each source and <code>null</code> if we need to
     * get another value from that source. The value for a source iterator that
     * has been exhausted will remain <code>null</code>. When all entries in
     * this array are <code>null</code> there are no more values to be visited
     * and we are done.
     */
    private final Value[] sourceTuple;

    /**
     * Index into {@link #sourceIterator} and {@link #sourceTuple} of the
     * iterator whose value will be returned next -or- <code>-1</code> if we
     * need to choose the next value to be visited.
     */
    private int current = -1;

    /**
     * 
     * @param sourceIterators
     *            Each source iterator MUST be in ascending {@link Value} order.
     */
    public MergeSortIterator(final Iterator<Value>[] sourceIterators) {

        assert sourceIterators != null;

        assert sourceIterators.length > 0;

        this.n = sourceIterators.length;

        for (int i = 0; i < n; i++) {

            assert sourceIterators[i] != null;

        }

        this.sourceIterator = sourceIterators;

        sourceTuple = new Value[n];

    }

    @Override
    public boolean hasNext() {

        /*
         * Until we find an undeleted tuple (or any tuple if DELETED is true).
         */
        while (true) {

            if (current != -1) {

                // if (log.isTraceEnabled())
                // log.trace("Already matched: source=" + current);

                return true;

            }

            /*
             * First, make sure that we have a tuple for each source iterator
             * (unless that iterator is exhausted).
             */

            int nexhausted = 0;

            for (int i = 0; i < n; i++) {

                if (sourceTuple[i] == null) {

                    if (sourceIterator[i].hasNext()) {

                        sourceTuple[i] = sourceIterator[i].next();

                        // if (log.isTraceEnabled())
                        // log.trace("read sourceTuple[" + i + "]="
                        // + sourceTuple[i]);

                    } else {

                        nexhausted++;

                    }

                }

            }

            if (nexhausted == n) {

                // the aggregate iterator is exhausted.

                return false;

            }

            /*
             * Now consider the current tuple for each source iterator in turn
             * and choose the _first_ iterator having a tuple whose key orders
             * LTE all the others (or GTE if [reverseScan == true]). This is the
             * next tuple to be visited by the aggregate iterator.
             */
            {

                // current is index of the smallest key so far.
                assert current == -1;

                Value key = null; // smallest key so far.

                for (int i = 0; i < n; i++) {

                    if (sourceTuple[i] == null) {

                        // This source is exhausted.

                        continue;

                    }

                    if (current == -1) {

                        current = i;

                        key = sourceTuple[i];

                        assert key != null;

                    } else {

                        final Value tmp = sourceTuple[i];

                        final int ret = compare(tmp, key);

                        if (ret < 0) {

                            /*
                             * This key orders LT the current key.
                             * 
                             * Note: This test MUST be strictly LT since LTE
                             * would break the precedence in which we are
                             * processing the source iterators and give us the
                             * key from the last source by preference when we
                             * need the key from the first source by preference.
                             */

                            current = i;

                            key = tmp;

                        }

                    }

                }

                assert current != -1;

            }

            // if (log.isDebugEnabled())
            // log.debug("Will visit next: source=" + current + ", tuple: "
            // + sourceTuple[current]);

            return true;

        }

    }

    @Override
    public Value next() {

        if (!hasNext())
            throw new NoSuchElementException();

        return consumeLookaheadTuple();

    }

    /**
     * Consume the {@link #current} source value.
     * 
     * @return The {@link #current} tuple.
     */
    private Value consumeLookaheadTuple() {

        final Value t = sourceTuple[current];

        // clear tuples from other sources having the same key as the
        // current tuple.
        clearCurrent();

        return t;

    }

    /**
     * <p>
     * Clear tuples from other sources having the same key as the current tuple
     * (eliminates duplicates).
     * </p>
     */
    protected void clearCurrent() {

        assert current != -1;

        final Value key = sourceTuple[current];

        for (int i = current + 1; i < n; i++) {

            if (sourceTuple[i] == null) {

                // this iterator is exhausted.

                continue;

            }

            final Value tmp = sourceTuple[i];

            final int ret = compare(key, tmp);

            if (ret == 0) {

                // discard tuple.

                sourceTuple[i] = null;

            }

        }

        // clear the tuple that we are returning so that we will read
        // another from that source.
        sourceTuple[current] = null;

        // clear so that we will look again.
        current = -1;

    }

    @Override
    public void remove() {

        throw new UnsupportedOperationException();

    }

    /**
     * Compare two {@link Value}s (which are actually {@link IV}s).
     */
    @SuppressWarnings("rawtypes")
    private int compare(final Value a, final Value b) {

        return IVUtility.compare((IV) a, (IV) b);
        
    }

}
