package com.bigdata.btree.filter;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.io.ByteArrayBuffer;

/**
 * Used to write logic that advances an {@link ITupleCursor} to another key
 * after it visits some element. For example, the "distinct term scan" for
 * the RDF DB is written in this manner.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The type of the objects visited by the source iterator.
 * 
 * @todo write test for remove(). in order for remove() to work we must note
 *       the key for the last visited tuple and then issue remove(key)
 *       against the underlying index.
 */
abstract public class Advancer<E> implements ITupleFilter<E> {

    protected transient static final Logger log = Logger.getLogger(Advancer.class);

    /**
     * Set by {@link #filter(ITupleCursor)}.
     */
    protected ITupleCursor<E> src;

    protected Advancer() {

    }

    /**
     * 
     * @param src
     *            The source iterator (MUST be an {@link ITupleCursor}).
     * @return
     */
    @SuppressWarnings("unchecked")
    final public ITupleIterator<E> filter(Iterator src) {

        this.src = (ITupleCursor<E>) src;

        return new Advancer.Advancerator<E>(this.src, this);

    }

    /**
     * Offers an opportunity to advance the source {@link ITupleCursor} to a
     * new key using {@link ITupleCursor#seek(byte[]).
     * 
     * @param tuple
     *            The current value.
     */
    protected abstract void advance(ITuple<E> tuple);

    /**
     * Implements the {@link Advancer} semantics as a layer iterator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class Advancerator<E> implements ITupleIterator<E> {

        final private ITupleCursor<E> src;

        final private Advancer<E> filter;

        /**
         * Used to retain a copy of the last key visited so that
         * {@link #remove()} can issue the request to remove that key from
         * the backing {@link IIndex}. This is necessary since the
         * underlying {@link ITupleCursor} may otherwise have been
         * "advanced" to an arbitrary tuple.
         */
        final private ByteArrayBuffer kbuf = new ByteArrayBuffer();

        public Advancerator(ITupleCursor<E> src, Advancer<E> filter) {

            this.src = src;

            this.filter = filter;

        }

        public boolean hasNext() {

            return src.hasNext();

        }

        public ITuple<E> next() {

            final ITuple<E> tuple = src.next();

            if (log.isInfoEnabled()) {

                log.info("next: " + tuple);

            }

            // copy the key for the current tuple.
            kbuf.reset().copyAll(tuple.getKeyBuffer());

            // skip to the next tuple of interest.
            filter.advance(tuple);

            return tuple;

        }

        public void remove() {

            final byte[] key = this.kbuf.toByteArray();

            if (log.isInfoEnabled()) {

                log.info("key=" + BytesUtil.toString(key));

            }

            src.getIndex().remove(key);

        }

    }

}
