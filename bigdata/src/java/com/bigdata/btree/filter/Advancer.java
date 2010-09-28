package com.bigdata.btree.filter;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyOutOfRangeException;
import com.bigdata.io.ByteArrayBuffer;

import cutthecrap.utils.striterators.FilterBase;

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
abstract public class Advancer<E> extends FilterBase implements ITupleFilter<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
    @Override
    final public ITupleIterator<E> filterOnce(final Iterator src,
            final Object context) {

        this.src = (ITupleCursor<E>) src;

        return new Advancer.Advancerator<E>(this.src, context, this);

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
        final protected Object context;

        final private Advancer<E> filter;

        /**
         * Set true iff we exceed the bounds on the {@link ITupleCursor}. For
         * example, if we run off the end of an index partition. This is used to
         * simulate the exhaustion of the cursor when you advance past its
         * addressable range.
         */
        private boolean exhausted = false;
        
        /**
         * Used to retain a copy of the last key visited so that
         * {@link #remove()} can issue the request to remove that key from
         * the backing {@link IIndex}. This is necessary since the
         * underlying {@link ITupleCursor} may otherwise have been
         * "advanced" to an arbitrary tuple.
         */
        final private ByteArrayBuffer kbuf = new ByteArrayBuffer();

        public Advancerator(final ITupleCursor<E> src, Object context,
                final Advancer<E> filter) {

            this.src = src;

            this.context = context;
            
            this.filter = filter;

        }

        public boolean hasNext() {

            if(exhausted) return false;
            
            return src.hasNext();

        }

        public ITuple<E> next() {

            final ITuple<E> tuple = src.next();

            if (log.isInfoEnabled()) {

                log.info("next: " + tuple);

            }

            // copy the key for the current tuple.
            kbuf.reset().copyAll(tuple.getKeyBuffer());

            try {

                // skip to the next tuple of interest.
                filter.advance(tuple);
                
            } catch(KeyOutOfRangeException ex) {

                /*
                 * We have advanced beyond a key range constraint imposed either
                 * by the ITupleCursor or by an index partition. In either case
                 * we treat the source iterator as if it was exhausted.
                 * 
                 * If the advancer is running over a partitioned index, then the
                 * partitioned iterator will automatically check the next index
                 * partition that would be spanned by the range constraints on
                 * the source cursor (not the local cursor).
                 */
                
                log.warn("Exhausted - advanced beyond key range constraint: " + ex);
                
                exhausted = true;
                
            }

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
