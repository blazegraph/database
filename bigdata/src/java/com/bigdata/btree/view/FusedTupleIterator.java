/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.btree.view;

import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DelegateTuple;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

/**
 * <p>
 * An aggregate iterator view of the one or more source {@link ITupleIterator}s.
 * </p>
 * 
 * @see FusedView#rangeIterator(byte[], byte[], int, int, com.bigdata.btree.filter.IFilterConstructor)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedTupleIterator<I extends ITupleIterator<E>, E> implements
        ITupleIterator<E> {

    protected static final Logger log = Logger.getLogger(FusedTupleIterator.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The flags specified to the ctor.
     */
    protected final int flags;
    
    /**
     * True iff {@link IRangeQuery#DELETED} semantics will be applied (that is,
     * true if the caller wants to see the deleted tuples).
     */
    protected final boolean deleted;

    /**
     * The #of source iterators.
     */
    protected final int n;

    /**
     * The source iterators in the order given to the ctor.
     */
    protected final I[] sourceIterator;

    /**
     * The current {@link ITuple} from each source and <code>null</code> if we
     * need to get another {@link ITuple} from that source. The value for a
     * source iterator that has been exhausted will remain <code>null</code>.
     * When all entries in this array are <code>null</code> there are no more
     * {@link ITuple}s to be visited and we are done.
     * <p>
     * Note: We process the iterators in the order given. Unless
     * {@link IRangeQuery#DELETED} are being materialized we will only visit the
     * {@link ITuple} for the first iterator having a entry for that key. This
     * is achieved by setting the elements in this array to <code>null</code>
     * for any iterator having a {@link ITuple} for the same key.
     */
    protected final ITuple<E>[] sourceTuple;

//    /**
//     * <code>true</code> iff {@link IRangeQuery#REVERSE} was specified for the
//     * source iterator. When {@link IRangeQuery#REVERSE} was specified then the
//     * source iterators will all use reverse traversal and we change the sense
//     * of the comparison for the keys so that we impose a total descending key
//     * order rather than a total ascending key order.
//     */
//    protected final boolean reverseScan;
    
    /**
     * Index into {@link #sourceIterator} and {@link #sourceTuple} of the iterator whose
     * tuple will be returned next -or- <code>-1</code> if we need to choose the
     * next {@link ITuple} to be visited.
     */
    protected int current = -1;
    
    /**
     * The index into {@link #sourceIterator} of the iterator whose tuple was last
     * returned by {@link #next()}.
     */
    protected int lastVisited = -1;
    
    /**
     * Create an {@link ITupleIterator} reading from an ordered set of source
     * {@link ITupleIterator}s. The order of the source iterators is important.
     * The first matching {@link ITuple} for a key will be the {@link ITuple}
     * that gets returned. Other {@link ITuple}s for the same key will be from
     * source iterators later in the precedence order will be silently skipped.
     * 
     * @param flags
     *            The flags specified for the source iterators (it is up to the
     *            caller to make sure that the same flags were used for all
     *            iterators).
     * @param deleted
     *            <code>false</code> unless you want to see the deleted tuples
     *            in your application.
     * @param sourceIterators
     *            Each source iterator MUST specify {@link IRangeQuery#DELETED}.
     *            This is NOT optional. The {@link IRangeQuery#DELETED} is
     *            required for the fused view iterator to recognize a deleted
     *            index entry and discard a historical undeleted entry later in
     *            the predence order for the view.
     */
    @SuppressWarnings("unchecked")
    public FusedTupleIterator(final int flags, final boolean deleted,
            final I[] sourceIterators) {

        assert sourceIterators != null;

        assert sourceIterators.length > 0;

        this.flags = flags;
        
        this.n = sourceIterators.length;

        // iff the aggregate iterator should visit deleted entries.
        this.deleted = deleted;
        
        for (int i = 0; i < n; i++) {

            assert sourceIterators[i] != null;

        }

        this.sourceIterator = sourceIterators;

        sourceTuple = new ITuple[n];

//        reverseScan = (flags & IRangeQuery.REVERSE) != 0;
        
        if (INFO)
            log.info("nsources=" + n + ", deleted=" + deleted);//+", reverseScan="+reverseScan);

    }
    
    public boolean hasNext() {

        /*
         * Until we find an undeleted tuple (or any tuple if DELETED is
         * true).
         */
        while (true) {

            if (current != -1) {

                if(INFO) log.info("Already matched: source=" + current);
                
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

                        if (DEBUG)
                            log.debug("read sourceTuple[" + i + "]="
                                    + sourceTuple[i]);
                        
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

                byte[] key = null; // smallest key so far.

                for (int i = 0; i < n; i++) {

                    if (sourceTuple[i] == null) {

                        // This source is exhausted.
                        
                        continue;
                        
                    }

                    if (current == -1) {

                        current = i;

                        key = sourceTuple[i].getKey();

                        assert key != null;

                    } else {

                        final byte[] tmp = sourceTuple[i].getKey();

                        final int ret = BytesUtil.compareBytes(tmp, key);

//                        if (reverseScan ? ret > 0 : ret < 0) {
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
            
            if (sourceTuple[current].isDeletedVersion() && !deleted) {

                /*
                 * The tuple is marked as "deleted" and the caller did not
                 * request deleted tuples so we skip this key and begin again
                 * with the next key visible under the fused iterator view.
                 */

                if(INFO) {
                    
                    log.info("Skipping deleted: source=" + current + ", tuple="
                            + sourceTuple[current]);
                    
                }

                /*
                 * Clear tuples from other sources having the same key as the
                 * current tuple.
                 */
                
                clearCurrent();
                
                continue;
                
            }

            if(INFO) {
                
                log.info("Will visit next: source=" + current + ", tuple: "
                        + sourceTuple[current]);
                
            }
            
            return true;
            
        }

    }

    public ITuple<E> next() {

        if (!hasNext())
            throw new NoSuchElementException();
        
        return consumeLookaheadTuple();
        
    }

    /**
     * {@link ITuple} revealing a tuple selected from a {@link FusedView} by a
     * {@link FusedTupleIterator}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class FusedTuple<E> extends DelegateTuple<E> {

        private final int sourceIndex;

        private final long nvisited;

        /**
         * Note: Not whether or not this tuple is deleted but rather whether or
         * not the {@link FusedTupleIterator} is visiting deleted tuples.
         * 
         * @see FusedTupleIterator#deleted
         */
        private final boolean deleted;
        
        public FusedTuple(final ITuple<E> delegate, final int sourceIndex,
                final long nvisited, final boolean deleted) {

            super(delegate);

            this.sourceIndex = sourceIndex;

            this.nvisited = nvisited;

            this.deleted = deleted;
            
        }

        /**
         * Turn off the deleted flag unless {@link IRangeQuery#DELETED} was
         * specified for the {@link FusedTupleIterator} view.
         */
        public int flags() {

            int flags = super.flags();

            if (!deleted) {

                flags &= ~IRangeQuery.DELETED;

            }

            return flags;

        }

        public int getSourceIndex() {

            return sourceIndex;

        }

        /** return the total visited count. */
        @Override
        public long getVisitCount() {

            return nvisited;

        }

    }

    /**
     * Consume the {@link #current} source {@link ITuple}.
     * 
     * @return The {@link #current} tuple.
     */
    protected ITuple<E> consumeLookaheadTuple() {

        final long nvisited = this.nvisited++;
        
        // save index of the iterator whose tuple is to be visited.
        final int sourceIndex = lastVisited = current;
        
        final ITuple<E> tuple = new FusedTuple<E>(sourceTuple[current],
                sourceIndex, nvisited, deleted);

        // clear tuples from other sources having the same key as the current tuple.
        clearCurrent();
        
        if(INFO) {

            log.info("returning: tuple=" + tuple);
            
        }
        
        return tuple;

    }
    
    /**
     * <p>
     * Clear tuples from other sources having the same key as the current tuple.
     * </p>
     * <p>
     * If any source has the same key then we clear it's tuple since we have
     * already returned a tuple for that key. This is necessary in order for the
     * aggregate iterator to skip over additional tuples in other source streams
     * once we identify a source stream having a tuple for a given key.
     * </p>
     * 
     * @todo if we wanted to see duplicate tuples when reading from sources that
     *       are NOT a {@link FusedView} then an additional flag could be
     *       introduced to the ctor and this method could be modified to NOT
     *       skip over the tuples whose key is EQ to the current key.
     */
    protected void clearCurrent() {

        assert current != -1;
        
        final byte[] key = sourceTuple[current].getKey();
        
        for (int i = current + 1; i < n; i++) {

            if (sourceTuple[i] == null) {
                
                // this iterator is exhausted.
                
                continue;
                
            }

            final byte[] tmp = sourceTuple[i].getKey();

            final int ret = BytesUtil.compareBytes(key, tmp);

//            if (ret < 0)
//                throw new AssertionError();

            if (ret == 0) {

                // discard tuple.
                
                sourceTuple[i] = null;

            }

        }

        // clear the tuple that we are returning so that we will read another from that source.
        sourceTuple[current] = null;

        // clear so that we will look again.
        current = -1;
        
    }
    
    /**
     * The #of tuples visited so far.
     */
    private long nvisited = 0L;

    /**
     * Operation is not supported.
     * <p>
     * Note: Remove is not supported at this level. Instead you must use a
     * {@link FusedTupleCursor}. This is handled automatically by
     * {@link FusedView#rangeIterator(byte[], byte[], int, int, com.bigdata.btree.filter.IFilterConstructor)}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }
    
}
