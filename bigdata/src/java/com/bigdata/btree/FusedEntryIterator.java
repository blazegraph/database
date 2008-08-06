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
package com.bigdata.btree;

import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.filter.IFilterConstructor;

/**
 * <p>
 * An aggregate iterator view of the one or more source {@link ITupleIterator}s.
 * </p>
 * <p>
 * Note: This implementations supports {@link IRangeQuery#REVERSE}. This may be
 * used to locate the {@link ITuple} before a specified key, which is a
 * requirement for several aspects of the overall architecture including atomic
 * append of file blocks, locating an index partition, and finding the last
 * member of a set or map.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedEntryIterator<E> implements ITupleIterator<E> {

    protected static final Logger log = Logger.getLogger(FusedEntryIterator.class);

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
     * The #of source iterators.
     */
    private final int n;

    /**
     * True iff {@link IRangeQuery#DELETED} semantics will be applied (that is,
     * true if the caller wants to see the deleted tuples).
     */
    private final boolean deleted;

    /**
     * The source iterators in the order given to the ctor.
     */
    private final ITupleIterator[] itrs;

    /**
     * The current key from each source and <code>null</code> if we need to
     * get the next key from that source. The value for an iterator that has
     * been exhausted will remain <code>null</code>. When all entries in this
     * array are <code>null</code> there are no more {@link ITuple}s to be
     * visited and we are done.
     * <p>
     * Note: We process the iterators in the order given. Unless
     * {@link IRangeQuery#DELETED} are being materialized we will only visit
     * the {@link ITuple} for the first iterator having a entry for that key.
     * This is achieved by setting the elements in this array to
     * <code>null</code> for any iterator having a tuple for the same key.
     */
    private final ITuple<E>[] sourceTuples;

    /**
     * <code>true</code> iff {@link IRangeQuery#REVERSE} was specified for the
     * iterator. When {@link IRangeQuery#REVERSE} was specified then the
     * underlying iterators will all use reverse traversal and we change the
     * sense of the comparison for the keys so that we impose a total descending
     * key order rather than a total ascending key order.
     */
    private final boolean reverseScan;
    
    /**
     * Index into {@link #itrs} and {@link #sourceTuples} of the iterator whose
     * tuple will be returned next -or- <code>-1</code> if we need to choose the
     * next {@link ITuple} to be visited.
     */
    private int current = -1;
    
    /**
     * The index into {@link #itrs} of the iterator whose tuple was last
     * returned by {@link #next()}.
     */
    private int lastVisited = -1;
    
    public FusedEntryIterator(AbstractBTree[] srcs, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IFilterConstructor filter) {

        FusedView.checkSources(srcs);
        
        assert srcs != null;

        assert srcs.length > 0;

        this.n = srcs.length;

        // iff the aggregate iterator should visit deleted entries.
        this.deleted = (flags & IRangeQuery.DELETED) != 0;
        
        itrs = new ITupleIterator[n];
        
        for (int i = 0; i < n; i++) {

            /*
             * Note: We request KEYS since we need to compare the keys in order
             * to decide which tuple to return next.
             * 
             * Note: We request DELETED so that we will see deleted entries.
             * This is necessary in order for processing to stop at the first
             * entry for a give key regardless of whether it is deleted or not.
             * If the caller does not want to see the deleted tuples, then they
             * are silently dropped from the aggregate iterator.
             */
            
            itrs[i] = srcs[i].rangeIterator(fromKey, toKey, capacity, flags
                    | IRangeQuery.KEYS | IRangeQuery.DELETED, filter);

        }

        sourceTuples = new ITuple[n];

        reverseScan = (flags & IRangeQuery.REVERSE) != 0;
        
        if (INFO)
            log.info("nsources=" + n + ", flags=" + flags + ", deleted="
                    + deleted + ", reverseScan=" + reverseScan);

    }

    /**
     * Variant that may be used on {@link IIndex} sources other than a
     * {@link FusedView}. The order of the source iterators is important. The
     * first matching tuple for a key will be the tuple that gets returned. When
     * the source indices use delete markers and should be interpreted as a
     * fused view, then the source iterators MUST specify
     * {@link IRangeQuery#DELETED} such that the deleted tuples are visible to
     * this iterator. If you do not want the deleted tuples to be visible to
     * your application then you would additionally specify
     * <code>deleted := false</code>.
     * 
     * @param flags
     *            The flags specified for the source iterators (it is up to the
     *            caller to make sure that the same flags were used for all
     *            iterators).
     * @param deleted
     *            <code>true</code> iff you want to see the deleted tuples.
     * @param srcs
     *            Each source iterator MUST specify {@link IRangeQuery#DELETED}
     *            in order for the fused view to be able to recognize a deleted
     *            index entry and discard a historical undeleted entry later in
     *            the predence order for the view.
     */
    public FusedEntryIterator(int flags, boolean deleted, ITupleIterator[] srcs) {

        assert srcs != null;

        assert srcs.length > 0;

        this.n = srcs.length;

        // iff the aggregate iterator should visit deleted entries.
        this.deleted = deleted;
        
        for (int i = 0; i < n; i++) {

            assert srcs[i] != null;

        }

        this.itrs = srcs;

        sourceTuples = new ITuple[n];

        reverseScan = (flags & IRangeQuery.REVERSE) != 0;
        
        if (INFO)
            log.info("nsources=" + n + ", deleted=" + deleted+", reverseScan="+reverseScan);

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

                if (sourceTuples[i] == null) {

                    if (itrs[i].hasNext()) {

                        sourceTuples[i] = itrs[i].next();

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

                    if (sourceTuples[i] == null) {

                        // This source is exhausted.
                        
                        continue;
                        
                    }

                    if (current == -1) {

                        current = i;

                        key = sourceTuples[i].getKey();

                        assert key != null;

                    } else {

                        final byte[] tmp = sourceTuples[i].getKey();

                        final int ret = BytesUtil.compareBytes(tmp, key);

                        if (reverseScan ? ret > 0 : ret < 0) {

                            /*
                             * This key orders LT the current key (or GT if
                             * [reverseScan] is true).
                             * 
                             * Note: This test MUST be strictly LT (or GE) since
                             * LTE (GTE) would break the precedence in which we
                             * are processing the source iterators and give us
                             * the key from the last source by preference when
                             * we need the key from the first source by
                             * preference.
                             */

                            current = i;

                            key = tmp;

                        }

                    }

                }

                assert current != -1;

            }
            
            if (sourceTuples[current].isDeletedVersion() && !deleted) {

                /*
                 * The tuple is marked as "deleted" and the caller did not
                 * request deleted tuples so we skip this key and begin again
                 * with the next key visible under the fused iterator view.
                 */

                if(INFO) {
                    
                    log.info("Skipping deleted: source=" + current + ", tuple="
                            + sourceTuples[current]);
                    
                }

                /*
                 * Clear tuples from other sources having the same key as the
                 * current tuple.
                 */
                
                clearCurrent();
                
                continue;
                
            }

            if(INFO) {
                
                log.info("Will visit: source=" + current + ", tuple: "
                        + sourceTuples[current]);
                
            }
            
            return true;
            
        }

    }

    public ITuple<E> next() {

        if (!hasNext())
            throw new NoSuchElementException();

        final long nvisited = this.nvisited++;
        
        // save index of the iterator whose tuple is to be visited.
        lastVisited = current;
        
        final ITuple<E> tuple = new DelegateTuple<E>(sourceTuples[current]) {

            /** return the total visited count. */
            @Override
            public long getVisitCount() {

                return nvisited;
                
            }

        };

        // clear tuples from other sources having the same key as the current tuple.
        clearCurrent();
        
        if(INFO) {

            log.info("Source=" + current + ", tuple=" + tuple);
            
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
     *       introduced to the variant ctor and this method could be modified to
     *       NOT skip over the tuples whose key is EQ to the current key.
     */
    private void clearCurrent() {

        assert current != -1;
        
        final byte[] key = sourceTuples[current].getKey();
        
        for (int i = current + 1; i < n; i++) {

            if (sourceTuples[i] == null) {
                
                // this iterator is exhausted.
                
                continue;
                
            }

            final byte[] tmp = sourceTuples[i].getKey();

            final int ret = BytesUtil.compareBytes(key, tmp);

//            if (ret < 0)
//                throw new AssertionError();

            if (ret == 0) {

                // discard tuple.
                
                sourceTuples[i] = null;

            }

        }

        // clear the tuple that we are returning so that we will read another from that source.
        sourceTuples[current] = null;

        // clear so that we will look again.
        current = -1;
        
    }
    
    private long nvisited = 0L;

    /**
     * Delegates the operation to the appropriate source iterator.
     */
    public void remove() {

        if (lastVisited == -1)
            throw new IllegalStateException();

        itrs[lastVisited].remove();

    }

}
