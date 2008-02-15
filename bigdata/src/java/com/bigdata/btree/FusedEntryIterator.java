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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.isolation.IsolatedFusedView;

/**
 * <p>
 * An aggregate iterator view of the one or more source {@link IEntryIterator}s.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedEntryIterator implements IEntryIterator {

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
     * True iff {@link IRangeQuery#DELETED} semantics will be applied.
     */
    private final boolean deleted;

    /**
     * The source iterators in the order given to the ctor.
     */
    private final IEntryIterator[] itrs;

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
    private final ITuple[] sourceTuples;

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
            byte[] toKey, int capacity, int flags, IEntryFilter filter) {

        FusedView.checkSources(srcs);
        
        assert srcs != null;

        assert srcs.length > 0;

        this.n = srcs.length;

        // visit deleted entries?
        this.deleted = (flags & IRangeQuery.DELETED) != 0;
        
        itrs = new IEntryIterator[n];
        
        for (int i = 0; i < n; i++) {

            /*
             * Note: We request DELETED so that we will see deleted entries.
             * This is necessary in order for processing to stop at the first
             * entry for a give key regardless of whether it is deleted or not.
             */
            
            itrs[i] = srcs[i].rangeIterator(fromKey, toKey, capacity, flags
                    | IRangeQuery.DELETED, filter);

        }

        sourceTuples = new ITuple[n];
        
        log.info("nsources="+n+", flags="+flags+", deleted="+deleted);

    }

    /**
     * 
     * @param deleted
     *            true iff you want the aggregate iterator to have
     *            {@link IRangeQuery#DELETED} semantics (i.e., deleted index
     *            entries will be visible in the aggregate iterator).
     * @param srcs
     *            Each source iterator MUST specify {@link IRangeQuery#DELETED}
     *            in order for the fused view to be able to recognize a deleted
     *            index entry and discard a historical undeleted entry later in
     *            the predence order for the view.
     */
    public FusedEntryIterator(boolean deleted, IEntryIterator[] srcs) {

        assert srcs != null;

        assert srcs.length > 0;

        this.n = srcs.length;

        this.deleted = deleted;
        
        for (int i = 0; i < n; i++) {

            assert srcs[i] != null;

        }

        this.itrs = srcs;

        sourceTuples = new ITuple[n];

        log.info("nsources="+n+", deleted="+deleted);

    }

    public boolean hasNext() {

        /*
         * Until we find an undeleted tuple (or any tuple if DELETED is
         * true).
         */
        while (true) {

            if (current != -1) {

                log.info("Already matched: source=" + current);
                
                return true;

            }

            /*
             * First, make sure that we have a tuple for each source iterator.
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
             * Now consider the current tuple for each source iterator and
             * choose the tuple whose key orders less than all the others. This
             * is the next tuple to be visited by the aggregate iterator.
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

    public ITuple next() {

        if (!hasNext())
            throw new NoSuchElementException();

        final long nvisited = this.nvisited++;
        
        // save index of the iterator whose tuple is to be visited.
        lastVisited = current;
        
        final ITuple tuple = new DelegateTuple(sourceTuples[current]) {

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
     * Clear tuples from other sources having the same key as the current tuple.
     * <p>
     * If any source has the same key then we clear it's tuple since we have
     * already returned a tuple for that key. This is necessary in order for the
     * aggregate iterator to skip over additional tuples in other source streams
     * once we identify a source stream having a tuple for a given key.
     * <p>
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
