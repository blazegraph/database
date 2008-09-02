/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 1, 2008
 */

package com.bigdata.btree;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.ITupleFilter;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;

/**
 * Chunked range iterator running against a local index or index view.
 * <p>
 * Note: When {@link IRangeQuery#REMOVEALL} is specified without the
 * {@link IRangeQuery#CURSOR} flag, the iterator will populate its buffers up to
 * the capacity and then delete behind once the buffer is full or as soon as the
 * iterator is exhausted. This approach works with the somewhat faster
 * {@link ITupleIterator} construct. When the {@link IRangeQuery#CURSOR} flag is
 * specified, we directly use the {@link ITupleCursor} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedLocalRangeIterator<E> extends AbstractChunkedTupleIterator<E> {

    /** The backing {@link IIndex}. */
    protected final IIndex ndx;
    
    /** Iff {@link IRangeQuery#REMOVEALL} was specified. */
    private final boolean removeAll;

    /** Iff {@link IRangeQuery#CURSOR} was specified. */
    private final boolean cursor;
    
    /**
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public ChunkedLocalRangeIterator(IIndex ndx, byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilterConstructor filter) {
        
        super(fromKey, toKey, capacity, flags, filter);
        
        if (ndx == null)
            throw new IllegalArgumentException();
        
        this.ndx = ndx;
        
        removeAll  = (flags & IRangeQuery.REMOVEALL) != 0;
        
        cursor = (flags & IRangeQuery.CURSOR) != 0;
        
    }

    /**
     * Note: <i>timestamp</i> is ignored since we are reading against a local
     * index object.
     */
    @Override
    protected ResultSet getResultSet(final long timestamp,
            final byte[] fromKey, final byte[] toKey, final int capacity,
            final int flags, final IFilterConstructor filter) {

        /*
         * Note: This turns off the REMOVEALL flag unless CURSOR was also
         * specified.
         */
        final int tmpFlags = (removeAll && !cursor) //
                ? flags & ~IRangeQuery.REMOVEALL //
                : flags;
        
        /*
         * Figure out the upper bound on the #of tuples that could be
         * materialized.
         * 
         * Note: the upper bound on the #of key-value pairs in the range is
         * Integer.MAX_VALUE.
         * 
         * @todo Shouldn't the upper bound be something a little less large,
         * such as 100,000? If the upper bound is in fact the limit on the #of
         * tuples to be buffered at a time, then Integer.MAX_VALUE is simply too
         * large.
         */
        
        final int rangeCount = (int) Math.min((long) Integer.MAX_VALUE, ndx
                .rangeCount(fromKey, toKey));

        final int limit = (rangeCount > capacity ? capacity : rangeCount);

        /*
         * Iterator that will visit the key range.
         * 
         * Note: We always visit the keys regardless of whether we pass them
         * on to the caller. This is necessary in order for us to set the
         * [lastKey] field on the result set and that is necessary to
         * support continuation queries.
         */
        
        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, limit,
                tmpFlags | IRangeQuery.KEYS, filter);
        
        /*
         * @todo shouldn't we pass [flags] (not [tmpFlags]) into the ctor? we
         * don't need to materialize the keys to the remote caller, just so we
         * can get the [lastKey].
         */ 
        return new ResultSet(ndx, limit, tmpFlags, itr);
        
    }

    /**
     * Visits the next tuple, queuing it for removal unless
     * {@link IRangeQuery#CURSOR} was specified, in which case it is deleted
     * immediately.
     * <p>
     * Note: Queuing for removal is done only for the local index so that data
     * service range iterators will do their deletes on the local index when
     * this range iterator runs rather than buffering the keys and then sending
     * back a batch delete to the index later (this would also make
     * {@link IRangeQuery#REMOVEALL} non-atomic).
     */
    public ITuple<E> next() {

        final ITuple<E> tuple = super.next();

        if (removeAll) {

            if (!cursor) {
                
                /*
                 * Queue the key for removal IFF CURSOR was NOT specified.
                 */

                remove();
                
            }

        }

        return tuple;

    }

    @Override
    protected void deleteBehind(int n, Iterator<byte[]> keys) {

        while(keys.hasNext()) {
        
            ndx.remove(keys.next());
        
        }
        
    }

    @Override
    protected void deleteLast(byte[] key) {

        ndx.remove(key);
        
    }

    @Override
    protected IBlock readBlock(final int sourceIndex, final long addr) {

        if(ndx instanceof AbstractBTree ) {
            
            /*
             * A local B+Tree.
             */

            if (sourceIndex != 0) {

                // Since this is not a view the source index MUST be zero.
                
                throw new IllegalArgumentException();
                
            }
            
            return new IBlock() {

                public long getAddress() {
                    
                    return addr;
                    
                }

                public InputStream inputStream() {
                    
                    final IRawStore store = ((AbstractBTree)ndx).getStore();

                    final ByteBuffer buf = store.read(addr);
                    
                    return new ByteBufferInputStream(buf);
                    
                }

                public int length() {
                    
                    final IRawStore store = ((AbstractBTree)ndx).getStore();
                    
                    return store.getByteCount(addr);
                    
                }
                
            };
            
        } else {
         
            /*
             * @todo A view onto two or more B+Trees. Some of these can be index
             * segments. Each index segment is in its own file store, so we have
             * to direct the request to the correct backing store. Make sure
             * that we do not double-open a index segment file store.
             */
            
            // using source index, read record from the identifed source.
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * Always returns 0L. This value is ignored by
     * {@link #getResultSet(long, byte[], byte[], int, int, ITupleFilter)}.
     */
    @Override
    protected long getTimestamp() {
        
        return 0L;
        
    }

    /**
     * Returns <code>true</code> since the read will be consistent (it reads
     * against the same index object for each {@link ResultSet}) but the values
     * returned by {@link #getTimestamp()} and {@link #getReadTime()} are
     * ignored by
     * {@link #getResultSet(long, byte[], byte[], int, int, ITupleFilter)}
     * (since it is reading against a local index object).
     */
    @Override
    public boolean getReadConsistent() {
        
        return false;
        
    }

}
