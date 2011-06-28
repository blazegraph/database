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
 * Created on Aug 4, 2008
 */

package com.bigdata.sparse;

import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.filter.TupleTransformer;
import com.bigdata.btree.filter.LookaheadTupleFilter.ILookaheadTupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.sparse.TPS.TPV;

/**
 * Transforms an {@link ITupleIterator} reading directly on an {@link IIndex}
 * backing a {@link SparseRowStore} into an {@link ITupleIterator} visiting
 * logical {@link ITPS} rows.
 * 
 * @todo Look at the remove semantics for the sparse row store. This would have
 *       to delete the logical row, or at least all property values that were
 *       materialized from the logical row. in fact, since this is a many:one
 *       transform, we must override the remove() impl for the inner class on
 *       {@link TupleTransformer}.
 * 
 * @todo You could replace the {@link AtomicRowRead} with this iterator by
 *       setting the capacity to ONE (1). However, that will do more work when
 *       we are only trying to read a single row on a local index since we will
 *       have to serialize and then de-serialize the {@link TPS} for that
 *       logical row. (For a remote read the effort should be the same).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRowFilter extends TupleTransformer<TPV, TPS> implements
        IRowStoreConstants {

    private static final long serialVersionUID = 4410286292657970569L;

    protected static transient final Logger log = Logger
            .getLogger(AtomicRowFilter.class);

//    protected static transient final boolean INFO = log.isInfoEnabled();
//
//    protected static transient final boolean DEBUG = log.isDebugEnabled();
    
    private final Schema schema;

    private final long fromTime;
    
    private final long toTime;

    private final INameFilter nameFilter;

    /**
     * @param schema
     *            The schema governing the row.
     * @param fromTime
     *            The first timestamp for which timestamped property values will
     *            be accepted.
     * @param toTime
     *            The first timestamp for which timestamped property values will
     *            NOT be accepted -or- {@link IRowStoreConstants#CURRENT_ROW} to
     *            accept only the most current binding whose timestamp is GTE
     *            <i>fromTime</i>.
     * @param nameFilter
     *            An optional filter used to select the values for property
     *            names accepted by that filter.
     */
    protected AtomicRowFilter(final Schema schema, final long fromTime,
            final long toTime, final INameFilter nameFilter) {

        super(TPSTupleSerializer.newInstance());

        SparseRowStore.assertArgs(schema, Boolean.TRUE/* fake */, fromTime,
                toTime);

        this.schema = schema;

        this.fromTime = fromTime;
        
        this.toTime = toTime;

        this.nameFilter = nameFilter;

    }

    @Override
    protected ITupleIterator<TPS> newTransformer(
            ILookaheadTupleIterator<TPV> src, final Object context) {

        return new Transformerator<TPV, TPS>(src, context);

    }
    
    private class Transformerator<E extends TPV/* src */, F extends TPS/* out */>
            implements ITupleIterator<F> {
    
    /** #of logical rows read so far. */
    private transient long nvisited = 0;

    /**
     * One step lookahead for the output iterator containing the next
     * {@link TPS} to be returned.
     */
    private transient TPS current = null;
    
    /**
     * The prefix key for the {@link #current} {@link TPS}. This is
     * identified when we read the first tuple from the logical row. Doing
     * it this way means that we do not need to encode the primary key and
     * avoids a dependency on a correctly configured {@link IKeyBuilder}.
     */
    private transient byte[] prefix;
    
    private final ILookaheadTupleIterator<E> src;
    
    private final Object context;
    
    /**
     * Builds iterator that reads the source tuples and visits the transformed
     * tuples.
     * 
     * @param src
     *            Visits the source tuples.
     */
    public Transformerator(final ILookaheadTupleIterator<E> src, final Object context) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;
        
        this.context = context;
        
    }
    
    /**
     * Each visited tuple corresponds to a logical row. The key for the
     * tuple is the {schema,primaryKey} for that logical row. The value is
     * the serialized {@link TPS} object for the corresponding logical row.
     * 
     * @see ITuple#getObject()
     */
    public ITuple<F> next() {

        /*
         * Note: Sets [current] as a side-effect.
         */
        if (!hasNext())
            throw new NoSuchElementException();

        assert current != null;

        final TPS tps = current;

        current = null;

        final AbstractTuple<F> tuple = new AbstractTuple<F>(IRangeQuery.DEFAULT) {

            /**
             * @todo This can't be implemented since the tuples may have
             *       come from different backing AbstractBTree's in the
             *       view. If blob references are to be supported they will
             *       have to be transformed during the atomic row read so
             *       that the incorporate the source index from which the
             *       block can be read.
             *       <p>
             *       There are some notes on introducing blob support into
             *       the {@link SparseRowStore} on that class. Basically
             *       there would need to be a blob value type and it would
             *       have to carry the UUID for the store from which the
             *       blob could be read as part of the property value stored
             *       in a TPS.
             *       <p>
             *       Currently, blobs are stored in the same index as the
             *       blob reference. In order to allow blobs to be stored in
             *       a different index the name of the scale out index would
             *       have to be in the blob reference.
             */
            public int getSourceIndex() {

                throw new UnsupportedOperationException();

            }

            public ITupleSerializer getTupleSerializer() {

                return tupleSer;

            }

        };

        tuple.copyTuple(prefix, tupleSer.serializeVal(tps));

        // visited another logical row.
        nvisited++;

        return tuple;

    }

    /**
     * One step lookahead for logical rows. When {@link #current} is
     * <code>null</code> this reads the next tuple and extracts the primary
     * key for the logical row for that tuple. It then reads tuples until it
     * reaches the first tuple that does not belong to the current logical row.
     * The resulting logical row is set on {@link #current}.
     */
    public boolean hasNext() {

        if (current != null) {

            // the next logical row is ready.
            return true;

        }

        // Result set object.
        final TPS tps = new TPS(schema, 0L);

        // clear the prefix - it serves as a flag for this loop.
        prefix = null;
        
        while (src.hasNext()) {

            final ITuple<E> tuple = src.next();

            final byte[] prefix = new KeyDecoder(tuple.getKey()).getPrefix();

            if (this.prefix == null) {

                // start of a new logical row.
                this.prefix = prefix;

            } else if (!BytesUtil.bytesEqual(this.prefix, prefix)) {

                // end of the current logical row.
                src.pushback();
                
                break;

            }

            // extract a property value from the tuple.
            handleTuple(tps, tuple);

        }

        if (prefix != null) {

            /*
             * Found at least one tuple belonging to a logical row.
             * 
             * Note: The logical row MAY be empty depending on the
             * INameFilter and timestamp, but we will visit it anyway.
             */

            if (toTime == CURRENT_ROW) {

                /*
                 * Strip out everything except the current row.
                 */

                current = tps.currentRow();
                
            } else {
            
                current = tps;
                
            }

            return true;

        }

        // Nothing left.
        return false;

    }

    /**
     * Extracts a property value from the tuple and adds it to the logical
     * row if the timestamp and optional {@link INameFilter} are satisified
     * for the tuple.
     * 
     * @param tps
     *            The logical row.
     * @param tuple
     *            The tuple.
     */
    private void handleTuple(final TPS tps, final ITuple tuple) {

        assert tps != null;

        assert tuple != null;

        final byte[] key = tuple.getKey();

        // Decode the key so that we can get the column name.
        final KeyDecoder keyDecoder = new KeyDecoder(key);

        // The column name.
        final String col = keyDecoder.getColumnName();

        if (nameFilter != null && !nameFilter.accept(col)) {

            // Skip property names that have been filtered out.

            if (log.isDebugEnabled()) {

                log.debug("Skipping property: name=" + col + " (filtered)");

            }

            return;

        }

        /*
         * Skip column values whose timestamp lies outside of the specified
         * half-open range.
         */
        final long columnValueTimestamp = keyDecoder.getTimestamp();
        {

            if (columnValueTimestamp < fromTime) {

                if (log.isDebugEnabled()) {

                    log.debug("Ignoring earlier revision: col=" + col
                            + ", fromTime=" + fromTime + ", timestamp="
                            + columnValueTimestamp);

                }

                return;

            }

            if (toTime != CURRENT_ROW && columnValueTimestamp >= toTime) {

                if (log.isDebugEnabled()) {

                    log.debug("Ignoring later revision: col=" + col
                            + ", toTime=" + toTime + ", timestamp="
                            + columnValueTimestamp);

                }

                return;

            }
            
        }

        /*
         * Decode the value. A [null] indicates a deleted property
         * value.
         */

        final byte[] val = tuple.getValue();

        final Object v = ValueType.decode(val);

        /*
         * Add this timestamped property value to the collection.
         */

        tps.set(col, columnValueTimestamp, v);

        if (log.isInfoEnabled())
                log.info("Accept: name=" + col + ", timestamp="
                        + columnValueTimestamp + ", value=" + v + ", key="
                        + BytesUtil.toString(key));

    }

    /**
     * Not supported.
     * 
     * @throws UnsupportedOperationException
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }
    
    }

}
