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
public class AtomicRowFilter extends TupleTransformer<TPV, TPS> {

    private static final long serialVersionUID = 4410286292657970569L;

    protected static final Logger log = Logger
            .getLogger(AtomicRowFilter.class);

    private final Schema schema;

    private final long timestamp;

    private final INameFilter nameFilter;

    /**
     * @param schema
     *            The schema governing the row.
     * @param timestamp
     *            A timestamp to obtain the value for the named property
     *            whose timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most
     *            recent value for the property.
     * @param nameFilter
     *            An optional filter used to select the values for property
     *            names accepted by that filter.
     */
    protected AtomicRowFilter(Schema schema, long timestamp,
            INameFilter nameFilter) {

        super(TPSTupleSerializer.newInstance());

        if (schema == null)
            throw new IllegalArgumentException();

        this.schema = schema;

        this.timestamp = timestamp;

        this.nameFilter = nameFilter;

    }

    /** #of logical rows read so far. */
    private int nvisited = 0;

    /**
     * One step lookahead for the output iterator containing the next
     * {@link TPS} to be returned.
     */
    private TPS current = null;
    
    /**
     * The prefix key for the {@link #current} {@link TPS}. This is
     * identified when we read the first tuple from the logical row. Doing
     * it this way means that we do not need to encode the primary key and
     * avoids a dependency on a correctly configured {@link IKeyBuilder}.
     */
    private byte[] prefix;

    /**
     * Each visited tuple corresponds to a logical row. The key for the
     * tuple is the {schema,primaryKey} for that logical row. The value is
     * the serialized {@link TPS} object for the corresponding logical row.
     * 
     * @see ITuple#getObject()
     */
    public ITuple<TPS> next(ILookaheadTupleIterator<TPV> src) {

        if (!hasNext(src))
            throw new NoSuchElementException();

        assert current != null;

        final TPS tps = current;

        current = null;

        final AbstractTuple<TPS> tuple = new AbstractTuple<TPS>(IRangeQuery.ALL) {

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
    public boolean hasNext(ILookaheadTupleIterator<TPV> src) {

        if (current != null) {

            // the next logical row is ready.
            return true;

        }

        // Result set object.
        final TPS tps = new TPS(schema, timestamp);

        // clear the prefix - it serves as a flag for this loop.
        prefix = null;
        
        while (src.hasNext()) {

            final ITuple<TPV> tuple = src.next();

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

            current = tps;

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
    private void handleTuple(TPS tps, ITuple tuple) {

        assert tps != null;

        assert tuple != null;

        final byte[] key = tuple.getKey();

        final byte[] val = tuple.getValue();

        // Decode the key so that we can get the column name.
        final KeyDecoder keyDecoder = new KeyDecoder(key);

        // The column name.
        final String col = keyDecoder.getColumnName();

        if (nameFilter != null && !nameFilter.accept(col)) {

            // Skip property names that have been filtered out.

            if (log.isDebugEnabled()) {

                log.debug("Skipping property: name=" + col);

            }

            return;

        }

        /*
         * Skip column values having a timestamp strictly greater than the
         * given value.
         */
        final long columnValueTimestamp = keyDecoder.getTimestamp();
        {

            if (columnValueTimestamp > timestamp) {

                if (log.isDebugEnabled()) {

                    log.debug("Ignoring newer revision: col=" + col
                            + ", timestamp=" + columnValueTimestamp);

                }

                return;

            }

        }

        /*
         * Decode the value. A [null] indicates a deleted property
         * value.
         */

        final Object v = ValueType.decode(val);

        /*
         * Add to the representation of the row.
         */

        tps.set(col, columnValueTimestamp, v);

        if (log.isInfoEnabled())
            log.info("Read: name=" + col + ", timestamp="
                    + columnValueTimestamp + ", value=" + v);

    }
    
}
