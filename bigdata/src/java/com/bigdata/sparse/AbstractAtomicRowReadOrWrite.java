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
 * Created on Jul 3, 2008
 */

package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.btree.proc.AbstractIndexProcedure;
import com.bigdata.btree.proc.ISimpleIndexProcedure;

/**
 * Abstract class implements the atomic read operation. However, it does NOT
 * declare itself to be a read-only operation since this class is extended by
 * both {@link AtomicRowRead} and {@link AtomicRowWriteRead}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractAtomicRowReadOrWrite extends
        AbstractIndexProcedure implements ISimpleIndexProcedure,
        IRowStoreConstants, Externalizable {

    protected static final Logger log = Logger.getLogger(AbstractAtomicRowReadOrWrite.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected final static boolean INFO = log.isInfoEnabled();
    
    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected final static boolean DEBUG = log.isDebugEnabled();
    
    protected Schema schema;
    protected Object primaryKey;
    protected long fromTime;
    protected long toTime;
    protected INameFilter filter;
    
    /**
     * De-serialization ctor.
     */
    protected AbstractAtomicRowReadOrWrite() {

        super();
        
    }
    
    /**
     * Constructor for an atomic write/read operation.
     * 
     * @param schema
     *            The schema governing the property set.
     * @param primaryKey
     *            The value of the primary key (identifies the logical row to be
     *            read).
     * @param fromTime
     *            The first timestamp for which timestamped property values will
     *            be accepted.
     * @param toTime
     *            The first timestamp for which timestamped property values will
     *            NOT be accepted -or- {@link IRowStoreConstants#CURRENT_ROW} to
     *            accept only the most current binding whose timestamp is GTE
     *            <i>fromTime</i>.
     * @param filter
     *            An optional filter used to restrict the property values that
     *            will be returned.
     */
    protected AbstractAtomicRowReadOrWrite(final Schema schema,
            final Object primaryKey, final long fromTime, final long toTime,
            final INameFilter filter) {

        SparseRowStore.assertArgs(schema, primaryKey, fromTime, toTime);

        this.schema = schema;
        
        this.primaryKey = primaryKey;
        
        this.fromTime = fromTime;
        
        this.toTime = toTime;

        this.filter = filter;
        
    }
    
    /**
     * Atomic read on the index.
     * 
     * @param ndx
     *            The index on which the data are stored.
     * @param schema
     *            The schema governing the row.
     * @param primaryKey
     *            The primary key identifies the logical row of interest.
     * @param fromTime
     *            The first timestamp for which timestamped property values will
     *            be accepted.
     * @param toTime
     *            The first timestamp for which timestamped property values will
     *            NOT be accepted -or- {@link IRowStoreConstants#CURRENT_ROW} to
     *            accept only the most current binding whose timestamp is GTE
     *            <i>fromTime</i>.
     * @param writeTime
     *            The resolved timestamp for an atomic write operation -or- ZERO
     *            (0L) IFF the operation is NOT a write.
     * @param filter
     *            An optional filter used to select the values for property
     *            names accepted by that filter.
     * 
     * @return The logical row for that primary key -or- <code>null</code> iff
     *         there is no data for the <i>primaryKey</i>.
     */
    protected static TPS atomicRead(final IIndex ndx, final Schema schema,
            final Object primaryKey, final long fromTime, final long toTime,
            final long writeTime, final INameFilter filter) {

        final byte[] fromKey = schema.getPrefix(ndx.getIndexMetadata()
                .getKeyBuilder(), primaryKey);

        final TPS tps = atomicRead(ndx, fromKey, schema, fromTime, toTime,
                filter, new TPS(schema, writeTime));

        if (tps == null) {

            if (INFO)
                log.info("No data for primaryKey: " + primaryKey);

        }
    
        return tps;
    
    }

    /**
     * Alternative form useful when you have the raw key (unsigned byte[])
     * rather than a primary key (application object).
     * 
     * @param tps
     * @param fromKey
     * @param ndx
     * @param schema
     * @param fromTime
     * @param toTime
     * @param filter
     * @param tps
     *            The object into which the timestamped property values will be
     *            read.
     * 
     * @return The {@link TPS} -or- <code>null</code> iff there is no data for
     *         the logical row which satisified the various criteria (the
     *         schema, fromTime, toTime, and filter).
     */
    protected static TPS atomicRead(final IIndex ndx, final byte[] fromKey,
            final Schema schema, final long fromTime, final long toTime,
            final INameFilter filter, final TPS tps) {

        assert ndx != null;

        assert schema != null;
        
        assert fromKey != null;
        
        assert tps != null;

        /*
         * Scan all entries within the fromKey/toKey range populating [tps] as
         * we go.
         */
    
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());
    
        if (INFO) {
            log.info("read: fromKey=" + BytesUtil.toString(fromKey)+"\n"+
                     "read:   toKey=" + BytesUtil.toString(toKey));
        }
    
//        /*
//         * Note: If we are only going to accept the most recent bindings then we
//         * read in reverse order. This allows us to efficiently ignore property
//         * values for which we already have a binding in our TPS.
//         */
//        
//        final boolean reverseScan = toTime == CURRENT_ROW; 
//        
//        if (reverseScan) {
//
//            if(INFO)
//                log.info("reverseScan: fromTime=" + fromTime);
//            
//        }
//        
//        final int flags = IRangeQuery.DEFAULT | IRangeQuery.READONLY
//                | (reverseScan ? IRangeQuery.REVERSE : 0);

        final int flags = IRangeQuery.DEFAULT | IRangeQuery.READONLY;
        
//        /*
//         * Used during reverse scans when the [toTime] is [CURRENT_ROW] to track
//         * which properties have bound values, including when a "deleted" entry
//         * is observed for a property.
//         */
//
//        final Set<String/* name */> bound = (reverseScan ? new HashSet<String>()
//                : null);
        
        // iterator scanning tuples encoding timestamped property values.
        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey,
                0/* capacity */, flags, null/* filter */);
    
        // #of entries scanned for that primary key.
        int nscanned = 0;
        
        while(itr.hasNext()) {
            
            final ITuple tuple = itr.next();
            
            final byte[] key = tuple.getKey();
            
            nscanned++;
            
            // Decode the key so that we can get the column name.
            final KeyDecoder keyDecoder = new KeyDecoder(key);
    
            // The column name.
            final String col = keyDecoder.getColumnName();
    
            if (filter != null && !filter.accept(col)) {
    
                // Skip property names that have been filtered out.
    
                if (DEBUG) {
    
                    log.debug("Skipping property: name=" + col + " (filtered)");
                    
                }
    
                continue;
    
            }
    
//            if (toTime == CURRENT_ROW) {
//
//                /*
//                 * This relies on the fact that we traverse the tuples in reverse
//                 * index order when we only want to collect the current bindings.
//                 * Therefore we simply ignore any property value if we already have
//                 * a binding for that property. [bound] is used to quickly detect
//                 * property values for which we have already collected a binding,
//                 * even if that binding was a "deleted" property marker.
//                 */
//
//                if (bound.contains(col)) {
//
//                    if (DEBUG) {
//
//                        log.debug("Skipping property: name=" + col
//                                + " (already bound)");
//
//                    }
//
//                    continue;
//
//                }
//
//                bound.add(col);
//
//            }

            /*
             * Skip column values whose timestamp lies outside of the specified
             * half-open range.
             */
           
            final long columnValueTimestamp = keyDecoder.getTimestamp();
            
            if (columnValueTimestamp < fromTime) {

                if (DEBUG) {

                    log.debug("Ignoring earlier revision: col=" + col
                            + ", fromTime=" + fromTime + ", timestamp="
                            + columnValueTimestamp);

                }

                continue;

            }

            if (toTime != CURRENT_ROW && columnValueTimestamp >= toTime) {

                if (DEBUG) {

                    log.debug("Ignoring later revision: col=" + col
                            + ", toTime=" + toTime + ", timestamp="
                            + columnValueTimestamp);

                }

                continue;

            }

            /*
             * Decode the value. A [null] indicates a deleted property value.
             */

            final byte[] val = tuple.getValue();

            final Object v = ValueType.decode(val);

            /*
             * Add this timestamped property value to the collection.
             */

            tps.set(col, columnValueTimestamp, v);

            if (INFO)
                log.info("Accept: name=" + col + ", timestamp="
                        + columnValueTimestamp + ", value=" + v);
    
        }
    
        if (nscanned == 0) {
            
            /*
             * Return null iff there are no column values for that primary key.
             * 
             * Note: this is a stronger criteria than none being matched.
             */
            
            return null;
    
        }
    
        if (toTime == CURRENT_ROW) {

            return tps.currentRow();
            
        }
        
        return tps;
        
    }

    /**
     * Return the current binding for the named property.
     * 
     * @param schema
     *            The schema.
     * @param primaryKey
     *            The primary key.
     * @param name
     *            The property name.
     * 
     * @return The current binding -or- <code>null</code> iff there is no
     *         current binding.
     * 
     * @todo this can be optimized by including the encoded column name in the
     *       generated [fromKey] and [toKey] so that we scan less data from the
     *       index and by using a reverse traversal iterator to read the most
     *       recent value in the key range first. This is especially important
     *       if timeseries data are being stored.
     */
    protected static ITPV getCurrentValue(final IIndex ndx,
            final Schema schema, final Object primaryKey, final String name) {

        final TPS tps = atomicRead(ndx, schema, primaryKey, MIN_TIMESTAMP,
                CURRENT_ROW, 0L/* writeTime */, new SingleColumnFilter(name));

        if (tps == null) {

            // never bound.
            return null;
            
        }
        
        final ITPV tpv = tps.get(name);

        if(tpv.getValue() == null) {
            
            // deleted property value.
            return null;
            
        }
        
        return tpv;

    }

    /**
     * The initial version.
     */
    private static final transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private static final transient byte VERSION = VERSION0;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        
        final byte version = in.readByte();

        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

        schema = (Schema) in.readObject();
        
        primaryKey = in.readObject();
        
        fromTime = in.readLong();

        toTime = in.readLong();
        
        filter = (INameFilter) in.readObject();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeByte(VERSION);

        out.writeObject(schema);
        
        out.writeObject(primaryKey);
        
        out.writeLong(fromTime);
        
        out.writeLong(toTime);
        
        out.writeObject(filter);
        
    }

}
