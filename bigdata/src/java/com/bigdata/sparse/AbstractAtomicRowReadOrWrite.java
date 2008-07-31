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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractIndexProcedure;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IReadOnlyOperation;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.SuccessorUtil;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;

/**
 * Abstract class implements the atomic read operation. However, it does NOT
 * declare itself to be a read-only operation since this class is extended by
 * both {@link AtomicRowRead} and {@link AtomicRowWriteRead}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractAtomicRowReadOrWrite extends AbstractIndexProcedure implements
        ISimpleIndexProcedure, Externalizable {

    protected static final Logger log = Logger.getLogger(AbstractAtomicRowReadOrWrite.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();
    
    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
                .toInt();
    
    protected Schema schema;
    protected Object primaryKey;
    protected long timestamp;
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
     *            The value of the primary key (identifies the logical row
     *            to be read).
     * @param timestamp
     *            A timestamp to obtain the value for the named property
     *            whose timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most
     *            recent value for the property.
     * @param filter
     *            An optional filter used to restrict the property values
     *            that will be returned.
     */
    protected AbstractAtomicRowReadOrWrite(Schema schema, Object primaryKey, long timestamp,
            INameFilter filter) {
        
        if (schema == null)
            throw new IllegalArgumentException("No schema");

        if (primaryKey == null)
            throw new IllegalArgumentException("No primary key");

        this.schema = schema;
        
        this.primaryKey = primaryKey;
        
        this.timestamp = timestamp;

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
     * @param timestamp
     *            A timestamp to obtain the value for the named property whose
     *            timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most recent
     *            value for the property.
     * @param filter
     *            An optional filter used to select the values for property
     *            names accepted by that filter.
     * 
     * @return The logical row for that primary key -or- <code>null</code> iff
     *         there is no data for the <i>primaryKey</i>.
     */
    protected static TPS atomicRead(IKeyBuilder keyBuilder, IIndex ndx,
            Schema schema, Object primaryKey, long timestamp, INameFilter filter) {

        final byte[] fromKey = schema.getPrefix(keyBuilder, primaryKey);

        // final byte[] toKey = schema.toKey(keyBuilder,primaryKey).getKey();

        final TPS tps = atomicRead(ndx, fromKey, schema, timestamp, filter);

        if (tps == null) {

            if (log.isInfoEnabled())
                log.info("No data for primaryKey: " + primaryKey);

        }
    
        return tps;
    
    }

    /**
     * Atomic read.
     * 
     * @return A {@link TPS} instance containing the selected data from the
     *         logical row identified by the {@link #primaryKey} -or-
     *         <code>null</code> iff the primary key was NOT FOUND in the
     *         index. I.e., iff there are NO entries for that primary key
     *         regardless of whether or not they were selected.
     */
    public TPS apply(final IIndex ndx) {
    
        return atomicRead(getKeyBuilder(ndx), ndx, schema, primaryKey, timestamp, filter);
        
    }

    /**
     * Alternative form useful when you have the raw key (unsigned byte[])
     * rather than a primary key (application object).
     * 
     * @param tps
     * @param fromKey
     * @param ndx
     * @param schema
     * @param timestamp
     * @param filter
     * 
     * @return {@link TPS} -or- <code>null</code> iff there is no data for the
     *         logical row.
     */
    protected static TPS atomicRead(final IIndex ndx, final byte[] fromKey,
            final Schema schema, final long timestamp, final INameFilter filter) {
    
        /*
         * Scan all entries within the fromKey/toKey range populating [tps] as
         * we go.
         */
    
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());
    
        if (log.isInfoEnabled()) {
            log.info("read: fromKey=" + BytesUtil.toString(fromKey));
            log.info("read:   toKey=" + BytesUtil.toString(toKey));
        }
    
        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey);
    
        // Result set object.
        final TPS tps = new TPS(schema, timestamp);
    
        // #of entries scanned for that primary key.
        int nscanned = 0;
        
        while(itr.hasNext()) {
            
            final ITuple tuple = itr.next();
            
            final byte[] key = tuple.getKey();
    
            final byte[] val = tuple.getValue();
            
            nscanned++;
            
            /*
             * Decode the key so that we can get the column name. We have the
             * advantage of knowing the last byte in the primary key. Since the
             * fromKey was formed as [schema][primaryKey], the length of the
             * fromKey is the index of the 1st byte in the column name.
             */
    
            final KeyDecoder keyDecoder = new KeyDecoder(key);
    
            // The column name.
            final String col = keyDecoder.getColumnName();
    
            if (filter != null && !filter.accept(col)) {
    
                // Skip property names that have been filtered out.
    
                if (log.isDebugEnabled()) {
    
                    log.debug("Skipping property: name=" + col);
                    
                }
    
                continue;
    
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
                    
                    continue;
    
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
    
        if (nscanned == 0) {
            
            /*
             * Return null iff there are no column values for that primary key.
             * 
             * Note: this is a stronger criteria than none being matched.
             */
            
            return null;
    
        }
    
        return tps;
        
    }

    private static final transient short VERSION0 = 0x0;

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = in.readShort();
        
        if(version!=VERSION0) {
            
            throw new IOException("Unknown version="+version);
            
        }
    
        schema = (Schema) in.readObject();
        
        primaryKey = in.readObject();
        
        timestamp = in.readLong();
        
        filter = (INameFilter) in.readObject();
        
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {
    
        out.writeShort(VERSION0);
        
        out.writeObject(schema);
        
        out.writeObject(primaryKey);
        
        out.writeLong(timestamp);
        
        out.writeObject(filter);
        
    }

}
