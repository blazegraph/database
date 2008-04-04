package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.journal.AbstractJournal;

/**
 * Atomic read of the logical row associated with some {@link Schema} and
 * primary key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRead implements ISimpleIndexProcedure, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 7240920229720302721L;

    protected static final Logger log = Logger.getLogger(SparseRowStore.class);

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

    protected Schema schema;
    protected Object primaryKey;
    protected long timestamp;
    protected INameFilter filter;
    
    /**
     * De-serialization ctor.
     */
    public AtomicRead() {
        
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
    public AtomicRead(Schema schema, Object primaryKey, long timestamp,
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
     * Atomic read.
     * 
     * @return A {@link TPS} instance containing the selected data from the
     *         logical row identified by the {@link #primaryKey} -or-
     *         <code>null</code> iff the primary key was NOT FOUND in the
     *         index. I.e., iff there are NO entries for that primary key
     *         regardless of whether or not they were selected.
     */
    public Object apply(IIndex ndx) {

        return atomicRead(ndx, schema, primaryKey, timestamp, filter);
        
    }

    /**
     * Return the thread-local key builder configured for the data service
     * on which this procedure is being run.
     * 
     * @param ndx The index.
     * 
     * @return The {@link IKeyBuilder}.
     */
    protected IKeyBuilder getKeyBuilder(IIndex ndx) {

        return ((AbstractJournal) ((AbstractBTree) ndx).getStore()).getKeyBuilder();

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
     *            A timestamp to obtain the value for the named property
     *            whose timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most
     *            recent value for the property.
     * @param filter
     *            An optional filter used to select the values for property
     *            names accepted by that filter.
     * 
     * @return The logical row for that primary key.
     */
    protected TPS atomicRead(IIndex ndx, Schema schema, Object primaryKey,
            long timestamp, INameFilter filter) {

        final IKeyBuilder keyBuilder = getKeyBuilder(ndx);
        
        final byte[] fromKey = schema.fromKey(keyBuilder,primaryKey).getKey(); 

        final byte[] toKey = schema.toKey(keyBuilder,primaryKey).getKey();
        
        if (DEBUG) {
            log.info("read: fromKey=" + Arrays.toString(fromKey));
            log.info("read:   toKey=" + Arrays.toString(toKey));
        }

        // Result set object.
        
        final TPS tps = new TPS(schema, timestamp);

        /*
         * Scan all entries within the fromKey/toKey range populating [tps]
         * as we go.
         */

        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey);

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

            final KeyDecoder keyDecoder = new KeyDecoder(schema,key,fromKey.length);

            // The column name.
            final String col = keyDecoder.col;
            
            if (filter != null && !filter.accept(col)) {

                // Skip property names that have been filtered out.
                
                log.debug("Skipping property: name="+col);

                continue;
                
            }
            
            /*
             * Skip column values having a timestamp strictly greater than
             * the given value.
             */
            final long columnValueTimestamp = keyDecoder.getTimestamp();
            {

                if (columnValueTimestamp > timestamp) {

                    if (DEBUG) {

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
            
            log.info("Read: name=" + col + ", timestamp="
                    + columnValueTimestamp + ", value=" + v);

        }

        if (nscanned == 0) {
            
            /*
             * Return null iff there are no column values for that primary
             * key.
             */
            
            log.info("No data for primaryKey: " + primaryKey);
        
            // Note: [null] return since no data for the primary key.
            
            return null;
            
        }
        
        // Note: MAY be empty.
        
        return tps;
        
    }
    
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

    private final static transient short VERSION0 = 0x0;
    
}