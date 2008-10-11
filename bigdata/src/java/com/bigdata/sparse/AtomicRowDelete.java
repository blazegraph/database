package com.bigdata.sparse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;

/**
 * Atomic delete of a logical row. All property values written will have the
 * same timestamp. An atomic read is performed as part of the procedure so that
 * the caller may obtain a consistent view of the post-update state of the
 * logical row. The server-assigned timestamp written may be obtained from the
 * returned {@link ITPS} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRowDelete extends AbstractAtomicRowReadOrWrite {

    /**
     * 
     */
    private static final long serialVersionUID = 7481235291210326044L;

    private long writeTime;
    
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization ctor.
     */
    public AtomicRowDelete() {
        
    }
    
    /**
     * Constructor for an atomic read+delete operation.
     * 
     * @param schema
     *            The schema governing the property set.
     * @param primaryKey
     *            The primary key for the logical row.
     * @param writeTime
     *            The timestamp to be assigned to the property values by an
     *            atomic write -or- {@link IRowStoreConstants#AUTO_TIMESTAMP} if a
     *            timestamp will be assigned by the server -or-
     *            {@link IRowStoreConstants#AUTO_TIMESTAMP_UNIQUE} if a unique
     *            timestamp will be assigned by the server.
     * @param filter
     *            An optional filter used to restrict the property values that
     *            will be returned.
     */
    public AtomicRowDelete(final Schema schema, final Object primaryKey,
            final long fromTime, final long toTime, long writeTime,
            final INameFilter filter) {

        super(schema, primaryKey, fromTime, toTime, filter);

        SparseRowStore.assertWriteTime(writeTime);
        
        this.writeTime = writeTime;
        
    }
    
    /**
     * An atomic read of the matching properties is performed and those
     * properties are then deleted atomically.
     * 
     * @return The matching properties as read before they were deleted.
     */
    public TPS apply(IIndex ndx) {

        final long timestamp = TimestampChooser.chooseTimestamp(ndx, this.writeTime);
                
        final byte[] fromKey = schema.getPrefix(ndx.getIndexMetadata()
                .getKeyBuilder(), primaryKey);

        // final byte[] toKey = schema.toKey(keyBuilder,primaryKey).getKey();

        final TPS tps = atomicDelete(ndx, fromKey, schema, timestamp, filter);

        if (tps == null) {

            if (INFO)
                log.info("No data for primaryKey: " + primaryKey);

        }
    
        return tps;
        
    }

    /**
     * Atomic row delete of the matching properties.
     * 
     * @param ndx
     * @param fromKey
     * @param schema
     * @param fromTime
     * @param toTime
     * @param writeTime
     * @param filter
     * 
     * @return {@link TPS}
     */
    private TPS atomicDelete(final IIndex ndx, final byte[] fromKey,
            final Schema schema, final long writeTime, final INameFilter filter) {

        /*
         * Read the matched properties (pre-condition state -- before we delete
         * anything).
         */
        final TPS tps = atomicRead(ndx, fromKey, schema, fromTime, toTime,
                filter, new TPS(schema, writeTime));

        /*
         * Now delete the matched properties.
         */
        if (tps != null) {

            final Map<String, Object> map = tps.asMap();

            if (INFO) {

                log.info("Will delete " + map.size() + " properties: names="
                        + map.keySet());

            }

            final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

            final Iterator<String> itr = map.keySet().iterator();

            while (itr.hasNext()) {

                final String col = itr.next();

                // encode the key.
                final byte[] key = schema.getKey(keyBuilder, primaryKey, col,
                        writeTime);

                /*
                 * Insert into the index.
                 * 
                 * Note: storing a null under the key causes the property value
                 * to be interpreted as "deleted" on a subsequent read.
                 */
                ndx.insert(key, null);

            }

        }
        
        return tps;
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        writeTime = in.readLong();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);

        out.writeLong(writeTime);
    
    }

}
