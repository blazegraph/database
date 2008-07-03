package com.bigdata.sparse;

import java.util.Iterator;
import java.util.Map;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ILocalTransactionManager;

/**
 * Atomic write on a logical row. All property values written will have the
 * same timestamp. An atomic read is performed as part of the procedure so
 * that the caller may obtain a consistent view of the post-update state of
 * the logical row. The server-assigned timestamp written may be obtained
 * from the returned {@link ITPS} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicRowDelete extends AbstractAtomicRowReadOrWrite {

    /**
     * 
     */
    private static final long serialVersionUID = 7481235291210326044L;
    
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
     * @param timestamp
     *            The timestamp to be assigned to the property values by an
     *            atomic write -or- either {@link SparseRowStore#AUTO_TIMESTAMP}
     *            or {@link SparseRowStore#AUTO_TIMESTAMP_UNIQUE} if the
     *            timestamp will be assigned by the server.
     * @param filter
     *            An optional filter used to restrict the property values that
     *            will be returned.
     */
    public AtomicRowDelete(Schema schema, Object primaryKey, long timestamp, INameFilter filter) {
        
        super(schema, primaryKey, timestamp, filter);

    }
    
    /**
     * An atomic read of the matching properties is performed and those
     * properties are then deleted atomically.
     * 
     * @return The matching properties as read before they were deleted.
     */
    public Object apply(IIndex ndx) {

        /*
         * Choose the timestamp.
         * 
         * When auto-timestamping is used the timestamp is assigned by the
         * data service.
         * 
         * Note: Timestamps can be locally generated on the server since
         * they must be consistent solely within a row, and all revisions of
         * column values for the same row will always be in the same index
         * partition and hence on the same server. The only way in which
         * time could go backward is if there is a failover to another
         * server for the partition and the other server has a different
         * clock time. If the server clocks are kept synchronized then this
         * should not be a problem.
         * 
         * Note: Revisions written with the same timestamp as a pre-existing
         * column value will overwrite the existing column value rather that
         * causing new revisions with their own distinct timestamp to be
         * written. There is therefore a choice for "auto" vs "auto-unique"
         * for timestamps.
         */
        long timestamp = this.timestamp;
        
        if (timestamp == SparseRowStore.AUTO_TIMESTAMP) {

            timestamp = System.currentTimeMillis();
            
        } else if (timestamp == SparseRowStore.AUTO_TIMESTAMP_UNIQUE) {

            final AbstractJournal journal = ((AbstractJournal) ((AbstractBTree) ndx)
                    .getStore());
            
            final ILocalTransactionManager transactionManager = journal.getLocalTransactionManager();

            timestamp = transactionManager.nextTimestampRobust();
            
        }
        
        final byte[] fromKey = schema.getPrefix(getKeyBuilder(ndx), primaryKey);

        // final byte[] toKey = schema.toKey(keyBuilder,primaryKey).getKey();

        final TPS tps = atomicDelete(ndx, fromKey, schema, timestamp, filter);

        if (tps == null) {

            if (log.isInfoEnabled())
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
     * @param timestamp
     * @param filter
     * 
     * @return {@link TPS}
     */
    protected TPS atomicDelete(IIndex ndx, final byte[] fromKey,
            final Schema schema, final long timestamp, final INameFilter filter) {

        /*
         * Read the matched properties (pre-condition state -- before we delete
         * anything).
         */
        final TPS tps = atomicRead(ndx, fromKey, schema, timestamp, filter);
    
        /*
         * Now delete the matched properties.
         */
        if (tps != null) {

            final Map<String,Object> map = tps.asMap();
            
            if(log.isInfoEnabled()) {
                
                log.info("Will delete " + map.size() + " properties: names="
                        + map.keySet());

            }

            final IKeyBuilder keyBuilder = getKeyBuilder(ndx);

            final Iterator<String> itr = map.keySet().iterator();

            while (itr.hasNext()) {

                final String col = itr.next();

                // encode the key.
                final byte[] key = schema.getKey(keyBuilder, primaryKey, col,
                        timestamp);

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

}
