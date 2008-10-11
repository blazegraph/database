package com.bigdata.sparse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;

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
public class AtomicRowWriteRead extends AbstractAtomicRowReadOrWrite {

    /**
     * 
     */
    private static final long serialVersionUID = 7481235291210326044L;

    private long writeTime;
    
    private IPrecondition precondition;
    
    private Map<String,Object> propertySet;
    
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization ctor.
     */
    public AtomicRowWriteRead() {
        
    }
    
    /**
     * Constructor for an atomic write/read operation.
     * 
     * @param schema
     *            The schema governing the property set.
     * @param propertySet
     *            The property set. An entry bound to a <code>null</code>
     *            value will cause the corresponding binding to be "deleted" in
     *            the index.
     * @param fromTime
     *            <em>During pre-condition and post-condition reads</em>, the
     *            first timestamp for which timestamped property values will be
     *            accepted.
     * @param toTime
     *            <em>During pre-condition and post-condition reads</em>, the
     *            first timestamp for which timestamped property values will NOT
     *            be accepted -or- {@link IRowStoreConstants#CURRENT_ROW} to
     *            accept only the most current binding whose timestamp is GTE
     *            <i>fromTime</i>.
     * @param writeTime
     *            The timestamp to be assigned to the property values by an
     *            atomic write -or- {@link IRowStoreConstants#AUTO_TIMESTAMP} if the
     *            timestamp will be assigned by the server -or-
     *            {@link IRowStoreConstants#AUTO_TIMESTAMP_UNIQUE} if a unique
     *            timestamp will be assigned by the server.
     * @param filter
     *            An optional filter used to restrict the property values that
     *            will be returned.
     * @param precondition
     */
    public AtomicRowWriteRead(final Schema schema,
            final Map<String, Object> propertySet, final long fromTime,
            final long toTime, final long writeTime, final INameFilter filter,
            final IPrecondition precondition) {
        
        super(schema, propertySet.get(schema.getPrimaryKeyName()), fromTime,
                toTime, filter);

        SparseRowStore.assertWriteTime(writeTime);
        
        SparseRowStore.assertPropertyNames(propertySet);
        
        this.writeTime = writeTime;
        
        this.precondition = precondition;
        
        this.propertySet = propertySet;
        
    }
    
    /**
     * If a property set was specified then do an atomic write of the property
     * set. Regardless, an atomic read of the property set is then performed and
     * the results of that atomic read are returned to the caller.
     * 
     * @return The set of tuples for the primary key as a {@link TPS} instance
     *         -or- <code>null</code> iff there is no data for the
     *         <i>primaryKey</i>.
     */
    public TPS apply(final IIndex ndx) {

        /*
         * Choose the write time.
         */ 
        final long writeTime = TimestampChooser.chooseTimestamp(ndx,
                this.writeTime);
        
        /*
         * Precondition test.
         */
        
        if (precondition != null) {

            /*
             * Apply the optional precondition test.
             */
            
            final TPS tps = atomicRead(ndx, schema, primaryKey, fromTime,
                    toTime, writeTime, filter);

            if(!precondition.accept(tps)) {

                if(INFO) {

                    log.info("precondition failed: "+tps);
                    
                }

                // precondition failed.
                tps.setPreconditionOk(false);
                
                // return the precondition state of the row.
                return tps;
                
            }
            
        }
        
        /*
         * Atomic write.
         */
        
        atomicWrite(ndx, schema, primaryKey, propertySet, writeTime);

        /*
         * Atomic read of the logical row.
         * 
         * Note: the task hold a lock on the unisolated index (partition)
         * throughout so the pre-condition test, the atomic write, and the
         * atomic read are atomic as a unit.
         */

        return atomicRead(ndx, schema, primaryKey, fromTime, toTime, writeTime,
                filter);
        
    }

    protected void atomicWrite(final IIndex ndx, final Schema schema,
            final Object primaryKey, final Map<String, Object> propertySet,
            final long writeTime) {

        if (INFO)
            log.info("Schema=" + schema + ", primaryKey="
                    + schema.getPrimaryKeyName() + ", value=" + primaryKey
                    + ", ntuples=" + propertySet.size());
        
        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

        final Iterator<Map.Entry<String, Object>> itr = propertySet
                .entrySet().iterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry<String, Object> entry = itr.next();
            
            final String col = entry.getKey();

            Object value = entry.getValue();
            
            if (value instanceof AutoIncIntegerCounter) {

                final long counter = inc(ndx, schema, primaryKey, writeTime, col);
                
                if (counter == Integer.MAX_VALUE + 1L) {
                    
                    throw new UnsupportedOperationException("No Successor: "
                            + col);
                    
                }
                
                value = Integer.valueOf((int)counter);
                
            } else if (value instanceof AutoIncLongCounter) {

                final long counter = inc(ndx, schema, primaryKey, writeTime, col);
                
                value = Long.valueOf( counter );

            }

            // encode the key.
            final byte[] key = schema.getKey(keyBuilder, primaryKey, col, writeTime);
            
            // encode the value.
            final byte[] val = ValueType.encode( value );

            /*
             * Insert into the index.
             * 
             * Note: storing a null under the key causes the property value to
             * be interpreted as "deleted" on a subsequent read.
             */
            ndx.insert(key, val);

            if(DEBUG) {
                
                log.debug("col=" + col + ", value=" + value);
                
            }
            
        }

    }
    
    /**
     * Return the increment of the named property value. Note that
     * auto-increment is only defined for {@link ValueType#Integer} and
     * {@link ValueType#Long}.
     * 
     * @throws UnsupportedOperationException
     *             if a property has an auto-increment type and the
     *             {@link ValueType} of the property does not support
     *             auto-increment.
     * @throws UnsupportedOperationException
     *             if there is no successor for the property value.
     */
    protected long inc(final IIndex ndx, final Schema schema,
            final Object primaryKey, final long timestamp, final String col) {

        /*
         * Read the current binding (we don't need historical values) for the
         * named property value of the logical row so that we can find the
         * previous value for the counter column. Note that the caller has a
         * lock on the unisolated index so the entire read-write-inc-read
         * operation is atomic.
         * 
         * Locate the previous non-null value for the counter column and then
         * add one to that value. If there is no previous non-null value then we
         * start the counter at zero(0).
         */
        
        long counter = 0;

        final ITPV tpv = getCurrentValue(ndx, schema, primaryKey, col);

        if (tpv != null) {

            final Object tmp = tpv.getValue();

            if (!(tmp instanceof Integer) && !(tmp instanceof Long)) {

                throw new UnsupportedOperationException(
                        "Unsupported value type: schema=" + schema + ", name="
                                + col + ", class=" + tmp.getClass());

            }

            counter = ((Number) tmp).longValue();

            if (counter == Long.MAX_VALUE) {
             
                throw new UnsupportedOperationException("No successor: " + col);
                
            }
            
            counter++;

        }
        
        // outcome of the auto-inc counter.
        
        if (INFO)
            log.info("Auto-increment: name=" + col + ", counter=" + counter);
        
        return counter;

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        writeTime = in.readLong();
        
        precondition = (IPrecondition) in.readObject();
        
        /*
         * De-serialize into a property set using a tree map so that the
         * index write operations will be fully ordered.
         */
        
        propertySet = new TreeMap<String, Object>();

        // #of property values.
        final int n = in.readInt();

        if (INFO)
            log.info("Reading " + n + " property values");

        for (int i = 0; i < n; i++) {

            final String name = in.readUTF();

            final Object value = in.readObject();

            propertySet.put(name, value);

            if (INFO)
                log.info("name=" + name + ", value=" + value);

        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);

        out.writeLong(writeTime);
        
        out.writeObject(precondition);

        // #of property values
        out.writeInt(propertySet.size());

        /*
         * write property values
         */

        final Iterator<Map.Entry<String, Object>> itr = propertySet.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, Object> entry = itr.next();

            out.writeUTF(entry.getKey());

            out.writeObject(entry.getValue());

        }

    }

}
