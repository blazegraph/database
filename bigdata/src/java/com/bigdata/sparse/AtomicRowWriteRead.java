package com.bigdata.sparse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.sparse.ValueType.AutoIncIntegerCounter;
import com.bigdata.sparse.ValueType.AutoIncLongCounter;

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
     *            value will cause the corresponding binding to be "deleted"
     *            in the index.
     * @param timestamp
     *            The timestamp to be assigned to the property values by an
     *            atomic write -or- either
     *            {@link SparseRowStore#AUTO_TIMESTAMP} or
     *            {@link SparseRowStore#AUTO_TIMESTAMP_UNIQUE} if the
     *            timestamp will be assigned by the server.
     * @param filter
     *            An optional filter used to restrict the property values
     *            that will be returned.
     *            @param precondition
     */
    public AtomicRowWriteRead(Schema schema, Map<String, Object> propertySet,
            long timestamp, INameFilter filter, IPrecondition precondition) {
        
        super(schema, propertySet.get(schema.getPrimaryKeyName()), timestamp,
                filter);

        if (propertySet.get(schema.getPrimaryKeyName()) == null) {

            throw new IllegalArgumentException(
                    "No value for primary key: name="
                            + schema.getPrimaryKeyName());

        }

        /*
         * Validate the column name productions.
         */

        final Iterator<String> itr = propertySet.keySet().iterator();

        while (itr.hasNext()) {

            final String col = itr.next();

            // validate the column name production.
            NameChecker.assertColumnName(col);

        }

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

        final IKeyBuilder keyBuilder = getKeyBuilder(ndx);

        // choose the timestamp for any writes.
        final long timestamp = TimestampChooser.chooseTimestamp(ndx,
                this.timestamp);
        
        /*
         * Precondition test.
         */
        
        if (precondition != null) {

            /*
             * Apply the optional precondition test.
             */
            
            final TPS tps = atomicRead(keyBuilder, ndx, schema, propertySet.get(schema
                    .getPrimaryKeyName()), timestamp, filter);

            if(!precondition.accept(tps)) {

                if(log.isInfoEnabled()) {

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
        
        atomicWrite(ndx, schema, primaryKey, propertySet, timestamp);

        /*
         * Atomic read
         * 
         * Note: the task hold a lock throughout so the pre-condition test, the
         * atomic write, and the atomic read are atomic as a unit.
         * 
         * Note: Read uses whatever timestamp was selected above!
         */

        return atomicRead(getKeyBuilder(ndx), ndx, schema, propertySet
                .get(schema.getPrimaryKeyName()), timestamp, filter);
        
    }

    protected void atomicWrite(IIndex ndx, Schema schema,
            Object primaryKey, Map<String, Object> propertySet,
            long timestamp) {

        if(log.isInfoEnabled())
        log.info("Schema=" + schema + ", primaryKey="
                + schema.getPrimaryKeyName() + ", value=" + primaryKey
                + ", ntuples=" + propertySet.size());
        
        final IKeyBuilder keyBuilder = getKeyBuilder(ndx);

        final Iterator<Map.Entry<String, Object>> itr = propertySet
                .entrySet().iterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry<String, Object> entry = itr.next();
            
            final String col = entry.getKey();

            Object value = entry.getValue();
            
            if (value instanceof AutoIncIntegerCounter) {

                final long counter = inc(ndx, schema, primaryKey, timestamp, col);
                
                if (counter == Integer.MAX_VALUE + 1L) {
                    
                    throw new RuntimeException("Counter would overflow: "+col);
                    
                }
                
                value = Integer.valueOf((int)counter);
                
            } else if (value instanceof AutoIncLongCounter) {

                final long counter = inc(ndx, schema, primaryKey, timestamp, col);
                
                value = Long.valueOf( counter );

            }

            // encode the key.
            final byte[] key = schema.getKey(keyBuilder, primaryKey, col, timestamp);
            
            // encode the value.
            final byte[] val = ValueType.encode( value );

            /*
             * Insert into the index.
             * 
             * Note: storing a null under the key causes the property value to
             * be interpreted as "deleted" on a subsequent read.
             */
            ndx.insert(key, val);

            if(log.isDebugEnabled()) {
                
                log.debug("col=" + col + ", value=" + value);
                
            }
            
        }

    }
    
    /**
     * This is a bit heavy weight, but what it does is read the current
     * state of the logical row so that we can find the previous value(s)
     * for the counter column.
     */
    protected long inc(IIndex ndx, Schema schema, Object primaryKey,
            long timestamp, final String col) {
        
        final TPS tps = atomicRead(getKeyBuilder(ndx), ndx, schema, primaryKey,
                timestamp, new SingleColumnFilter(col));
        
        /*
         * Locate the previous non-null value for the counter column and
         * then add one to that value. If there is no previous non-null
         * value then we start the counter at zero(0).
         */
        
        long counter = 0;
        
        {

            final Iterator<ITPV> vals = tps.iterator();

            while (vals.hasNext()) {
                
                final ITPV val = vals.next();
                
                if (val.getValue() != null) {
                    
                    try {

                        counter = ((Number) val.getValue()).longValue();
                        
                        if(log.isInfoEnabled())
                        log.info("Previous value: name=" + col
                                + ", counter=" + counter + ", timestamp="
                                + val.getTimestamp());
                        
                        counter++;
                        
                    } catch(ClassCastException ex) {
                        
                        log.warn("Non-Integer value: schema="+schema+", name="+col);

                        continue;
                        
                    }
                    
                }
                
            }

        }
    
        // outcome of the auto-inc counter.
        
        log.info("Auto-increment: name="+col+", counter="+counter);
        
        return counter;

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readExternal(in);
        
        final short version = in.readShort();
        
        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        precondition = (IPrecondition)in.readObject();
        
        /*
         * De-serialize into a property set using a tree map so that the
         * index write operations will be fully ordered.
         */
        
        propertySet = new TreeMap<String, Object>();
            
        // #of property values.
        final int n = in.readInt();

        if(log.isInfoEnabled())
        log.info("Reading "+n+" property values");

        for(int i=0; i<n; i++) {

            final String name = in.readUTF();
            
            final Object value = in.readObject();

            propertySet.put(name,value);
            
            if(log.isInfoEnabled())
            log.info("name=" + name + ", value=" + value);
            
        }
        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        // serialization version.
        out.writeShort(VERSION0);
        
        out.writeObject(precondition);
        
        // #of property values
        out.writeInt(propertySet.size());
        
        /*
         * write property values
         */
        
        Iterator<Map.Entry<String,Object>> itr = propertySet.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String,Object> entry = itr.next();
            
            out.writeUTF(entry.getKey());
            
            out.writeObject(entry.getValue());
                            
        }
        
    }

    /**
     * 
     */
    private static final short VERSION0 = 0x0;

}
