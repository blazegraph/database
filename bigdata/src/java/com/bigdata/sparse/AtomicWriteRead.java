package com.bigdata.sparse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ILocalTransactionManager;
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
public class AtomicWriteRead extends AtomicRead {

    /**
     * 
     */
    private static final long serialVersionUID = 7481235291210326044L;

    private Map<String,Object> propertySet;
    
    /**
     * De-serialization ctor.
     */
    public AtomicWriteRead() {
        
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
     */
    public AtomicWriteRead(Schema schema, Map<String, Object> propertySet,
            long timestamp, INameFilter filter) {
        
        super(schema, propertySet.get(schema.getPrimaryKey()), timestamp,
                filter);

        if (propertySet.get(schema.getPrimaryKey()) == null) {

            throw new IllegalArgumentException(
                    "No value for primary key: name="
                            + schema.getPrimaryKey());

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

        this.propertySet = propertySet;
        
    }
    
    /**
     * If a property set was specified then do an atomic write of the
     * property set. Regardless, an atomic read of the property set is then
     * performed and the results of that atomic read are returned to the
     * caller.
     * 
     * @return The set of tuples for the primary key as a {@link TPS}
     *         instance.
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
        
        atomicWrite(ndx, schema, primaryKey, propertySet, timestamp);

        /*
         * Note: Read uses whatever timestamp was selected above!
         */

        return atomicRead(getKeyBuilder(ndx), ndx, schema, propertySet
                .get(schema.getPrimaryKey()), timestamp, filter);
        
    }

    protected void atomicWrite(IIndex ndx, Schema schema,
            Object primaryKey, Map<String, Object> propertySet,
            long timestamp) {

        if(log.isInfoEnabled())
        log.info("Schema=" + schema + ", primaryKey="
                + schema.getPrimaryKey() + ", value=" + primaryKey
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

            // insert into the index.
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
                timestamp, new INameFilter() {

            public boolean accept(String name) {
                
                return name.equals(col);

            }
            
        });
        
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

        /*
         * De-serialize into a property set using a tree map so that the
         * index write operations will be fully ordered.
         */
        
        propertySet = new TreeMap<String, Object>();
            
        // #of property values.
        final int n = in.readInt();

        log.info("Reading "+n+" property values");

        for(int i=0; i<n; i++) {

            final String name = in.readUTF();
            
            final Object value = in.readObject();

            propertySet.put(name,value);
            
            log.info("name=" + name + ", value=" + value);
            
        }
        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        // serialization version.
        out.writeShort(VERSION0);
                    
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