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
 * Created on Jan 22, 2008
 */

package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TPS implements ITPS, Externalizable, IRowStoreConstants {

    protected static final transient Logger log = Logger.getLogger(TPS.class);
    
    protected static final transient boolean INFO = log.isInfoEnabled();

    protected static final transient boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -2757723215430771209L;
    
    private Schema schema;
    
    private long writeTime;
    
    /**
     * The {schema,property,timestamp,value} tuples.
     * <p>
     * Note: The key is {property,timestamp}. The value is the value. The schema
     * is implicit since this class represents the data for a single schema.
     */
    private TreeMap<TP,ITPV> tuples;

    /**
     * Used to pass back additional metadata for an atomic write with an
     * {@link IPrecondition} test.
     */
    private boolean preconditionOk = true;
    
    /**
     * <code>true</code> unless an atomic write operation specified an
     * {@link IPrecondition} and that {@link IPrecondition} was not satisified.
     */
    public boolean isPreconditionOk() {
        
        return preconditionOk;
        
    }
    
    void setPreconditionOk(boolean flag) {
        
        this.preconditionOk = flag;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public TPS() {
        
    }

    /**
     * 
     * @param schema
     *            The schema.
     * @param timestamp
     *            When the data were read back as part of an atomic write, then
     *            this MUST be the timestamp of the atomic write. That can be
     *            either a caller given timestamp or a server assigned
     *            timestamp. Regardless the value will be returned by
     *            {@link #getWriteTimestamp()}. When the operation was an atomic
     *            read, then the timestamp MUST be ZERO (0L).
     */
    public TPS(final Schema schema, final long timestamp) {

        if (schema == null)
            throw new IllegalArgumentException();

        this.schema = schema;
        
        this.tuples = new TreeMap<TP,ITPV>();
        
        this.writeTime = timestamp;
        
    }
    
    /**
     * The value of the primary key.
     * <p>
     * Note: This looks up and returns the value of the
     * {@link Schema#getPrimaryKeyName()} property
     * 
     * @return The value of the primary key -or- <code>null</code> if there
     *         is no property value bound for the property named by
     *         {@link Schema#getName()}.
     */
    public Object getPrimaryKey() {
        
        return get(schema.getPrimaryKeyName()).getValue();
        
    }

    public long getWriteTimestamp() {

        if (writeTime == 0L)
            throw new IllegalStateException();

        return writeTime;

    }

    public int size() {
        
        return tuples.size();
        
    }
    
//    /**
//     * Remove all existing bindings for that property regardless of the
//     * associated timestamp.
//     * 
//     * @param name
//     *            The property name.
//    * 
//    * @todo This is not wildly efficient since the map is in order by timestamp
//    *       not name. It is currently used by the {@link AtomicRowFilter}, so
//    *       that is not going to be wildly efficient when only the current row
//    *       is desired if the #of tuples in that row is even modestly large. We
//    *       would do better to post-filter.
//     */
//    void removeAll(String name) {
//
//        final Iterator<Map.Entry<TP, ITPV>> itr = tuples.entrySet().iterator();
//
//        while (itr.hasNext()) {
//
//            final Map.Entry<TP, ITPV> entry = itr.next();
//
//            if (name.equals(entry.getKey().name)) {
//
//                itr.remove();
//
//            }
//
//        }
//
//    }

    /**
     * Set the value of the named property as of the specified timestamp.
     * 
     * @param name
     *            The property name.
     * @param timestamp
     *            The timestamp.
     * @param value
     *            The property value -or- <code>null</code> if the property
     *            was deleted as of the given timestamp.
     */
    public void set(String name, long timestamp, Object value) {
        
        tuples.put(new TP(name, timestamp), new TPV(schema, name, timestamp,
                value));
        
    }
    
    public ITPV get(final String name, final long timestamp) {

        /*
         * Lookup a tuple for exactly that timestamp. If it exists then it is
         * the one that we want and we can return it directly.
         */
        {

            final TPV tpv = (TPV) tuples.get(new TP(name, timestamp));

            if (tpv != null) {

                if (INFO)
                    log.info("Exact timestamp match: name="
                            + name
                            + (timestamp == Long.MAX_VALUE ? ", current value"
                                    : ", timestamp=" + timestamp));
                
                return tpv;

            }
            
        }

        /*
         * Scan for the tuple having the largest timestamp not greater than the
         * given timestamp.
         * 
         * @todo this would be more efficient as a reverse scan starting from
         * the successor of the timestamp.
         */
        
        final Iterator<ITPV> itr = tuples.values().iterator();
        
        // last known value for that property.
        TPV last = null;
        
//        // largest timestamp seen so far.
//        long lastTS = -1L;
        
        while(itr.hasNext()) {
            
            final TPV tmp = (TPV)itr.next();
            
            if(tmp.name.equals(name)) {
                
                last = tmp;
                
            }
            
//            lastTS = tmp.timestamp;
            
        }
        
        if (last == null) {

            /*
             * No value was bound for that property.
             * 
             * Note: the timestamp will be 0L if no property values were
             * encountered by the iterator.
             */

            if (INFO)
                log.info("No match: name=" + name);

            return new TPV(schema, name, 0L, null);

        }

        if (INFO)
            log.info("Most recent match: name=" + name + ", value="
                    + last.value + ", timestamp=" + last.timestamp);

        return last;
        
    }

// /**
//     * Return <code>true</code> if there are any bindings for the property.
//     * 
//     * @param name
//     *            The property name.
//     *            
//     * @return <code>true</code> iff there is a binding for that property.
//     */
//    public boolean contains(String name) {
//        
//        return get(name).getTimestamp() != 0L;
//        
//    }
    
    public ITPV get(String name) {

        return get(name, Long.MAX_VALUE);
        
    }

    public Schema getSchema() {

        return schema;
        
    }
    
    public Iterator<ITPV> iterator() {

        return Collections.unmodifiableCollection(tuples.values()).iterator();

    }

    /**
     * Filters for the current row (most recent bindings)
     */
    public TPS currentRow() {

        return currentRow(null/* filter */);
        
    }
    
    /**
     * Filters for the current row (most recent bindings)
     * 
     * @param filter
     *            An optional property name filter.
     */
    public TPS currentRow(final INameFilter filter) {
        
        if(DEBUG) {
            
            log.debug("filter=" + filter + ", preFilter=" + this);
            
        }
        
        // note: maintains the original map order and has fast iterator.
        final LinkedHashMap<String, TPV> m = new LinkedHashMap<String, TPV>();

        final Iterator<ITPV> itr = tuples.values().iterator();

        while (itr.hasNext()) {

            final TPV tpv = (TPV) itr.next();

            if (filter != null && !filter.accept(tpv.name)) {

                if(DEBUG) {
                    
                    log.debug("rejecting on filter: "+tpv);
                    
                }
                
                continue;
                
            }

            if (tpv.value == null) {

                // remove binding.
                final TPV old = m.remove(tpv.name);
                
                if (DEBUG && old != null) {
                    
                    log.debug("removed binding: " + old);
                    
                }
                
            } else {
                
                // (over)write binding.
                final TPV old = m.put(tpv.name, tpv);

                if (DEBUG && old != null) {

                    log.debug("overwrote: \nold=" + old + "\nnew=" + tpv);
                    
                }
                
            }
            
        }
        
        /*
         * At this point we have only the most recent property values of
         * interest in [m].
         * 
         * Now we generate another TPS using the same TPV instances and
         * [writeTime] as this TPS.
         */
        
        final TPS tps = new TPS(this.schema, this.writeTime);
        
        for(Map.Entry<String, TPV> entry : m.entrySet()) {

            final String name = entry.getKey();
            
            final TPV tpv = entry.getValue();
            
            tps.tuples.put(new TP(name, tpv.timestamp), tpv);
            
        }
        
        if(DEBUG) {
            
            log.debug("postFilter: "+tps);
            
        }
        
        return tps;
        
    }

    /**
     * Filters for only those bindings whose timestamp is GTE to the given
     * timestamp.
     */
    public TPS filter(final long fromTime, final long toTime) {

        return filter(fromTime, toTime, null/* filter */);

    }

    /**
     * Filters for only those bindings that satisify the given filter.
     * 
     * @param filter
     *            An optional filter.
     */
    public TPS filter(final INameFilter filter) {

        return filter(MIN_TIMESTAMP, MAX_TIMESTAMP, filter);

    }
    
    /**
     * Filters for only those bindings whose timestamp is GTE to the given
     * timestamp and which satisify the optional property name filter.
     */
    public TPS filter(final long fromTime, final long toTime,
            final INameFilter filter) {

        if (fromTime < MIN_TIMESTAMP)
            throw new IllegalArgumentException();
        
        if (toTime <= fromTime)
            throw new IllegalArgumentException();
        
        if(DEBUG) {
            
            log.debug("fromTime=" + fromTime + ", toTime=" + toTime
                    + ", filter=" + filter + ", preFilter=" + this);
            
        }
        
        /*
         * Generate another TPS using the same TPV instances and [writeTime] as
         * this TPS.
         */
        
        final TPS tps = new TPS(this.schema, this.writeTime);
        
        final Iterator<Map.Entry<TP,ITPV>> itr = tuples.entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<TP,ITPV> entry = itr.next();
            
            final TP tp = entry.getKey();

            if (tp.timestamp < fromTime || tp.timestamp >= toTime) {

                // Outside of the half-open range.

                if (DEBUG) {

                    log.debug("rejecting on timestamp: " + tp);

                }

                continue;

            }

            if (filter != null && !filter.accept(tp.name)) {

                if (DEBUG) {

                    log.debug("rejecting on filter: " + tp);

                }
                
                continue;
                
            }

            // copy the entry.
            tps.tuples.put(tp, entry.getValue());
            
        }
        
        if(DEBUG) {
            
            log.debug("postFilter: "+tps);
            
        }
        
        return tps;
        
    }
    
    public Map<String,Object> asMap() {
        
        return asMap(Long.MAX_VALUE);
        
    }
    
    public Map<String,Object> asMap(long timestamp) {
        
        return asMap(timestamp, null/* filter */);

    }

    /**
     * Note: A {@link LinkedHashMap} is returned to reduce the overhead with
     * iterators while preserving the ordering imposed by {@link #tuples}.
     */
    public LinkedHashMap<String, Object> asMap(final long timestamp,
            final INameFilter filter) {

        final LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();

        final Iterator<ITPV> itr = tuples.values().iterator();
        
        while (itr.hasNext()) {

            final TPV tpv = (TPV)itr.next();

            if (tpv.timestamp > timestamp) {

                // Exceeded timestamp of interest.
                
                continue;
                
            }
            
            if (filter != null && !filter.accept(tpv.name)) {

                continue;
                
            }

            if (tpv.value == null) {
                
                m.remove(tpv.name);
                
            } else {
                
                m.put(tpv.name, tpv.value);
                
            }
            
        }
        
        return m;
        
    }
    
    /*
     * Externalizable support.
     */
    
//    private final transient Integer ONE = Integer.valueOf(1);
//    
//    /**
//     * Return a frequency distribution for the distinct timestamps.
//     */
//    private HashMap<Long,Integer> timestamps() {
//
//        final HashMap<Long,Integer> fd = new HashMap<Long,Integer>();
//        
//        final Iterator<ITPV> itr = tuples.values().iterator();
//
//        while(itr.hasNext()) {
//            
//            final TPV tpv = (TPV)itr.next();
//            
//            final Long ts = tpv.timestamp;
//            
//            final Integer freq = fd.get(ts);
//            
//            if (freq == null) {
//                
//                fd.put(ts, ONE);
//
//            } else {
//
//                fd.put(ts, Integer.valueOf(freq.intValue() + 1));
//
//            }
//            
//        }
//        
//        return fd;
//        
//    }
//    
//    /**
//     * Return a frequency distribution for the distinct property names.
//     */
//    private LinkedHashMap<String,Integer> names() {
//
//        final LinkedHashMap<String,Integer> fd = new LinkedHashMap<String,Integer>();
//        
//        final Iterator<ITPV> itr = tuples.values().iterator();
//
//        while(itr.hasNext()) {
//            
//            final TPV tpv = (TPV)itr.next();
//            
//            final String name = tpv.name;
//            
//            final Integer freq = fd.get(name);
//            
//            if (freq == null) {
//                
//                fd.put(name, ONE);
//                
//            } else {
//                
//                fd.put(name, Integer.valueOf(freq.intValue() + 1));
//                
//            }
//            
//        }
//        
//        return fd;
//        
//    }
    
    /**
     * 
     * FIXME use compression for the names and timestamps, refactoring the logic
     * already in {@link AbstractKeyArrayIndexProcedure}.
     * 
     * @todo use huffman compression for the name and timestamp dictionaries
     *       (fewer bits for the more frequent symbols, but at what added cost)?
     */
    public void writeExternal(ObjectOutput out) throws IOException {

        // serialization version.
        out.writeShort(VERSION0);
        
        // the schema.
        out.writeObject(schema);

        out.writeLong(writeTime);
        
        /*
         * Setup bit stream.
         */
//        final OutputBitStream obs;
//        final ByteArrayOutputStream baos;
//        if(out instanceof OutputStream) {
//            baos = null;
//            obs = new OutputBitStream((OutputStream)out);
//        } else {
//            baos = new ByteArrayOutputStream();
//            obs = new OutputBitStream(baos);
//        }
        
        out.writeBoolean(preconditionOk);
        
        // #of tuples.
        out.writeInt(tuples.size());
        
        // @todo property name codec.
        
        // @todo timestamp codec.
        
        /*
         * write tuples.
         */
        
        final Iterator<ITPV> itr = tuples.values().iterator();
        
        while(itr.hasNext()) {
            
            final TPV tpv = (TPV)itr.next();
            
            out.writeUTF(tpv.name);
            
            out.writeLong(tpv.timestamp);

            final byte[] val = ValueType.encode(tpv.value);

            out.writeInt(val == null ? 0 : val.length + 1); // #of bytes + 1

            if (val != null) {

                out.write(val); // encoded value.

            }
            
        }

// obs.flush();
//        if(baos!=null) {
//            out.write(baos.toByteArray());
//        }
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final short version = in.readShort();
        
        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        this.schema = (Schema)in.readObject();

        this.writeTime = in.readLong();
        
        this.tuples = new TreeMap<TP,ITPV>();

        this.preconditionOk = in.readBoolean();
        
        // #of tuples.
        final int n = in.readInt();

        if (INFO)
            log.info("n=" + n + ", schema=" + schema);

        for (int i = 0; i < n; i++) {

            final String name = in.readUTF();
            
            final long timestamp = in.readLong();
            
            final int nbytes = in.readInt() - 1;
            
            final byte[] val = (nbytes == -1 ? null : new byte[nbytes]);
            
            if (val != null) {

                in.readFully(val);
                
            }
            
            final Object value = ValueType.decode(val);
            
            final TPV tpv = new TPV(schema,name,timestamp,value);
            
            tuples.put(new TP(name, timestamp), tpv);
            
            if (INFO)
                log.info("tuple: name=" + name + ", timestamp=" + timestamp
                        + ", value=" + value);

        }
        
    }

    /**
     * No compression.
     */
    private static final short VERSION0 = 0x0;

    /**
     * A {property, timestamp} tuple.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class TP implements Comparable<TP> {
        
        public final String name;

        public final long timestamp;
        
        public TP(String name, long timestamp) {

            if (name == null)
                throw new IllegalArgumentException();

            this.name = name;
            
            this.timestamp = timestamp;
            
        }

        /**
         * Note: The order is imposed by timestamp (ascending) then property
         * name (ascending). This means that a scan may abort once it reads a
         * timestamp greater than the largest timestamp of interest.
         */
        public int compareTo(TP o) {

            if (this == o)
                return 0;

            int ret = timestamp < o.timestamp ? -1
                    : timestamp > o.timestamp ? 1 : 0;

            if (ret == 0) {

                ret = name.compareTo(o.name);

            }

            return ret;

        }

        public String toString() {
            
            return "TP{name="+name+", timestamp="+timestamp+"}";
            
        }
        
    }
    
    /**
     * Helper class models a single property value as of a given timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TPV implements ITPV {

        private static final long serialVersionUID = -3301002622055086380L;

        private Schema schema;
        private String name;
        private long timestamp;
        private Object value;
        
        /**
         * 
         * @param schema
         *            The schema.
         * @param name
         *            The property name.
         * @param timestamp
         *            The timestamp.
         * @param value
         *            The property value -or- <code>null</code> if the
         *            property was deleted as of the given timestamp.
         */
        public TPV(Schema schema, String name, long timestamp, Object value) {

            if (schema == null)
                throw new IllegalArgumentException();
            
            if (name == null)
                throw new IllegalArgumentException();
            
            this.schema = schema;
            
            this.name = name;
            
            this.timestamp = timestamp;
            
            this.value = value;
            
        }
        
        public Schema getSchema() {
            
            return schema;
            
        }

        public String getName() {
            
            return name;
            
        }

        public long getTimestamp() {

            return timestamp;
            
        }

        public Object getValue() {

            return value;
            
        }
        
        public String toString() {
            
            return "TPS{name="+name+",timestamp="+timestamp+",value="+value+"}";
            
        }

    }

    /**
     * Imposes ordering based on schema, property name, and timestamp. It is an
     * error to have a collection with multiple values for the same schema,
     * property name, and timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class TPVComparator implements Comparator<TPV> {

        public static transient final Comparator<TPV> INSTANCE = new TPVComparator();

        private TPVComparator() {
        
        }
        
        public int compare(TPV o1, TPV o2) {

            if (o1 == o2)
                return 0;

            int ret = o1.getSchema().getName().compareTo(
                    o2.getSchema().getName());

            if (ret == 0) {

                ret = o1.name.compareTo(o2.name);

                if (ret == 0) {

                    ret = o1.timestamp < o2.timestamp ? -1
                            : o1.timestamp > o2.timestamp ? 1 : 0;

                }

            }

            return ret;

        }
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("TPS{schema=" + schema + ", timestamp=" + writeTime
                + ", tuples={");
        
        final int n = 0;
        
        for(ITPV tpv : tuples.values()) {
            
            if(n>0) {

                sb.append(",\n");
                
            } else {
                
                sb.append("\n");
                
            }
            
            sb.append(tpv.toString());
            
        }
        
        sb.append("}}");
        
        return sb.toString();
        
    }
    
}
