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
 * Created on Jan 7, 2008
 */

package com.bigdata.btree;

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.service.IDataService;
import com.bigdata.service.ResultSet;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * Abstract base class supports compact serialization and compression for remote
 * {@link IIndexProcedure} execution (procedures may be executed on a local
 * index, but they are only (de-)serialized when executed on a remote index).
 * 
 * @todo reconcile with {@link ResultSet} (sends keys and/or values with some
 *       additional metadata) and with rangeCount. Both query and range count
 *       could be procedures, which would drammatically simplify the
 *       {@link IDataService} API.
 * 
 * @todo write prefix scan procedure (aka distinct term scan). possible use for
 *       both the {@link SparseRowStore} and the triple store?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class IndexProcedure implements IIndexProcedure, Externalizable {

    /**
     * #of keys/values.
     */
    private int n;

    /**
     * Offset into {@link #keys} and {@link #vals} (always zero when
     * de-serializing). This makes it possible to reuse the original keys[] and
     * vals[] by specifying the offset identified for a {@link Split}.
     */
    private int offset;

    /**
     * The keys.
     */
    private byte[][] keys;

    /**
     * The values.
     */
    private byte[][] vals;

    /**
     * Offset into {@link #keys} and {@link #vals} (always zero when
     * de-serializing). This makes it possible to reuse the original keys[] and
     * vals[] by specifying the offset identified for a {@link Split}.
     */
    public int offset() {

        return offset;

    }

    /**
     * The #of keys/tuples
     */
    public int getKeyCount() {

        return n;

    }

    /**
     * Return the key at the given index (after adjusting for the
     * {@link #offset}).
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The key at that index.
     */
    protected byte[] getKey(int i) {

        return keys[offset + i];

    }

    /**
     * Return the value at the given index (after adjusting for the
     * {@link #offset}).
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The value at that index.
     */
    protected byte[] getValue(int i) {

        return vals[offset + i];

    }

    // /**
    // * Counters for procedure and node/leaf (de-)serialization costs.
    // *
    // * @todo in order to instrument we need something that corresponds to the
    // * RPC connection. Perhaps make it thread-local or even just static.
    // *
    // * @todo #keys, #vals, #keyBytes, #valBytes?
    // *
    // * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    // Thompson</a>
    // * @version $Id$
    // */
    // public static class Counters {
    //        
    // long nread, nwritten;
    // long nbytesRead, nbytesWritten;
    // long readNanos, writeNanos;
    //        
    // public String toString() {
    //            
    // StringBuilder sb = new StringBuilder();
    //            
    // sb.append("#read="+nread+", #bytesRead="+nbytesRead+",
    // elapsed="+readNanos+"(nanos)\n");
    // sb.append("#written="+nwritten+", #bytesWritten="+nbytesWritten+",
    // elapsed="+writeNanos+"(nanos)\n");
    //
    // return sb.toString();
    //            
    // }
    //        
    // };

    /**
     * De-serialization constructor.
     */
    protected IndexProcedure() {

    }

    /**
     * 
     * @param n
     *            The #of tuples.
     * @param offset
     *            The offset into <i>keys</i> and <i>vals</i> of the 1st
     *            tuple.
     * @param keys
     *            The keys (<em>unsigned</em> variable length byte[]s) MUST
     *            be in sorted order (the logic to split procedures across
     *            partitioned indices depends on this, plus ordered reads and
     *            writes on indices are MUCH more efficient).
     * @param vals
     *            The values (optional, must be co-indexed with <i>keys</i>
     *            when non-<code>null</code>).
     */
    protected IndexProcedure(int n, int offset, byte[][] keys, byte[][] vals) {

        if (n <= 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);

        if (offset < 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);

        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if (keys.length < offset + n)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);
//
//        if (vals == null)
//            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        if (vals != null && vals.length < offset + n)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);

        this.n = n;

        this.offset = offset;

        this.keys = keys;

        this.vals = vals;

    }

    /**
     * Return the object used to (de-)serialize the keys.
     */
    protected IDataSerializer getKeySerializer() {
        
        return new DefaultDataSerializer();
        
    }
    
    /**
     * Return the object used to (de-)serialize the values.
     */
    protected IDataSerializer getValSerializer() {
        
        return new DefaultDataSerializer();
        
    }

    /**
     * Used by some implementations to specify the serializer for the returned
     * data.
     */
    protected IDataSerializer getResultSerializer() {

        return new DefaultDataSerializer();
        
    }
    
    final public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        readMetadata(in);
        
        keys = getKeySerializer().read(in);

        vals = getValSerializer().read(in);
        
    }

    final public void writeExternal(ObjectOutput out) throws IOException {

        writeMetadata(out);
        
        getKeySerializer().write(n,offset,keys,out);
        
        getValSerializer().write(n,offset,vals,out);
        
    }

    /**
     * Reads metadata written by {@link #writeMetadata(ObjectOutput)}.
     * <p>
     * The default implementation reads and sets {@link #n}. The
     * {@link #offset} is always set to zero on de-serialization.
     * 
     * @param in
     * 
     * @throws IOException
     */
    protected void readMetadata(ObjectInput in) throws IOException {
        
        n = (int) LongPacker.unpackLong(in);
       
        offset = 0;
        
    }

    /**
     * Writes metadata (not the keys or values, but just other metadata used by
     * the procedure).
     * <p>
     * The default implementation writes {@link #n}.
     * 
     * @param out
     * 
     * @throws IOException
     */
    protected void writeMetadata(ObjectOutput out) throws IOException {
        
        LongPacker.packLong(out, n);
        
    }

    /**
     * A class useful for sending some kinds of data back from a remote
     * procedure call (those readily expressed as a <code>byte[][]</code>).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ResultBuffer implements Externalizable {
        
        /**
         * 
         */
        private static final long serialVersionUID = -5705501700787163863L;

        private int n;
        private byte[][] a;
        private IDataSerializer resultSerializer;

        /**
         * De-serialization ctor.
         *
         */
        public ResultBuffer() {
            
        }
        
        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         * @param resultSerializer
         *            Used to (de-)serialize the data.
         */
        public ResultBuffer(int n, byte[][] a, IDataSerializer resultSerializer) {

            this.n = n;

            this.a = a;
         
            this.resultSerializer = resultSerializer;
            
        }

        public int getResultCount() {
            
            return n;
            
        }
        
        public byte[][] getResult() {

            return a;

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            resultSerializer = (IDataSerializer)in.readObject();

            a = resultSerializer.read(in);
            
            n = a.length; // always.

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeObject(resultSerializer);

            resultSerializer.write(n, 0/* offset */, a, out);

        }
        
    }

    /**
     * A class useful for sending a logical <code>boolean[]</code> back from a
     * remote procedure call.
     * 
     * @todo provide run-length coding for bits.
     * 
     * @todo is a byte[] a more efficient storage than a boolean[] for java?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ResultBitBuffer implements Externalizable {
        
        /**
         * 
         */
        private static final long serialVersionUID = -5705501700787163863L;

        private int n;
        private boolean[] a;

        /**
         * De-serialization ctor.
         *
         */
        public ResultBitBuffer() {
            
        }
        
        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         */
        public ResultBitBuffer(int n, boolean[] a) {

            this.n = n;

            this.a = a;
            
        }

        public int getResultCount() {
            
            return n;
            
        }
        
        public boolean[] getResult() {

            return a;

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            /*
             * @todo this relies on being able to cast to an input stream. in
             * order to work for the DataInputStream you would obtain the
             * backing byte[] and pass that along (PROBLEM : we would need to
             * specify a limit and InputBitStream does not support that).
             */
            InputBitStream ibs = new InputBitStream((InputStream) in);

            n = ibs.readNibble();
            
            a = new boolean[n];
            
            for(int i=0; i<n; i++) {
                
                a[i] = ibs.readBit() == 1 ? true : false;
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            /*
             * @todo The success of this relies on being able to cast to an
             * OutputStream. That rules out the DataOutputStream since it
             * has a different base class. Modify the OutputBitStream so
             * that it is more flexible for us. Also, may be faster with a
             * backing byte[] but it currently lacks an auto-extend
             * capability.
             */
            final OutputBitStream obs = new OutputBitStream(
                    (OutputStream) out);

            obs.writeNibble(n);
            
            for(int i=0; i<n; i++) {
                
                obs.writeBit(a[i]);
                
            }

            obs.flush();
            
        }
        
    }

}
