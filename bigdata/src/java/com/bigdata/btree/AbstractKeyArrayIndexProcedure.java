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

import it.unimi.dsi.fastutil.io.RepositionableStream;
import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.btree.IIndexProcedure.IKeyArrayIndexProcedure;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * Abstract base class supports compact serialization and compression for remote
 * {@link IKeyArrayIndexProcedure} execution (procedures may be executed on a
 * local index, but they are only (de-)serialized when executed on a remote
 * index).
 * 
 * FIXME use efficient serialization for the rangeIterator for bigdata-rdf and
 * validate when applied to a partitioned index. I think that the right way to
 * do this is to have the key and value serializer specified for the btree used
 * by the {@link ResultSet}. I believe that this is going to require moving the
 * timestamp and delete markers into the node/leaf data structures -- this
 * appears to be necessary in order to be able to apply custom serialization and
 * compression to values stored in nodes and leafs for an index
 * 
 * FIXME custom key compression can already be done - all we have to do is to
 * align the {@link IKeyBuffer}'s serializer with {@link IDataSerializer} and I
 * will be able to write compressed keys for the RDF statement indices. See
 * {@link IKeySerializer} and {@link KeyBufferSerializer}. The
 * {@link ValueSerializer} can be "adapted" to accept a delegate that handles
 * the byte[] values without changing the btree architecture.
 * <p>
 * The main point of misalignment is direct access to the byte[][] vs random
 * access to the individual keys. That could be fixed by allocating a byte[][]
 * and copying over the key references before applying an
 * {@link IDataSerializer}.
 * <P>
 * The other mismatch is that {@link DataOutputBuffer} vs an
 * {@link OutputStream} for serialization. Going to, e.g., a
 * {@link ByteArrayOutputStream} is going to cause pain since the backing buffer
 * will wind up NOT being reused for each node or leaf written.
 * <p>
 * Add {@link RepositionableStream} to the {@link DataOutputBuffer} or the
 * fastbytearrayoutputstream and that will allow buffer rewind and reuse.
 * <p>
 * 
 * FIXME logical row scan for the {@link SparseRowStore} using extension of the
 * {@link RangeIteratorProcedure}. Right now this is not being realized as an
 * index procedure since there is a bunch of custom logic for re-issuing the
 * range query until the iterator is exhausted on each index partition in turn.
 * This is definately limiting the generality of the key range scan since you
 * can not easily customize it, e.g., to handle logical row scans for the sparse
 * row store.
 * 
 * FIXME term prefix scan.
 * 
 * @todo write prefix scan procedure (aka distinct term scan). possible use for
 *       both the {@link SparseRowStore} and the triple store?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
// * <pre>
// * @param R
// *            The data type of the <em>R</em>esult obtained by applying the
// *            procedure to a local index or local index view. Instances of this
// *            interface are logically "mapped" across one or more index
// *            partitions, with one <em>R</em>esult obtained per index
// *            partition.
// * 
// * @param H
// *            The data type of the {@link IResultHandler} operation that is
// *            applied to the <em>R</em>esult obtained for each index
// *            partition.
// * 
// * @param A
// *            The data type of the <em>A</em>ggregated <em>R</em>esults.
// * 
// *    &lt;R, H extends IResultHandler&lt;R, A&gt;, A&gt;
// * </pre>
abstract public class AbstractKeyArrayIndexProcedure implements
        IKeyArrayIndexProcedure, Externalizable {

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

    /**
     * De-serialization constructor.
     */
    protected AbstractKeyArrayIndexProcedure() {

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
    protected AbstractKeyArrayIndexProcedure(int n, int offset, byte[][] keys, byte[][] vals) {

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
     * Return the object that will be the result from
     * {@link IIndexProcedure#apply(IIndex)}.
     */
    abstract protected Object newResult();
    
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

        /**
         * De-serialization ctor.
         *
         */
        public ResultBuffer() {
            
        }

        /**
         * Return the object used to (de-)serialize the data.
         * <p>
         * Note: this uses {@link DefaultDataSerializer} by default. You can
         * override this method and {@link AbstractKeyArrayIndexProcedure} in order to
         * customize the serialization for an index procedure. For example, if
         * you are using custom serialization to send values to a procedure and
         * this object is returning values then you can use the same custom
         * serialization here.
         */
        protected IDataSerializer getDataSerializer() {            
            
            return DefaultDataSerializer.INSTANCE;
            
        }
        
        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         */
        public ResultBuffer(int n, byte[][] a) {

            setResult( n, a );
            
        }

        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         */
        public void setResult(int n, byte[][] a) {

            this.n = n;
            
            this.a = a;
            
        }
        
        public int getResultCount() {
            
            return n;
            
        }
        
        public byte[][] getResult() {

            return a;

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            a = getDataSerializer().read(in);
            
            n = a.length; // always.

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            getDataSerializer().write(n, 0/* offset */, a, out);

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

            setResult( n, a );
            
        }
        
        public void setResult(int n, boolean[] a ) {
            
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

            InputBitStream ibs = new InputBitStream((InputStream) in,0/*unbuffered*/);

            n = ibs.readNibble();
            
            a = new boolean[n];
            
            for(int i=0; i<n; i++) {
                
                a[i] = ibs.readBit() == 1 ? true : false;
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            final OutputBitStream obs = new OutputBitStream((OutputStream) out,0/*unbuffered!*/);

            obs.writeNibble(n);
            
            for(int i=0; i<n; i++) {
                
                obs.writeBit(a[i]);
                
            }

            obs.flush();
            
        }
        
    }
    
}
