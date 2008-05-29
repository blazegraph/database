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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IIndexProcedure.IKeyArrayIndexProcedure;
import com.bigdata.service.Split;

/**
 * Abstract base class supports compact serialization and compression for remote
 * {@link IKeyArrayIndexProcedure} execution (procedures may be executed on a
 * local index, but they are only (de-)serialized when executed on a remote
 * index).
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
abstract public class AbstractKeyArrayIndexProcedure extends
        AbstractIndexProcedure implements IKeyArrayIndexProcedure,
        Externalizable {

    protected static final Logger log = Logger.getLogger(AbstractKeyArrayIndexProcedure.class);
    
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

    /**
     * The object used to (de-)serialize the keys when they are sent to the
     * remote service.
     */
    private IDataSerializer keySer;

    /**
     * The object used to (de-)serialize the values when they are sent to the
     * remote service.
     */
    private IDataSerializer valSer;
    
    /**
     * Index of the first element to be used in {@link #keys} and {@link #vals}
     * and serialized as <code>0</code>. This makes it possible to reuse the
     * original keys[] and vals[] when the procedure is mapped across a
     * key-range partitioned index while only sending the minimum amount of data
     * when the procedure is serialized.
     */
    private int fromIndex;
    
    /**
     * Index of the first element to NOT be used in {@link #keys} and
     * {@link #vals} and serialized as <code>(toIndex - fromIndex)</code>.
     * This makes it possible to reuse the original keys[] and vals[] when the
     * procedure is mapped across a key-range partitioned index while only
     * sending the minimum amount of data when the procedure is serialized.
     */
    private int toIndex;

    /**
     * The keys.
     * 
     * @todo make private again.
     */
    protected IRandomAccessByteArray keys;

    /**
     * The values.
     */
    private IRandomAccessByteArray vals;

    /**
     * Index of the first element to be used in {@link #keys} and {@link #vals}
     * and serialized as <code>0</code>. This makes it possible to reuse the
     * original keys[] and vals[] when the procedure is mapped across a
     * key-range partitioned index while only sending the minimum amount of data
     * when the procedure is serialized.
     */
    final public int getFromIndex() {

        return fromIndex;

    }

    /**
     * Index of the first element to NOT be used in {@link #keys} and
     * {@link #vals} and serialized as <code>(toIndex - fromIndex)</code>.
     * This makes it possible to reuse the original keys[] and vals[] when the
     * procedure is mapped across a key-range partitioned index while only
     * sending the minimum amount of data when the procedure is serialized.
     */
    final public int getToIndex() {

        return toIndex;
        
    }
    
    /**
     * The #of keys/tuples
     */
    final public int getKeyCount() {

        return toIndex - fromIndex;

    }

    /**
     * Return the key at the given index (after adjusting for the
     * {@link #fromIndex}).
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The key at that index.
     */
    public byte[] getKey(int i) {

        return keys.getKey(i);

    }

    /**
     * Return the value at the given index (after adjusting for the
     * {@link #fromIndex}).
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The value at that index.
     */
    public byte[] getValue(int i) {

        return vals.getKey( i );

    }

    /**
     * De-serialization constructor.
     */
    protected AbstractKeyArrayIndexProcedure() {

    }

    /**
     * @param keySer
     *            The object used to serialize the <i>keys</i>.
     * @param valSer
     *            The object used to serialize the <i>vals</i> (optional IFF
     *            <i>vals</i> is <code>null</code>).
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            The keys (<em>unsigned</em> variable length byte[]s) MUST
     *            be in sorted order (the logic to split procedures across
     *            partitioned indices depends on this, plus ordered reads and
     *            writes on indices are MUCH more efficient).
     * @param vals
     *            The values (optional, must be co-indexed with <i>keys</i>
     *            when non-<code>null</code>).
     */
    protected AbstractKeyArrayIndexProcedure(IDataSerializer keySer,
            IDataSerializer valSer, int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals) {

        if (keySer == null)
            throw new IllegalArgumentException();
        
        if (valSer == null && vals != null)
            throw new IllegalArgumentException();
        
        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if (fromIndex < 0)
            throw new IllegalArgumentException(Errors.ERR_FROM_INDEX);

        if (fromIndex >= toIndex )
            throw new IllegalArgumentException(Errors.ERR_FROM_INDEX);

        if (toIndex > keys.length )
            throw new IllegalArgumentException(Errors.ERR_TO_INDEX);

        if (vals != null && toIndex > vals.length)
            throw new IllegalArgumentException(Errors.ERR_TO_INDEX);

        this.keySer = keySer;
        
        this.valSer = valSer;
        
        this.fromIndex = fromIndex;
        
        this.toIndex = toIndex;

        this.keys = new RandomAccessByteArray(fromIndex,toIndex,keys);

        this.vals = (vals == null ? null : new RandomAccessByteArray(fromIndex,
                toIndex, vals));

    }

    /**
     * Return the object used to (de-)serialize the keys when they are sent to
     * the remote service.
     */
    protected IDataSerializer getKeySerializer() {
        
        return keySer;
        
    }
    
    /**
     * Return the object used to (de-)serialize the values when they are sent to
     * the remote service.
     */
    protected IDataSerializer getValSerializer() {
        
        return valSer;
        
    }

    /**
     * Formats the data into a {@link String}.
     * 
     * @param data An array of unsigned byte arrays.
     * 
     * @return
     */
    protected String toString(IRandomAccessByteArray keys) {
       
        StringBuilder sb = new StringBuilder();
        
        final int n = keys.getKeyCount();
        
        sb.append("data(n=" + n + ")={");

        for (int i = 0; i < n; i++) {

            final byte[] a = keys.getKey(i);
            
            sb.append("\n");

            sb.append("data[" + i + "]=");

            sb.append(BytesUtil.toString(a));

            if (i + 1 < n)
                sb.append(",");
            
        }
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    final public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        readMetadata(in);

        final boolean haveVals = in.readBoolean();
        
        final int n = toIndex - fromIndex;
        
        keys = new RandomAccessByteArray( 0, 0, new byte[n][] );
        
        getKeySerializer().read(in, keys );

        if(haveVals) {
        
            vals = new RandomAccessByteArray( 0, 0, new byte[n][] );
        
            getValSerializer().read(in, vals);
            
        } else {
            
            vals = null;
            
        }
        
    }

    final public void writeExternal(ObjectOutput out) throws IOException {

        writeMetadata(out);

        out.writeBoolean(vals != null); // haveVals

        getKeySerializer().write(out, keys);

        if (vals != null) {

            getValSerializer().write(out, vals);

        }
        
    }

    /**
     * Reads metadata written by {@link #writeMetadata(ObjectOutput)}.
     * <p>
     * The default implementation reads and sets {@link #toIndex}. The
     * {@link #fromIndex} is always set to zero on de-serialization.
     * 
     * @param in
     * 
     * @throws IOException
     * @throws ClassNotFoundException 
     */
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {
        
        fromIndex = 0;
        
        toIndex = (int) LongPacker.unpackLong(in);
        
        keySer = (IDataSerializer)in.readObject();

        valSer = (IDataSerializer)in.readObject();
        
    }

    /**
     * Writes metadata (not the keys or values, but just other metadata used by
     * the procedure).
     * <p>
     * The default implementation writes <code>toIndex - fromIndex</code>, which
     * is the #of keys.
     * 
     * @param out
     * 
     * @throws IOException
     */
    protected void writeMetadata(ObjectOutput out) throws IOException {
        
        final int n = toIndex - fromIndex;
        
        LongPacker.packLong(out, n);
        
        out.writeObject(keySer);

        out.writeObject(valSer);
        
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

        private RandomAccessByteArray a;

        private IDataSerializer valSer;
        
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
         * @param valSer
         *            The data are serialized using using this object. Typically
         *            this is the value returned by
         *            {@link IndexMetadata#getLeafValueSerializer()}.
         */
        public ResultBuffer(int n, byte[][] a, IDataSerializer valSer) {

            assert n >= 0;
            assert a != null;
            assert valSer != null;
                        
            this.a = new RandomAccessByteArray(0/*fromIndex*/,n/*toIndex*/,a);
            
            this.valSer = valSer;
            
        }
        
        public int getResultCount() {
            
            return a.getKeyCount();
            
        }
        
        public byte[] getResult(int index) {

            return a.getKey(index);

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            final int n = in.readInt();

            valSer = (IDataSerializer) in.readObject();
            
            a = new RandomAccessByteArray(0, 0, new byte[n][]);

            valSer.read(in, a);
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeInt(a.getKeyCount());
            
            out.writeObject(valSer);
            
            valSer.write(out, a);

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

    /**
     * Knows how to aggregate {@link ResultBuffer} objects.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ResultBufferHandler implements
            IResultHandler<ResultBuffer, ResultBuffer> {

        private final byte[][] results;
        private final IDataSerializer valueSerializer;

        public ResultBufferHandler(int nkeys, IDataSerializer valueSerializer) {

            this.results = new byte[nkeys][];

            this.valueSerializer = valueSerializer;
            
        }

        public void aggregate(ResultBuffer result, Split split) {

            for (int i = 0, j = split.fromIndex; i < split.ntuples; i++, j++) {

                results[j] = result.getResult(i);
                
            }
            
//            System.arraycopy(result.getResult(), 0, results, split.fromIndex,
//                    split.ntuples);

        }

        /**
         * The aggregated results.
         */
        public ResultBuffer getResult() {

            return new ResultBuffer(results.length, results, valueSerializer);

        }

    }

    /**
     * Knows how to aggregate {@link ResultBitBuffer} objects.
     */
    public static class ResultBitBufferHandler implements
            IResultHandler<ResultBitBuffer, ResultBitBuffer> {

        private final boolean[] results;

        public ResultBitBufferHandler(int nkeys) {

            results = new boolean[nkeys];

        }

        public void aggregate(ResultBitBuffer result, Split split) {

            System.arraycopy(result.getResult(), 0, results, split.fromIndex,
                    split.ntuples);

        }

        /**
         * The aggregated results.
         */
        public ResultBitBuffer getResult() {

            return new ResultBitBuffer(results.length, results);

        }

    }

}
