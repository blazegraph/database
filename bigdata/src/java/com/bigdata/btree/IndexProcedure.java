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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.CognitiveWeb.extser.LongPacker;
import org.apache.log4j.Logger;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.ResultSet;
import com.bigdata.service.Split;

/**
 * Abstract base class supports compact serialization and compression for remote
 * {@link IIndexProcedure} execution (procedures may be executed on a local
 * index, but they are only (de-)serialized when executed on a remote index).
 * 
 * FIXME efficient (de-)serialization. There is a high impedence right now in
 * serialization and de-serialization since the procedure state is in terms of
 * byte[][] keys and byte[][] vals while serialization is in terms if
 * {@link IKeyBuffer} and {@link IValueBuffer} objects. Also see
 * {@link ResultSet} which should use efficient serialization for its keys
 * and/or values (when present).
 * <p>
 * Another starting point is {@link ResultSet}. It uses a trivial data model
 * which means that changes will not impact the {@link BTree} implementation
 * until I get things worked out.
 * <p>
 * Hu-tucker and huffman encodings should be able to interpret keys as long[]s
 * for the RDF statement indices.
 * 
 * @todo The actual use of keys and values by the procedure logic SHOULD be
 *       restricted to materializing a key or value one at a time in a shared
 *       buffer. Right now it is generally necessary to make an exact length
 *       copy of the key or value in order to perform index operations, however
 *       that should be changed as part of a move toward greater efficiency of
 *       the heap.
 * 
 * @todo support compression (hu-tucker, huffman, delta-coding of keys,
 *       application defined compression)
 *       <p>
 *       user-defined tokenization into symbols (hu-tucker, huffman)
 *       <p>
 *       decompression of individual keys or values by copying their
 *       decompressed state onto a shared buffer compressed without requiring
 *       the de-serialization of the whole record.
 *       <p>
 * 
 * @todo reconcile with {@link IKeyBuffer} and {@link IValueBuffer} (the latter
 *       is not used by the {@link BTree} internally and has barely been
 *       developed).
 * 
 * @todo reconcile with {@link ResultSet} (sends keys and/or values with some
 *       additional metadata).
 * 
 * @todo support prefix scan (distinct term scan).
 * 
 * @todo reconcile with the batch operations - {@link BatchInsert},
 *       {@link BatchRemove}, {@link BatchContains}, and {@link BatchLookup}.
 *       Those operations should all accept an [offset] so that they can be used
 *       with auto-split more efficiently on a partitioned index. (Note that
 *       these operations are not being used anywhere - in practice index
 *       procedures are far more flexible, even though a {@link BatchInsert} can
 *       be more optimized.)
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

    /**
     * Return the object that will be used to (de-)serialize this object. This
     * allows extensible serialization approaches while reusing some basic
     * techniques.
     * 
     * @todo this is not a very nice way to handle extensibility - it is here
     *       really as a placeholder while I experiment with
     *       {@link IndexProcedure} specific compression techniques. the choice
     *       of the compression technique is static (per procedure
     *       implementation) but it really needs to be (a) broken down into key
     *       vs value compression methods; and (b) needs to allow extensible
     *       data to be sent along with the procedure.
     */
    protected AbstractCompression getCompression() {

        return NoCompression.INSTANCE;

    }

    // /**
    // * Counters for procedure (de-)serialization costs.
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
     * Type-safe enumeration of known serialization / compression models.
     * 
     * @todo an enumeration will not work in general since it fixes the
     *       compression techniques in advance and does not allow for
     *       application defined compression.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static enum CompressionEnum {

        /**
         * Each key / value is serialized as a full length byte[]s.
         */
        None,
        /**
         * The keys are converted to an {@link ImmutableKeyBuffer} and that is
         * serialized (factors out the common prefix for the set of keys).
         */
        BTree,
        /**
         * Custom serialization for writing on the RDF statement indices.
         */
        FastRDF,
        /**
         * The data are compressed using huffman encoding where each byte is
         * interpreted as a symbol.
         */
        Huffman8,
        /**
         * The data are compressed using huffman encoding where each 64-bit
         * integer is interpreted as a symbol (useful for RDF statement
         * indices). The data must be an even multiple of 64-bit integers.
         */
        Huffman64,
        /**
         * The data are compressed using hu-tucker encoding where each byte is
         * interpreted as a symbol.
         */
        HuTucker8,
        /**
         * The data are compressed using hu-tucker encoding where each 64-bit
         * integer is interpreted as a symbol (useful for RDF statement
         * indices). The data must be an even multiple of 64-bit integers.
         */
        HuTucker64

    };

    /**
     * 
     * @param n
     *            The #of tuples.
     * @param offset
     *            The offset into <i>keys</i> and <i>vals</i> of the 1st
     *            tuple.
     * @param keys
     *            The keys.
     * @param vals
     *            The values (optional, must be co-indexed with {@link #keys}
     *            when non-<code>null</code>).
     */
    protected IndexProcedure(int n, int offset, byte[][] keys, byte[][] vals) {

        assert n > 0;

        assert offset >= 0;

        assert keys != null;

        assert keys.length >= offset + n;

        if (vals != null) {

            assert vals.length >= offset + n;

        }

        this.n = n;

        this.offset = offset;

        this.keys = keys;

        this.vals = vals;

    }

    /**
     * Interface for alternative serialization and compression schemes.
     * 
     * @todo subclasses will need to be able to access
     *       {@link IndexProcedure#keys}. The only way to really do this is to
     *       have the serialization classes be inner classes on the Procedure
     *       implementation classes - just like extSer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public abstract class AbstractCompression {

        protected static final Logger log = Logger
                .getLogger(AbstractCompression.class);

        abstract public void write(DataOutput out, IndexProcedure proc)
                throws IOException;

        abstract public void read(DataInput in, IndexProcedure proc)
                throws IOException;

    }

    // /**
    // * @todo different compression models for the keys and the values (when
    // * present).
    // */
    // // private final CompressionEnum compression = CompressionEnum.None;
    // private final CompressionEnum compression = CompressionEnum.BTree;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        getCompression().read(in, this);

        // switch(compression) {
        //            
        // case None:
        // NoCompression.INSTANCE.read(in, this);
        // break;
        //            
        // case BTree:
        // BTreeCompression.INSTANCE.read(in, this);
        // break;
        //            
        // default:
        //
        // throw new UnsupportedOperationException(compression.toString());
        //        
        // }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        getCompression().write(out, this);

        // /*
        // * Setup a buffer for serialization.
        // *
        // * @todo reuse this buffer for each request? make it a thread-local
        // * variable?
        // */
        // // DataOutputBuffer buf = new DataOutputBuffer();
        //        
        // switch(compression) {
        //        
        // case None:
        // NoCompression.INSTANCE.write(out, this);
        // break;
        //            
        // case BTree:
        // BTreeCompression.INSTANCE.write(out, this);
        // break;
        //            
        // default:
        //
        // throw new UnsupportedOperationException(compression.toString());
        //        
        // }

        // /*
        // * Copy the serialized form onto the caller's output stream.
        // */
        // out.write(buf.array(),0,buf.position());

    }

    /**
     * Implements {@link CompressionEnum#None}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class NoCompression extends AbstractCompression {

        public final static AbstractCompression INSTANCE = new NoCompression();

        public void read(DataInput in, IndexProcedure proc) throws IOException {

            final int n = (int) LongPacker.unpackLong(in);
            proc.n = n;
            final boolean haveKeys = in.readBoolean();
            final boolean haveVals = in.readBoolean();
            if (haveKeys) {
                final byte[][] keys = new byte[n][];
                for (int i = 0; i < n; i++) {
                    int size = (int) LongPacker.unpackLong(in);
                    byte[] tmp = new byte[size];
                    in.readFully(tmp);
                    keys[i] = tmp;
                }
                proc.keys = keys;
            } else {
                proc.keys = null;
            }
            if (haveVals) {
                final byte[][] vals = new byte[n][];
                for (int i = 0; i < n; i++) {
                    // when lenPlus == 0 the value is null (vs byte[0]).
                    final int lenPlus1 = (int) LongPacker.unpackLong(in);
                    if (lenPlus1 > 0) {
                        byte[] tmp = new byte[lenPlus1 - 1];
                        in.readFully(tmp);
                        vals[i] = tmp;
                    } else
                        vals[i] = null;
                }
                proc.vals = vals;
            } else {
                proc.vals = null;
            }

        }

        public void write(DataOutput out, IndexProcedure proc)
                throws IOException {

            final int n = proc.n;
            final int offset = proc.offset;
            final byte[][] keys = proc.keys;
            final byte[][] vals = proc.vals;
            LongPacker.packLong(out, n);
            out.writeBoolean(keys != null);
            out.writeBoolean(vals != null);
            if (keys != null) {
                for (int i = 0; i < n; i++) {
                    // keys are never null.
                    final byte[] key = keys[offset + i];
                    LongPacker.packLong(out, key.length);
                    out.write(key);
                }
            }
            if (vals != null) {
                for (int i = 0; i < n; i++) {
                    final byte[] val = vals[i];
                    // this differentiates a null value from an empty byte[].
                    final int lenPlus1 = val == null ? 0 : val.length + 1;
                    LongPacker.packLong(out, lenPlus1);
                    if (val != null) {
                        out.write(val);
                    }
                }
            }

        }

    }

    /**
     * Reuses the {@link KeyBufferSerializer}, which is based on the
     * {@link ImmutableKeyBuffer}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class BTreeCompression extends AbstractCompression {

        public final static AbstractCompression INSTANCE = new BTreeCompression();

        public void read(DataInput in, IndexProcedure proc) throws IOException {

            /*
             * Read the entire input stream into a buffer.
             */
            DataOutputBuffer buf = new DataOutputBuffer((InputStream) in);

            /*
             * Unpack the buffer.
             */
            DataInputBuffer is = new DataInputBuffer(buf.array(), 0, buf
                    .position());

            /*
             * The offset is always zero when the de-serialized since we only
             * send along the relevant data for the split.
             */

            proc.offset = 0;

            final boolean hasValues = is.readBoolean();

            proc.keys = ((ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                    .getKeys(is)).toKeyArray();

            proc.n = proc.keys.length;

            if (hasValues) {

                proc.vals = ((MutableValueBuffer) ValueBufferSerializer.INSTANCE
                        .deserialize(is)).vals;

            }

            // assert keys.getKeyCount() == vals.getValueCount();

        }

        public void write(DataOutput out, IndexProcedure proc)
                throws IOException {

            DataOutputBuffer buf = new DataOutputBuffer();

            final int n = proc.n;
            final int offset = proc.offset;
            final byte[][] vals = proc.vals;
            final byte[][] keys = proc.keys;

            /*
             * Write boolean flag - true iff values are also serialized.
             */
            out.writeBoolean(vals != null ? true : false);

            /*
             * Serialize the keys onto the buffer.
             */
            {

                IKeyBuffer tmp = new ImmutableKeyBuffer(offset, n,
                        n/* maxKeys */, keys);

                KeyBufferSerializer.INSTANCE.putKeys(buf, tmp);

            }

            /*
             * Serialize the values onto the buffer.
             */
            if (vals != null) {

                IValueBuffer tmp = new MutableValueBuffer(n, offset, vals);

                ValueBufferSerializer.INSTANCE.serialize(buf, tmp);

            }

            out.write(buf.array(), 0, buf.position());

        }

    }

    /**
     * A fast bit-coding of the keys and values for an RDF statement index. The
     * approach uses a fixed length code for the statement keys and a fixed bit
     * length (3 bits) for the statement values.
     * <p>
     * Each key is logically N 64-bit integers, where N is 3 for a triple store
     * or 4 for a quad store. The distinct long values in the keys are
     * identified - these form the symbols of the alphabet. Rather than using a
     * frequency distribution over those symbols (ala hamming or hu-tucker) a
     * fixed code length is choosen based on the #of distinctions that we need
     * to preserve and codes are assigned based an arbitrary sequence. Since the
     * codes are fixed length we do not need the prefix property. The goal of
     * this approach is to compress the keys as quickly as possible. Non-goals
     * include minimum information entropy and order-preserving compression (the
     * keys need to be decompressed before they can be processed by the
     * procedure on in the data service so there is no reason to use an order
     * preserving compression).
     * 
     * @todo try a variant that uses huffman and another that uses hu-tucker in
     *       order to assess the relative cost of those methods.
     * 
     * @todo refactor so that we can expand keys into a buffer that is then used
     *       for B+Tree operations. This requires both an abstraction to obtain
     *       the buffer and its current length and a modification to the B+Tree
     *       API to allow a byte[] with a length (or length and offset) so that
     *       only the identified bytes are interpreted as the key. this will
     *       allow us to directly wrap the compressed record, decompressing keys
     *       as we go into a shared buffer.
     *       <p>
     *       This issue gains importance since RPCs result in far more heap
     *       churn since we need to de-serialize data structures all the time.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class FastRDFCompression extends AbstractCompression {

        // @todo private ctors for the compression impls.
        public static final AbstractCompression INSTANCE = new FastRDFCompression();

        /**
         * Either 3 or 4 depending on whether it is a triple or a quad store
         * index.
         */
        final static private int N = 3;

        /**
         * The natural log of 2.
         */
        static final double LOG2 = Math.log(2);

        static void add(HashMap<Long, Integer> symbols, Long v) {

            if (symbols.containsKey(v))
                return;

            symbols.put(v, symbols.size());

        }

        /**
         * Identifies the distinct symbols (64-bit long integers) in the keys
         * and assigns each symbol a unique integer code.
         * 
         * @param nkeys
         * @param offset
         * @param keys
         * @return A map from the long value to the size of the map at the time
         *         that the value was encountered (a one up integer in [0:n-1]).
         */
        static HashMap<Long, Integer> getSymbols(int nkeys, int offset,
                byte[][] keys) {

            final HashMap<Long, Integer> symbols = new HashMap<Long, Integer>(
                    nkeys * N);

            for (int i = 0; i < nkeys; i++) {

                final byte[] key = keys[offset + i];

                assert key.length == N * Bytes.SIZEOF_LONG : "Expecting key with "
                        + N * Bytes.SIZEOF_LONG + " bytes, not " + key.length;

                for (int j = 0, off = 0; j < N; j++, off += 8) {

                    add(symbols, KeyBuilder.decodeLong(key, off));

                }

            }

            return symbols;

        }

        public void write(DataOutput out, IndexProcedure proc)
                throws IOException {

            final int nkeys = proc.n;
            final int offset = proc.offset;
            final byte[][] keys = proc.keys;
            final byte[][] vals = proc.vals;

            final HashMap<Long, Integer> symbols = getSymbols(nkeys, offset,
                    keys);

            final int nsymbols = symbols.size();

            /*
             * The bit length of the code.
             * 
             * Note: The code for a long value is simply its index in the
             * symbols[].
             */
            final int codeBitLength = (int) Math
                    .ceil(Math.log(nsymbols) / LOG2);

            {

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

                /*
                 * write the header {#keys, nsymbols, codeBitLength}.
                 */
                obs.writeNibble(nkeys);
                obs.writeNibble(nsymbols);
                obs.writeNibble(codeBitLength);

                /*
                 * write the dictionary:
                 * 
                 * {packed(symbol) -> bits(code)}*
                 * 
                 * The entries are written in an arbitrary order.
                 */
                {

                    Iterator<Map.Entry<Long, Integer>> itr = symbols.entrySet()
                            .iterator();

                    while (itr.hasNext()) {

                        Map.Entry<Long, Integer> entry = itr.next();

                        obs.writeLongNibble(entry.getKey());

                        obs.writeInt(entry.getValue(), codeBitLength);

                    }

                }

                /*
                 * write the codes for the keys.
                 */
                {

                    for (int i = 0; i < nkeys; i++) {

                        final byte[] key = keys[offset + i];

                        for (int j = 0, off = 0; j < N; j++, off += 8) {

                            final long v = KeyBuilder.decodeLong(key, off);

                            obs.writeInt(symbols.get(v).intValue(),
                                    codeBitLength);

                        }

                    }

                }

                /*
                 * write the values.
                 * 
                 * We encode the value in 3 bits per statement. The 1st bit is
                 * the override flag. The remaining two bits are the statement
                 * type {inferred, explicit, or axiom}.
                 * 
                 * Note: the 'override' flag is NOT store in the statement
                 * indices, but it is passed by the procedure that writes on the
                 * statement indices so that we can decide whether or not to
                 * override the type when the statement is pre-existing in the
                 * index.
                 */
                {

                    for (int i = 0; i < nkeys; i++) {

                        final byte[] val = vals[offset + i];

                        obs.writeInt((int) val[0], 3);

                    }

                }

                obs.flush();

                /*
                 * @todo counters for time in each phase of this process.
                 * 
                 * @todo since the procedure tends to write large byte[] make
                 * sure that the RPC buffers are good at that, e.g., either they
                 * auto-extend aggressively or they are writing onto a fixed
                 * buffer that writes on a socket.
                 */
                log.warn(nkeys + " statements were serialized in "
                        + obs.writtenBits() + " bytes using " + nsymbols
                        + " symbols with a code length of " + codeBitLength
                        + " bits.");

                // // copy onto the output buffer.
                // out.write(buf);
            }

        }

        public void read(DataInput in, IndexProcedure proc) throws IOException {

            /*
             * @todo this relies on being able to cast to an input stream. in
             * order to work for the DataInputStream you would obtain the
             * backing byte[] and pass that along (PROBLEM : we would need to
             * specify a limit and InputBitStream does not support that).
             */
            InputBitStream ibs = new InputBitStream((InputStream) in);

            /*
             * read the header.
             */
            final int nkeys = ibs.readNibble();
            final int nsymbols = ibs.readNibble();
            final int codeBitLength = ibs.readNibble();

            /*
             * read the dictionary, building a reverse lookup from code to
             * value.
             */
            final HashMap<Integer, Long> symbols = new HashMap<Integer, Long>(
                    nkeys * N);
            {

                for (int i = 0; i < nsymbols; i++) {

                    final long v = ibs.readLongNibble();

                    final int code = ibs.readInt(codeBitLength);

                    symbols.put(code, v);

                }

            }

            /*
             * read the codes, expanding them into keys.
             */
            final byte[][] keys = new byte[nkeys][];
            {

                KeyBuilder keyBuilder = new KeyBuilder(N * Bytes.SIZEOF_LONG);

                for (int i = 0; i < nkeys; i++) {

                    keyBuilder.reset();

                    for (int j = 0; j < N; j++) {

                        final int code = ibs.readInt(codeBitLength);

                        final long v = symbols.get(code).longValue();

                        keyBuilder.append(v);

                    }

                    keys[i] = keyBuilder.getKey();

                }

            }

            /*
             * read the values.
             */
            final byte[][] vals = new byte[nkeys][];
            {

                for (int i = 0; i < nkeys; i++) {

                    vals[i] = new byte[] {

                    (byte) ibs.readInt(3)

                    };

                }

            }

            /*
             * set the data on the proc.
             */
            proc.n = nkeys;
            proc.keys = keys;
            proc.vals = vals;

        }

    }

}
