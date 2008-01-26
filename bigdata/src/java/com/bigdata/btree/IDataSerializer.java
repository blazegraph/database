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
 * Created on Jan 25, 2008
 */

package com.bigdata.btree;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.Value;

/**
 * Interface for custom serialization of <code>byte[][]</code> data such are
 * used to represent keys or values. Some implementations work equally well for
 * both (e.g., no compression, huffman compression, dictionary compression)
 * while others can require that the data are fully ordered (e.g., hu-tucker
 * compression which only works for keys).
 * 
 * FIXME Support dictionary compression.
 * 
 * FIXME reconcile with btree node and leaf serialization.
 * 
 * @todo test suites for each of the {@link IDataSerializer}s, including when
 *       the reference is a null byte[][], when it has zero length, and when
 *       there are null elements in the byte[][].
 * 
 * @todo The actual use of keys and values by the procedure logic SHOULD be
 *       restricted to materializing a key or value one at a time in a shared
 *       buffer. Right now it is generally necessary to make an <b>exact length
 *       copy</b> of the key or value in order to perform index operations.
 *       That should be changed as part of a move toward greater efficiency of
 *       the heap such that we can do, e.g., insert(key,off,len,val,off,len) and
 *       thereby reuse buffers consistently throughout.
 * 
 * @todo support compression (hu-tucker, huffman, delta-coding of keys,
 *       application defined compression, etc.)
 *       <p>
 *       user-defined tokenization into symbols (hu-tucker, huffman)
 * 
 * @todo reconcile with {@link IKeyBuffer} and {@link IValueBuffer} (the latter
 *       is not used by the {@link BTree} internally and has barely been
 *       developed).
 *       <p>
 *       In order to use the {@link IDataSerializer}s for the {@link BTree} we
 *       have to do two things: <br>
 *       (a) NOT convert to the immutable key buffer representation when it a
 *       node becomes immutable or gets serialized (it makes it more expensive
 *       to serialize the KEYS for the node since we need to convert them back
 *       to a byte[][]); and <br>
 *       (b) the VALUES are objects in the general case rather than just byte[]s -
 *       in particular, handling of isolation {@link Value}s is going to make
 *       dictionary based encoding of the values difficult or impossible.
 *       <p>
 *       Perhaps the {@link BTree} should be forced to (byte[],byte[]) for both
 *       keys and values and the version counters or deletion markers should be
 *       handled directly by the {@link Leaf}?
 *       <p>
 *       Another twist is that it is more efficient from the perspective of the
 *       garbage collector to use a single byte[] to represent all of the keys
 *       or values (or both) for a node or leaf. E.g., using an extensible
 *       byte[] buffer with a maximum capacity and an append only strategy with
 *       compaction when the buffer would overflow.
 *       <p>
 *       So, yet another reconcilation plan is to use an iterator strategy in
 *       which the caller gets an updated {byte[],off,len} tuple each time they
 *       call next(). This would allow us to reconcile the various ways in which
 *       the data might be stored - except for the one where the prefix is
 *       factored out, which might be treated purely as a serialization form.
 *       So, there should also be a method that reports whether the data are
 *       sorted, and another to either access the data randomly or to compute
 *       the prefix for any two indices.
 *       <p>
 *       One last refinement is to make the interpretation of {byte[],off,len}
 *       in terms of <i>bits</i> so that we can store non-aligned keys if
 *       necessary - of course that only works for hu-tucker encoding and that
 *       again seems more a serialization issue that anything else.
 */
public interface IDataSerializer extends Serializable {

    /**
     * Read a byte[][], possibly empty and possibly <code>null</code>.
     * 
     * @param in
     * @return
     * @throws IOException
     */
    byte[][] read(ObjectInput in) throws IOException;

    /**
     * Write a byte[][].
     * 
     * @param n
     *            The #of items in the array that contain data.
     * @param offset
     *            The offset of the first item in the array to be
     *            serialized.
     * @param keys
     *            The data (may be a <code>null</code> reference, may be
     *            an empty array, and may contain <code>null</code>
     *            elements in the array).
     * @param out
     * 
     * @throws IOException
     */
    void write(int n, int offset, byte[][] keys, ObjectOutput out)
            throws IOException;

    /**
     * No compression.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class DefaultDataSerializer implements IDataSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -7960255442029142379L;

        public byte[][] read(ObjectInput in) throws IOException {
            final boolean haveVals = in.readBoolean();
            if (!haveVals) {
                return null;
            }
            final int n = (int) LongPacker.unpackLong(in);
            final byte[][] a = new byte[n][];
            for (int i = 0; i < n; i++) {
                // when lenPlus == 0 the value is null (vs byte[0]).
                final int lenPlus1 = (int) LongPacker.unpackLong(in);
                if (lenPlus1 > 0) {
                    byte[] tmp = new byte[lenPlus1 - 1];
                    in.readFully(tmp);
                    a[i] = tmp;
                } else
                    a[i] = null;
            }
            return a;
        }

        public void write(int n, int offset, byte[][] a, ObjectOutput out)
                throws IOException {
            out.writeBoolean(a != null);
            if (a != null) {
                LongPacker.packLong(out, n);
                for (int i = 0; i < n; i++) {
                    final byte[] e = a[offset + i];
                    // this differentiates a null value from an empty byte[].
                    final int lenPlus1 = e == null ? 0 : e.length + 1;
                    LongPacker.packLong(out, lenPlus1);
                    if (e != null) {
                        out.write(e);
                    }
                }
            }
        }

    }

    /**
     * Builds an {@link ImmutableKeyBuffer} from the keys and then uses the
     * {@link KeyBufferSerializer} to write them onto the output stream.
     * <p>
     * Note: This is not very efficient since we have to create the immutable
     * node from the keys[] first, write it onto a temporary buffer, and then
     * write the buffer onto the output stream.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BTreeKeySerializer implements IDataSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -2142221841898109636L;

        public byte[][] read(ObjectInput in) throws IOException {

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

            return ((ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                    .getKeys(is)).toKeyArray();

        }

        public void write(int n, int offset, byte[][] a, ObjectOutput out)
                throws IOException {

            DataOutputBuffer buf = new DataOutputBuffer();

            IKeyBuffer tmp = new ImmutableKeyBuffer(offset, n, n/* maxKeys */,
                    a);

            KeyBufferSerializer.INSTANCE.putKeys(buf, tmp);

            out.write(buf.array(), 0, buf.position());

        }

    }

///**
// * Dictionary based compression for the values. Each byte[] value is treated
// * as a single term for the purposes of the dictionary.
// * 
// * @todo this is not finished yet. it needs a test suite, it needs to handle
// *       null elements, and it needs to write out byte[] elements
// *       efficiently before creating the {@link OutputBitStream} which means
// *       writing those data first (or last) and the codes and other bit
// *       packed stuff when we have the {@link OutputBitStream} open.
// * 
// * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
// * @version $Id$
// */
//public static class DictionaryValSerializer implements IDataSerializer {
//
//    protected Logger log = Logger.getLogger(DictionaryValSerializer.class);
//    
//    /**
//     * The natural log of 2.
//     */
//    final private double LOG2 = Math.log(2);
//
//    protected void add(HashMap<byte[], Integer> symbols, byte[] v) {
//
//        if (symbols.containsKey(v))
//            return;
//
//        symbols.put(v, symbols.size());
//
//    }
//
//    /**
//     * Identifies the distinct symbols (byte[] values) and assigns each
//     * symbol a unique integer code.
//     * 
//     * @param nkeys
//     * @param offset
//     * @param vals
//     * @return A map from the distinct symbols to the size of the map at the
//     *         time that the value was encountered (a one up integer in
//     *         [0:n-1]).
//     */
//    protected HashMap<byte[], Integer> getSymbols(int nkeys, int offset,
//            byte[][] vals) {
//
//        final HashMap<byte[], Integer> symbols = new HashMap<byte[], Integer>(
//                nkeys);
//
//        for (int i = 0; i < nkeys; i++) {
//
//            final byte[] key = vals[offset + i];
//
//            add(symbols, key);
//
//        }
//
//        return symbols;
//
//    }
//
//    public byte[][] read(ObjectInput in) throws IOException {
//
//        /*
//         * @todo this relies on being able to cast to an input stream. in
//         * order to work for the DataInputStream you would obtain the
//         * backing byte[] and pass that along (PROBLEM : we would need to
//         * specify a limit and InputBitStream does not support that).
//         */
//        InputBitStream ibs = new InputBitStream((InputStream) in);
//
//        /*
//         * read the header.
//         */
//        final boolean isNull = ibs.readBit() == 1 ? true : false;
//        if (isNull)
//            return null;
//        final int n = ibs.readNibble();
//        if (n == 0)
//            return new byte[0][];
//        final int nsymbols = ibs.readNibble();
//        final int codeBitLength = ibs.readNibble();
//
//        /*
//         * read the dictionary, building a reverse lookup from code to
//         * value.
//         */
//        final HashMap<Integer, byte[]> symbols = new HashMap<Integer, byte[]>(
//                n);
//        {
//
//            for (int i = 0; i < nsymbols; i++) {
//
//                // @todo handle null elements.
//                final int len = ibs.readNibble();
//
//                // @todo efficient read of the byte[] symbol (this method reads _bits_).
//                final byte[] v = ibs.read(arg0, len);
//                
//                final int code = ibs.readInt(codeBitLength);
//
//                symbols.put(code, v);
//
//            }
//
//        }
//
//        /*
//         * read the codes, expanding them into values.
//         * 
//         * @todo iff values are defined.
//         */
//        final byte[][] a = new byte[n][];
//        {
//
//            for (int i = 0; i < n; i++) {
//
//                final int code = ibs.readInt(codeBitLength);
//
//                final byte[] v = symbols.get(code);
//
//                a[i] = v;
//
//            }
//
//        }
//        
//        return a;
//        
//    }
//
//    public void write(int n, int offset, byte[][] a, ObjectOutput out) throws IOException {
//
//        final HashMap<byte[], Integer> symbols = getSymbols(n, offset, a);
//
//        final int nsymbols = symbols.size();
//
//        /*
//         * The bit length of the code.
//         * 
//         * Note: The code for a long value is simply its index in the
//         * symbols[].
//         */
//        final int codeBitLength = (int) Math
//                .ceil(Math.log(nsymbols) / LOG2);
//
//        {
//
//            /*
//             * @todo The success of this relies on being able to cast to an
//             * OutputStream. That rules out the DataOutputStream since it
//             * has a different base class. Modify the OutputBitStream so
//             * that it is more flexible for us. Also, may be faster with a
//             * backing byte[] but it currently lacks an auto-extend
//             * capability.
//             */
//            final OutputBitStream obs = new OutputBitStream(
//                    (OutputStream) out);
//
//            /*
//             * write the header {nsymbols, codeBitLength}.
//             */
//            obs.writeBit(a == null); // isNull
//            if (a == null)
//                return; // done.
//            obs.writeNibble(n);
//            if (n == 0)
//                return; // done.
//            obs.writeNibble(nsymbols);
//            obs.writeNibble(codeBitLength);
//
//            /*
//             * write the dictionary:
//             * 
//             * {packed(symbol) -> bits(code)}*
//             * 
//             * The entries are written in an arbitrary order.
//             */
//            {
//
//                Iterator<Map.Entry<byte[], Integer>> itr = symbols.entrySet()
//                        .iterator();
//
//                while (itr.hasNext()) {
//
//                    final Map.Entry<byte[], Integer> entry = itr.next();
//
//                    final byte[] v = entry.getKey();
//                    
//                    // @todo efficiently write bytes (not bits); handle null values.
////                    obs.writeLongNibble(entry.getKey());
//                    obs.write(v_bytes,v.length);
//
//                    obs.writeInt(entry.getValue(), codeBitLength);
//
//                }
//
//            }
//
//            /*
//             * write the codes for the keys.
//             */
//            {
//
//                for (int i = 0; i < n; i++) {
//
//                    final byte[] e = a[offset + i];
//
//                    obs.writeInt(symbols.get(e).intValue(), codeBitLength);
//
//                }
//
//            }
//
//            obs.flush();
//
//            /*
//             * @todo counters for time in each phase of this process.
//             * 
//             * @todo since the procedure tends to write large byte[] make
//             * sure that the RPC buffers are good at that, e.g., either they
//             * auto-extend aggressively or they are writing onto a fixed
//             * buffer that writes on a socket.
//             */
//            log.warn(n + " statements were serialized in "
//                    + obs.writtenBits() + " bytes using " + nsymbols
//                    + " symbols with a code length of " + codeBitLength
//                    + " bits.");
//
//            // // copy onto the output buffer.
//            // out.write(buf);
//
//        }
//    }
//    
//}

}