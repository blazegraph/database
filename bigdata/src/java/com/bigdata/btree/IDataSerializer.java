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

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.proc.IKeyArrayIndexProcedure;

/**
 * Interface for custom serialization of logical <code>byte[][]</code> data
 * such are used to represent keys or values. Some implementations work equally
 * well for both (e.g., no compression, huffman compression, dictionary
 * compression) while others can require that the data are fully ordered (e.g.,
 * hu-tucker compression which only works for keys).
 * <p>
 * Note: there are a few odd twists to this API. It is designed to be used both
 * for the keys and values in the {@link AbstractBTree} and for the keys and
 * values in {@link IKeyArrayIndexProcedure}s, which are auto-split against the
 * index partitions and hence use a {fromIndex, toIndex}. This leads to the
 * requirement that the caller allocate the {@link IRandomAccessByteArray} into
 * which the data will be de-serialized so that they can specify the capacity of
 * that object.
 * <p>
 * The standard practice is to write out <code>toIndex</code> as (toIndex -
 * fromIndex), which is the #of keys to be written. When the data are read back
 * in the fromIndex is assumed to be zero and toIndex will be the #of elements
 * to read. This has the effect of making the array compact. Likewise, when
 * <code>deserializedSize == 0</code> the size of the array to be allocated on
 * deserialization is normally computed as <code>toIndex - fromIndex</code> so
 * that the resulting array will be compact on deserialization. A non-zero value
 * is specified by the {@link NodeSerializer} so that the array is correctly
 * dimensioned for the branching factor of the {@link AbstractBTree} when it is
 * de-serialized.
 * <p>
 * Note: When the {@link IDataSerializer} will be used for values stored in an
 * {@link AbstractBTree} then it MUST handle [null] elements. The B+Tree both
 * allows <code>null</code>s and the {@link MutableKeyBuffer} will report a
 * <code>null</code> when the corresponding index entry is deleted (in an
 * index that supports deletion markers).
 * 
 * FIXME Verify that RMI serialization is heavily buffered since we are NOT
 * buffering the {@link OutputBitStream} and {@link InputBitStream} objects in
 * order to minimize heap churn and since we have the expectation that they are
 * writing/reading on buffered streams.
 * 
 * @todo support compression (hu-tucker, huffman, delta-coding of keys,
 *       application defined compression, etc.)
 *       <p>
 *       user-defined tokenization into symbols (hu-tucker, huffman)
 * 
 * @todo test suites for each of the {@link IDataSerializer}s, including when
 *       the reference is a null byte[][], when it has zero length, and when
 *       there are null elements in the byte[][].
 */
public interface IDataSerializer extends Serializable {

    /**
     * Write a byte[][], possibly empty and possibly <code>null</code>.
     * 
     * @param out
     *            An {@link OutputStream} that implements the {@link DataOutput}
     *            interface.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            The data (MAY be a <code>null</code> reference IFF
     *            <code>toIndex-fromIndex == 0</code>, MAY be an empty array,
     *            and MAY contain <code>null</code> elements in the array).
     * 
     * @throws IOException
     */
    void write(DataOutput out, IRandomAccessByteArray raba) throws IOException;

//    void write(DataOutput out, int fromIndex, int toIndex, byte[][] keys) throws IOException;

//    public byte[][] read(DataInput in) throws IOException;

    /**
     * Variant that may be used to read into a caller provided
     * {@link IKeyBuffer}. The <code>byte[]</code>s are appended to the
     * {@link IKeyBuffer}. An exception is thrown if the {@link IKeyBuffer} is
     * not large enough.
     * 
     * @param in The elements will be read from here.
     * @param raba The elements will be appended to this object.
     * @throws IOException
     * 
     * @todo make sure that we can obtain an OutputStream that will let us
     *       append a key to the {@link IKeyBuffer} so that we can avoid
     *       allocation of byte[]s during de-serialization.
     */
    public void read(DataInput in, IRandomAccessByteArray raba) throws IOException;
    
//    /**
//     * Return an interface that may be used to read the data. When efficient
//     * this method MAY be implemented such that it buffers the data as a byte[]
//     * operates directly on that buffer. For example, this may be used to allow
//     * access to data stored in compressed form.
//     * 
//     * @param in
//     * @return
//     * @throws IOException
//     */
//    void read(DataInput in,Constructor<IRandomAccessByteArray>ctor) throws IOException; 
    
//    /**
//     * Read a byte[][], possibly empty and possibly <code>null</code>.
//     * 
//     * @param in
//     *            An {@link InputStream} that implements the {@link DataInput}
//     *            interface.
//     * 
//     * @return The keys.
//     * 
//     * @throws IOException
//     */
//    byte[][] read(DataInput in) throws IOException;

    /**
     * No compression.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class DefaultDataSerializer implements IDataSerializer, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = -7960255442029142379L;

        public transient static DefaultDataSerializer INSTANCE = new DefaultDataSerializer();
        
        /**
         * Sole constructor (handles de-serialization also).
         */
        public DefaultDataSerializer() {
            
        }
        
        public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {
//            final boolean notNull = in.readBoolean();
//            if (!notNull) {
//                return null;
//            }
            final int n = (int) LongPacker.unpackLong(in);
            for (int i = 0; i < n; i++) {
                // when lenPlus == 0 the value is null (vs byte[0]).
                final int lenPlus1 = (int) LongPacker.unpackLong(in);
                if (lenPlus1 > 0) {
                    byte[] tmp = new byte[lenPlus1 - 1];
                    in.readFully(tmp);
                    raba.add(tmp);
                } else
                    raba.add(null);
            }
//            return new RandomAccessByteArray(0/*fromIndex*/,n/*toIndex*/,a);
        }

        public void write(DataOutput out, IRandomAccessByteArray raba)
                throws IOException {
            final int n = raba.getKeyCount();
//            if (a == null && n != 0)
//                throw new IllegalArgumentException();
//            out.writeBoolean(a != null); // notNull
//            if (a != null) {
                LongPacker.packLong(out, n);
//                for (int i = fromIndex; i < toIndex; i++) {
//                    final byte[] e = a[i];
//                for(final byte[] e : raba) {
                  for(int i=0; i<n; i++) {
                      // differentiate a null value from an empty byte[].
                      final boolean isNull = raba.isNull(i);
                      final int lenPlus1 = isNull ? 0 : raba.getLength(i) + 1;
                      LongPacker.packLong(out, lenPlus1);
                      if (!isNull) {
                          raba.copyKey(i, out);
                      }
                }
//            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            // NOP
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            // NOP
            
        }

    }

    /**
     * Simple prefix compression of ordered keys. The common prefix shared by
     * all keys is factored out and written once followed by the remainder for
     * each key. This implementation does not permit null values.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo could use hamming codes for bytes.
     * 
     * @todo variants for custom parsing into symbols for hamming codes.
     * 
     * @todo use a delta from key to key specifying how much to be reused -
     *       works well if deserializing keys or for a linear key scan, but not
     *       for binary search on the raw node/leaf record.
     * 
     * @todo verify that keys are monotonic during constructor from mutable keys
     *       and during de-serialization.
     * 
     * @todo add nkeys to the abstract base class and keep a final (read-only)
     *       copy on this class. if the value in the base class is changed then
     *       it is an error but this allows direct access to the field in the
     *       base class for the mutable key buffer implementation.
     * 
     * @todo even more compact serialization of sorted byte[]s is doubtless
     *       possible, perhaps as a patricia tree, trie, or other recursive
     *       decomposition of the prefixes. figure out what decomposition is the
     *       most compact, easy to compute from sorted byte[]s, and supports
     *       fast search on the keys to identify the corresponding values.
     */
    public static class SimplePrefixSerializer implements IDataSerializer, Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = -8354230015113732733L;

        public transient static SimplePrefixSerializer INSTANCE = new SimplePrefixSerializer();

        public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {
            
            // #of keys in the node or leaf.
            final int nkeys = (int) LongPacker.unpackLong(in);
            
            if(nkeys==0) return;
            
//            // maximum #of keys allowed in the node or leaf.
//            final int maxKeys = (int) LongPacker.unpackLong(in);
            
            // length of the byte[] containing the prefix and the remainder for each key.
            final int bufferLength = (int) LongPacker.unpackLong(in);
            
            /*
             * Read in deltas for each key and re-create the offsets.
             */
            final int[] offsets = new int[nkeys];
            
            int lastOffset = 0; // prefixLength;

            for( int i=0; i<nkeys; i++ ) {
                
                final int delta = (int) LongPacker.unpackLong(in);
                
                final int offset = lastOffset + delta;
                
                offsets[i] = offset;
                
                lastOffset = offset;
                
            }
            
            final byte[] buf = new byte[bufferLength];
            
            in.readFully(buf);

//            final byte[][] keys = new byte[maxKeys][];

            final int prefixLength = offsets[0];
            
            for (int i = 0; i < nkeys; i++) {

//              final int remainderLength = getRemainderLength(index);
                final int remainderLength;
                {
                    int offset = offsets[i];

                    int next_offset = i == nkeys - 1 ? buf.length
                            : offsets[i + 1];

                    remainderLength = next_offset - offset;
                }

                final int len = prefixLength + remainderLength;

                final byte[] key = new byte[len];
                
                System.arraycopy(buf, 0, key, 0, prefixLength);

                System.arraycopy(buf, offsets[i], key, prefixLength, remainderLength);
                
                raba.add(key);

            }
            
        }

        public void write(DataOutput out, IRandomAccessByteArray raba) throws IOException {

            final int nkeys = raba.getKeyCount();
                
            // #of keys in the node or leaf.
//            LongPacker.packLong(os, nkeys);
            LongPacker.packLong(out,nkeys);
            
            if(nkeys==0) return;

            if(raba instanceof ImmutableKeyBuffer) {
                
                /*
                 * Optimized since the source is generally an
                 * ImmutableKeyBuffer, at least until we remove that dependency
                 * in the BTree code.
                 */
                
                final ImmutableKeyBuffer keys = (ImmutableKeyBuffer)raba;
                
                final int bufferLength = keys.buf.length;
                
                // maximum #of keys allowed in the node or leaf.
//                LongPacker.packLong(os, keys.maxKeys);

                // length of the byte[] buffer containing the prefix and remainder for each key.
                LongPacker.packLong(out, bufferLength);
//                os.packLong(bufferLength);
                
                /*
                 * Write out deltas between offsets.
                 */
                int lastOffset = 0; // keys.prefixLength;
                
                for( int i=0; i<nkeys; i++) {
                    
                    int offset = keys.offsets[i];
                    
                    int delta = offset - lastOffset;
                    
                    LongPacker.packLong(out, delta);
//                    os.packLong(delta);
                    
                    lastOffset = offset;
                    
                }
                
                out.write(keys.buf, 0, bufferLength);
//                os.write(keys.buf, 0, bufferLength);
                
                return;
                
            } else {
                
                /*
                 * Computes the length of the prefix by computed by counting the #of leading
                 * bytes that match for the first and last key in the buffer.
                 */
    
                final int prefixLength;// = keys.getPrefixLength();
                
                if (nkeys == 0) {
                    
                    prefixLength = 0;
                    
                } else if (nkeys == 1) {
                    
                    prefixLength = raba.getKey(0).length;
                    
                } else {
                 
                    prefixLength = BytesUtil.getPrefixLength(raba.getKey(0), raba.getKey(nkeys - 1));
                    
                }
                
                // offsets into the serialized key buffer.
                final int[] offsets = new int[nkeys];
                
                // compute the total length of the key buffer.
                int bufferLength = prefixLength;
                
                for(int i=0; i<nkeys; i++) {
                    
                    // offset to the remainder of the ith key in the buffer.
                    offsets[i] = bufferLength;
                    
                    int remainder = raba.getKey(i).length - prefixLength;
                    
                    assert remainder >= 0;
                    
                    bufferLength += remainder;
                    
                }
    
                // maximum #of keys allowed in the node or leaf.
    //            LongPacker.packLong(os, keys.getMaxKeys());
    //            LongPacker.packLong(os,(deserializedSize==0?nkeys:deserializedSize));//keys.getMaxKeys());
    
                // length of the byte[] buffer containing the prefix and remainder for each key.
    //            LongPacker.packLong(os, bufferLength);
                LongPacker.packLong(out,bufferLength);
                
                /*
                 * Write out deltas between offsets.
                 * 
                 * Note: this is 60% of the cost of this method. This is not pack long
                 * so much as doing individual byte put operations on the output stream
                 * (which is over a ByteBuffer).  Just using a BAOS here doubles the 
                 * index segment build throughput.
                 */
                {
    //                ByteArrayOutputStream baos = new ByteArrayOutputStream(nkeys*8);
    //                DataOutputStream dbaos = new DataOutputStream(baos);
    
                    int lastOffset = 0;
                
                for( int i=0; i<nkeys; i++) {
                    
                    int offset = offsets[i];
                    
                    int delta = offset - lastOffset;
                    
                    LongPacker.packLong(out, delta);
    //                os.packLong(delta);
                    
                    lastOffset = offset;
                    
                }
                
    //            dbaos.flush();
    //            
    //            os.write(baos.toByteArray());
                }
                
                /*
                 * write out the prefix followed by the remainder of each key in
                 * turn.
                 */
    
                if (nkeys > 0) {
    
                    out.write(raba.getKey(0), 0, prefixLength);
    
                    for (int i = 0; i < nkeys; i++) {
    
                        final byte[] key = raba.getKey(i);
                        
                        final int remainder = key.length - prefixLength;
    
                        out.write(key, prefixLength, remainder);
    
                    }
                    
                }

            }
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            // NOP
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            
            // NOP.
            
        }
        
    }
    
// /**
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

//    /**
//     * A cache for recycling byte[] buffers. This is designed to minimize the
//     * impact on the heap when there is rapid turnover for byte[]s of a fixed
//     * capacity. Buffers are allocated on demand and must be released
//     * explicitly. Released buffers are recycled unless the internal queue is at
//     * capacity, in which case they are presumably swept by the garbage
//     * collector.
//     * <p>
//     * Note: Weak references can not be used to create caches with the desired
//     * semantics since the weak reference will have been cleared before we are
//     * notified (via the reference queue). This makes it impossible to trap the
//     * reference for reuse. Therefore this class uses an explicit close protocol
//     * on the {@link RecycleBuffer} to indicate that a buffer is available for
//     * reuse.
//     * 
//     * @todo I have NOT proven that a JVM Nursery does a worse job than this
//     *       class. It is possible for a Nursery to do something very similar
//     *       for short lived but relatively large byte[]s. You would have to
//     *       test it out to be sure. This could be done by creating a variant
//     *       (or using an option to the ctor) that never cached references (or
//     *       setting the cacheSize to zero). This would expose the JVM behavior.
//     * 
//     * @todo problems: (1) trading off the cache size against the consumers of
//     *       the cache; (2) cache is for buffers of a fixed length, so this
//     *       would work {@link OutputBitStream} or the {@link DiskOnlyStrategy}
//     *       but it can not cache byte[]s of varying lengths such as records for
//     *       the {@link Journal}; and (3) the scope in which the cache is used,
//     *       for example a single static reference, one per journal, etc. It
//     *       does not make sense to use one per thread. Likewise, a thread-local
//     *       buffer is not sufficient for {@link OutputBitStream} in case there
//     *       are nested calls that allocate an {@link OutputBitStream} (this is
//     *       more likely than you think since delegating serializers may well
//     *       wrap the underlying {@link OutputStream} and then find that it is
//     *       wrapped again by the delegate). The best choice may be the static
//     *       option. E.g., static getCache(int capacity). If we then adjust the
//     *       queue size dynamically based on demand all might be good.
//     * 
//     * @todo the buffer for a {@link BufferedInputStream} or
//     *       {@link BufferedOutputStream} would be another place where this
//     *       class would be useful IF we were allowed to pass in the buffer.
//     * 
//     * @todo since the {@link IRawStore} API is defined in terms of ByteBuffer
//     *       we could modify (or encapsulate) this class to keep a variety of
//     *       frequently used byte[]s on hand and then wrap them up in a
//     *       {@link ByteBuffer} - but there is no way to handle the recycle
//     *       semantics. I would have to replace the {@link ByteBuffer} with
//     *       another class for this purpose, perhaps the {@link ByteArrayBuffer}
//     *       if I added a read-only mode to that class.
//     *       <p>
//     *       Perhaps the {@link BufferCache} could simply be stated in terms of
//     *       {@link ByteBuffer}s? While a read-only byte buffer would not let
//     *       me gain access to the backing byte[] I could track that internally
//     *       in a hash map indexed by the ByteBuffer - no, again, the wrong byte
//     *       buffer reference would be in the hash map - the read-write vs the
//     *       read-only.
//     */
//    public static class BufferCache {
//        
//        protected static final Logger log = Logger.getLogger(BufferCache.class);
//        
//        private final int bufSize;
//        private final int cacheSize;
//        private final BlockingQueue<byte[]> queue; 
//
//        /**
//         * #of byte[]s allocated and not yet recycled.
//         * 
//         * @todo When this is counter is small compared to the queue size we
//         * release byte[]s back to the JVM. When it is large compared to the
//         * queue size, we start to retain more byte[]s. This needs to be done
//         * carefully so that periodic bursts do not wind up causing byte[]s to
//         * be held just long enough to get out of the nursery and into the
//         * mature heap where they are more expensive to GC.
//         */
//        private AtomicInteger nallocated = new AtomicInteger(0);
//        
//        /**
//         * 
//         * @param bufSize
//         *            The size of the byte[] buffers to be allocated and
//         *            managed. Instances of this class are generally useful when
//         *            byte[]s are relatively large (kilobytes or better)
//         * 
//         * @param cacheSize
//         *            The capacity of the internal queue. When zero (0) the
//         *            internal queue is disabled and a new byte[] will always be
//         *            created by {@link #get()} and recycled byte[]s will be
//         *            left in the hands of the garbage collector. This choice
//         *            makes sense if your JVM does a better job than this class.
//         */
//        public BufferCache(int bufSize, int cacheSize) {
//            
//            assert bufSize > 0;
//
//            assert cacheSize >= 0;
//
//            this.bufSize = bufSize;
//
//            this.cacheSize = cacheSize;
//
//            if (cacheSize == 0) {
//
//                queue = null;
//
//            } else {
//
//                queue = new LinkedBlockingQueue<byte[]>(cacheSize);
//
//            }
//
//        }
//        
//        /**
//         * Recycle a buffer. When the queue is under its capacity, the buffer
//         * reference will be cached and made available for reuse. When the queue
//         * is at capacity the reference will be not be saved and will become
//         * available for garbage collection.
//         * 
//         * @param buffer
//         *            A buffer
//         */
//        protected void recycle(final RecycleBuffer buffer) {
//            
//            assert buffer != null;
//
//            // reference to byte[].
//            final byte[] tmp = buffer.buffer;
//
//            // clear reference on RecycleBuffer, marking it as "closed".
//            buffer.buffer = null;
//
//            assert tmp != null;
//            
//            assert tmp.length == bufSize;
//
//            nallocated.decrementAndGet();
//            
//            if (queue == null) return;
//            
//            // offer byte[] to the queue.
//            if (!queue.offer(tmp)) {
//
//                // queue is over capacity, so byte[] will get swept by GC.
//                log.info("Over capacity: discarding buffer");
//
//            }
//            
//        }
//
//        /**
//         * Obtain a byte[] buffer. When the queue contains a byte[], that
//         * reference will be returned. Otherwise a new byte[] will be allocated
//         * and returned to the caller.
//         */
//        public RecycleBuffer get() {
//            
//            // Note: null iff queue is empty (no wait)
//            byte[] buffer = (queue != null ? queue.poll() : null);
//
//            if (buffer == null) {
//
//                // allocate new buffer.
//                buffer = new byte[bufSize];
//
//            }
//
//            nallocated.incrementAndGet();
//            
//            // return to caller.
//            return new RecycleBuffer(this, buffer);
//
//        }
//     
//    }
//
//    /**
//     * Instances of this object are managed via the {@link BufferCache}. The
//     * wrapped byte[] MUST be explicitly "recycled" by calling {@link #close()}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class RecycleBuffer {
// 
//        private BufferCache cache;
//        private byte[] buffer;
//
//        protected RecycleBuffer(BufferCache cache, byte[] buffer) {
//
//            assert cache != null;
//            assert buffer != null;
//
//            this.cache = cache;
//
//            this.buffer = buffer;
//            
//        }
//        
//        /**
//         * Return the buffer.
//         */
//        public byte[] buffer() {
//
//            if (buffer == null)
//                throw new IllegalStateException();
//
//            return buffer;
//            
//        }
//        
//        /**
//         * Release the buffer, declaring that it MAY be reused by others.
//         * <p>
//         * Note: It is an undetectable error to retain a reference to the
//         * {@link RecycleBuffer#buffer() buffer} after calling this method.
//         */
//        synchronized public void close() {
//
//            if (buffer == null)
//                throw new IllegalStateException();
//            
//            // Note: clears the byte[] reference.
//            cache.recycle(this);
//
//        }
//
//    }

//    /**
//     * An {@link OutputBitStream} that uses a {@link BufferCache} to recycle its
//     * internal byte[] buffer, which it uses for efficiency. The contents of the
//     * internal buffer are copied en-mass onto the backing {@link OutputBuffer}
//     * by {@link #flush()}, when the {@link OutputBitStream} is repositioned,
//     * and when it is closed. There should be very little overhead incurred by
//     * these byte[] copies.
//     * <p>
//     * Note: The caller can access the underlying {@link OutputBuffer} in order
//     * to perform random or sequential data type or byte oriented writes but you
//     * MUST:
//     * <ol>
//     * 
//     * <li> {@link #flush()} the {@link OutputBitStream} in order to byte align
//     * the bit stream to force the data in the internal buffer to the backing
//     * {@link DataOutputBuffer}</li>
//     * 
//     * <li> re-position the {@link OutputBitStream} on the underlying buffer
//     * afterwards. </li>
//     * </ol>
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class OutputBitStream extends it.unimi.dsi.mg4j.io.OutputBitStream {
//
//        /**
//         * Note: The byte[] buffer size is set at 1K since I am presuming that
//         * the {@link OutputBitStream} is wrapping an {@link OutputStream}
//         * backed by a byte[]. Hence periodic copies take place from the
//         * {@link OutputBitStream}s internal buffer to the byte[] in the
//         * backing {@link OutputStream}. Those copies should be quite fast so
//         * there is less need to minimize them when compared with an
//         * {@link OutputBitStream} backed by disk.
//         */
//        static private BufferCache bufferCache = new BufferCache(
//                Bytes.kilobyte32, 20);
//        
//        private RecycleBuffer recycleBuffer;
//        
//        /**
//         * Return the underlying {@link OutputStream} - {@link #flush()} before
//         * writing on the returned stream and re-position the
//         * {@link OutputBitStream} afterwards!!!
//         * 
//         * @todo the flush/position could be taken care of by wrapping the
//         *       underlying {@link OutputStream} so that the position changes
//         *       were tracked by the {@link OutputBitStream}.
//         */
//        public OutputStream getOutputStream() {
//
//            return os;
//
//        }
//
//        public OutputBitStream factory(OutputStream os) {
//         
//            return new OutputBitStream(os, bufferCache.get());
//            
//        }
//        
//        /**
//         * @param os
//         */
//        protected OutputBitStream(OutputStream os, RecycleBuffer recycleBuffer) {
//            
//            super(os, recycleBuffer.buffer());
//
//            this.recycleBuffer = recycleBuffer;
//            
//        }
//        
//        public void close() throws IOException {
//            
//            super.close();
//            
//            recycleBuffer.close();
//            
//        }
//
//    }

    /**
     * Useful when no data will be written.
     */
    public static class NoDataSerializer implements IDataSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = 6683355231100666183L;

        public transient static NoDataSerializer INSTANCE = new NoDataSerializer();

        public void write(DataOutput out, IRandomAccessByteArray raba) throws IOException {

            // NOP
            
        }

        public void read(DataInput in, IRandomAccessByteArray raba) throws IOException {

            // NOP
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            // NOP

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            // NOP

        }

    }

}
