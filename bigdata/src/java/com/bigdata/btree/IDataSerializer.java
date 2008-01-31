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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.CognitiveWeb.extser.LongPacker;
import org.apache.log4j.Logger;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.Value;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.IRawStore;

/**
 * Interface for custom serialization of <code>byte[][]</code> data such are
 * used to represent keys or values. Some implementations work equally well for
 * both (e.g., no compression, huffman compression, dictionary compression)
 * while others can require that the data are fully ordered (e.g., hu-tucker
 * compression which only works for keys).
 * 
 * FIXME Verify that RMI serialization is heavily buffered since we are NOT
 * buffering the {@link OutputBitStream} and {@link InputBitStream} objects in
 * order to minimize heap churn and since we have the expectation that they are
 * writing/reading on buffered streams.
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
 * @todo reconcile with {@link IKeyBuffer} and the use of {@link Value}s for
 *       isolation.
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
     *            An {@link InputStream} that implements the {@link DataInput}
     *            interface.
     * 
     * @return The keys.
     * 
     * @throws IOException
     */
    byte[][] read(DataInput in) throws IOException;

    /**
     * Write a byte[][].
     * 
     * @param n
     *            The #of items in the array that contain data.
     * @param offset
     *            The offset of the first item in the array to be serialized.
     * @param keys
     *            The data (may be a <code>null</code> reference, may be an
     *            empty array, and may contain <code>null</code> elements in
     *            the array).
     * @param out
     *            An {@link OutputStream} that implements the {@link DataOutput}
     *            interface.
     * 
     * @throws IOException
     */
    void write(int n, int offset, byte[][] keys, DataOutput out)
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

        public static DefaultDataSerializer INSTANCE = new DefaultDataSerializer();
        
        public byte[][] read(DataInput in) throws IOException {
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

        public void write(int n, int offset, byte[][] a, DataOutput out)
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

        public byte[][] read(DataInput in) throws IOException {

//            /*
//             * Read the entire input stream into a buffer.
//             */
//            DataOutputBuffer buf = new DataOutputBuffer((InputStream) in);
//
//            /*
//             * Unpack the buffer.
//             */
//            DataInputBuffer is = new DataInputBuffer(buf.array(), 0, buf
//                    .pos());

            /*
             * The offset is always zero when the de-serialized since we only
             * send along the relevant data for the split.
             */

            return ((ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                    .getKeys(in)).toKeyArray();

        }

        public void write(int n, int offset, byte[][] a, DataOutput out)
                throws IOException {

            IKeyBuffer tmp = new ImmutableKeyBuffer(offset, n,
                    n/* maxKeys */, a);
            
            if (out instanceof DataOutputBuffer) {

                KeyBufferSerializer.INSTANCE.putKeys((DataOutputBuffer)out, tmp);

            } else {
                
                DataOutputBuffer buf = new DataOutputBuffer();

                KeyBufferSerializer.INSTANCE.putKeys(buf, tmp);

                out.write(buf.array(), 0, buf.pos());
                
            }
            
        }

    }

    /**
     * Wraps an {@link IDataSerializer} so that it may be used as an
     * {@link IKeySerializer}.
     * 
     * @todo if things work out then this class and the {@link IKeySerializer}
     *       interface can be discarded and we can use the
     *       {@link IDataSerializer} interface directly for the btree.
     * 
     * FIXME In order to be efficient we need to discard or modify the
     * {@link ImmutableKeyBuffer} class which currently requires us to create a
     * new byte[] each time we need to materialize a key, which we do below in
     * order to serialize the data. Likewise, the de-serialization is returning
     * an {@link ImmutableKeyBuffer} because that is the expectation on read
     * from the store. The sense of "immutable" is Ok, but the implementation
     * should probably do something like use random access to decode the keys in
     * the original byte[] buffer. The {@link IDataSerializer} API would have to
     * be changed to support such random access.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class WrappedKeySerializer implements IKeySerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -7460457251255645482L;
        
        private IDataSerializer delegate;
        
        public WrappedKeySerializer(IDataSerializer delegate) {
        
            assert delegate != null;
            
            this.delegate = delegate;
            
        }
        
        public IKeyBuffer getKeys(DataInput is) throws IOException {
            
            // max keys
            final int maxKeys = is.readInt();
            
            // read keys using delegate.
            final byte[][] a = delegate.read(is);
            
            return new ImmutableKeyBuffer(0/* offset */, a.length/* nkeys */,
                    maxKeys, a/* keys */);
            
        }

        public void putKeys(DataOutputBuffer os, IKeyBuffer keys) throws IOException {

            // max keys.
            os.writeInt(keys.getMaxKeys());
            
            // nkeys
            final int nkeys = keys.getKeyCount();

            // copy byte[] keys into byte[][] for API alignment.
            final byte[][] a = new byte[nkeys][];
            
            for(int i=0; i<nkeys; i++) {
                
                a[i] = keys.getKey( i );
                
            }

            // write keys using delegate.
            delegate.write(nkeys, 0/*offset*/, a, os);
            
        }
        
    }

    /**
     * A delegation pattern using an {@link IDataSerializer} to serialize byte[]
     * values.
     * 
     * @todo There is a bit of an API mismatch since the
     *       {@link IValueSerializer} is handed an array pre-dimensioned array
     *       whose dimensions correspond to the branching factor of the btree
     *       but the {@link IDataSerializer} assumes that the array size is
     *       exactly the size of the serialized byte[][].
     *       <p>
     *       To align those APIs better we might define an alterative method
     *       signature for {@link IDataSerializer} in which the array is
     *       pre-dimensioned.
     *       <p>
     *       The {@link IValueSerializer} happens to know the #of values in the
     *       array since that is written separately by the
     *       {@link NodeSerializer} but it could be refactored such that it was
     *       only written with the keys and values and not by the
     *       {@link NodeSerializer} itself. The reason for this redundent
     *       writing on the #of elements with valid data is that it allows reuse
     *       of the {@link IDataSerializer} for keys, values, keys and values,
     *       or a btree node or leaf.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class WrappedValueSerializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -2197255334001683007L;
        
        private IDataSerializer delegate;
        
        public WrappedValueSerializer(IDataSerializer delegate) {
        
            assert delegate != null;
            
            this.delegate = delegate;
            
        }

        /**
         * @throws ClassCastException
         *             if the values are not byte[]s.
         */
        public void getValues(DataInput is, Object[] values, int nvals)
                throws IOException {

            // read values.
            final byte[][] b = delegate.read(is);

            assert b.length == nvals;

            // copy the byte[]s onto the caller's object.
            for (int i = 0; i < nvals; i++) {

                values[i] = b[i];

            }

        }

        /**
         * @throws ClassCastException
         *             if the values are not byte[]s.
         */
        public void putValues(DataOutputBuffer os, Object[] values,
                final int nvals) throws IOException {

            /*
             * @todo This copies references into a new byte[][] since values
             * was allocated as an Object[].  If that is changes then this
             * can just pass through the (possibly cast) reference to the
             * (byte[][])values).
             */
            final byte[][] a = new byte[nvals][];
            
            for(int i=0; i<nvals; i++) {
                
                a[i] = (byte[])values[i];
                
            }

            // write values.
            delegate.write(nvals, 0/* offset */, a, os);

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

    /**
     * A cache for recycling byte[] buffers. This is designed to minimize the
     * impact on the heap when there is rapid turnover for byte[]s of a fixed
     * capacity. Buffers are allocated on demand and must be released
     * explicitly. Released buffers are recycled unless the internal queue is at
     * capacity, in which case they are presumably swept by the garbage
     * collector.
     * <p>
     * Note: Weak references can not be used to create caches with the desired
     * semantics since the weak reference will have been cleared before we are
     * notified (via the reference queue). This makes it impossible to trap the
     * reference for reuse. Therefore this class uses an explicit close protocol
     * on the {@link RecycleBuffer} to indicate that a buffer is available for
     * reuse.
     * 
     * @todo I have NOT proven that a JVM Nursery does a worse job than this
     *       class. It is possible for a Nursery to do something very similar
     *       for short lived but relatively large byte[]s. You would have to
     *       test it out to be sure. This could be done by creating a variant
     *       (or using an option to the ctor) that never cached references (or
     *       setting the cacheSize to zero). This would expose the JVM behavior.
     * 
     * @todo problems: (1) trading off the cache size against the consumers of
     *       the cache; (2) cache is for buffers of a fixed length, so this
     *       would work {@link OutputBitStream} or the {@link DiskOnlyStrategy}
     *       but it can not cache byte[]s of varying lengths such as records for
     *       the {@link Journal}; and (3) the scope in which the cache is used,
     *       for example a single static reference, one per journal, etc. It
     *       does not make sense to use one per thread. Likewise, a thread-local
     *       buffer is not sufficient for {@link OutputBitStream} in case there
     *       are nested calls that allocate an {@link OutputBitStream} (this is
     *       more likely than you think since delegating serializers may well
     *       wrap the underlying {@link OutputStream} and then find that it is
     *       wrapped again by the delegate). The best choice may be the static
     *       option. E.g., static getCache(int capacity). If we then adjust the
     *       queue size dynamically based on demand all might be good.
     * 
     * @todo the buffer for a {@link BufferedInputStream} or
     *       {@link BufferedOutputStream} would be another place where this
     *       class would be useful IF we were allowed to pass in the buffer.
     * 
     * @todo since the {@link IRawStore} API is defined in terms of ByteBuffer
     *       we could modify (or encapsulate) this class to keep a variety of
     *       frequently used byte[]s on hand and then wrap them up in a
     *       {@link ByteBuffer} - but there is no way to handle the recycle
     *       semantics. I would have to replace the {@link ByteBuffer} with
     *       another class for this purpose, perhaps the {@link ByteArrayBuffer}
     *       if I added a read-only mode to that class.
     *       <p>
     *       Perhaps the {@link BufferCache} could simply be stated in terms of
     *       {@link ByteBuffer}s? While a read-only byte buffer would not let
     *       me gain access to the backing byte[] I could track that internally
     *       in a hash map indexed by the ByteBuffer - no, again, the wrong byte
     *       buffer reference would be in the hash map - the read-write vs the
     *       read-only.
     */
    public static class BufferCache {
        
        protected static final Logger log = Logger.getLogger(BufferCache.class);
        
        private final int bufSize;
        private final int cacheSize;
        private final BlockingQueue<byte[]> queue; 

        /**
         * #of byte[]s allocated and not yet recycled.
         * 
         * FIXME When this is counter is small compared to the queue size we
         * release byte[]s back to the JVM. When it is large compared to the
         * queue size, we start to retain more byte[]s. This needs to be done
         * carefully so that periodic bursts do not wind up causing byte[]s to
         * be held just long enough to get out of the nursery and into the
         * mature heap where they are more expensive to GC.
         */
        private AtomicInteger nallocated = new AtomicInteger(0);
        
        /**
         * 
         * @param bufSize
         *            The size of the byte[] buffers to be allocated and
         *            managed. Instances of this class are generally useful when
         *            byte[]s are relatively large (kilobytes or better)
         * 
         * @param cacheSize
         *            The capacity of the internal queue. When zero (0) the
         *            internal queue is disabled and a new byte[] will always be
         *            created by {@link #get()} and recycled byte[]s will be
         *            left in the hands of the garbage collector. This choice
         *            makes sense if your JVM does a better job than this class.
         */
        public BufferCache(int bufSize, int cacheSize) {
            
            assert bufSize > 0;

            assert cacheSize >= 0;

            this.bufSize = bufSize;

            this.cacheSize = cacheSize;

            if (cacheSize == 0) {

                queue = null;

            } else {

                queue = new LinkedBlockingQueue<byte[]>(cacheSize);

            }

        }
        
        /**
         * Recycle a buffer. When the queue is under its capacity, the buffer
         * reference will be cached and made available for reuse. When the queue
         * is at capacity the reference will be not be saved and will become
         * available for garbage collection.
         * 
         * @param buffer
         *            A buffer
         */
        protected void recycle(final RecycleBuffer buffer) {
            
            assert buffer != null;

            // reference to byte[].
            final byte[] tmp = buffer.buffer;

            // clear reference on RecycleBuffer, marking it as "closed".
            buffer.buffer = null;

            assert tmp != null;
            
            assert tmp.length == bufSize;

            nallocated.decrementAndGet();
            
            if (queue == null) return;
            
            // offer byte[] to the queue.
            if (!queue.offer(tmp)) {

                // queue is over capacity, so byte[] will get swept by GC.
                log.info("Over capacity: discarding buffer");

            }
            
        }

        /**
         * Obtain a byte[] buffer. When the queue contains a byte[], that
         * reference will be returned. Otherwise a new byte[] will be allocated
         * and returned to the caller.
         */
        public RecycleBuffer get() {
            
            // Note: null iff queue is empty (no wait)
            byte[] buffer = (queue != null ? queue.poll() : null);

            if (buffer == null) {

                // allocate new buffer.
                buffer = new byte[bufSize];

            }

            nallocated.incrementAndGet();
            
            // return to caller.
            return new RecycleBuffer(this, buffer);

        }
     
    }

    /**
     * Instances of this object are managed via the {@link BufferCache}. The
     * wrapped byte[] MUST be explicitly "recycled" by calling {@link #close()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class RecycleBuffer {
 
        private BufferCache cache;
        private byte[] buffer;

        protected RecycleBuffer(BufferCache cache, byte[] buffer) {

            assert cache != null;
            assert buffer != null;

            this.cache = cache;

            this.buffer = buffer;
            
        }
        
        /**
         * Return the buffer.
         */
        public byte[] buffer() {

            if (buffer == null)
                throw new IllegalStateException();

            return buffer;
            
        }
        
        /**
         * Release the buffer, declaring that it MAY be reused by others.
         * <p>
         * Note: It is an undetectable error to retain a reference to the
         * {@link RecycleBuffer#buffer() buffer} after calling this method.
         */
        synchronized public void close() {

            if (buffer == null)
                throw new IllegalStateException();
            
            // Note: clears the byte[] reference.
            cache.recycle(this);

        }

    }

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

}
