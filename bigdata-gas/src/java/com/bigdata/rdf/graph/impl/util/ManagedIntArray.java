/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.util;

import org.apache.log4j.Logger;

/**
 * A view on a mutable int[] that may be extended.
 * <p>
 * Note: The backing int[] always has an {@link #off() offset} of ZERO (0) and a
 * {@link #len() length} equal to the capacity of the backing int[].
 * <p>
 * This class is NOT thread-safe for mutation. The operation which replaces the
 * {@link #array()} when the capacity of the backing buffer must be extended is
 * not atomic.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ByteArrayBuffer.java 6279 2012-04-12 15:27:30Z thompsonbry $
 * 
 * TODO Refactor to create a test suite for this.
 */
public class ManagedIntArray implements IManagedIntArray {

    private static final transient Logger log = Logger
            .getLogger(ManagedIntArray.class);

    /**
     * The default capacity of the buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 128;// 1024;

    /**
     * The backing array. This is re-allocated whenever the capacity of the
     * buffer is too small and reused otherwise.
     */
    private int[] buf;

    /**
     * {@inheritDoc} This is re-allocated whenever the capacity of the buffer is
     * too small and reused otherwise.
     */
    @Override
    final public int[] array() {

        return buf;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The offset of the slice into the backing byte[] is always zero.
     */
    @Override
    final public int off() {

        return 0;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The length of the slice is always the capacity of the backing byte[].
     */
    @Override
    final public int len() {

        return buf.length;

    }

    /**
     * Throws exception unless the value is non-negative.
     * 
     * @param msg
     *            The exception message.
     * @param v
     *            The value.
     * 
     * @return The value.
     * 
     * @exception IllegalArgumentException
     *                unless the value is non-negative.
     */
    protected static int assertNonNegative(final String msg, final int v) {

        if (v < 0)
            throw new IllegalArgumentException(msg);

        return v;

    }

    /**
     * Creates a buffer with an initial capacity of
     * {@value #DEFAULT_INITIAL_CAPACITY} bytes. The capacity of the buffer will
     * be automatically extended as required.
     */
    public ManagedIntArray() {

        this(DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * Creates a buffer with the specified initial capacity. The capacity of the
     * buffer will be automatically extended as required.
     * 
     * @param initialCapacity
     *            The initial capacity.
     */
    public ManagedIntArray(final int initialCapacity) {

        this.buf = new int[assertNonNegative("initialCapacity", initialCapacity)];

    }

    /**
     * Create a view wrapping the entire array.
     * <p>
     * Note: the caller's reference will be used until and unless the array is
     * grown, at which point the caller's reference will be replaced by a larger
     * array having the same data.
     * 
     * @param array
     *            The array.
     */
    public ManagedIntArray(final int[] array) {

        if (array == null)
            throw new IllegalArgumentException();

        this.buf = array;

    }

    final public void ensureCapacity(final int capacity) {

        if (capacity < 0)
            throw new IllegalArgumentException();

        if (buf == null) {

            buf = new int[capacity];

            return;

        }

        final int overflow = capacity - buf.length;

        if (overflow > 0) {

            // Extend to at least the target capacity.
            final int[] tmp = new int[extend(capacity)];

            // copy all bytes to the new byte[].
            System.arraycopy(buf, 0, tmp, 0, buf.length);

            // update the reference to use the new byte[].
            buf = tmp;

        }

    }

    @Override
    final public int capacity() {

        return buf == null ? 0 : buf.length;

    }

    /**
     * Return the new capacity for the buffer (default is always large enough
     * and will normally double the buffer capacity each time it overflows).
     * 
     * @param required
     *            The minimum required capacity.
     * 
     * @return The new capacity.
     * 
     * @todo this does not need to be final. also, caller's could set the policy
     *       including a policy that refuses to extend the capacity.
     */
    private int extend(final int required) {

        final int capacity = Math.max(required, capacity() * 2);

        if (log.isInfoEnabled())
            log.info("Extending buffer to capacity=" + capacity + " bytes.");

        return capacity;

    }

    /*
     * Absolute put/get methods.
     */

    @Override
    final public void put(final int pos, //
            final int[] b) {

        put(pos, b, 0, b.length);

    }

    @Override
    final public void put(final int pos,//
            final int[] b, final int off, final int len) {

        ensureCapacity(pos + len);

        System.arraycopy(b, off, buf, pos, len);

    }

    @Override
    final public void get(final int srcoff, final int[] dst) {

        get(srcoff, dst, 0/* dstoff */, dst.length);

    }

    @Override
    final public void get(final int srcoff, final int[] dst, final int dstoff,
            final int dstlen) {

        System.arraycopy(buf, srcoff, dst, dstoff, dstlen);

    }

    @Override
    final public void putInt(final int pos, final int v) {

        if (pos + 1 > buf.length)
            ensureCapacity(pos + 1);

        buf[pos] = v;

    }

    @Override
    final public int getInt(final int pos) {

        return buf[pos];

    }

    @Override
    final public int[] toArray() {

        final int[] tmp = new int[buf.length];

        System.arraycopy(buf, 0, tmp, 0, buf.length);

        return tmp;

    }

    @Override
    public IIntArraySlice slice(final int off, final int len) {

        return new SliceImpl(off, len);

    }

    /**
     * A slice of the outer {@link ManagedArray}. The slice will always reflect
     * the backing {@link #array()} for the instance of the outer class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class SliceImpl implements IIntArraySlice {

        /**
         * The start of the slice in the {@link #array()}.
         */
        private final int off;

        /**
         * The length of the slice in the {@link #array()}.
         */
        private final int len;

        @Override
        final public int off() {

            return off;

        }

        @Override
        final public int len() {

            return len;

        }

        /**
         * Protected constructor used to create a slice. The caller is
         * responsible for verifying that the slice is valid for the backing
         * byte[] buffer.
         * 
         * @param off
         *            The offset of the start of the slice.
         * @param len
         *            The length of the slice.
         */
        protected SliceImpl(final int off, final int len) {

            if (off < 0)
                throw new IllegalArgumentException("off<0");

            if (len < 0)
                throw new IllegalArgumentException("len<0");

            this.off = off;

            this.len = len;

        }

        @Override
        public String toString() {

            return super.toString() + "{off=" + off() + ",len=" + len() + "}";

        }

        @Override
        public int[] array() {

            return ManagedIntArray.this.array();

        }

        /*
         * Absolute get/put operations.
         */

        /**
         * Verify that an operation starting at the specified offset into the
         * slice and having the specified length is valid against the slice.
         * 
         * @param aoff
         *            The offset into the slice.
         * @param alen
         *            The #of bytes to be addressed starting from that offset.
         * 
         * @return <code>true</code>.
         * 
         * @throws IllegalArgumentException
         *             if the operation is not valid.
         */
        private boolean rangeCheck(final int aoff, final int alen) {

            if (aoff < 0)
                throw new IndexOutOfBoundsException();

            if (alen < 0)
                throw new IndexOutOfBoundsException();

            if ((aoff + alen) > len) {

                /*
                 * The operation run length at that offset would extend beyond
                 * the end of the slice.
                 */

                throw new IndexOutOfBoundsException();

            }

            return true;

        }

        @Override
        final public void put(final int pos, final int[] b) {

            put(pos, b, 0, b.length);

        }

        @Override
        final public void put(final int dstoff,//
                final int[] src, final int srcoff, final int srclen) {

            assert rangeCheck(dstoff, srclen);

            System.arraycopy(src, srcoff, array(), off + dstoff, srclen);

        }

        @Override
        final public void get(final int srcoff, final int[] dst) {

            get(srcoff, dst, 0/* dstoff */, dst.length);

        }

        @Override
        final public void get(final int srcoff, final int[] dst,
                final int dstoff, final int dstlen) {

            assert rangeCheck(srcoff, dstlen);

            System.arraycopy(array(), off + srcoff, dst, dstoff, dstlen);

        }

        @Override
        final public void putInt(final int pos, final int v) {

            assert rangeCheck(pos, 1);

            array()[pos] = v;

        }

        @Override
        final public int getInt(final int pos) {

            assert rangeCheck(pos, 1);

            final int v = array()[pos];

            return v;

        }

        @Override
        final public int[] toArray() {

            final int[] tmp = new int[len];

            System.arraycopy(array(), off/* srcPos */, tmp/* dst */,
                    0/* destPos */, len);

            return tmp;

        }

        @Override
        public IIntArraySlice slice(final int aoff, final int alen) {

            final SliceImpl outer = this;

            assert rangeCheck(aoff, alen);

            return new SliceImpl(off() + aoff, alen) {

                @Override
                public int[] array() {

                    return outer.array();

                }

            };

        }

    } // class SliceImpl

}
