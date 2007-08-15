/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 30, 2007
 */

package com.bigdata.btree;

import java.util.Locale;
import java.util.UUID;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A class that may be used to form multi-component keys but which does not
 * support Unicode. An instance of this class is quite light-weight and SHOULD
 * be used when Unicode support is not required.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo update successor tests for GOM index helper classes, perhaps by
 *       refactoring the successor utilties into a base class and then isolating
 *       their test suite from that of the key builder.
 * 
 * @see UnicodeKeyBuilder
 * 
 * @see SuccessorUtil, which may be used to compute the successor of a value before
 *      encoding it as a component of a key.
 * 
 * @see BytesUtil#successor(byte[]), which may be used to compute the successor
 *      of an encoded key.
 */
public class KeyBuilder implements IKeyBuilder {

    /**
     * The default capacity of the key buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;
    
    /**
     * A non-negative integer specifying the #of bytes of data in the buffer
     * that contain valid data starting from position zero(0).
     */
    protected int len;
    
    /**
     * The key buffer. This is re-allocated whenever the capacity of the buffer
     * is too small and reused otherwise.
     */
    protected byte[] buf;
    
    /**
     * Used to encode unicode strings into compact unsigned byte[]s that have
     * the same sort order (aka sort keys).
     * <p>
     * Note: When <code>null</code> the IKeyBuilder does NOT support Unicode
     * and the optional Unicode methods will all throw an
     * {@link UnsupportedOperationException}.
     */
    protected final RuleBasedCollator collator;

//  /**
//  * This class integrates with {@link RuleBasedCollator} for efficient
//  * generation of compact sort keys.  Since {@link RawCollationKey} is
//  * final, but its fields are public, we just set the fields directly
//  * each time before we use this object.
//  */
// private RawCollationKey b2;

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
    protected static int assertNonNegative(String msg,int v) {
       
        if(v<0) throw new IllegalArgumentException(msg);
        
        return v;
        
    }
    
    /**
     * Creates a key builder with an initial buffer capacity of
     * <code>1024</code> bytes.
     */
    public KeyBuilder() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * Creates a key builder with the specified initial buffer capacity.
     * 
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys.
     */
    public KeyBuilder(int initialCapacity) {
        
        this(0, new byte[assertNonNegative("initialCapacity", initialCapacity)]);
        
    }
    
    /**
     * Creates a key builder using an existing buffer with some data.
     * 
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     */
    public KeyBuilder(int len, byte[] buf) {

        this( null /* no unicode support*/, len, buf );
        
    }    
    
    /**
     * Creates a key builder using an existing buffer with some data (designated
     * constructor).
     * 
     * @param collator
     *            The collator used to encode Unicode strings (when
     *            <code>null</code> Unicode collation support is disabled).
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    protected KeyBuilder(RuleBasedCollator collator, int len, byte[] buf) {
        
        if (len < 0)
            throw new IllegalArgumentException("len");

        if (buf == null)
            throw new IllegalArgumentException("buf");

        if (len > buf.length)
            throw new IllegalArgumentException("len>buf.length");

        this.len = len;

        this.buf = buf;

        this.collator = collator; // MAY be null.
        
//        /*
//         * Note: The bytes and size fields MUST be reset before each use of
//         * this object!
//         */
//        this.b2 = new RawCollationKey(buf,len);
        
    }
    
    final public int getLength() {
        
        return len;
        
    }

    final public IKeyBuilder append(int off, int len, byte[] a) {
        
        ensureFree(len);
        
        System.arraycopy(a, off, buf, this.len, len);
        
        this.len += len;
        
//        assert this.len <= buf.length;
        
        return this;
        
    }

    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer. The
     * {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * <p>
     * This operation is equivilent to
     * 
     * <pre>
     * ensureCapacity(this.len + len)
     * </pre>
     * 
     * and the latter is often used as an optimization.
     * 
     * @param len
     *            The minimum #of free bytes.
     */
    final public void ensureFree(int len) {
        
        ensureCapacity(this.len + len );
        
    }

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total bytes.
     * The {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * 
     * @param capacity
     *            The minimum #of bytes in the buffer.
     */
    final public void ensureCapacity(int capacity) {
        
        if(capacity<0) throw new IllegalArgumentException();
//        assert capacity >= 0;
        
        final int overflow = capacity - buf.length;
        
        if(overflow>0) {
        
            /*
             * extend to at least the target capacity.
             */
            final byte[] tmp = new byte[capacity];
            
            // copy only the defined bytes.
            System.arraycopy(buf, 0, tmp, 0, this.len);
            
            buf = tmp;
            
        }

    }

    final public byte[] getKey() {
        
        byte[] tmp = new byte[this.len];
        
        System.arraycopy(buf, 0, tmp, 0, this.len);
        
        return tmp;
        
    }
    
    /*
     * The problem with this method is that it encourages us to reuse a key
     * buffer but the btree (at least when used as part of a local api) requires
     * that we donate the key buffer to the btree.
     */
//    /**
//     * Copy the key from the internal buffer into the supplied buffer.
//     * 
//     * @param b
//     *            A byte[].
//     * 
//     * @exception IndexOutOfBoundsException
//     *                if the supplied buffer is not large enough.
//     */
//    final public void copyKey(byte[] b) {
//    
//        System.arraycopy(this.buf, 0, b, 0, this.len);
//        
//    }
    
    final public IKeyBuilder reset() {
        
        len = 0;
        
        return this;
        
    }

    /*
     * Optional Unicode operations.
     */

    private final void assertUnicodeEnabled() {
        
        if (collator == null)
            throw new UnsupportedOperationException();
        
    }

    final public RuleBasedCollator getCollator() {
        
        return collator;
        
    }

    final public IKeyBuilder append(String s) {
        
        assertUnicodeEnabled();
        
//        set public fields on the RawCollationKey
//        b2.bytes = this.buf;
//        b2.size = this.len;
//        collator.getRawCollationKey(s, b2);
//        
//        /*
//         * take the buffer, which may have changed.
//         * 
//         * note: we do not take the last byte since it is always zero per
//         * the ICU4J documentation. if you want null bytes between
//         * components of a key you have to put them there yourself.
//         */
//        this.buf = b2.bytes;
//        this.len = b2.size - 1;

//        RawCollationKey raw = new RawCollationKey(this.buf, this.len);
//        
//        collator.getRawCollationKey(s, raw );
//
//        this.buf = raw.bytes;
//        this.len = raw.size;

        /*
         * Note: This is the only invocation that appears to work reliably. The
         * downside is that it grows a new byte[] each time we encode a unicode
         * string rather than being able to leverage the existing array on our
         * class.
         * 
         * @todo look into the source code for RawCollationKey and its base
         * class, ByteArrayWrapper, and see if I can resolve this issue for
         * better performance and less heap churn. Unfortunately the
         * RawCollationKey class is final so we can not subclass it.
         */
        RawCollationKey raw = collator.getRawCollationKey(s, null);

        append(0,raw.size,raw.bytes);
        
        return this;
        
    }

    final public IKeyBuilder append(char[] v) {

        return append(new String(v));
        
    }
    
    final public IKeyBuilder append(char v) {

        return append("" + v);
                    
    }

    /*
     * Non-optional operations.
     */
    
    /**
     * @todo This could be written using {@link String#toCharArray()} or {@link
     *       String#getBytes(int, int, byte[], int)} with a post-processing
     *       fixup of the bytes into ones complement values. The latter method
     *       would doubtless be the fastest approach but it is deprecated in the
     *       {@link String} api.
     */
    public IKeyBuilder appendASCII(String s) {
        
        int len = s.length();
        
        ensureFree(len);
        
        for(int i=0; i<len; i++) {
            
            char ch = s.charAt(i);
            
            append((byte)(ch & 0xff));
            
        }
        
        return this;
        
    }

    final public IKeyBuilder append(byte[] a) {
        
        return append(0,a.length,a);
        
    }
    
    final public IKeyBuilder append(double d) {
        
        // performance tweak.
        if (len + 8 > buf.length) ensureCapacity(len+8);
//        ensureFree(8);

        long v = Double.doubleToLongBits(d);
        
        // convert to twos-complement long.

        if (v < 0) {
            
            v = 0x8000000000000000L - v;

        }

        // delegate to append(long)
        return append( v );

    }

    final public IKeyBuilder append(float f) {

        // performance tweak.
        if (len + 4 > buf.length) ensureCapacity(len+4);
//        ensureFree(4);

        int v = Float.floatToIntBits(f);

        // convert to twos complement int.
        if (v < 0) {

            v = 0x80000000 - v;

        }

        // delegate to append(int)
        return append(v);

    }

    final public IKeyBuilder append(UUID uuid) {

        if (len + 16 > buf.length) ensureCapacity(len+16);

        append( uuid.getMostSignificantBits() );
        
        append( uuid.getLeastSignificantBits() );
        
        return this;
        
    }
    
    final public IKeyBuilder append(long v) {

        // performance tweak adds .3% on rdfs bulk load.
        if (len + 8 > buf.length) ensureCapacity(len+8);
//        ensureFree(8);
//        ensureCapacity( len + 8 );
        
        // lexiographic ordering as unsigned long integer.

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = v + 0x8000000000000000L;
            
        }

        // big-endian.
        buf[len++] = (byte)(v >>> 56);
        buf[len++] = (byte)(v >>> 48);
        buf[len++] = (byte)(v >>> 40);
        buf[len++] = (byte)(v >>> 32);
        buf[len++] = (byte)(v >>> 24);
        buf[len++] = (byte)(v >>> 16);
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);

        return this;
        
    }

    final public IKeyBuilder append(int v) {

        // performance tweak.
        if (len + 4 > buf.length) ensureCapacity(len+4);
//        ensureFree(4);
        
        // lexiographic ordering as unsigned int.
        
        if (v < 0) {

            v = v - 0x80000000;

        } else {
            
            v = 0x80000000 + v;
            
        }

        // big-endian
        buf[len++] = (byte)(v >>> 24);
        buf[len++] = (byte)(v >>> 16);
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);
                    
        return this;
        
    }

    final public IKeyBuilder append(short v) {

        // performance tweak.
        if (len + 2 > buf.length) ensureCapacity(len+2);
//        ensureFree(2);
        
        // lexiographic ordering as unsigned short.
        
        if (v < 0) {

            v = (short)(v - (short)0x8000);

        } else {
            
            v = (short) ((short)0x8000 + v);
            
        }

        // big-endian
        buf[len++] = (byte)(v >>>  8);
        buf[len++] = (byte)(v >>>  0);
                    
        return this;
        
    }
    
    final public IKeyBuilder append(final byte v) {

        // performance tweak
        if (len + 1 > buf.length) ensureCapacity(len+1);
        // ensureFree(1);

        // lexiographic ordering as unsigned byte.
        
        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }
        
        buf[len++] = (byte)(i & 0xff);
        
        return this;
        
    }

    final public IKeyBuilder appendNul() {
        
        // performance tweak.
        if (len + 1 > buf.length) ensureCapacity(len+1);
//        ensureFree(1);
        
        buf[len++] = (byte) 0;
        
        return this;
        
    }

    /*
     * static helper methods.
     */
    
    /**
     * Converts an unsigned byte into a signed byte.
     * 
     * @param v
     *            The unsigned byte.
     *            
     * @return The corresponding signed value.
     */
    static public byte decodeByte(byte v) {

        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }

        return (byte)(i & 0xff);
        
    }

    /**
     * Encodes a double precision floating point value as an int64 value that
     * has the same total ordering (you can compare two doubles encoded by this
     * method and the long values will have the same ordering as the double
     * values). The method works by converting the double to the IEEE 754
     * floating-point "double format" bit layout using
     * {@link Double#doubleToLongBits(double)} and then converting the resulting
     * long into a two's complement number.
     * 
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param d
     *            The double precision floating point value.
     * 
     * @return The corresponding long integer value that maintains the same
     *         total ordering.
     */
    public static long d2l(double d) {

        long aLong = Double.doubleToLongBits(d);
        
        if (aLong < 0) {
            
            aLong = 0x8000000000000000L - aLong;

        }
        
        return aLong;

    }

    /**
     * Encodes a floating point value as an int32 value that has the same total
     * ordering (you can compare two floats encoded by this method and the int
     * values will have the same ordering as the float values). The method works
     * by converting the float to the IEEE 754 floating-point "single format"
     * bit layout using {@link Float#floatToIntBits(float)} and then converting
     * the resulting int into a two's complement number.
     * 
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param f
     *            The floating point value.
     * 
     * @return The corresponding integer value that maintains the same total
     *         ordering.
     */
    public static int f2i(float f) {

        int aInt = Float.floatToIntBits(f);
        
        if (aInt < 0) {
            
            aInt = 0x80000000 - aInt;

        }
        
        return aInt;

    }
    
    /**
     * Decodes a signed long value as encoded by {@link #append(long)}.
     * 
     * @param buf
     *            The buffer containing the encoded key.
     * @param off
     *            The offset at which to decode the key.
     *            
     * @return The signed long value.
     */
    static public long decodeLong(byte[] buf,int off) {

        long v = 0L;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 56;
        v += (0xffL & buf[off++]) << 48;
        v += (0xffL & buf[off++]) << 40;
        v += (0xffL & buf[off++]) << 32;
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;

        if (v < 0) {
            
            v = v + 0x8000000000000000L;

        } else {
            
            v = v - 0x8000000000000000L;
            
        }

        return v;
        
    }
    
}
