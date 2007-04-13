package com.bigdata.btree;

import java.util.Locale;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * <p>
 * Helper class for building up variable <code>unsigned byte[]</code> keys
 * from one or more primitive data types values and/or Unicode strings. An
 * instance of this class may be {@link #reset()} and reused to encode a series
 * of keys.
 * </p>
 * <p>
 * This class uses <a href="http://icu.sourceforge.net">ICU4J</a>. There are
 * several advantages to the ICU libraries: (1) the collation keys are
 * compressed; (2) the libraries are faster than the jdk classes; (3) the
 * libraries support Unicode 5; and (4) the libraries have the same behavior
 * under Java and C/C++ so you can have interoperable code. There is also JNI
 * (Java Native Interface) implementation for many platforms for even greater
 * performance and compatibility.
 * </p>
 * 
 * FIXME verify runtime with ICU4JNI, optimize generation of collation keys ( by
 * using lower level iterators over the collation groups), and remove dependency
 * on ICU4J if possible (it should have a lot of stuff that we do not need if we
 * require the JNI integration; alternatively make sure that the code will run
 * against both the ICU4JNI and ICU4J interfaces).
 * 
 * FIXME there appears to be an issue between ICU4JNI and jrockit under win xp
 * professional. try the icu4jni 3.6.1 patch (done, but does not fix the
 * problem) and ask on the mailing list.
 * 
 * FIXME TestAvailableCharsets Charset.availableCharsets() returned a number
 * less than the number returned by icu -- is this a serious error? - it
 * prevents the test suite from completing correctly. check on the mailing list.
 * 
 * FIXME Bundle a linux elf32 version of ICU.
 * 
 * FIXME Apply the 3.6.1 patch for ICU4JNI and rebuild the distribution.
 * 
 * @todo try out the ICU boyer-moore search implementation if it is defined for
 *       sort keys not just char[]s.
 * 
 * @todo introduce a mark and restore feature for generating multiple keys that
 *       share some leading prefix. in general, this is as easy as resetting the
 *       len field to the mark. keys with multiple components could benefit from
 *       allowing multiple marks.
 * 
 * @todo cross check index metadata for the correct locale and collator
 *       configuration and version code.
 * 
 * @todo transparent use of ICU4JNI when available.
 * 
 * @todo update successor tests for GOM index helper classes, perhaps by
 *       refactoring the successor utilties into a base class and then isolating
 *       their test suite from that of the key builder.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SuccessorUtil, which may be used to compute the successor of a value before
 *      encoding it as a component of a key.
 * 
 * @see BytesUtil#successor(byte[]), which may be used to compute the successor
 *      of an encoded key.
 */
public class KeyBuilder {

    /**
     * The default capacity of the key buffer.
     */
    final public static int DEFAULT_INITIAL_CAPACITY = 1024;
    
    /**
     * Used to encode unicode strings into compact unsigned byte[]s that
     * have the same sort order (aka sort keys).
     */
    protected final RuleBasedCollator collator;

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
    
//    /**
//     * This class integrates with {@link RuleBasedCollator} for efficient
//     * generation of compact sort keys.  Since {@link RawCollationKey} is
//     * final, but its fields are public, we just set the fields directly
//     * each time before we use this object.
//     */
//    private RawCollationKey b2;

    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and an
     * initial buffer capacity of <code>1024</code> bytes.
     * 
     * Note: The according to the ICU4j documentation, the default strength
     * for the Collator is TERTIARY unless specified by the locale.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public KeyBuilder() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
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
    protected static int assertNonNegative(String msg,int v) {
       
        if(v<0) throw new IllegalArgumentException(msg);
        
        return v;
        
    }
    
    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and the
     * specified initial buffer capacity.
     * 
     * Note: The according to the ICU4j documentation, the default strength for
     * the Collator is TERTIARY unless specified by the locale.
     * 
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public KeyBuilder(int initialCapacity) {
        
        this((RuleBasedCollator) Collator.getInstance(Locale.getDefault()),
                initialCapacity);
        
    }
    
    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and the
     * specified initial buffer capacity.
     * 
     * Note: The according to the ICU4j documentation, the default strength for
     * the Collator is TERTIARY unless specified by the locale.
     * 
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
    public KeyBuilder(int len,byte[] buf) {
        
        this((RuleBasedCollator) Collator.getInstance(Locale.getDefault()),
                len, buf);
        
    }

    /**
     * Creates a key builder.
     * 
     * @param collator
     *            The collator used to encode Unicode strings.
     *            
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public KeyBuilder(RuleBasedCollator collator, int initialCapacity) {

        this(collator,0,new byte[assertNonNegative("initialCapacity",initialCapacity)]);
        
    }
    
    /**
     * Creates a key builder using an existing buffer with some data.
     * 
     * @param collator
     *            The collator used to encode Unicode strings.
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
    public KeyBuilder(RuleBasedCollator collator, int len, byte[] buf) {
            
        if(collator == null) throw new IllegalArgumentException("collator");
        
        if(len<0) throw new IllegalArgumentException("len");

        if(buf==null) throw new IllegalArgumentException("buf");

        if(len>buf.length) throw new IllegalArgumentException("len>buf.length");
        
        this.collator = collator;
        
        this.len = len;
        
        this.buf = buf;
        
//        /*
//         * Note: The bytes and size fields MUST be reset before each use of
//         * this object!
//         */
//        this.b2 = new RawCollationKey(buf,len);
        
    }

    final public RuleBasedCollator getCollator() {
        
        return collator;
        
    }
    
    /**
     * The #of bytes of data in the key.
     */
    final public int getLength() {
        
        return len;
        
    }

    /**
     * Append <i>len</i> bytes starting at <i>off</i> in <i>a</i> to the key
     * buffer.
     * 
     * @param off
     *            The offset.
     * @param len
     *            The #of bytes to append.
     * @param a
     *            The array containing the bytes to append.
     *            
     * @return this.
     */
    final public KeyBuilder append(int off, int len, byte[] a) {
        
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

    /**
     * Return the encoded key. Comparison of keys returned by this method MUST
     * treat the array as an array of <em>unsigned bytes</em>.
     * <p>
     * Note that keys are <em>donated</em> to the btree so it is important to
     * allocate new keys when running in the same process space.  When using a
     * network api, the api provides the necessary decoupling. 
     * 
     * @return A new array containing the key.
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     */
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
    
    /**
     * Reset the key length to zero before building another key.
     * 
     * @return This {@link KeyBuilder}.
     */
    final public KeyBuilder reset() {
        
        len = 0;
        
        return this;
        
    }

    /**
     * Encodes a unicode string using the rules of the {@link #collator} and
     * appends the resulting sort key to the buffer (without a trailing nul
     * byte).
     * 
     * @param s
     *            A string.
     */
    public KeyBuilder append(String s) {
        
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

    /**
     * Encodes a uncode string by assuming that its contents are ASCII
     * characters. For each character, this method simply chops of the high byte
     * and converts the low byte to an unsigned byte.
     * <p>
     * Note: This method is potentially much faster since it does not use the
     * {@link RuleBasedCollator}. This method is NOT uncode aware and non-ASCII
     * characters will not be encoded correctly. This method MUST NOT be mixed
     * with keys whose corresponding component is encoded by the unicode aware
     * methods, e.g., {@link #append(String)}.
     * 
     * @param s
     *            A String containing US-ASCII characters.
     * 
     * @return This key builder.
     * 
     * @todo This could be written using {@link String#toCharArray()} or {@link
     *       String#getBytes(int, int, byte[], int)} with a post-processing
     *       fixup of the bytes into ones complement values. The latter method
     *       would doubtless be the fastest approach but it is deprecated in the
     *       {@link String} api.
     */
    public KeyBuilder appendASCII(String s) {
        
        int len = s.length();
        
        ensureFree(len);
        
        for(int i=0; i<len; i++) {
            
            char ch = s.charAt(i);
            
            append((byte)(ch & 0xff));
            
        }
        
        return this;
        
    }
    
    /**
     * Encodes a character as a unicode sort key by first converting it to a
     * unicode string of length N and then encoding it using
     * {@link #append(String)}.
     */
    public KeyBuilder append(char[] v) {

        return append(new String(v));
        
    }
    
    /**
     * Appends an array of bytes - the bytes are treated as
     * <code>unsigned</code> values.
     * 
     * @param a
     *            The array of bytes.
     */
    final public KeyBuilder append(byte[] a) {
        
        return append(0,a.length,a);
        
    }
    
    /**
     * Appends a double precision floating point value by first converting it
     * into a signed long integer using {@link Double#doubleToLongBits(double)},
     * converting that values into a twos-complement number and then appending
     * the bytes in big-endian order into the key buffer.
     * 
     * Note: this converts -0d and +0d to the same key.
     * 
     * @param d
     *            The double-precision floating point value.
     */
    final public KeyBuilder append(double d) {
        
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

    /**
     * Appends a single precision floating point value by first converting it
     * into a signed integer using {@link Float#floatToIntBits(float)}
     * converting that values into a twos-complement number and then appending
     * the bytes in big-endian order into the key buffer.
     *
     * Note: this converts -0f and +0f to the same key.
     * 
     * @param f
     *            The single-precision floating point value.
     */
    final public KeyBuilder append(float f) {

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

    /**
     * Appends a signed long integer to the key by first converting it to a
     * lexiographic ordering as an unsigned long integer and then appending it
     * into the buffer as 8 bytes using a big-endian order.
     */
    final public KeyBuilder append(long v) {

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
    
    /**
     * Appends a signed integer to the key by first converting it to a
     * lexiographic ordering as an unsigned integer and then appending it into
     * the buffer as 4 bytes using a big-endian order.
     */
    final public KeyBuilder append(int v) {

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

    /**
     * Appends a signed short integer to the key by first converting it to a
     * two-complete representation supporting unsigned byte[] comparison and
     * then appending it into the buffer as 2 bytes using a big-endian order.
     */
    final public KeyBuilder append(short v) {

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

    /**
     * Encodes a character as a unicode sort key by first converting it to a
     * unicode string of length 1 and then encoding it using
     * {@link #append(String)}.
     * 
     * @param v
     *            The character.
     */
    public KeyBuilder append(char v) {

        return append("" + v);
                    
    }
    
    /**
     * Converts the signed byte to an unsigned byte and appends it to the key.
     * 
     * @param v
     *            The signed byte.
     */
    final public KeyBuilder append(final byte v) {

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
     * Append an unsigned zero byte to the key.
     * 
     * @return this.
     */
    final public KeyBuilder appendNul() {
        
        // performance tweak.
        if (len + 1 > buf.length) ensureCapacity(len+1);
//        ensureFree(1);
        
        buf[len++] = (byte) 0;
        
        return this;
        
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
    
}
