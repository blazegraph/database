package com.bigdata.objndx;

import java.util.Locale;

import com.bigdata.objndx.ndx.NoSuccessorException;
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
 * @todo update successor methods and tests for GOM index helper classes.
 * 
 * @todo drop the package (com.bigdata.objndx.ndx) containing type-specific
 *       successor and comparator methods since they are no longer needed with
 *       the advent of unsigned byte[] keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

    public RuleBasedCollator getCollator() {
        
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
    public KeyBuilder append(int off, int len, byte[] a) {
        
        ensureFree(len);
        
        System.arraycopy(a, off, buf, this.len, len);
        
        this.len += len;
        
        assert this.len <= buf.length;
        
        return this;
        
    }

    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer. The
     * {@link #buf buffer} may be grown by this operation but it will not be
     * truncated.
     * 
     * @param len
     *            The minimum #of free bytes.
     */
    public void ensureFree(int len) {
        
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
    public void ensureCapacity(int capacity) {
        
        if(capacity<0) throw new IllegalArgumentException();
        
        int overflow = capacity - buf.length;
        
        if(overflow>0) {
        
            // extend to the target capacity.
            byte[] tmp = new byte[capacity];
            
            // copy only the defined bytes.
            System.arraycopy(buf, 0, tmp, 0, this.len);
            
            buf = tmp;
            
        }

    }

    /**
     * Return the encoded key. Comparison of keys returned by this method MUST
     * treat the array as an array of <em>unsigned bytes</em>.
     * 
     * @return A new array containing the key.
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     */
    public byte[] getKey() {
        
        byte[] tmp = new byte[this.len];
        
        System.arraycopy(buf, 0, tmp, 0, this.len);
        
        return tmp;
        
    }
    
    /**
     * Reset the key length to zero before building another key.
     * 
     * @return This {@link KeyBuilder}.
     */
    public KeyBuilder reset() {
        
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
     */
    public KeyBuilder appendASCII(String s) {
        
        int len = s.length();
        
        ensureFree(len);
        
        for(int i=0; i<s.length(); i++) {
            
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
    public KeyBuilder append(byte[] a) {
        
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
    public KeyBuilder append(double d) {
        
        ensureFree(8);

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
    public KeyBuilder append(float f) {

        ensureFree(4);

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
    public KeyBuilder append(long v) {

        ensureFree(8);
        
        // lexiographic ordering as unsigned long integer.

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = 0x8000000000000000L + v;
            
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
     * Appends a signed integer to the key by first converting it to a
     * lexiographic ordering as an unsigned integer and then appending it into
     * the buffer as 4 bytes using a big-endian order.
     */
    public KeyBuilder append(int v) {

        ensureFree(4);
        
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
    public KeyBuilder append(short v) {

        ensureFree(2);
        
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
    public KeyBuilder append(final byte v) {

        ensureFree(1);

        // lexiographic ordering as unsigned byte.
        
        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = 0x80 + i;
            
        }
        
        buf[len++] = (byte)(i & 0xff);
        
        return this;
        
    }

    /**
     * Append an unsigned zero byte to the key.
     * 
     * @return this.
     */
    public KeyBuilder appendNul() {
        
        ensureFree(1);
        
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
    
    /**
     * Computes the successor of a <code>byte</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */

    public static byte successor( byte n ) throws NoSuccessorException {

        if (Byte.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (byte) (n + 1);

        }

    }
    
    /**
     * Computes the successor of a <code>char</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */

    public static char successor( char n ) throws NoSuccessorException
    {
        
        if (Character.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (char) (n + 1);

        }
        
    }
    
    /**
     * Computes the successor of a <code>short</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */

    public static short successor( short n ) throws NoSuccessorException
    {
        
        if (Short.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (short) (n + 1);

        }
        
    }
    
    /**
     * Computes the successor of an <code>int</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */

    public static int successor( int n ) throws NoSuccessorException
    {
    
        if (Integer.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return n + 1;

        }

    }

    /**
     * Computes the successor of a <code>long</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */

    public static long successor( long n ) throws NoSuccessorException
    {

        if (Long.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return n + 1L;

        }
   
    }
    
    /**
     * <p>
     * Computes the successor of a <code>float</code> value.
     * </p>
     * <p>
     * The IEEE floating point standard provides a means for computing the next
     * larger or smaller floating point value using a bit manipulation trick.
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson. The Java
     * {@link Float} and {@link Double} clases provide the static methods
     * required to convert a float or double into its IEEE 754 floating point
     * bit layout, which can be treated as an int (for floats) or a long (for
     * doubles). By testing for the sign, you can just add (or subtract) one (1)
     * to get the bit pattern of the successor (see the above referenced
     * article). Special exceptions must be made for NaNs, negative infinity and
     * positive infinity.
     * </p>
     * 
     * @param f
     *            The float value.
     * 
     * @return The next value in the value space for <code>float</code>.
     * 
     * @exception NoSuccessorException
     *                if there is no next value in the value space.
     */

    static public float successor( float f )
        throws NoSuccessorException
    {
        
        if (f == Float.MAX_VALUE) {

            return Float.POSITIVE_INFINITY;

        }

        if (Float.isNaN(f)) {

            throw new NoSuccessorException("NaN");

        }

        if (Float.isInfinite(f)) {

            if (f > 0) {

                throw new NoSuccessorException("Positive Infinity");

            } else {

                /* no successor for negative infinity (could be the largest
                 * negative value).
                 */

                throw new NoSuccessorException("Negative Infinity");

            }

        }

        int bits = Float.floatToIntBits(f);

        if (bits == 0x80000000) {
            
            /*
             * the successor of -0.0f is +0.0f
             * 
             * @todo Java defines the successor of floating point zeros as the
             * first non-zero value so maybe we should change this.
             */
            return +0.0f;
            
        }

        if (f >= +0.0f) {

            bits += 1;

        } else {

            bits -= 1;

        }

        float nxt = Float.intBitsToFloat(bits);
        
        return nxt;
        
    }
    
    /**
     * <p>
     * Computes the successor of a <code>double</code> value.
     * </p>
     * <p>
     * The IEEE floating point standard provides a means for computing the next
     * larger or smaller floating point value using a bit manipulation trick.
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson. The Java
     * {@link Float} and {@link Double} clases provide the static methods
     * required to convert a float or double into its IEEE 754 floating point
     * bit layout, which can be treated as an int (for floats) or a long (for
     * doubles). By testing for the sign, you can just add (or subtract) one (1)
     * to get the bit pattern of the successor (see the above referenced
     * article). Special exceptions must be made for NaNs, negative infinity and
     * positive infinity.
     * </p>
     * 
     * @param d The double value.
     * 
     * @return The next value in the value space for <code>double</code>.
     * 
     * @exception NoSuccessorException
     *                if there is no next value in the value space.
     */

    public static double successor( double d ) 
        throws NoSuccessorException
    {
        
        if (d == Double.MAX_VALUE) {

            return Double.POSITIVE_INFINITY;

        }

        if (Double.isNaN(d)) {

            throw new NoSuccessorException("Nan");

        }

        if (Double.isInfinite(d)) {

            if (d > 0) {

                throw new NoSuccessorException("Positive Infinity");

            } else {

                // The successor of negative infinity.

                return Double.MIN_VALUE;

            }

        }

        long bits = Double.doubleToLongBits(d);

        if (bits == 0x8000000000000000L) {
            
            /* the successor of -0.0d is +0.0d
             * 
             * @todo Java defines the successor of floating point zeros as the
             * first non-zero value so maybe we should change this.
             */
            return +0.0d;
            
        }

//        if (f >= +0.0f) {

        if (d >= +0.0) {

            bits += 1;

        } else {

            bits -= 1;

        }

        double nxt = Double.longBitsToDouble(bits);

        return nxt;

    }
    
    /**
     * The successor of a string value is formed by appending a <code>nul</code>.
     * The successor of a <code>null</code> string reference is an empty
     * string. The successor of a string value is defined unless the string is
     * too long.
     * 
     * @param s The string reference or <code>null</code>.
     * 
     * @return The successor and never <code>null</code>
     */

    public static String successor(String s) {

        if (s == null)
            return "\0";

        return s + "\0";

    }

}
