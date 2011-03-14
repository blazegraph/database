/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 30, 2007
 */

package com.bigdata.btree.keys;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Collator;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import org.apache.log4j.Logger;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;

/**
 * A class that may be used to form multi-component keys but which does not
 * support Unicode. An instance of this class is quite light-weight and SHOULD
 * be used when Unicode support is not required.
 * <p>
 * Note: Avoid any dependencies within this class on the ICU libraries so that
 * the code may run without those libraries when they are not required.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SuccessorUtil Compute the successor of a value before encoding it as a
 *      component of a key.
 * 
 * @see BytesUtil#successor(byte[]) Compute the successor of an encoded key.
 * 
 * @todo introduce a mark and restore feature for generating multiple keys that
 *       share some leading prefix. in general, this is as easy as resetting the
 *       len field to the mark. keys with multiple components could benefit from
 *       allowing multiple marks (the sparse row store is the main use case).
 * 
 * @todo Integrate support for ICU versioning into the client and perhaps into
 *       the index metadata so clients can discover which version and
 *       configuration properties to use when generating keys for an index.
 */
public class KeyBuilder implements IKeyBuilder {

    private static final transient Logger log = Logger
            .getLogger(KeyBuilder.class);

    /**
     * Text of the exception thrown when the ICU library is required but is not
     * available.
     */
    final private static transient String ERR_ICU_NOT_AVAILABLE = "The ICU library is not available.";

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
     * The object used to generate sort keys from Unicode strings (optional).
     * <p>
     * Note: When <code>null</code> the IKeyBuilder does NOT support Unicode
     * and the optional Unicode methods will all throw an
     * {@link UnsupportedOperationException}.
     */
    protected final UnicodeSortKeyGenerator sortKeyGenerator;
    
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
     *            keys. When zero (0) the {@link #DEFAULT_INITIAL_CAPACITY} will
     *            be used.
     */
    public KeyBuilder(int initialCapacity) {
        
        this(0, createBuffer(initialCapacity));
        
    }
    
    /**
     * Create a buffer of the specified initial capacity.
     * 
     * @param initialCapacity
     *            The initial size of the buffer.
     * 
     * @return The byte[] buffer.
     * 
     * @exception IllegalArgumentException
     *                if the initial capacity is negative.
     */
    protected static byte[] createBuffer(int initialCapacity) {

        if(initialCapacity<0) {
            
            throw new IllegalArgumentException("initialCapacity must be non-negative");
            
        }
        
        final int capacity = initialCapacity == 0 ? DEFAULT_INITIAL_CAPACITY
                : initialCapacity;
        
        return new byte[capacity];
        
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
    /*public*/ KeyBuilder(final int len, final byte[] buf) {

        this( null /* no unicode support*/, len, buf );
        
    }    
    
    /**
     * Creates a key builder using an existing buffer with some data (designated
     * constructor).
     * 
     * @param sortKeyGenerator
     *            The object used to generate sort keys from Unicode strings
     *            (when <code>null</code> Unicode collation support is
     *            disabled).
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     */
    protected KeyBuilder(final UnicodeSortKeyGenerator sortKeyGenerator,
            final int len, final byte[] buf) {

        if (len < 0)
            throw new IllegalArgumentException("len");

        if (buf == null)
            throw new IllegalArgumentException("buf");

        if (len > buf.length)
            throw new IllegalArgumentException("len>buf.length");

        this.len = len;

        this.buf = buf;

        this.sortKeyGenerator = sortKeyGenerator; // MAY be null.
        
    }
    
    final public int getLength() {
        
        return len;
        
    }

    final public byte[] getBuffer() {
        
        return buf;
        
    }
    
    /**
     * Sets the position to any non-negative length less than the current
     * capacity of the buffer.
     */
    final public void position(final int pos) {
        
        if (len < 0 || len > buf.length) {

            throw new IndexOutOfBoundsException("pos=" + pos
                    + ", but capacity=" + buf.length);
            
        }

        len = pos;
        
    }
    
    final public KeyBuilder append(final int off, final int len, final byte[] a) {
        
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
     * This operation is equivalent to
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
    
    final public KeyBuilder reset() {
        
        len = 0;
        
        return this;
        
    }

    /*
     * Unicode operations.
     */

    final public boolean isUnicodeSupported() {

        if (sortKeyGenerator == null) return false;

        return true;
        
    }
    
    /**
     * The object responsible for generating sort keys from Unicode strings.
     * 
     * The {@link UnicodeSortKeyGenerator} -or- <code>null</code> if Unicode
     * is not supported by this {@link IKeyBuilder} instance.
     */
    final public UnicodeSortKeyGenerator getSortKeyGenerator() {
        
        return sortKeyGenerator;
        
    }

    final public KeyBuilder append(final String s) {
        
        if (sortKeyGenerator == null) {
         
            // Force ASCII semantics on the Unicode text.
            
            appendASCII( s );
            
        } else {

            sortKeyGenerator.appendSortKey( this, s );
            
        }
        
        return this;
        
    }

    /*
     * Note: Dropped from the API to minimize confusion.
     */
//    final public IKeyBuilder append(char[] v) {
//
//        return append(new String(v));
//        
//    }
    
    /*
     * Non-optional operations.
     */
    
    public KeyBuilder appendASCII(final String s) {
        
        int tmpLen = s.length();
        
        ensureFree(tmpLen);
        
        for(int j=0; j<tmpLen; j++) {
            
            char ch = s.charAt(j);
            
//            append((byte)(ch & 0xff));

            // lexiographic ordering as unsigned byte.
            
            int v = (byte)ch;
            
            if (v < 0) {

                v = v - 0x80;

            } else {
                
                v = v + 0x80;
                
            }
            
            buf[this.len++] = (byte)(v & 0xff);

        }
        
        return this;
        
    }

    /**
     * Decodes an ASCII string from a key.
     * 
     * @param key
     *            The key.
     * @param off
     *            The offset of the start of the string.
     * @param len
     *            The #of bytes to decode (one byte per character).
     * 
     * @return The ASCII characters decoded from the key.
     * 
     * @see #appendASCII(String)
     */
    public static String decodeASCII(final byte[] key, final int off,
            final int len) {

        final byte[] b = new byte[len];

        System.arraycopy(key, off, b, 0, len);

        for (int i = 0; i < len; i++) {

            b[i] = decodeByte(b[i]);

        }

        try {
            
            return new String(b, "US-ASCII");

        } catch (UnsupportedEncodingException e) {

            throw new RuntimeException(e);
            
        }
        
    }
    
    /**
     * The default pad character (a space).
     * <p>
     * Note: Any character may be chosen as the pad character as long as it has
     * a one byte representation. In practice this means you can choose 0x20 (a
     * space) or 0x00 (a nul). This limit arises in
     * {@link #appendText(String, boolean, boolean)} which assumes that it can
     * write a pad character (or its successor) in one byte. 0xff will NOT work
     * since its successor is not defined within an bit string of length 8.
     * 
     * @todo make this a configuration option? if so then verify that the choice
     *       (and its successor) fit in 8 bits.
     */
    final public byte pad = 0x20;

    /**
     * Normalize the text by truncating to no more than {@link #maxlen}
     * characters and then stripping off trailing {@link #pad} characters.
     */
    /*public*/ String normalizeText(String text) {

        if (text.length() > maxlen) {

            /*
             * Truncate the encoded text field to maxlen characters to prevent
             * overflow. While the number of bytes generated by the resulting
             * encoding is variable, the order semantics will respect only the
             * 1st maxlen characters _regardless_ of how many bytes are required
             * to encode those characters.
             */

            text = text.substring(0, maxlen);

        }

        /*
         * Strip trailing pad "characters" from the text. This helps to ensure a
         * canonical representation. If we did not do this then text with
         * trailing pad characters would be encoded differently from text
         * without trailing pad characters - even though they are supposed to be
         * the "same".
         */
        {
            int npadded = 0;
            for (int i = text.length() - 1; i >= 0; i--) {
                if (text.charAt(i) == pad) {
                    npadded++;
                    continue;
                }
                break;
            }
            if (npadded > 0) {
                int begin = 0;
                int end = text.length() - npadded;
                text = text.substring(begin,end);
            }
        }

        return text;

    }
    
    public KeyBuilder appendText(String text, final boolean unicode,
            final boolean successor) {

        // current length of the encoded key.
        final int pos = this.len;

        /*
         * Normalize the text by truncating to no more than [maxlen] characters
         * and then stripping off trailing pad characters.
         */
        
        text = normalizeText( text );
        
        /*
         * Encode the text as ASCII or Unicode as appropriate.
         */
        
        if(unicode) {

            append(text);
            
        } else {
            
            appendASCII(text);
            
        }

        // #of bytes in the encoded text field.
        final int encoded_len = this.len - pos;

        // #of characters (not bytes) in the text.
        final int textlen;
        
        if(successor) {

            if (encoded_len == 0) {

                /*
                 * Note: The successor of an empty string is not defined since
                 * it maps to an empty byte[] (an empty value space). However an
                 * empty string is semantically equivalent to all pad characters
                 * so we use the successor of a string containing a single pad
                 * character, which is equivalent to a string containing a
                 * single byte whose value is pad+1.
                 */
                
                append((byte)(pad+1));
                
                textlen = 1;
                
            } else {

                /*
                 * Note: This generates the successor of the encoded text by
                 * treading the encoded byte[] as a fixed length bit string and
                 * finding the successor of that bit-string. The bytes in the
                 * buffer are modified as a side-effect. A runtime exception is
                 * thrown if there is no successor to the bit string (this is
                 * not a plausible scenario for either ASCII or Unicode text as
                 * the encoding would have to be all 0xff bytes for the
                 * successor to not be defined).
                 */

                SuccessorUtil.successor(buf, pos, encoded_len);
                
                textlen = text.length();
                
            }
            
        } else {
            
            textlen = text.length();
            
        }

        if (textlen < maxlen) {

            /*
             * append a single pad byte.
             * 
             * Note: Changed this to append the pad character (a space) as if it
             * was already an unsigned value (0x20) rather than its signed value
             * (0x160). This causes "bro" to sort before "brown", which is
             * desired. (bbt, 10/1/08)
             */
            appendUnsigned(pad);
            
            // append the run length for the trailing pad characters.
            final int runLength = maxlen - textlen;

            append((short) runLength);
            
        }

        return this;
        
    }
    
    final public KeyBuilder append(final byte[] a) {
        
        return append(0, a.length, a);
        
    }
    
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

    static public double decodeDouble(byte[] key,int off) {
        
        long v = decodeLong(key, off);
        
        // convert to twos-complement long.
        if (v < 0) {
            
            v = 0x8000000000000000L - v;

        }
        
        return Double.longBitsToDouble(v);
        
    }

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

    static public float decodeFloat(final byte[] key, final int off) {
        
        int v = decodeInt(key, off);
        
        // convert to twos complement int.
        if (v < 0) {

            v = 0x80000000 - v;

        }
        
        return Float.intBitsToFloat(v);
        
    }
    
    final public KeyBuilder append(final UUID uuid) {

        if (len + 16 > buf.length) ensureCapacity(len+16);

        append( uuid.getMostSignificantBits() );
        
        append( uuid.getLeastSignificantBits() );
        
        return this;
        
    }
    
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
     * Return the value that will impose the lexiographic ordering as an
     * unsigned long integer.
     * 
     * @param v
     *            The signed long integer.
     * 
     * @return The value that will impose the lexiographic ordering as an
     *         unsigned long integer.
     * 
     * @todo This is unused and untested.
     */
    static final /*public*/ long encode(long v) {

        if (v < 0) {
            
            v = v - 0x8000000000000000L;

        } else {
            
            v = v + 0x8000000000000000L;
            
        }

        return v;
        
    }

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
    
    /*
     * Note: this method has been dropped from the API to reduce the
     * possibility of confusion.  If you want Unicode semantics then use
     * append(String).  If you want ASCII semantics then use appendASCII().
     * If you want signed integer semantics then use append(short).
     */
//    final public IKeyBuilder append(char v) {
//
//        /*
//         * Note: converting to String first produces significantly larger keys
//         * which, more important, violate the sort order expectations for
//         * characters. For example, successor in the value space of 'z' is '{'.
//         * However, the sort key generated from the String representation of the
//         * character '{' is NOT ordered after the sort key generated from the
//         * String representation of the character 'z'.  Unicode wierdness.
//         */
//
//        return append((short) v);
//        
//    }

    final public KeyBuilder appendUnsigned(final byte v) {

        // performance tweak
        if (len + 1 > buf.length) ensureCapacity(len+1);
        // ensureFree(1);

        buf[len++] = (byte)v;
        
        return this;
        
    }

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

//    /**
//     * Return the value that will impose the lexiographic ordering as an
//     * unsigned byte.
//     * 
//     * @param v
//     *            The signed byte.
//     * 
//     * @return The value that will impose the lexiographic ordering as an
//     *         unsigned byte.
//     */
//    final static /*public*/ byte encode(byte v) {
//
//        int i = v;
//        
//        if (i < 0) {
//
//            i = i - 0x80;
//
//        } else {
//            
//            i = i + 0x80;
//            
//        }
//        
//        byte tmp = (byte)(i & 0xff);
//        
//        return tmp;
//        
//    }
    
    final public KeyBuilder appendNul() {

//        return append(0);
        
        // performance tweak.
        if (len + 1 > buf.length) ensureCapacity(len+1);
//        ensureFree(1);
        
        buf[len++] = (byte) 0;
        
        return this;
        
    }

    public KeyBuilder append(final BigInteger i) {

        // Note: BigInteger.ZERO is represented as byte[]{0}.
        final byte[] b = i.toByteArray();
        
        final int runLength = i.signum() == -1 ? -b.length : b.length;
        
        ensureFree(b.length + 2);
        
        append((short) runLength);
        
        append(b);

        return this;
        
    }

    /**
     * Return the #of bytes in the unsigned byte[] representation of the
     * {@link BigInteger} value.
     * 
     * @param value
     *            The {@link BigInteger} value.
     *            
     * @return The byte length of its unsigned byte[] representation.
     */
    static public int byteLength(final BigInteger value) {
        
        return 2/* runLength */+ (value.bitLength() / 8 + 1)/* data */;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Precision is NOT preserved by this encoding.
     * <h2>Implementation details</h2>
     * The encoding to a BigDecimal requires the expression of scale and length
     * {@link BigDecimal#scale()} indicates the precision of the number, where
     * '3' is three decimal places and '-3' rounded to '000'
     * {@link BigDecimal#precision()} is the number of unscaled digits therefore
     * <code>precision - scale</code> is an expression of the exponent of the
     * normalized number. This means that the exponent could be zero or negative
     * so the sign of the number cannot be indicated by adding to the exponent.
     * Instead an explicit sign byte,'0' or '1' is used. The actual
     * {@link BigDecimal} serialization uses the {@link String} conversions
     * supported by {@link BigDecimal}, less the '-' sign character if
     * applicable. The length of this data is terminated by a trailing byte. The
     * value of that byte depends on the sign of the original {@link BigDecimal}
     * and is used to impose the correct sort order on negative
     * {@link BigDecimal} values which differ only in the digits in the decimal
     * portion.
     *<p>
     * The variable encoding of BigNumbers requires this String representation
     * and negative representations are further encoded using
     * {@link #flipDigits(String)} for the equivalent of 2s compliment negative
     * representation.
     * 
     * There are two cases where scale and trailing zeros interact.  The
     * case of "0.000" is represented as precision of 1 and scale of 3, 
     * indicating the "0" is shifted down 3 decimal places.  While "5.000"
     * is represented as precision of 4 and scale of 3.  The special case
     * of zero is allowed because shifting zero to the right leaves a new
     * zero on the left, so a zero value must be checked for explicitly, while
     * if we want to compare "5", "5.00" and "5.0000" as equal we must
     * remove and compensate for trailing zeros. 
     * 
     * @see #decodeBigDecimal(int, byte[])
     */
    public KeyBuilder append(final BigDecimal d) {
    	final int sign = d.signum(); 
    	
    	if (sign == 0) {
    		append((byte) 0);
    		
    		return this;
    	}
    	
    	BigDecimal nd = d.stripTrailingZeros();
    	
    	String unscaledStr = nd.unscaledValue().toString();
    	
    	final int precision = nd.precision();
    	final int scale =  nd.scale();
    	
    	int exponent = precision - scale;
    	if (sign == -1) {
    		exponent = -exponent;
    	}
    	
    	append((byte) sign);
    	append(exponent);   	
    	
    	// Note: coded as digits 
    	
    	if (sign == -1) {
    		unscaledStr = flipDigits(unscaledStr);
    	}
    	appendASCII(unscaledStr); // the unscaled BigInteger representation
    	// Note: uses unsigned 255 if negative and unsigned 0 if positive. 
        append(sign == -1 ? (byte) Byte.MAX_VALUE: (byte) 0);
    	
        return this;
    }

    /**
     * Return the #of bytes in the unsigned byte[] representation of the
     * {@link BigDecimal} value.
     * 
     * @param value
     *            The {@link BigDecimal} value.
     *            
     * @return The byte length of its unsigned byte[] representation.
     */
    static public int byteLength(final BigDecimal value) {
        
        final int byteLength;
        
    	if (value.signum() == 0) {
    		byteLength = 1;
    	} else {
    		final BigDecimal nbd = value.stripTrailingZeros();
    		
    		final int dataLen = nbd.unscaledValue().toString().length();
        
    		byteLength = 
            + 1 /* sign */ 
            + 4 /* exponent */
            + dataLen /* data */
            + 1 /* termination byte */
            ;
    	} 
    	
        return byteLength;
       
    }

    public byte[] getSortKey(final Object val) {
        
        reset();
        
        append( val );
        
        return getKey();
        
    }
    
    public KeyBuilder append(final Object val) {
        
        if (val == null) {

            throw new IllegalArgumentException();
            
        }

        if(val instanceof byte[]) {

            append((byte[])val);

        } else if (val instanceof Byte) {
            
            append(((Byte) val).byteValue());

        } else if (val instanceof Character) {

            // append(((Character) val).charValue());

            throw new UnsupportedOperationException(
                    "Character is not supported.  Use Short or String depending on the semantics that you want.");

        } else if (val instanceof Short) {

            append(((Short) val).shortValue());

        } else if (val instanceof Integer) {

            append(((Integer) val).intValue());

        } else if (val instanceof Long) {

            append(((Long) val).longValue());

        } else if (val instanceof BigInteger) {

            append((BigInteger) val);

        } else if (val instanceof BigDecimal) {

            append((BigDecimal) val);

        } else if (val instanceof Float) {

            append(((Float) val).floatValue());

        } else if (val instanceof Double) {

            append(((Double) val).doubleValue());

        } else if (val instanceof String) {

            append((String) val);

        } else if (val instanceof UUID) {

            append(((UUID) val));

        } else {

            throw new UnsupportedOperationException("Can not encode key: "
                    + val.getClass());

        }

        return this;
        
    }
    
    /**
     * Converts a signed byte into an unsigned byte.
     * 
     * @param v
     *            The signed byte.
     *            
     * @return The corresponding unsigned value.
     */
    static public byte encodeByte(final int v) {
        
        int i = v;
        
        if (i < 0) {

            i = i - 0x80;

        } else {
            
            i = i + 0x80;
            
        }
        
        return (byte)(i & 0xff);

    }
    
    /**
     * Converts an unsigned byte into a signed byte.
     * 
     * @param v
     *            The unsigned byte.
     *            
     * @return The corresponding signed value.
     */
    static public byte decodeByte(final int v) {

        int i = v;
        
        if (i < 0) {

            i = i + 0x80;

        } else {
            
            i = i - 0x80;
            
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
    public static long d2l(final double d) {

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
    public static int f2i(final float f) {

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
    static public long decodeLong(final byte[] buf, int off) {

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
     * Decode a {@link UUID} as encoded by {@link #append(UUID)}.
     * 
     * @param buf
     *            The buffer containing the encoded key.
     * @param off
     *            The offset at which to decode the key.
     *            
     * @return The decoded {@link UUID}.
     */
    static public UUID decodeUUID(final byte[] buf, int off) {

        final long msb = decodeLong(buf, off);

        off += 8;

        final long lsb = decodeLong(buf, off);

        return new UUID(msb, lsb);

    }

    /**
     * Decodes a signed int value as encoded by {@link #append(int)}.
     * 
     * @param buf
     *            The buffer containing the encoded key.
     * @param off
     *            The offset at which to decode the key.
     *            
     * @return The signed int value.
     */
    static public int decodeInt(final byte[] buf, int off) {

        int v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;

        if (v < 0) {
            
            v = v + 0x80000000;

        } else {
            
            v = v - 0x80000000;
            
        }

        return v;
        
    }

    /**
     * Decodes a signed short value as encoded by {@link #append(short)}.
     * 
     * @param buf
     *            The buffer containing the encoded key.
     * @param off
     *            The offset at which to decode the key.
     *            
     * @return The signed short value.
     */
    static public short decodeShort(final byte[] buf, int off) {

        int v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;

        if (v < 0) {
            
            v = v + 0x8000;

        } else {
            
            v = v - 0x8000;
            
        }

        return (short) v;
        
    }

    /**
     * Convert an unsigned byte[] into a {@link BigInteger}.
     * 
     * @param key
     *            The bytes.
     *            
     * @return The big integer value.
     */
    static public BigInteger decodeBigInteger(final int offset, final byte[] key) {

        return new BigInteger(decodeBigInteger2(offset, key));
        
    }

    /**
     * Decodes a {@link BigInteger} key, returning a byte[] which may be used to
     * construct a {@link BigInteger} having the decoded value. The number of
     * bytes consumed by the key component is <code>2 + runLength</2>. The
     * <code>2</code> is a fixed length field coding the signum of the value and
     * its runLength. The length of the returned array is the runLength of the
     * variable length portion of the value. This method may be used to scan
     * through a key containing {@link BigInteger} components.
     * 
     * @param offset
     *            The offset of the start of the {@link BigInteger} in the key.
     * @param key
     *            The key.
     * @return The byte[] to be passed to {@link BigInteger#BigInteger(byte[])}.
     */
    static public byte[] decodeBigInteger2(final int offset, final byte[] key) {

        final int tmp = KeyBuilder.decodeShort(key, offset);

        /*
         * Note: The signum is thrown away when we decode the runLength field.
         * Signum is actually in the key twice: once in the runLength to put the
         * BigInteger values into total order and once in the representation of
         * the BigInteger as a byte[].
         */
        final int runLength = tmp < 0 ? -tmp : tmp;
        
        final byte[] b = new byte[runLength];
        
        System.arraycopy(key/* src */, offset + 2/* srcpos */, b/* dst */,
                0/* destPos */, runLength);
        
        return b;
        
    }

    /**
     * Decodes a {@link BigDecimal} key, returning a byte[] which may be used to
     * construct a {@link BigDecimal} having the decoded value.
     * 
     * The number of bytes consumed by the key component is
     * <code>2 + runLength</2>. The
     * <code>2</code> is a fixed length field coding the signum of the value and
     * its runLength. The length of the returned array is the runLength of the
     * variable length portion of the value.
     * 
     * This method may be used to scan through a key containing
     * {@link BigDecimal} components.
     * 
     * @param offset
     *            The offset of the start of the {@link BigDecimal} in the key.
     * @param key
     *            The key.
     * @return The byte[] to be passed to {@link BigDecimal#BigInteger(byte[])}.
     * 
     * @todo update javadoc
     * 
     * FIXME We need a version which has all the metadata to support scanning
     *       through a key as well as one that does a simple decode.
     */
    static public BigDecimal decodeBigDecimal(final int offset, final byte[] key) {
    	int curs = offset;
        final byte sign = key[curs++];
        
        if (sign == decodeZero) {
        	return new BigDecimal(0);
        }
        
        int exponent = decodeInt(key, curs);
        final boolean neg = sign == negSign;
        if (neg) {
        	exponent = -exponent;
        }
        curs += 4;
        int len = 0;
        for (int i = curs; key[i] != (neg ? eos2 : eos); i++) len++;
        String unscaledStr = decodeASCII(key, curs, len);
        if (neg) {
        	unscaledStr = flipDigits(unscaledStr);
        }
        
        final BigInteger unscaled = new BigInteger(unscaledStr);
        
        final int precision = len;
        final int scale  = precision - exponent - (neg ? 1 : 0);
        
        final BigDecimal ret = new BigDecimal(unscaled, scale);

        return ret; // relative scale adjustment
    }
    
    private static final byte decodeZero = decodeByte(0);
    private static final byte eos = decodeZero;
    private static final byte eos2 = decodeByte(Byte.MAX_VALUE);
    private static final byte negSign = decodeByte(-1);

    private static final char[] flipMap = {'0', '1', '2', '3', '4',
    	'5', '6', '7', '8', '9'
    };

    /**
     * Flip numbers such that <code>0/9,1/8,2/7,3/6,4/5</code> - this is the
     * equivalent of a two-complement representation for the base 10 character
     * digits.
     */
    static private String flipDigits(final String str) {
    	final char[] chrs = str.toCharArray();
    	for (int i = 0; i < chrs.length; i++) {
    		final int flip = '9' - chrs[i];
    		if (flip >= 0 && flip < 10) {
    			chrs[i] = flipMap[flip];
    		}
    	}
    	
    	return new String(chrs);
    }
    
    /**
     * Create an instance for ASCII keys.
     * 
     * @return The new instance.
     */
    public static IKeyBuilder newInstance() {

        return newInstance(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * Create an instance for ASCII keys with the specified initial capacity.
     * 
     * @param initialCapacity
     *            The initial capacity.
     * 
     * @return The new instance.
     */
    public static IKeyBuilder newInstance(final int initialCapacity) {
     
        return newInstance(initialCapacity, CollatorEnum.ASCII, null/* locale */,
                null/* strength */, null/* decomposition mode */);
        
    }

    /**
     * Configuration options for {@link DefaultKeyBuilderFactory} and the
     * {@link KeyBuilder} factory methods. <strong>The use of
     * {@link DefaultKeyBuilderFactory} is highly recommended as it will cause
     * the configuration to be serialized. In combination with the use of an
     * {@link ITupleSerializer}, this means that Unicode keys for an index will
     * be interpreted in the same manner on any machine where {@link ITuple}s
     * for that index are (de-)materialized. </strong>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
       
        /**
         * Optional property specifies the library that will be used to generate
         * sort keys from Unicode data. The ICU library is the default.
         * You may explicitly specify
         * the library choice using one of the {@link CollatorEnum} values. The
         * {@link CollatorEnum#ASCII} value may be used to disable Unicode
         * support entirely, treating the characters as if they were ASCII. If
         * your data is not actually Unicode then this offers a substantial
         * performance benefit.
         * 
         * @see CollatorEnum
         */
        public String COLLATOR = KeyBuilder.class.getName()+".collator";

        /**
         * Optional string -or- integer property whose value is the strength to
         * be set on the collator. When specified, the value must be either one
         * of the type-safe {@link StrengthEnum}s -or- one of those supported
         * by the ICU or JDK library, as appropriate. The following values are
         * shared by both libraries:
         * <dl>
         * <dt>0</dt>
         * <dd>{@link Collator#PRIMARY}</dd>
         * <dt>1</dt>
         * <dd>{@link Collator#SECONDARY}</dd>
         * <dt>2</dt>
         * <dd>{@link Collator#TERTIARY}</dd>
         * </dl>
         * The ICU library also supports
         * <dl>
         * <dt>3</dt>
         * <dd>Quaternary</dd>
         * </dl>
         * While both libraries define <strong>IDENTICAL</strong> they use
         * different values for this strength, hence the use of the type safe
         * enums is recommended.
         * 
         * @see StrengthEnum
         */
        public String STRENGTH = KeyBuilder.class.getName()+".collator.strength";

        /**
         * Optional string property whose value is one of the type safe
         * {@link DecompositionEnum}s. The default decomposition mode will be
         * overridden on the collator one is explicitly specified using this
         * property.
         * 
         * @see DecompositionEnum
         */
        public String DECOMPOSITION = KeyBuilder.class.getName()+".collator.decomposition";

        /**
         * The pre-defined System property {@value #USER_LANGUAGE} determines
         * the <em>language</em> for the default {@link Locale}.
         * 
         * @see Locale#setDefault(Locale)
         * 
         * @see <a
         *      href="http://java.sun.com/developer/technicalArticles/J2SE/locale/">http://java.sun.com/developer/technicalArticles/J2SE/locale/</a>
         */
        public String USER_LANGUAGE = "user.language";
        
        /**
         * The pre-defined System property {@value #USER_COUNTRY} determines the
         * <em>country</em> for the default {@link Locale}.
         * 
         * @see <a
         *      href="http://java.sun.com/developer/technicalArticles/J2SE/locale/">http://java.sun.com/developer/technicalArticles/J2SE/locale/</a>
         */
        public String USER_COUNTRY = "user.country";
        
        /**
         * The pre-defined System property {@value #USER_VARIANT} determines the
         * <em>variant</em> for the default {@link Locale}.
         * 
         * @see <a
         *      href="http://java.sun.com/developer/technicalArticles/J2SE/locale/">http://java.sun.com/developer/technicalArticles/J2SE/locale/</a>
         */
        public String USER_VARIANT = "user.variant";
        
    }

    /**
     * Create a factory for {@link IKeyBuilder} instances configured using the
     * system properties. The factory will support Unicode unless
     * {@link CollatorEnum#ASCII} is explicitly specified for the
     * {@link Options#COLLATOR} property.
     * 
     * @param properties
     *            The properties to be used (optional). When <code>null</code>
     *            the {@link System#getProperties() System properties} are used.
     * 
     * @see Options
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath. See
     *             {@link Options#COLLATOR}.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICU4JNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newUnicodeInstance() {

        return new DefaultKeyBuilderFactory(null/* properties */)
                .getKeyBuilder();
        
    }

    /**
     * Create a factory for {@link IKeyBuilder} instances configured according
     * to the specified <i>properties</i>. Any properties NOT explicitly given
     * will be defaulted from {@link System#getProperties()}. The pre-defined
     * properties {@link Options#USER_LANGUAGE}, {@link Options#USER_COUNTRY},
     * and {@link Options#USER_VARIANT} MAY be overriden. The factory will
     * support Unicode unless {@link CollatorEnum#ASCII} is explicitly specified
     * for the {@link Options#COLLATOR} property.
     * 
     * @param properties
     *            The properties to be used (optional). When <code>null</code>
     *            the {@link System#getProperties() System properties} are used.
     * 
     * @see Options
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath. See
     *             {@link Options#COLLATOR}.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICU4JNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newUnicodeInstance(Properties properties) {
     
        return new DefaultKeyBuilderFactory(properties).getKeyBuilder();

    }

    /**
     * Create a new instance that optionally supports Unicode sort keys.
     * 
     * @param capacity
     *            The initial capacity of the buffer. When zero (0) the
     *            {@link #DEFAULT_INITIAL_CAPACITY} will be used.
     * @param collatorChoice
     *            Identifies the collator that will be used to generate sort
     *            keys from Unicode values.
     * @param locale
     *            When <code>null</code> the
     *            {@link Locale#getDefault() default locale} will be used.
     * @param strength
     *            Either an {@link Integer} or a {@link StrengthEnum} specifying
     *            the strength to be set on the collator object (optional). When
     *            <code>null</code> the default strength of the collator will
     *            not be overridden.
     * @param mode
     *            The decomposition mode to be set on the collator object
     *            (optional). When <code>null</code> the default decomposition
     *            mode of the collator will not be overridden.
     * 
     * @return The new instance.
     * 
     * @throws UnsupportedOperationException
     *             <p>
     *             The ICU library was required but was not located. Make sure
     *             that the ICU JAR is on the classpath.
     *             </p>
     *             <p>
     *             Note: If you are trying to use ICUJNI then that has to be
     *             locatable as a native library. How you do this is different
     *             for Windows and Un*x.
     *             </p>
     */
    public static IKeyBuilder newInstance(int capacity,
            CollatorEnum collatorChoice, Locale locale, Object strength,
            DecompositionEnum mode) {

        if (collatorChoice == CollatorEnum.ASCII) {

            /*
             * No Unicode support.
             */

            return new KeyBuilder(capacity);

        }

        /*
         * Unicode support.
         */

        if (locale == null) {

            locale = Locale.getDefault();

            if(log.isInfoEnabled())
                log.info("Using default locale: " + locale.getDisplayName());

        }

        // true iff ICU or ICU4JNI was choosen.
        final boolean icu = (collatorChoice == CollatorEnum.ICU || collatorChoice == CollatorEnum.ICU4JNI);

        if (icu && !DefaultKeyBuilderFactory.isICUAvailable()) {

            /*
             * The ICU library was required but was not located. Make sure that
             * the ICU JAR is on the classpath.
             * 
             * Note: If you are trying to use ICU4JNI then that has to be
             * locatable as a native library. How you do this is different for
             * Windows and Un*x.
             */

            throw new UnsupportedOperationException(ERR_ICU_NOT_AVAILABLE);

        }

        // create the initial buffer.
        final byte[] buf = createBuffer(capacity);

        // the buffer is initially empty.
        final int len = 0;

        switch (collatorChoice) {

        case ICU4JNI:
            /*
             * @todo verify the the ICU4JNI library is accessible.
             */
        case ICU:
            return new KeyBuilder(new ICUSortKeyGenerator(locale, strength,
                    mode), len, buf);

        case JDK:
            return new KeyBuilder(new JDKSortKeyGenerator(locale, strength,
                    mode), len, buf);

        default:
            throw new UnsupportedOperationException("Collator not supported: "
                    + collatorChoice);
        }

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{sortKeyGenerator=" + getSortKeyGenerator());
        sb.append("}");
        return sb.toString();
    }

}
