/*

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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KeyBuilder.Options;

/**
 * <p>
 * Interface for building up variable <code>unsigned byte[]</code> keys from
 * one or more primitive data types values and/or Unicode strings. An instance
 * of this interface may be {@link #reset()} and reused to encode a series of
 * keys.
 * </p>
 * <p>
 * A sort key is an unsigned byte[] that preserves the total order of the
 * original data. Sort keys may potentially be formed from multiple fields but
 * field markers do not appear within the resulting sort key. While the original
 * values can be extracted from sort keys (this is true of all the fixed length
 * fields, such as int, long, float, or double) they can not be extracted from
 * Unicode variable length fields (the collation ordering for a Unicode string
 * depends on the {@link Locale}, the collation strength, and the decomposition
 * mode and is a non-reversable operation).
 * </p>
 * <h2>Unicode</h2>
 * <p>
 * Factory methods are defined by {@link KeyBuilder} for obtaining instances of
 * this interface that optionally support Unicode. Instances may be created for
 * a given {@link Locale}, collation strength, decomposition mode, etc.
 * </p>
 * <p>
 * The ICU library supports generation of compressed Unicode sort keys and is
 * used by default when available. The JDK {@link java.text} package also
 * supports the generation of Unicode sort keys, but it does NOT produce
 * compressed sort keys. The resulting sort keys are therefore (a) incompatible
 * with those produced by the ICU library and (b) much larger than those
 * produced by the ICU library.
 * </p>
 * <p>
 * Support for Unicode MAY be disabled using {@link Options#COLLATOR}, by using
 * {@link KeyBuilder#newInstance()} or another factory method that does not
 * enable Unicode support, or by using one of the {@link KeyBuilder}
 * constructors that does not support Unicode.
 * </p>
 * <h2>Multi-field keys with variable length fields</h2>
 * <p>
 * Multi-field keys in which variable length fields are embedded within the key
 * present a special problem. Any run of fixed length fields can be compared as
 * unsigned byte[]s. Likewise, any any key with a fixed length prefix (including
 * zero) but a variable length field in its tail can also be compared directly
 * as unsigned byte[]s. However, the introduction of a variable length field
 * into any non-terminal position in a multi-field key must be handled specially
 * since simple concatenation of the field keys will NOT produce the correct
 * total ordering. (This is why SQL requires that text fields compare as if they
 * were padded out with ASCII blanks (0x20) to some maximum length for the
 * field.) A utility method exists specifically for this purpose - see
 * {@link #appendText(String, boolean, boolean)}.
 * </p>
 * 
 * @see KeyBuilder#newInstance()
 * @see KeyBuilder#newUnicodeInstance()
 * @see KeyBuilder#newUnicodeInstance(Properties)
 * @see SuccessorUtil
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBuilder extends ISortKeyBuilder<Object> {

    /**
     * The #of bytes of data in the key.
     */
    public int getLength();

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
     * @return <i>this</i>
     */
    public IKeyBuilder append(int off, int len, byte[] a);

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
    public byte[] getKey();

    /**
     * Reset the key length to zero before building another key.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder reset();

    /*
     * Optional operations.
     */
    
    /**
     * Encodes a Unicode string using the configured {@link Options#COLLATOR}
     * and appends the resulting sort key to the buffer (without a trailing nul
     * byte).
     * <p>
     * Note: The {@link SuccessorUtil#successor(String)} of a string is formed
     * by appending a trailing <code>nul</code> character. However, since
     * <code>IDENTICAL</code> appears to be required to differentiate between
     * a string and its successor (with the trailing <code>nul</code>
     * character), you MUST form the sort key first and then its successor (by
     * appending a trailing <code>nul</code>). Failure to follow this pattern
     * will lead to the successor of the key comparing as EQUAL to the key. For
     * example,
     * 
     * <pre>
     *            
     *            IKeyBuilder keyBuilder = ...;
     *            
     *            String s = &quot;foo&quot;;
     *            
     *            byte[] fromKey = keyBuilder.reset().append( s );
     *            
     *            // right.
     *            byte[] toKey = keyBuilder.reset().append( s ).appendNul();
     *            
     *            // wrong!
     *            byte[] toKey = keyBuilder.reset().append( s+&quot;\0&quot; );
     *            
     * </pre>
     * 
     * @param s
     *            A string.
     * 
     * @throws UnsupportedOperationException
     *             if Unicode is not supported.
     * 
     * @return <i>this</i>
     * 
     * @see SuccessorUtil#successor(String)
     * @see SuccessorUtil#successor(byte[])
     * @see TestICUUnicodeKeyBuilder#test_keyBuilder_unicode_trailingNuls()
     * 
     * FIXME update the javadoc further to speak to handling of multi-field
     * keys.
     * 
     * @todo provide a more flexible interface for handling Unicode, including
     *       the means to encode using a specified language family (such as
     *       could be identified with an <code>xml:lang</code> attribute).
     */
    public IKeyBuilder append(String s);

    /**
     * Encodes a variable length text field into the buffer. The text is
     * truncated to {@link IKeyBuilder#maxlen} characters. The sort keys for
     * strings that differ after truncation solely in the #of trailing
     * {@link #pad} characters will be identical (trailing pad characters are
     * implicit out to {@link #maxlen} characters).
     * <p>
     * Note: Trailing pad characters are normalized to a representation as a
     * single pad character (1 byte) followed by the #of actual or implied
     * trailing pad characters represented as an unsigned short integer (2
     * bytes). This technique serves to keep multi-field keys with embedded
     * variable length text fields aligned such that the field following a
     * variable length text field does not bleed into the lexiographic ordering
     * of the variable length text field.
     * <p>
     * Note: While the ASCII encoding happens to use one byte for each character
     * that is NOT true of the Unicode encoding. The space requirements for the
     * Unicode encoding depend on the text, the Local, the collator strength,
     * and the collator decomposition mode.
     * <p>
     * Note: The <i>successor</i> option is designed to encapsulate some
     * trickiness around forming the successor of a variable length text field
     * embedded in a multi-field key. In particular, simply appending a
     * <code>nul</code> byte will NOT work (it works fine when the text field
     * is the last field in the key or when it is the only component in the
     * key). This approach breaks encapsulation of the field boundaries such
     * that the resulting "successor" is actually ordered before the original
     * key. This happens because you introduce a 0x0 byte right on the boundary
     * of the next field, effectively causing the next field to have a smaller
     * value. Consider the following example (in hex) where "|" represents the
     * end of the "text" field:
     * 
     * <pre>
     *     ab cd | 12
     * </pre>
     * 
     * if you compute the successor by appending a nul byte to the text field
     * you get
     * 
     * <pre>
     *     ab cd | 00 12
     * </pre>
     * 
     * which is ordered before the original key!
     * 
     * @param text
     *            The text.
     * @param unicode
     *            When true the text is interpreted as Unicode according to the
     *            {@link Options#COLLATOR} option. Otherwise it is interpreted
     *            as ASCII.
     * @param successor
     *            When true, the successor of the text will be encoded.
     *            Otherwise the text will be encoded.
     * 
     * @return The {@link IKeyBuilder}.
     * 
     * @see http://www.unicode.org/reports/tr10/tr10-10.html#Interleaved_Levels
     */
    public IKeyBuilder appendText(String text, boolean unicode,
            boolean successor);
    
    /*
     * Note: This operation is not implemented since it can cause confusion so
     * easily.  If you want Unicode encoding use append(String).  If you want
     * ASCII encoding, use appendASCII(String).
     */
//    /**
//     * Encodes a character as a Unicode sort key by first converting it to a
//     * unicode string of length N and then encoding it using
//     * {@link #append(String)} (optional operation).
//     * 
//     * @throws UnsupportedOperationException
//     *                if Unicode is not supported.
//     *                
//     * @return <i>this</i>
//     */
//    public IKeyBuilder append(char[] v);

    /*
     * Required operations.
     */
    
    /**
     * Return <code>true</code> iff Unicode is supported by this object
     * (returns <code>false</code> if only ASCII support is configured).
     */
    public boolean isUnicodeSupported();
    
    /**
     * The maximum length of a variable length text field is <code>65535</code> (<code>pow(2,16)-1</code>).
     * <p>
     * Note: This restriction only applies to multi-field keys where the text
     * field appears in a non-terminal position within the key - that is as encoded by . When a text
     * field appears in such a non-terminal position trailing pad characters are
     * used to maintain lexiographic ordering over the multi-field key.
     */
    final public int maxlen = 65535;

    /**
     * Encodes a unicode string by assuming that its contents are ASCII
     * characters. For each character, this method simply chops of the high byte
     * and converts the low byte to an unsigned byte.
     * <p>
     * Note: This method is potentially much faster than the Unicode aware
     * {@link #append(String)}. However, this method is NOT unicode aware and
     * non-ASCII characters will not be encoded correctly. This method MUST NOT
     * be mixed with keys whose corresponding component is encoded by the
     * unicode aware methods, e.g., {@link #append(String)}.
     * 
     * @param s
     *            A String containing US-ASCII characters.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder appendASCII(String s);

    /**
     * Appends an array of bytes - the bytes are treated as
     * <code>unsigned</code> values.
     * 
     * @param a
     *            The array of bytes.
     *            
     * @return <i>this</i>
     */
    public IKeyBuilder append(byte[] a);

    /**
     * Appends a double precision floating point value by first converting it
     * into a signed long integer using {@link Double#doubleToLongBits(double)},
     * converting that values into a twos-complement number and then appending
     * the bytes in big-endian order into the key buffer.
     * <p>
     * Note: this converts -0d and +0d to the same key.
     * 
     * @param d
     *            The double-precision floating point value.
     *            
     * @return <i>this</i>
     */
    public IKeyBuilder append(double d);

    /**
     * Appends a single precision floating point value by first converting it
     * into a signed integer using {@link Float#floatToIntBits(float)}
     * converting that values into a twos-complement number and then appending
     * the bytes in big-endian order into the key buffer.
     * <p>
     * Note: this converts -0f and +0f to the same key.
     * 
     * @param f
     *            The single-precision floating point value.
     *            
     * @return <i>this</i>
     */
    public IKeyBuilder append(float f);

    /**
     * Appends the UUID to the key using the MSB and then the LSB.
     * 
     * @param uuid
     *            The UUID.
     *            
     * @return <i>this</i>
     */
    public IKeyBuilder append(UUID uuid);

    /**
     * Appends a signed long integer to the key by first converting it to a
     * lexiographic ordering as an unsigned long integer and then appending it
     * into the buffer as 8 bytes using a big-endian order.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder append(long v);

    /**
     * Appends a signed integer to the key by first converting it to a
     * lexiographic ordering as an unsigned integer and then appending it into
     * the buffer as 4 bytes using a big-endian order.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder append(int v);

    /**
     * Appends a signed short integer to the key by first converting it to a
     * two-complete representation supporting unsigned byte[] comparison and
     * then appending it into the buffer as 2 bytes using a big-endian order.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder append(short v);

    /*
     * Note: this method has been dropped from the API to reduce the
     * possibility of confusion.  If you want Unicode semantics then use
     * append(String).  If you want ASCII semantics then use appendASCII().
     * If you want signed integer semantics then use append(short).
     */
//    /**
//     * Encodes a character as a 16-bit unsigned integer.
//     * <p>
//     * Note: Characters are encoded as unsigned integers rather than as Unicode
//     * values since the semantics of Unicode collation sequences often violate
//     * the semantics of the character code points, even for ASCII. For example,
//     * the character 'z' has the successor '{', but Unicode collation would
//     * place order the string "{" BEFORE the string "z".
//     * 
//     * @param v
//     *            The character.
//     *            
//     * @return <i>this</i>
//     */
//    public IKeyBuilder append(char v);

    /**
     * Converts the signed byte to an unsigned byte and appends it to the key.
     * 
     * @param v
     *            The signed byte.
     *            
     * @return <i>this</i>
     */
    public IKeyBuilder append(final byte v);

    /**
     * Append an unsigned zero byte to the key.
     * 
     * @return <i>this</i>
     */
    public IKeyBuilder appendNul();

    /**
     * Encode a {@link BigInteger} into an unsigned byte[] and append it into
     * the key buffer.
     * <P>
     * The encoding is a 2 byte run length whose leading bit is set iff the
     * {@link BigInteger} is negative followed by the <code>byte[]</code> as
     * returned by {@link BigInteger#toByteArray()}.
     * 
     * @param The
     *            {@link BigInteger} value.
     * 
     * @return The unsigned byte[].
     */
    public IKeyBuilder append(final BigInteger i);

    /**
     * Encode a {@link BigDecimal} into an unsigned byte[] and append it into
     * the key buffer.
     * 
     * @param The
     *            {@link BigDecimal} value.
     * 
     * @return The unsigned byte[].
     */
    public IKeyBuilder append(final BigDecimal d);

    /**
     * Append the value to the buffer, encoding it as appropriate based on the
     * class of the object.  This method handles all of the primitive data types
     * plus {@link UUID} and Unicode {@link String}s.
     * 
     * @param val
     *            The value.
     * 
     * @return <i>this</i>
     * 
     * @throws IllegalArgumentException
     *             if <i>val</i> is <code>null</code>.
     * @throws UnsupportedOperationException
     *             if <i>val</i> is an instance of an unsupported class.
     */
    public IKeyBuilder append(Object val);
    
}
