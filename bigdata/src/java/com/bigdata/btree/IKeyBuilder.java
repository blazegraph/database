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

package com.bigdata.btree;

import java.util.UUID;

import com.ibm.icu.text.Collator;

/**
 * <p>
 * Interface for building up variable <code>unsigned byte[]</code> keys from
 * one or more primitive data types values and/or Unicode strings. An instance
 * of this class may be {@link #reset()} and reused to encode a series of keys.
 * However, the instances of necessity carry state (their internal buffer) and
 * are NOT thread-safe.
 * </p>
 * <p>
 * Note: In order to provide for lightweight implementations, not all
 * implementations support the Unicode operations defined by this interface.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBuilder {

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
     * @return this.
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
     * @return This {@link IKeyBuilder}.
     */
    public IKeyBuilder reset();

    /*
     * Optional operations.
     */
    
    /**
     * Encodes a unicode string using the rules of the {@link #collator} and
     * appends the resulting sort key to the buffer (without a trailing nul
     * byte) (optional operation).
     * <p>
     * Note: The {@link SuccessorUtil#successor(String)} of a string is formed
     * by appending a trailing <code>nul</code> character. However, since
     * {@link Collator#IDENTICAL} appears to be required to differentiate
     * between a string and its successor (with the trailing <code>nul</code>
     * character), you MUST form the sort key first and then its successor (by
     * appending a trailing <code>nul</code>). Failure to follow this pattern
     * will lead to the successor of the key comparing as EQUAL to the key. For
     * example,
     * 
     * <pre>
     *         
     *         IKeyBuilder keyBuilder = ...;
     *         
     *         String s = &quot;foo&quot;;
     *         
     *         byte[] fromKey = keyBuilder.reset().append( s );
     *         
     *         // right.
     *         byte[] toKey = keyBuilder.reset().append( s ).appendNul();
     *         
     *         // wrong!
     *         byte[] toKey = keyBuilder.reset().append( s+&quot;\0&quot; );
     *         
     * </pre>
     * 
     * @param s
     *            A string.
     * 
     * @exception UnsupportedOperationException
     *                if Unicode is not supported.
     * 
     * @see SuccessorUtil#successor(String)
     * @see TestUnicodeKeyBuilder#test_keyBuilder_unicode_trailingNuls()
     * 
     * @todo provide a more flexible interface for handling Unicode, including
     * the means to encode using a specified language family (such as could be
     * identified with an <code>xml:lang</code> attribute).
     */
    public IKeyBuilder append(String s);

    /**
     * Encodes a character as a Unicode sort key by first converting it to a
     * unicode string of length N and then encoding it using
     * {@link #append(String)} (optional operation).
     * 
     * @exception UnsupportedOperationException
     *                if Unicode is not supported.
     */
    public IKeyBuilder append(char[] v);

    /**
     * Encodes a character as a unicode sort key by first converting it to a
     * unicode string of length 1 and then encoding it using
     * {@link #append(String)} (optional operation).
     * 
     * @param v
     *            The character.
     * 
     * @exception UnsupportedOperationException
     *                if Unicode is not supported.
     */
    public IKeyBuilder append(char v);

    /*
     * Required operations.
     */
    
    /**
     * Encodes a uncode string by assuming that its contents are ASCII
     * characters. For each character, this method simply chops of the high byte
     * and converts the low byte to an unsigned byte.
     * <p>
     * Note: This method is potentially much faster than the Unicode aware
     * {@link #append(String)}. However, this method is NOT uncode aware and
     * non-ASCII characters will not be encoded correctly. This method MUST NOT
     * be mixed with keys whose corresponding component is encoded by the
     * unicode aware methods, e.g., {@link #append(String)}.
     * 
     * @param s
     *            A String containing US-ASCII characters.
     * 
     * @return This key builder.
     */
    public IKeyBuilder appendASCII(String s);

    /**
     * Appends an array of bytes - the bytes are treated as
     * <code>unsigned</code> values.
     * 
     * @param a
     *            The array of bytes.
     */
    public IKeyBuilder append(byte[] a);

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
    public IKeyBuilder append(double d);

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
    public IKeyBuilder append(float f);

    /**
     * Appends the UUID to the key using the MSB and then the LSB.
     * 
     * @param uuid
     *            The UUID.
     */
    public IKeyBuilder append(UUID uuid);

    /**
     * Appends a signed long integer to the key by first converting it to a
     * lexiographic ordering as an unsigned long integer and then appending it
     * into the buffer as 8 bytes using a big-endian order.
     */
    public IKeyBuilder append(long v);

    /**
     * Appends a signed integer to the key by first converting it to a
     * lexiographic ordering as an unsigned integer and then appending it into
     * the buffer as 4 bytes using a big-endian order.
     */
    public IKeyBuilder append(int v);

    /**
     * Appends a signed short integer to the key by first converting it to a
     * two-complete representation supporting unsigned byte[] comparison and
     * then appending it into the buffer as 2 bytes using a big-endian order.
     */
    public IKeyBuilder append(short v);

    /**
     * Converts the signed byte to an unsigned byte and appends it to the key.
     * 
     * @param v
     *            The signed byte.
     */
    public IKeyBuilder append(final byte v);

    /**
     * Append an unsigned zero byte to the key.
     * 
     * @return this.
     */
    public IKeyBuilder appendNul();

}
