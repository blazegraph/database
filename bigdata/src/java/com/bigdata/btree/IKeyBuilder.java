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

import java.util.UUID;

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
     * 
     * @param s
     *            A string.
     * 
     * @exception UnsupportedOperationException
     *                if Unicode is not supported.
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
