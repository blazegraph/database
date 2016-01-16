/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.util;

import it.unimi.dsi.fastutil.bytes.custom.CustomByteArrayFrontCodedList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Class supporting operations on variable length byte[] keys.
 * <p>
 * Comparison operations that accept a starting offset are used when the byte[]s
 * are known to share a leading prefix that may be skipped during comparison.
 * <p>
 * Comparison operations that accept a starting offset and length are used when
 * immutable keys are stored in a single byte[] and an index into starting
 * positions in that byte[] is maintained.
 * <p>
 * JNI methods are provided for unsigned byte[] comparison. However, note that
 * the JNI methods do not appear to be as fast as the pure Java methods -
 * presumably because of the overhead of going from Java to C. In order to
 * execute using the JNI methods you MUST define the optional boolean system
 * property, e.g.,
 * 
 * <pre>
 * java -Dcom.bigdata.btree.BytesUtil.jni=true ...
 * </pre>
 * 
 * See BytesUtil.c in this package for instructions on compiling the JNI
 * methods.
 * </p>
 * See {@link #main(String[])} which provides a test for the JNI integration and
 * some pointers on how to get this running on your platform.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BytesUtil {

	private static final transient Logger log = Logger.getLogger(BytesUtil.class);

	/**
	 * An empty <code>byte[]</code>.
	 */
	public static final byte[] EMPTY = new byte[0];

	/**
	 * An empty <code>byte[][]</code>.
	 */
	public static final byte[][] EMPTY2 = new byte[0][];

	/**
	 * Flag set iff JNI linking succeeds. When this flag is false we run with
	 * the pure Java implementations of these methods. When the flag is true,
	 * the JNI versions are used.
	 */
	static boolean linked = false;

	/**
	 * JNI routines are not invoked unless we will compare byte[]s with at least
	 * this many potential bytes to compare (the actual# may be much less of
	 * course since comparisons may fail fast).
	 */
	static public final int minlen = 100;

	static private native int _compareBytes(int alen, byte[] a, int blen, byte[] b);

	static private native int _compareBytesWithOffsetAndLen(int aoff, int alen, byte[] a, int boff, int blen, byte[] b);

	static {

		final boolean jni;

		String val = System.getProperty("com.bigdata.btree.BytesUtil.jni");

		if (val != null) {

			jni = Boolean.parseBoolean(val);

		} else {

			jni = false; // Note: We will not even try to use JNI by default!

		}

		if (jni) {

			/*
			 * Attempt to load the JNI library.
			 */

			loadJNILibrary();

		}

	}

	/**
	 * Attempt to load the JNI library.
	 * <p>
	 * Note: this is done automatically if the optional boolean system property
	 * <code>com.bigdata.btree.BytesUtil.jni=true</code> is specified, e.g.,
	 * using
	 * 
	 * <pre>
	 *    java -Dcom.bigdata.btree.BytesUtil.jni=true ...
	 * </pre>
	 * 
	 * @return True iff the JNI library was successfully linked.
	 */
	public static boolean loadJNILibrary() {

		if (!linked) {

			try {

				System.loadLibrary("BytesUtil");

				if (log.isInfoEnabled())
					log.info("BytesUtil JNI linked");

				linked = true;

			} catch (UnsatisfiedLinkError ex) {

				log.warn("BytesUtil JNI NOT linked: " + ex);

				linked = false;

			}
		}

		return linked;

	}

	/**
	 * True iff the two arrays compare as equal. This is somewhat optimized in
	 * that it tests the array lengths first, assumes that it is being used on
	 * sorted data and therefore compares the last bytes first, and does not
	 * convert the bytes to unsigned integers before testing for equality.
	 * 
	 * @param a
	 *            A byte[].
	 * @param b
	 *            Another byte[].
	 * 
	 * @return If the two arrays have the same reference (including
	 *         <code>null</code>) or if they have the same data.
	 */
	final public static boolean bytesEqual(final byte[] a, final byte[] b) {

		if (a == b)
			return true;

		final int alen = a.length;

		final int blen = b.length;

		if (alen != blen)
			return false;

		int i = alen - 1;

		while (i >= 0) {

			if (a[i] != b[i])
				return false;

			i--;

		}

		// for (int i = 0; i < alen; i++) {
		//        
		// if( a[i] != b[i] ) return false;
		//
		// }

		return true;

	}

	/**
	 * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
	 * unsigned bytes).
	 * 
	 * @param a
	 *            A byte[].
	 * 
	 * @param b
	 *            A byte[].
	 * 
	 * @return a negative integer, zero, or a positive integer if the first
	 *         argument is less than, equal to, or greater than the second.
	 * 
	 * @todo Return the index of the byte at which the difference with the sign
	 *       adjusted to indicate the relative order of the data rather than the
	 *       difference of the bytes at that index. The index would be negative
	 *       or positive depending on which way the comparison went. See
	 *       {@link CustomByteArrayFrontCodedList} for an implementation
	 *       guideline.
	 *       <p>
	 *       Change all implementations in this class and also BytesUtil.c,
	 *       which needs to be recompiled for Windows. Also makes sure that it
	 *       gets compiled and linked for Un*x. That should be tested from the
	 *       ant installer and the result reported. Do the same for ICU4JNI.
	 */
	final public static int compareBytes(final byte[] a, final byte[] b) {
		if (a == b) // includes a and b both null
			return 0;
		
		// Handle null values
		if (a == null) {
			return -1;
		}
		if (b == null) {
			return 1;
		}
		
		// neither are null
		final int alen = a.length;
		final int blen = b.length;
		if (linked && alen > minlen && blen > minlen) {
			/*
			 * JNI implementation.
			 * 
			 * @todo test for trade off when max(len) is short. unroll loop for
			 * small N.
			 */
			return _compareBytes(alen, a, blen, b);
		}
		for (int i = 0; i < alen && i < blen; i++) {
			// promotes to signed integers in [0:255] for comparison.
			final int ret = (a[i] & 0xff) - (b[i] & 0xff);
			// int ret = a[i] - b[i];
			if (ret != 0)
				return ret;
		}
		return alen - blen;
	}

	// /**
	// * Byte-wise comparison of a {@link ByteBuffer} and a byte[]. The data are
	// * treated as arrays of unsigned bytes. The {@link ByteBuffer} position,
	// * limit and mark are unchanged by this procedure.
	// *
	// * @param a
	// * A {@link ByteBuffer}.
	// * @param aoff
	// * The offset of the starting byte in the buffer.
	// * @param blen
	// * The number of bytes to be compared.
	// * @param b
	// * A byte[].
	// *
	// * @return a negative integer, zero, or a positive integer if the first
	// * argument is less than, equal to, or greater than the second.
	// */
	// final public static int compareBytes(final ByteBuffer a, final int aoff,
	// final int alen, final byte[] b) {
	// final int blen = b.length;
	// for (int i = 0; i < alen && i < blen; i++) {
	// // promotes to signed integers in [0:255] for comparison.
	// final int ret = (a.get(aoff + i) & 0xff) - (b[i] & 0xff);
	// if (ret != 0)
	// return ret;
	// }
	// return alen - blen;
	// }

	// /**
	// * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
	// * unsigned bytes).
	// *
	// * @param aoff
	// * The offset into <i>a</i> at which the comparison will
	// * begin.
	// * @param a
	// * A byte[].
	// * @param boff
	// * The offset into <i>b</i> at which the comparison will
	// * begin.
	// * @param b
	// * A byte[].
	// *
	// * @return a negative integer, zero, or a positive integer as the first
	// * argument is less than, equal to, or greater than the second.
	// */
	// final public static int compareBytes(int aoff, final byte[] a, int boff,
	// final byte[] b) {
	// final int alen = a.length;
	// final int blen = b.length;
	// for (int i = aoff, j = boff; i < alen && j < blen; i++, j++) {
	// // promotes to signed integers in [0:255] for comaprison.
	// int ret = (a[i] & 0xff) - (b[j] & 0xff);
	// // int ret = a[i] - b[j];
	// if (ret != 0)
	// return ret;
	// }
	// return (alen - aoff) - (blen - boff);
	// }

	/**
	 * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
	 * unsigned bytes).
	 * 
	 * @param aoff
	 *            The offset into <i>a</i> at which the comparison will begin.
	 * @param alen
	 *            The #of bytes in <i>a</i> to consider starting at <i>aoff</i>.
	 * @param a
	 *            A byte[].
	 * @param boff
	 *            The offset into <i>b</i> at which the comparison will begin.
	 * @param blen
	 *            The #of bytes in <i>b</i> to consider starting at <i>boff</i>.
	 * @param b
	 *            A byte[].
	 * 
	 * @return a negative integer, zero, or a positive integer as the first
	 *         argument is less than, equal to, or greater than the second.
	 */
	final public static int compareBytesWithLenAndOffset(//
			int aoff, int alen, final byte[] a,//
			int boff, int blen, final byte[] b//
	) {

		if (linked && alen > minlen && blen > minlen) {

			// JNI implementation.
			return _compareBytesWithOffsetAndLen(aoff, alen, a, boff, blen, b);

		}

		// last index to consider in a[].
		final int alimit = aoff + alen;

		// last index to consider in b[].
		final int blimit = boff + blen;

		for (int i = aoff, j = boff; i < alimit && j < blimit; i++, j++) {

			// promotes to signed integers in [0:255] for comaprison.
			int ret = (a[i] & 0xff) - (b[j] & 0xff);

			if (ret != 0)
				return ret;

		}

		return alen - blen;

	}

	/**
	 * Return the #of leading bytes in common. This is used to compute the
	 * prefix for a node or leaf, which is formed by the leading bytes in common
	 * between the first and last key for a node or leaf.
	 * 
	 * @param a
	 *            A variable length unsigned byte array.
	 * @param b
	 *            A variable length unsigned byte array.
	 * 
	 * @return The #of leading bytes in common (aka the index of the first byte
	 *         in which the two arrays differ, although that index could lie
	 *         beyond the end of one of the arrays).
	 */
	public final static int getPrefixLength(final byte[] a, final byte[] b) {

		final int alen = a.length;

		final int blen = b.length;

		int i;

		for (i = 0; i < alen && i < blen; i++) {

			if (a[i] != b[i])
				break;

		}

		return i;

	}

	/**
	 * Return a new byte[] containing the leading bytes in common between two
	 * byte[]s. This is often used to compute the minimum length separator key.
	 * 
	 * @param a
	 *            A variable length unsigned byte array[].
	 * @param b
	 *            A variable length unsigned byte array[].
	 * 
	 * @return A new byte[] containing the leading bytes in common between the
	 *         two arrays.
	 */
	public final static byte[] getPrefix(final byte[] a, final byte[] b) {

		final int len = getPrefixLength(a, b);

		final byte[] prefix = new byte[len];

		System.arraycopy(a, 0, prefix, 0, len);

		return prefix;

	}

	/**
	 * Computes the successor of a variable length byte array by appending a
	 * unsigned zero(0) byte to the end of the array.
	 * 
	 * @param key
	 *            A variable length unsigned byte array.
	 * 
	 * @return A new unsigned byte[] that is the successor of the key.
	 */
	public final static byte[] successor(final byte[] key) {

		final int keylen = key.length;

		final byte[] tmp = new byte[keylen + 1];

		System.arraycopy(key, 0, tmp, 0, keylen);

		return tmp;

	}

	/**
	 * <p>
	 * The keys in the nodes of a btree are known as <i>separator keys</i>. The
	 * role of the separator keys is to direct search towards the leaf in which
	 * a key exists or would exist by always searching the first child having a
	 * separator key that is greater than or equal to the search key.
	 * </p>
	 * <p>
	 * Separator keys separate leaves and must be choosen with that purpose in
	 * mind. The simplest way to choose the separator key is to just take the
	 * first key of the leaf - this is always correct. However, shorter
	 * separator keys may be choosen by defining the separator key as the
	 * shortest key that is less than or equal to the first key of a leaf and
	 * greater than the last key of the left sibling of that leaf (that is, the
	 * key for the entry that immediately proceeds the first entry on the leaf).
	 * </p>
	 * <p>
	 * There are several advantages to always choosing the shortest separator
	 * key. The original rationale (in "Prefix <i>B</i>-Trees" by Bayer and
	 * Unterauer) was to increase the branching factors for fixed size pages.
	 * Since we use variable size serialized record, that is not an issue.
	 * However, using the shortest separator keys in this implementation
	 * provides both smaller serialized records for nodes and faster search
	 * since fewer bytes must be tested.
	 * </p>
	 * <p>
	 * Note that this trick can not be used at higher levels in the btree -
	 * separator keys are always formed based on the keys in the leaves and then
	 * propagated through the tree.
	 * </p>
	 * <p>
	 * The rules are simple enough:
	 * <ol>
	 * <li>The separator contains all bytes in the shared prefix (if any) plus
	 * the 1st byte at which the given key differs from the prior key.</li>
	 * <li>If the separator key would equal the given key by value then return
	 * the reference to the given key.</li>
	 * </ol>
	 * </p>
	 * 
	 * @param givenKey
	 *            A key.
	 * 
	 * @param priorKey
	 *            Another key that <em>proceeds</em> the <i>givenKey</i>.
	 * 
	 * @return The shortest key that is less than or equal to <i>givenKey</i>
	 *         and greater than <i>priorKey</i>. This will be a reference to the
	 *         <i>givenKey</i> iff that is also the shortest separator.
	 * 
	 * @see http://portal.acm.org/citation.cfm?doid=320521.320530
	 * 
	 * @throws IllegalArgumentException
	 *             if either key is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if both keys are the same reference.
	 */
	// * @throws IllegalArgumentException
	// * if the keys are equal.
	// * @throws IllegalArgumentException
	// * if the keys are out of order.
	final public static byte[] getSeparatorKey(final byte[] givenKey, final byte[] priorKey) {

		if (givenKey == null)
			throw new IllegalArgumentException();

		if (priorKey == null)
			throw new IllegalArgumentException();

		if (givenKey == priorKey)
			throw new IllegalArgumentException();

		final int prefixLen = getPrefixLength(givenKey, priorKey);

		if (prefixLen == givenKey.length - 1) {

			/*
			 * The given key is the shortest separator. Examples would include:
			 * 
			 * given: 0 1 2 prior: 0 1
			 * 
			 * or
			 * 
			 * given: 0 1 2 prior: 0 1 1
			 * 
			 * or
			 * 
			 * given: 0 1 2 prior: 0 1 1 2
			 */

			return givenKey;

		}

		/*
		 * The separator includes all bytes in the shared prefix plus the next
		 * byte from the given key.
		 */

		// allocate to right size.
		final byte[] tmp = new byte[prefixLen + 1];

		// copy shared prefix plus the following byte.
		System.arraycopy(givenKey, 0, tmp, 0, prefixLen + 1);

		return tmp;

	}

	/**
	 * Formats a key as a series of comma delimited unsigned bytes.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The string representation of the array as unsigned bytes.
	 */
	final public static String toString(final byte[] key) {

		if (key == null)
			return NULL;

		return toString(key, 0, key.length);

	}

	/**
	 * Formats a key as a series of comma delimited unsigned bytes.
	 * 
	 * @param key
	 *            The key.
	 * @param off
	 *            The index of the first byte that will be visited.
	 * @param len
	 *            The #of bytes to visit.
	 * 
	 * @return The string representation of the array as unsigned bytes.
	 */
	final public static String toString(final byte[] key, final int off, final int len) {

		if (key == null)
			return NULL;

		final StringBuilder sb = new StringBuilder(len * 4 + 2);

		sb.append("[");

		for (int i = off; i < off + len; i++) {

			if (i > 0)
				sb.append(",");

			// as an unsigned integer.
			// sb.append(Integer.toHexString(key[i] & 0xff));
			sb.append(Integer.toString(key[i] & 0xff));

		}

		sb.append("]");

		return sb.toString();

	}

	private static transient String NULL = "null";

	/**
	 * Formats the data into a {@link String}.
	 * 
	 * @param data
	 *            An array of unsigned byte arrays.
	 */
	static public String toString(final byte[][] data) {

		final StringBuilder sb = new StringBuilder();

		final int n = data.length;

		sb.append("data(n=" + n + ")={");

		for (int i = 0; i < n; i++) {

			final byte[] a = data[i];

			sb.append("\n");

			sb.append("data[" + i + "]=");

			sb.append(BytesUtil.toString(a));

			if (i + 1 < n)
				sb.append(",");

		}

		sb.append("}");

		return sb.toString();

	}

	/**
	 * Binary search on an array whose members are variable length unsigned
	 * byte[]s.
	 * 
	 * @param keys
	 *            The buffer.
	 * @param base
	 *            The offset of the base of the array within the buffer.
	 * @param nmem
	 *            The #of members in the array. When [nmem == 0], the array is
	 *            empty.
	 * @param key
	 *            The key for the search.
	 * 
	 * @return index of the search key, if it is contained in <i>keys</i>;
	 *         otherwise, <code>(-(insertion point) - 1)</code>. The insertion
	 *         point is defined as the point at which the key would be inserted
	 *         into the array of keys. Note that this guarantees that the return
	 *         value will be >= 0 if and only if the key is found.
	 */
	static final public int binarySearch(final byte[][] keys, final int base, final int nmem, final byte[] key) {

		int low = 0;

		int high = nmem - 1;

		while (low <= high) {

			final int mid = (low + high) >> 1;

			final int offset = base + mid;

			final byte[] midVal = keys[offset];

			// compare actual vs probe
			final int tmp = BytesUtil.compareBytes(midVal, key);

			if (tmp < 0) {

				// Actual LT probe, restrict lower bound and try again.
				low = mid + 1;

			} else if (tmp > 0) {

				// Actual GT probe, restrict upper bound and try again.
				high = mid - 1;

			} else {

				// Actual EQ probe. Found : return offset.

				return offset;

			}

		}

		// Not found: return insertion point.

		final int offset = (base + low);

		return -(offset + 1);

	}

	/**
	 * Compares two unsigned byte[]s.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
	public static class UnsignedByteArrayComparator implements Comparator<byte[]> {

		public static transient final Comparator<byte[]> INSTANCE = new UnsignedByteArrayComparator();

		public int compare(final byte[] o1, final byte[] o2) {

			return BytesUtil.compareBytes(o1, o2);

		}

	}

	/**
	 * Return <code>true</code> if the <i>key</i> lies inside of the optional
	 * half-open range constraint.
	 * 
	 * @param key
	 *            The key.
	 * @param fromKey
	 *            The inclusive lower bound -or- <code>null</code> if there is
	 *            no lower bound.
	 * @param toKey
	 *            The exclusive upper bound -or- <code>null</code> if there is
	 *            no upper bound.
	 * 
	 * @return <code>true</code> unless the <i>key</i> is LT [fromKey] or GTE
	 *         [toKey].
	 */
	final static public boolean rangeCheck(final byte[] key, final byte[] fromKey, final byte[] toKey) {

		if (fromKey == null && toKey == null) {

			// no range constraint.
			return true;

		}

		if (fromKey != null) {

			if (BytesUtil.compareBytes(key, fromKey) < 0) {

				if (log.isDebugEnabled()) {

					log.debug("key=" + BytesUtil.toString(key) + " LT fromKey" + BytesUtil.toString(fromKey));

				}

				// key is LT then the optional inclusive lower bound.
				return false;

			}

		}

		if (toKey != null) {

			if (BytesUtil.compareBytes(key, toKey) >= 0) {

				if (log.isDebugEnabled()) {

					log.debug("key=" + BytesUtil.toString(key) + " GTE toKey" + BytesUtil.toString(toKey));

				}

				// key is GTE the optional exclusive upper bound
				return false;

			}

		}

		return true;

	}

	/**
	 * This method forces the load of the JNI library and tries to execute the
	 * JNI methods.
	 * <p>
	 * In order to use the JNI library under Windows, you must specify the JNI
	 * library location using the PATH environment variable, e.g.,
	 * 
	 * <pre>
	 *   cd bigdata
	 *   set PATH=%PATH%;lib
	 *   java -cp bin com.bigdata.btree.BytesUtil
	 * </pre>
	 * 
	 * <p>
	 * In order to use the JNI library under un*x, you must specify the JNI
	 * library location
	 * 
	 * <pre>
	 *    java -Djava.library.path=lib com.bigdata.btree.BytesUtil
	 * </pre>
	 * 
	 * @param args
	 * 
	 * @exception UnsatisfiedLinkError
	 *                if the JNI methods can not be resolved.
	 * @exception AssertionError
	 *                if the JNI methods do not produce the expected answers.
	 */
	public static void main(final String[] args) {

		// Force load of the JNI library.
		loadJNILibrary();

		if (0 != BytesUtil._compareBytes(3, new byte[] { 1, 2, 3 }, 3, new byte[] { 1, 2, 3 })) {

			throw new AssertionError();

		}

		if (0 != BytesUtil._compareBytesWithOffsetAndLen(0, 3, new byte[] { 1, 2, 3 }, 0, 3, new byte[] { 1, 2, 3 })) {

			throw new AssertionError();

		}

		System.out.println("JNI library routines Ok.");

	}

	/**
	 * Return the #of bytes required to bit code the specified #of bits.
	 * 
	 * @param nbits
	 *            The #of bit flags.
	 * 
	 * @return The #of bytes required. This will be zero iff <i>nbits</i> is
	 *         zero.
	 */
	final public static int bitFlagByteLength(final int nbits) {

		return nbits / 8 + (nbits % 8 == 0 ? 0 : 1);

		// return nbits>>>3;

		// if (nbits == 0)
		// return 0;
		//        
		// return ((int) ((nbits / 8) + 1));

	}

	/**
	 * Return the index of the byte in which the bit with the given index is
	 * encoded.
	 * 
	 * @param bitIndex
	 *            The bit index.
	 * 
	 * @return The byte index.
	 */
	final public static int byteIndexForBit(final long bitIndex) {

		return ((int) (bitIndex / 8));

	}

	/**
	 * Return the offset within the byte in which the bit is coded of the bit
	 * (this is just the remainder <code>bitIndex % 8</code>).
	 * <p>
	 * Note, the computation of the bit offset is intentionally aligned with
	 * {@link OutputBitStream} and {@link InputBitStream}.
	 * 
	 * @param bitIndex
	 *            The bit index into the byte[].
	 * 
	 * @return The offset of the bit in the appropriate byte.
	 */
	final public static int withinByteIndexForBit(final long bitIndex) {

		return 7 - ((int) bitIndex) % 8;

	}

	/**
	 * Get the value of a bit.
	 * <p>
	 * Note, the computation of the bit offset is intentionally aligned with
	 * {@link OutputBitStream} and {@link InputBitStream}.
	 * 
	 * @param bitIndex
	 *            The index of the bit.
	 * 
	 * @return The value of the bit.
	 */
	final public static boolean getBit(final byte[] buf, final long bitIndex) {

		final int mask = (1 << withinByteIndexForBit(bitIndex));

		final int off = byteIndexForBit(bitIndex);

		final byte b = buf[off];

		return (b & mask) != 0;

	}

	/**
	 * Set the value of a bit - this is NOT thread-safe (contention for the byte
	 * in the backing buffer can cause lost updates).
	 * <p>
	 * Note, the computation of the bit offset is intentionally aligned with
	 * {@link OutputBitStream} and {@link InputBitStream}.
	 * 
	 * @param bitIndex
	 *            The index of the bit.
	 * 
	 * @return The old value of the bit.
	 */
	final public static boolean setBit(final byte[] buf, final long bitIndex, final boolean value) {

		final int mask = (1 << withinByteIndexForBit(bitIndex));

		final int off = byteIndexForBit(bitIndex);

		// current byte at that index.
		byte b = buf[off];

		final boolean oldValue = (b & mask) != 0;

		if (value)
			b |= mask;
		else
			b &= ~mask;

		buf[off] = b;

		return oldValue;

	}

	/**
	 * An array of 32-bit mask values. The index in the array is the #of bits of
	 * the hash code to be considered. The value at that index in the array is
	 * the mask to be applied to mask off to zero the high bits of the hash code
	 * which are to be ignored.
	 */
	static private final int[] masks32;
	static {

		// Populate the array of masking values.
		masks32 = new int[33];

		for (int i = 0; i < 33; i++)
			masks32[i] = getMSBMask(i);

	}

	/**
	 * Return a bit mask which reveals only the MSB (Most Significant Bits) N
	 * bits of an int32 value.
	 * 
	 * @param nbits
	 *            The #of bits to be revealed.
	 * 
	 * @return The mask.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>nbits</i> is LT ZERO (0).
	 * @throws IllegalArgumentException
	 *             if <i>nbits</i> is GT 32.
	 */
	public static/* private */int getMSBMask(final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

		final int limit = (32 - nbits);
		int mask = 0;

		for (int i = 31; i >= limit; i--) {

			final int bit = (1 << i);

			mask |= bit;

		}

		return mask;

	}

	/**
	 * Mask off all but the MSB <i>nbits</i> of the hash value and shift them
	 * down such that the masked bits appear at bits (nbits:0] of the returned
	 * value. This is used to index into a dictionary page based on the revealed
	 * bits.
	 * 
	 * @param h
	 *            The hash value.
	 * @param nbits
	 *            The #of bits already accounted for by the path from the root.
	 * 
	 * @return The hash value considering only the MSB <i>nbits</i> and shifted
	 *         down into an <i>nbits</i> integer.
	 */
	public static int maskOffMSB(final int h, final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

		final int v = h & masks32[nbits];

		final int x = v >>> (32 - nbits);

		return x;

	}

	/**
	 * Mask off all but the LSB <i>nbits</i> of the hash value.
	 * 
	 * @param h
	 *            The hash value.
	 * @param nbits
	 *            The #of LSB bits to be retained.
	 * 
	 * @return The LSB <i>nbits</i>.
	 * 
	 *         TODO unit test.
	 */
	public static int maskOffLSB(final int h, final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

		final int v = h & ~masks32[32-nbits];

		return v;

	}

	/**
	 * Return the n-bit integer corresponding to the inclusive bit range of the
	 * byte[]. Bit ZERO (0) is the Most Significant Bit (MSB). Bit positions
	 * increase from zero up to <code>a.length * 8 - 1</code>. The return value
	 * is an int32 and the bit range must not be greater than 32 bits.
	 * <p>
	 * For example, given the following data and the bit range (0,2)
	 * 
	 * <pre>
	 * bit index: 01234567 
	 * ---------+---------- 
	 * bit value: 10110000
	 * </pre>
	 * 
	 * TWO (2) bits starting at bit offset ZERO (0) would be extracted and
	 * returned as a 2-bit integer. For those data, the return value would be an
	 * int32 value whose binary representation was <code>10</code> (with leading
	 * zeros suppressed).
	 * <p>
	 * Note: This method is design for use with the unsigned byte[] keys in a
	 * bigdata hash index. All keys in bigdata are internally represented as
	 * unsigned byte[]s, which is why this method accepts a byte[] rather than
	 * an long[] for the bits. Also, while the length of an unsigned byte[] key
	 * can vary, they are never huge and an int32 value is sufficient to index
	 * into the bits in the byte[]. Finally, the return value is an int because
	 * it will be used in hash table designs to index into a hash table based on
	 * those bits in a hash code key which are masked as relevant to that hash
	 * table. 32bits is far more than we will need to index into a hash table.
	 * For an 8k page, we might expect a fan out of at most 1024 which is only
	 * 10 bits.
	 * 
	 * @param a
	 *            A byte[].
	 * @param off
	 *            The index of the first bit to be included.
	 * @param len
	 *            The number of bits to be returned in [0:32]. However, a bit
	 *            length of zero will always return zero.
	 * 
	 * @return The integer extracted from the specified bit range.
	 */
	public static int getBits(final byte[] a, final int off, final int len) {
		if (a == null || off < 0 || len < 0 || len > 32)
			throw new IllegalArgumentException();
		
		final int maxbits = a.length * 8;
		
		if (off + len > maxbits)
			throw new IllegalArgumentException("off: " + off + ", len: " + len + ", a.length: " + a.length);

		if (len == 0) // zero length is always a zero.
			return 0;

		/*
		 * Build int32 value having the desired bits.
		 */

		// byte in which the bit range begins.
		final int fromByteOffset = off / 8; // byteIndexForBit(off);
		final int lastByte = (off + len - 1);
		// byte in which the bit range ends (inclusive).
		final int toByteOffset = lastByte / 8; // byteIndexForBit(off +
														// len - 1);

		/*
		 * The data are assembled into the int64 value by copying each byte in
		 * turn having data for the slice. This will copy at most 5 bytes. For
		 * example, when a 32-bit window starts in the middle of a byte. Once
		 * the bytes are assembled into the int64 buffer, they are shifted down
		 * to put the last bit extracted at bit index ZERO (0) of the int32
		 * word. Finally, the unused high bits are cleared to zero using a mask.
		 */

		final int nbytes = toByteOffset - fromByteOffset + 1;
		long v = 0L; // buffer for up to 5 source bytes.
		for (int i = fromByteOffset, j = 1; i <= toByteOffset; i++, j++) {
			final long x = 0xFF & a[i]; // next byte.
			final int shift = ((nbytes - j) << 3); //  
			v |= (x << shift); // mask off high bits and shift into buf.

			// inlining the above expressions produces SLOWER code
			// v |= ((0xFF & a[i]) << ((nbytes-j)<<3));
		} // next byte in the byte[].

		// NOTE an explicit select on byte count to avoid the for loop
		// produces SLOWER code

		final int rshift = 7 - (lastByte % 8); // final right shift to word align.
		final int w = (int) (v >>> rshift); // int32 result.

		// int mask = masks32[32 - len]; // lookup mask with [len] LSB ZEROs.
		// mask = ~mask; // flip bits to get [len] LSB ONEs.
		// w &= mask; // mask off the lower [len] bits (handles sign extension
		// and
		// starting offset within byte).
		// return w;
		// inlining the above is a little FASTER
		return w & ~masks32[32 - len];
	}

	static final int[] bitmasks = new int[] { 0x00, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F, 0x7F, 0xFF };

	/*
	 * Given a byte and a start and length copy the bits to a long and mask off
	 * those not required. The third argument defines the destination start bit
	 * in the long.
	 */
	private static long getLongVal(final byte b, final int s, final int l, final int t) {
		final int lastBit = s + l;
		final int shift = 64 - t - l;

		final long ret;
		if (lastBit < 8) {
			ret = b >>> (8 - lastBit);
		} else {
			ret = b;
		}

		return (ret & bitmasks[l]) << shift;
	}

	private static int getIntVal(final byte b, final int s, final int l, final int t) {
		final int lastBit = s + l;
		final int shift = 64 - t - l;

		final int ret;
		if (lastBit < 8) {
			ret = b >>> (8 - lastBit);
		} else {
			ret = b;
		}

		return (ret & bitmasks[l]) << shift;
	}

	public static long altGetBits64(final byte[] a, final int off, final int len) {

		if (a == null || off < 0 || len < 0 || len > 64)
			throw new IllegalArgumentException();
		if (len == 0) // zero length is always a zero.
			return 0;
		if (off + len > a.length * 8)
			throw new IllegalArgumentException();

		/*
		 * Build int64 value having the desired bits.
		 */

		// byte in which the bit range begins.
		int bi = off/8;
		// start bit offset
		int bo = off % 8;
		// bits remaining in current byte
		int br = 8 - bo;
		// long target start bit
		int lt = 64 - len;
		int lr = len;

		long ret = 0;
		for (int t = 0; t < len; t += br, bo = 0, br = 8, bi++) {
			if (br > lr) {
				br = lr;
			}

			ret |= getLongVal(a[bi], bo, br, lt);

			lr -= br;
			lt += br;
		}

		return ret;
	}
	
	/*
	 * Is there any advantage in 32-bit math over 64-bit?
	 */
	public static int altGetBits32(final byte[] a, final int off, final int len) {

		if (a == null || off < 0 || len < 0 || len > 64)
			throw new IllegalArgumentException();
		if (len == 0) // zero length is always a zero.
			return 0;
		if (off + len > a.length * 8)
			throw new IllegalArgumentException();

		/*
		 * Build int32 value having the desired bits.
		 */

		// byte in which the bit range begins.
		int bi = off/8;
		// start bit offset
		int bo = off % 8;
		// bits remaining in current byte
		int br = 8 - bo;
		// long target start bit
		int lt = 32 - len;
		int lr = len;

		int ret = 0;
		for (int t = 0; t < len; t += br, bo = 0, br = 8, bi++) {
			if (br > lr) {
				br = lr;
			}

			ret |= getIntVal(a[bi], bo, br, lt);

			lr -= br;
			lt += br;
		}

		return ret;
	}

	public static long getBits64(final byte[] a, final int off, final int len) {
		long ret = 0;
		if (len <= 32) {
			ret = 0xFFFFFFFFL & getBits(a, off, len);
		} else {
			final int hilen = len - 32;
			ret = getBits(a, off, hilen);
			ret <<= 32;
			ret |= (0xFFFFFFFFL & getBits(a, off + hilen, 32));
		}

		return ret;
	}
	
	/**
	 * Some benchmarks seem to indicate that altGetBits32 is faster than getBits
	 * for smaller byte counts.  OTOH the cost of the redirection may outweigh
	 * any benefit.
	 */
	public static int optGetBits(final byte[] a, final int off, final int len) {
		if (len <= 16) {
			return altGetBits32(a, off, len);
		} else {
			return getBits(a, off, len);
		}
	}


	/**
	 * Return the n-bit integer corresponding to the inclusive bit range of the
	 * byte[]. Bit ZERO (0) is the Most Significant Bit (MSB). Bit positions
	 * increase from zero up to <code>31</code>. The return value is an int32
	 * and the bit range must not be greater than 32 bits.
	 * <p>
	 * For example, given the following data and the bit range (0,2)
	 * 
	 * <pre>
	 * bit index: 01234567 
	 * ---------+---------- 
	 * bit value: 10110000
	 * </pre>
	 * 
	 * TWO (2) bits starting at bit offset ZERO (0) would be extracted and
	 * returned as a 2-bit integer. For those data, the return value would be an
	 * int32 value whose binary representation was <code>10</code> (with leading
	 * zeros suppressed).
	 * <p>
	 * Note: This method is design for use in a bigdata hash index having native
	 * int32 keys rather than unsigned byte[] keys.
	 * 
	 * @param a
	 *            An integer.
	 * @param off
	 *            The index of the first bit to be included.
	 * @param len
	 *            The number of bits to be returned in [0:32]. However, a bit
	 *            length of zero will always return zero.
	 * 
	 * @return The integer extracted from the specified bit range.
	 */
	public static int getBits(final int a, final int off, final int len) {
		if (off < 0)
			throw new IllegalArgumentException();
		if (len < 0 || len > 32)
			throw new IllegalArgumentException();
		if (len == 0) // zero length is always a zero.
			return 0;
		if (off + len > 32)
			throw new IllegalArgumentException();

		final int last = off + len - 1; // index of the last bit (inclusive).
		final int rshift = 31 - last; // right shift to word align.
		int w = (int) (a >>> rshift); // int32 result.
		int mask = masks32[32 - len]; // lookup mask with [len] LSB ZEROs.
		mask = ~mask; // flip bits to get [len] LSB ONEs.
		w &= mask; // mask off the lower [len] bits (handles sign extension and
		// starting offset within byte).
		return w;
	}

	/**
	 * Return the binary representation of the unsigned byte[].
	 * 
	 * @param b
	 *            The unsigned byte[].
	 * 
	 * @return The representation of the bits in that unsigned byte[].
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is <code>null</code>.
	 */
	public static String toBitString(final byte[] b) {
		if (b == null)// Note: fromKey/toKey may be null; caller must check 1st
			throw new IllegalArgumentException();
		final char[] chars = new char[b.length << 3]; // one char per bit.
		int bitIndex = 0; // start at the msb.
		for (int i = 0; i < b.length; i++) {
			final byte x = b[i]; // next byte.
			for (int withinByteIndex = 7; withinByteIndex >= 0; withinByteIndex--) {
				final int mask = 1 << withinByteIndex;
				final boolean bit = (x & mask) != 0;
				chars[bitIndex++] = bits[bit ? 1 : 0];
			} // next bit in the current byte.
		} // next byte in the byte[].
		// System.err.println("b[]=" + BytesUtil.toString(b) + ", chars="
		// + Arrays.toString(chars));
		return new String(chars);
	}

	/** binary digits. */
	private final static char[] bits = { '0', '1' };

	/**
	 * Decode a string of the form <code>[0-9]+(k|kb|m|mb|g|gb)?</code>,
	 * returning the number of bytes. When a suffix indicates kilobytes,
	 * megabytes, or gigabytes then the returned value is scaled accordingly.
	 * The suffix is NOT case sensitive.
	 * 
	 * @param s
	 *            The string value.
	 * 
	 * @return The byte count.
	 * 
	 * @throws IllegalArgumentException
	 *             if there is a problem with the argument (<code>null</code>,
	 *             ill-formed, etc).
	 */
	static public long getByteCount(final String s) {

		if (s == null)
			throw new IllegalArgumentException();

		final Matcher m = PATTERN_BYTE_COUNT.matcher(s);

		if (!m.matches())
			throw new IllegalArgumentException(s);

		// the numeric component.
		final String g1 = m.group(1);

		final long c = Long.valueOf(g1);

		// the units (null if not given).
		final String g2 = m.group(2);

		final long count;
		if (g2 == null) {
			count = c;
		} else if (g2.equalsIgnoreCase("k") || g2.equalsIgnoreCase("kb")) {
			count = c * Bytes.kilobyte;
		} else if (g2.equalsIgnoreCase("m") || g2.equalsIgnoreCase("mb")) {
			count = c * Bytes.megabyte;
		} else if (g2.equalsIgnoreCase("g") || g2.equalsIgnoreCase("gb")) {
			count = c * Bytes.gigabyte;
		} else {
			throw new AssertionError();
		}
		return count;
	}

	static final private Pattern PATTERN_BYTE_COUNT = Pattern.compile("([0-9]+)(k|kb|m|mb|g|gb)?",
			Pattern.CASE_INSENSITIVE);

	/**
	 * Return a byte[] having the data in the {@link ByteBuffer} from the
	 * {@link ByteBuffer#position()} to the {@link ByteBuffer#limit()}. The
	 * position, limit, and mark are not affected by this operation. When the
	 * {@link ByteBuffer} has a backing array, the array offset is ZERO (0), and
	 * the {@link ByteBuffer#limit()} is equal to the
	 * {@link ByteBuffer#capacity()} then the backing array is returned.
	 * Otherwise, a new byte[] is allocated and the data are copied into that
	 * byte[], which is then returned.
	 * 
	 * @param b
	 *            The {@link ByteBuffer}.
	 * 
	 * @return The byte[].
	 */
	static public byte[] toArray(final ByteBuffer b) {

        return toArray(b, false/* forceCopy */, null/* dst */);

	}

    /**
     * Return a byte[] having the data in the {@link ByteBuffer} from the
     * {@link ByteBuffer#position()} to the {@link ByteBuffer#limit()}. The
     * position, limit, and mark are not affected by this operation.
     * <p>
     * Under certain circumstances it is possible and may be desirable to return
     * the backing {@link ByteBuffer#array}. This behavior is enabled by
     * <code>forceCopy := false</code>.
     * <p>
     * It is possible to return the backing byte[] when the {@link ByteBuffer}
     * has a backing array, the array offset is ZERO (0), and the
     * {@link ByteBuffer#limit()} is equal to the {@link ByteBuffer#capacity()}
     * then the backing array is returned. Otherwise, a new byte[] must be
     * allocated, and the data are copied into that byte[], which may then be
     * returned.
     * 
     * @param b
     *            The {@link ByteBuffer}.
     * @param forceCopy
     *            When <code>false</code>, the backing array will be returned if
     *            possible.
     * @param dst
     *            A byte[] provided by the caller (optional). When non-
     *            <code>null</code> and having a length GTE
     *            {@link ByteBuffer#remaining()}, this array will be preferred
     *            to a newly allocated array.
     * 
     * @return The byte[] having the data. When <i>dst</i> is non-
     *         <code>null</code> this MAY be the caller's array. When it is the
     *         caller's array, it MAY be larger than the #of bytes actually
     *         read.
     */
    static public byte[] toArray(final ByteBuffer b, final boolean forceCopy,
            final byte[] dst) {

        if (!forceCopy && b.hasArray() && b.arrayOffset() == 0
                && b.position() == 0) {

			// && b.limit() == b.capacity()

			final byte[] a = b.array();

			if (a.length == b.limit()) {

				return a;

			}

		}

		/*
		 * Copy the data into a byte[] using a read-only view on the buffer so
		 * that we do not mess with its position, mark, or limit.
		 */
		final ByteBuffer tmp = b.asReadOnlyBuffer();

		final int len = tmp.remaining();

        final byte[] a = dst != null && dst.length >= len ? dst : new byte[len];

        // Transfer only the available bytes.
        tmp.get(a, 0, len);

		return a;

	}

    static private final char[] HEX_CHAR_TABLE = {
        '0', '1','2','3',
        '4','5','6','7',
        '8','9','a','b',
        'c','d','e','f'
       };    

    /**
     * Utility to convert an int array to a hex string
     * 
     * @param buf
     *            The data.
     *            
     * @return The hex string.
     */
    static public String toHexString(final int[] ibuf) {
        final byte[] buf = new byte[ibuf.length*4];
        for (int i = 0; i < ibuf.length; i++) {
        	final int v = ibuf[i];
        	final int sb = i * 4;
        	buf[sb] = (byte) (v >>> 24 & 0xFF);
        	buf[sb+1] = (byte) (v >>> 16 & 0xFF);
        	buf[sb+2] = (byte) (v >>> 8 & 0xFF);
        	buf[sb+3] = (byte) (v & 0xFF);
        }
        
        return toHexString(buf, buf.length);
        
    }
    
    /**
     * Utility to convert a byte array to a hex string.
     * 
     * @param buf
     *            The data.
     *            
     * @return The hex string.
     */
    static public String toHexString(final byte[] buf) {
        
        if (buf == null)
            return "NULL";

        return toHexString(buf, buf.length);
        
    }
    
    /**
     * Utility to display byte array of maximum i bytes as hexString.
     * 
     * @param buf
     *            The data.
     * @param n
     *            The #of bytes to convert.
     * 
     * @return The hex string.
     */
    static public String toHexString(final byte[] buf, int n) {

        if (buf == null)
            return "NULL";

        n = n < buf.length ? n : buf.length;
        final StringBuffer out = new StringBuffer();
        for (int i = 0; i < n; i++) {
            final int v = buf[i] & 0xFF;
            out.append(HEX_CHAR_TABLE[v >>> 4]);
            out.append(HEX_CHAR_TABLE[v & 0xF]);
        }
        return out.toString();
    }

    /**
     * Formats hex dta into 64 byte rows.
     * 
     * @param sb
     *            Where to format the data.
     * @param hexData
     *            The data.
     */
    static public void printHexString(final StringBuilder sb,
            final String hexData) {
        
        int rem = hexData.length();
        int curs = 0;
        while (rem >= 64) {
            sb.append(String.format("%8d: ", curs));
            sb.append(hexData.substring(curs, curs + 64) + "\n");
            curs += 64;
            rem -= 64;
        }

    }

    /**
     * Return the data in the buffer. When possible, the backing array is
     * returned. Otherwise, a new byte[] is allocated, the data are copied into
     * the array, and the new array is returned.
     */
    public static byte[] getBytes(ByteBuffer buf) {

        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0
                && buf.limit() == buf.capacity()) {

            /*
             * Return the backing array.
             */

            return buf.array();

        }

        /*
         * Copy the expected data into a byte[] using a read-only view on the
         * buffer so that we do not mess with its position, mark, or limit.
         */
        final byte[] a;
        {

            buf = buf.asReadOnlyBuffer();

            final int len = buf.remaining();

            a = new byte[len];

            buf.get(a);

        }

        return a;

    }
    

    /**
     * Converts a byte array into a binary string. Useful for debugging.
     * 
     * @param zOrderByteArray
     * @return
     */
    public static String byteArrToBinaryStr(byte[] zOrderByteArray) {
       
       final StringBuffer buf = new StringBuffer();
       for (int i=0; i<zOrderByteArray.length*8; i++) {
          buf.append(BytesUtil.getBit(zOrderByteArray, i) ? "1" : "0");
       }
       
       return buf.toString();
    }

}
