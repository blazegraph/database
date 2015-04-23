/*
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
package com.bigdata.ganglia.util;

/**
 * Utility class
 */
public class BytesUtil {

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
				sb.append(", ");

			// as an unsigned integer.
			// sb.append(Integer.toHexString(key[i] & 0xff));
			sb.append(Integer.toString(key[i] & 0xff));

		}

		sb.append("]");

		return sb.toString();

	}

	private static transient String NULL = "null";
}
