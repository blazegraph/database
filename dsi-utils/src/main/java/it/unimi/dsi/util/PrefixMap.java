package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2004-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.fastutil.objects.Object2ObjectFunction;

/** A map from prefixes to string intervals (and possibly <i>vice versa</i>).
 * 
 * <p>Instances of this class provide the services of a {@link StringMap}, but by assuming
 * the strings are lexicographically ordered, they can provide further information by
 * exposing a {@linkplain #rangeMap() function from string prefixes to intervals} and a
 * {@linkplain #prefixMap() function from intervals to string prefixes}.
 * 
 * <p>In the first case, given a prefix, we can ask for the range of strings starting
 * with that prefix, expressed as an {@link Interval}. This information is very useful to 
 * satisfy prefix queries (e.g., <samp>monitor*</samp>) with a brute-force approach.
 * 
 * <P>Optionally, a prefix map may provide the opposite service: given an interval of terms, it
 *  may provide the maximum common prefix. This feature can be checked for by calling 
 *  {@link #prefixMap()}.
 *
 * @author Sebastiano Vigna 
 * @since 0.9.2
 */

public interface PrefixMap<S extends CharSequence> extends StringMap<S> {
	/** Returns a function mapping prefixes to ranges of strings.
	 * 
	 * @return a function mapping prefixes to ranges of strings.
	 */
	public Object2ObjectFunction<CharSequence, Interval> rangeMap();
	
	/** Returns a function mapping ranges of strings to common prefixes (optional operation).
	 * 
	 * @return a function mapping ranges of strings to common prefixes, or <code>null</code> if this
	 * map does not support prefixes.
	 */
	public Object2ObjectFunction<Interval, S> prefixMap();
}
