package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2003-2009 Paolo Boldi and Sebastiano Vigna 
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

import java.util.Comparator;

                                                                                                                                                             
/** A class providing static methods and objects that do useful things with intervals. 
 * 
 * @see Interval
 */

public class Intervals {

	private Intervals() {}

	public final static Interval[] EMPTY_ARRAY = {};

	/** An empty (singleton) interval. */
	public final static Interval EMPTY_INTERVAL = new Interval( 1, 0 );

	/** A singleton located at &minus;&infty;. */
	public final static Interval MINUS_INFINITY = new Interval( Integer.MIN_VALUE, Integer.MIN_VALUE );

	/** A comparator between intervals defined as follows: 
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>'] iff
	 * the first interval starts before or prolongs the second one, that is,  
	 * iff <var>a</var> &lt; <var>a</var>' or <var>a</var>=<var>a</var>' and <var>b</var>' &lt; <var>b</var>.
	 */
	public final static Comparator<Interval> STARTS_BEFORE_OR_PROLONGS = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.left != i2.left ) return i1.left < i2.left ? -1 : 1;
			if ( i1.right != i2.right ) return i2.right < i1.right ? -1 : 1;
			return 0;
		}
	};

	/** A comparator between intervals defined as follows: 
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>'] iff
	 * the first interval ends before or is a suffix of the second one, that is,  
	 * iff <var>b</var> &lt; <var>b</var>' or <var>b</var>=<var>b</var>' and <var>a</var>' &lt; <var>a</var>.
	 */
	public final static Comparator<Interval> ENDS_BEFORE_OR_IS_SUFFIX = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.right != i2.right ) return i1.right < i2.right ? -1 : 1;
			if ( i1.left != i2.left ) return i2.left < i1.left ? -1 : 1;
			return 0;
		}
	};

	/** A comparator between intervals defined as follows:
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>']
	 * iff the first interval starts <em>after</em> the second one, that is, 
	 * iff <var>a</var>' &lt; <var>a</var>.
	 */
	public final static Comparator<Interval> STARTS_AFTER = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.left != i2.left ) return i2.left < i1.left ? -1 : 1;
			return 0;
		}
	};                                                                                                                    

	/** A comparator between intervals defined as follows:
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>']
	 * iff the first interval starts <em>before</em> the second one, that is, 
	 * iff <var>a</var>' &gt; <var>a</var>.
	 */
	public final static Comparator<Interval> STARTS_BEFORE = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.left != i2.left ) return i2.left < i1.left ? 1 : -1;
			return 0;
		}
	};                                                                                                                    

	/** A comparator between intervals defined as follows:
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>']
	 * iff the first interval ends <em>after</em> the second one, that is, 
	 * iff <var>b</var>' &lt; <var>b</var>.
	 */
	public final static Comparator<Interval> ENDS_AFTER = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.right != i2.right ) return i2.right < i1.right ? -1 : 1;
			return 0;
		}
	};                                                                                                                    

	/** A comparator between intervals defined as follows:
	 * [<var>a</var>..<var>b</var>] is less than [<var>a</var>'..<var>b</var>']
	 * iff the first interval ends <em>before</em> the second one, that is, 
	 * iff <var>b</var>' &gt; <var>b</var>.
	 */
	public final static Comparator<Interval> ENDS_BEFORE = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			if ( i1.right != i2.right ) return i2.right < i1.right ? 1 : -1;
			return 0;
		}
	};                                                                                                                    

	/** A comparator between intervals based on their length. */
	public final static Comparator<Interval> LENGTH_COMPARATOR = new Comparator<Interval>() {
		public int compare( final Interval i1, final Interval i2 ) {
			return i1.length() - i2.length();
		}
	};                                                                                                                    
}
                                                                                                                                                        