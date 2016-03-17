package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2007-2009 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectFunction;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.Object2ObjectFunction;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.MutableString;

import java.io.Serializable;

/** An abstract implementation of a prefix map.
 * 
 * <p>This class provides the full serives of a {@link PrefixMap} by implementing just
 * {@link #getInterval(CharSequence)} and {@link #getTerm(int, MutableString)}
 */

public abstract class AbstractPrefixMap extends AbstractObject2LongFunction<CharSequence> implements PrefixMap<MutableString>, Serializable {
	private static final long serialVersionUID = 1L;
	protected Object2ObjectFunction<CharSequence, Interval> rangeMap;
	protected Object2ObjectFunction<Interval, MutableString> prefixMap;
	protected ObjectList<MutableString> list;
	
	// We must guarantee that, unless the user says otherwise, the default return value is -1.
	{
		defaultReturnValue( -1 );
	}
	/** Returns the range of strings having a given prefix.
	 * 
	 * @param prefix a prefix.
	 * @return the corresponding range of strings as an interval.
	 */
	protected abstract Interval getInterval( CharSequence prefix );
	/** Writes a string specified by index into a {@link MutableString}.
	 * 
	 * @param index the index of a string.
	 * @param string a mutable string.
	 * @return <code>string</code>.
	 */
	protected abstract MutableString getTerm( int index, MutableString string );
	
	public Object2ObjectFunction<CharSequence, Interval> rangeMap() {
		if ( rangeMap == null ) rangeMap = new AbstractObject2ObjectFunction<CharSequence, Interval>() {
			private static final long serialVersionUID = 1L;

			public boolean containsKey( final Object o ) {
				return get( o ) != Intervals.EMPTY_INTERVAL;
			}

			public int size() {
				return -1;
			}

			public Interval get( final Object o ) {
				return getInterval( (CharSequence)o );
			}
			
		};
		
		return rangeMap;
	}

	public Object2ObjectFunction<Interval, MutableString> prefixMap() {
		if ( prefixMap == null ) prefixMap = new AbstractObject2ObjectFunction<Interval, MutableString>() {
			private static final long serialVersionUID = 1L;

			public MutableString get( final Object o ) {
				final Interval interval = (Interval)o;
				final MutableString prefix = new MutableString();
				if ( interval == Intervals.EMPTY_INTERVAL || interval.left < 0 || interval.right < 0 ) throw new IllegalArgumentException();
				getTerm( interval.left, prefix );
				if ( interval.length() == 1 ) return prefix;
				final MutableString s = getTerm( interval.right, new MutableString() );
				final int l = Math.min( prefix.length(), s.length() );
				int i;
				for( i = 0; i < l; i++ ) if ( s.charAt( i ) != prefix.charAt( i ) ) break;
				return prefix.length( i );
			}

			public boolean containsKey( final Object o ) {
				Interval interval = (Interval)o;
				return interval != Intervals.EMPTY_INTERVAL && interval.left >= 0 && interval.right < AbstractPrefixMap.this.size();
			}

			public int size() {
				return -1;
			}
		};
		
		return prefixMap;
	}

	public ObjectList<MutableString> list() {
		if ( list == null ) list = new AbstractObjectList<MutableString>() {
			public int size() {
				return AbstractPrefixMap.this.size();
			}
			public MutableString get( int index ) {
				return getTerm( index, new MutableString() );
			}
		};
		
		return list;
	}		
}
