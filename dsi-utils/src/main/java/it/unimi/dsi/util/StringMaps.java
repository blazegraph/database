package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2008-2009 Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.objects.Object2ObjectFunctions;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectLists;

import java.io.Serializable;

/** A class providing static methods and objects that do useful things with {@linkplain StringMap string maps}
 * and {@linkplain PrefixMap prefix maps}.
 * 
 * @see StringMap
 * @see PrefixMap
 * @author Sebastiano Vigna
 */

public class StringMaps {
	private StringMaps() {}
	
	protected static class SynchronizedStringMap<S extends CharSequence> implements StringMap<S>, Serializable {
		private static final long serialVersionUID = 1L;
		protected final StringMap<S> stringMap;
		protected ObjectList<? extends S> list;

		public SynchronizedStringMap( final StringMap<S> stringMap ) {
			this.stringMap = stringMap;
		}

		public synchronized int size() {
			return stringMap.size();
		}

		public synchronized ObjectList<? extends S> list() {
			if ( list == null ) {
				list = stringMap.list();
				if( list != null ) list = ObjectLists.synchronize( list, this ); 
			}
			return list;
		}

		public synchronized long getLong( Object s ) {
			return stringMap.getLong( s );
		}	

		public synchronized Long get( Object key ) {
			return stringMap.get( key );
		}

		public synchronized long put( CharSequence key, long value ) {
			return stringMap.put(  key, value );
		}

		public synchronized Long put( CharSequence key, Long value ) {
			return stringMap.put( key, value );
		}

		public synchronized Long remove( Object key ) {
			return stringMap.remove( key );
		}

		public synchronized long removeLong( Object key ) {
			return stringMap.removeLong( key );
		}
		
		public synchronized void clear() {
			stringMap.clear();
		}

		public synchronized boolean containsKey( Object key ) {
			return stringMap.containsKey( key );
		}

		public synchronized long defaultReturnValue() {
			return stringMap.defaultReturnValue();
		}

		public synchronized void defaultReturnValue( long rv ) {
			stringMap.defaultReturnValue( rv );
		}
	}
	

	protected static class SynchronizedPrefixMap<S extends CharSequence> extends SynchronizedStringMap<S> implements PrefixMap<S>, Serializable {
		private static final long serialVersionUID = 1L;
		protected final PrefixMap<S> map;
		protected Object2ObjectFunction<Interval, S> prefixMap;
		protected Object2ObjectFunction<CharSequence, Interval> rangeMap;

		public SynchronizedPrefixMap( final PrefixMap<S> map ) {
			super( map );
			this.map = map;
		}

		public synchronized Object2ObjectFunction<Interval, S> prefixMap() {
			if ( prefixMap == null ) {
				prefixMap = map.prefixMap();
				if ( prefixMap != null ) prefixMap = Object2ObjectFunctions.synchronize( prefixMap, this );
			}
			return prefixMap;
		}

		public synchronized Object2ObjectFunction<CharSequence, Interval> rangeMap() {
			if ( rangeMap == null ) {
				rangeMap = map.rangeMap();
				if ( rangeMap != null ) rangeMap = Object2ObjectFunctions.synchronize( rangeMap, this );
			}
			return rangeMap;
		}


	}
	
	/** Returns a synchronized string map backed by the given string map.
     *
     * @param stringMap the string map to be wrapped in a synchronized map.
     * @return a synchronized view of the specified string map.
     */
	public static <T extends CharSequence> StringMap<T> synchronize( final StringMap<T> stringMap ) {
		return stringMap instanceof PrefixMap ? new SynchronizedPrefixMap<T>( (PrefixMap<T>)stringMap ) : new SynchronizedStringMap<T>( stringMap );
	}

	/** Returns a synchronized prefix map backed by the given prefix map.
    *
    * @param prefixMap the prefix map to be wrapped in a synchronized map.
    * @return a synchronized view of the specified prefix map.
    */
	public static <T extends CharSequence> PrefixMap<T> synchronize( final PrefixMap<T> prefixMap ) {
		return new SynchronizedPrefixMap<T>( prefixMap );
	}
}
