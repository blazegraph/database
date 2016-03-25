package it.unimi.dsi.lang;

import java.lang.reflect.Array;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2006-2009 Paolo Boldi and Sebastiano Vigna 
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

/** A class providing static methods and objects that do useful things 
 * with {@linkplain it.unimi.dsi.lang.FlyweightPrototype flyweight protoypes}. 
 */

public class FlyweightPrototypes {

	protected FlyweightPrototypes() {}
	
	/** Creates a flyweight copy of an array of {@linkplain it.unimi.dsi.lang.FlyweightPrototype flyweight prototypes}.
	 * 
	 * @param <T> the type of {@link FlyweightPrototype} you want to copy, that is, the
	 * type of the elements of <code>prototype</code>.
	 * @param prototype an array of prototypes.
	 * @return a flyweight copy of <code>prototype</code>, obtained by invoking
	 * {@link FlyweightPrototype#copy()} on each element.
	 */
	
	@SuppressWarnings("unchecked")
	public static <T extends FlyweightPrototype<T>> T[] copy( final T[] prototype ) {
		final T[] result = (T[])Array.newInstance( prototype.getClass().getComponentType(), prototype.length );
		for( int i = 0; i < result.length; i++ ) result[ i ] = prototype[ i ].copy();
		return result;
	}

	/** Creates a flyweight copy of the given object, or returns <code>null</code> if the given object is <code>null</code>.
	 * 
	 * @param <T> the type of {@link FlyweightPrototype} you want to copy, that is, the
	 * type of <code>prototype</code>.
	 * @param prototype a prototype to be copied, or <code>null</code>.
	 * @return <code>null</code>, if <code>prototype</code> is <code>null</code>;
	 * otherwise,a flyweight copy of <code>prototype</code>.
	 */
	public static <T extends FlyweightPrototype<T>> T copy( final T prototype ) {
		return prototype != null ? prototype.copy() : null;
	}
}
