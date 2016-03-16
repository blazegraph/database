package it.unimi.dsi.lang;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2006-2009 Sebastiano Vigna 
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

/** A prototype providing flyweight copies.
 * 
 * <p><em>Flyweight copies</em> are useful to implement multithreading on read-only
 * (but maybe stateful) classes. An instance of a class implementing this interface 
 * is not necessarily thread safe,
 * but it can be (thread-) safely copied many times (i.e., it can be used as a prototype). 
 * All copies will share as much as possible of the class read-only
 * state (so they are flyweight).
 * 
 * <p>In the case an implementation is stateless, it can of course return always the same singleton
 * instance as a copy. At the other extreme, a stateful class may decide to synchronise its
 * methods and return itself as a copy instead. Note that in general the object returned
 * by {@link #copy()} must replicate the <em>current state</em> of the object, not
 * the object state at creation time. This might require some calls to methods that
 * modify the class internal state: in particular, one should always check whether such
 * methods are pointed out in the documentation of superclasses.
 * 
 * <p><strong>Warning</strong>: if {@link #copy()} accesses mutable internal state, setters
 * and {@link #copy()} must be suitably synchronised.
 * 
 * <p>Implementing subclasses are invited to use covariant return-type overriding to
 * make {@link #copy()} return the right type.
 */

public interface FlyweightPrototype<T extends FlyweightPrototype<T>> {

	/** Returns a copy of this object, sharing state with this object as much as possible. */ 
	public T copy();
}
