package it.unimi.dsi.bits;

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

import java.io.Serializable;

/** A generic transformation from objects of a given type to bit vector. Most useful
 * when adding strings, etc. to a trie.
 */

public interface TransformationStrategy<T> extends Serializable {
	/** Returns a bit vector representation of the given object.
	 * 
	 * @param object the object to be turned into bit representation.
	 * @return a bit vector representation of <code>object</code>.
	 */
	public BitVector toBitVector( T object );
	
	/** The (approximate) number of bits occupied by this transformation.
	 * 
	 * @return the (approximate) number of bits occupied by this transformation.
	 */
	public long numBits();
	
	/** Returns a copy of this transformation strategy.
	 * 
	 * @return a copy of this transformation strategy.
	 */
	public TransformationStrategy<T> copy();
}