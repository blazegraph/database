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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;

import java.io.Serializable;

/** A boolean-list based implementation of {@link BitVector}.
 * 
 * <P>This implementation of a bit vector is based on a backing
 * list of booleans. It is rather inefficient, but useful for
 * wrapping purposes, for covering completely the code in 
 * {@link AbstractBitVector} and for creating mock objects.
 */
public class BooleanListBitVector extends AbstractBitVector implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The backing list. */
	final private BooleanList list;
	
	protected static final void ensureIntegerIndex( final long index ) {
		if ( index > Integer.MAX_VALUE ) throw new IllegalArgumentException( "This BitVector implementation accepts integer indices only" );
	}
	
	public static BooleanListBitVector getInstance( final long capacity ) {
		if ( capacity > Integer.MAX_VALUE ) throw new IllegalArgumentException( "This BitVector implementation accepts integer indices only" );
		return new BooleanListBitVector( (int)capacity );
	}

	/** Creates a new empty bit vector. */
	public static BooleanListBitVector getInstance() {
		return new BooleanListBitVector( 0 );
	}
	
	/** Creates a new bit vector with given bits. */
	public static BooleanListBitVector of( final int... bit ) {
		final BooleanListBitVector bitVector = BooleanListBitVector.getInstance( bit.length );
		for( int b : bit ) bitVector.add( b );
		return bitVector;
	}
	

	protected BooleanListBitVector( final BooleanList list ) { this.list = list; }
	
	protected BooleanListBitVector( final int capacity ) {
		this( new BooleanArrayList( capacity ) );
	}
	
	public static BooleanListBitVector wrap( final BooleanList list ) {
		return new BooleanListBitVector( list );
	}
	
	public long length() {
		return list.size();
	}
	
	public boolean set( final long index, final boolean value ) {
		ensureIntegerIndex( index );
		return list.set( (int)index, value );
	}
	
	public boolean getBoolean( final long index ) {
		ensureIntegerIndex( index );
		return list.getBoolean( (int)index );
	}
	
	public void add( final long index, final boolean value ) {
		ensureIntegerIndex( index );
		list.add( (int)index, value );
	}

	public boolean removeBoolean( final long index ) {
		ensureIntegerIndex( index );
		return list.removeBoolean( (int)index );
	}
	
	public BitVector copy( final long from, final long to ) {
		BitVectors.ensureFromTo( length(), from, to );
		return new BooleanListBitVector( list.subList( (int)from, (int)to ) );
	}
	
	public BitVector copy() {
		return new BooleanListBitVector( new BooleanArrayList( list ) );
	}

	public BitVector ensureCapacity( final long numBits ) {
		if ( numBits > Integer.MAX_VALUE ) throw new IllegalArgumentException( "This BitVector implementation accepts integer indices only" );
		list.size( (int)numBits );
		return this;
	}

	public BitVector length( final long numBits ) {
		if ( numBits > Integer.MAX_VALUE ) throw new IllegalArgumentException( "This BitVector implementation accepts integer indices only" );
		list.size( (int)numBits );
		return this;
	}
}
