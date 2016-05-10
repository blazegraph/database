package it.unimi.dsi.util;

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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/** A set of interned mutable strings.
 *
 * <p>This class extends {@link it.unimi.dsi.fastutil.objects.ObjectOpenHashSet} by
 * providing an {@link #intern(MutableString)} method with a semantics similar to
 * that of {@link String#intern()}.
 */

public class InternedMutableStringSet extends ObjectOpenHashSet<MutableString> {

	public final static class Term extends MutableString {
		private static final long serialVersionUID = 0L;
		public int lastDocument;
		public int lastPosition = -1;
		public final FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream( 1 );
		public final OutputBitStream obs = new OutputBitStream( fbaos, 0 );
		public Term( MutableString s ) {
			super( s );
		}
		
		public void addOccurrence( int document, int position ) throws IOException {
			obs.writeDelta( document - lastDocument );
			if ( document != lastDocument ) lastPosition = -1;
			obs.writeDelta( position - lastPosition - 1 );
			lastDocument = document;
			lastPosition = position;
		}
	}

	private static final long serialVersionUID = 0L;
	private int free;
	private int p;
	private int count;
	private byte[] state;

	public InternedMutableStringSet() {
		super();
	}

	public InternedMutableStringSet( final int n, final float f ) {
		super( n, f );
	}

	public InternedMutableStringSet( final int n ) {
		super( n );
	}
	
	/** Returns an interned, canonical copy contained in this set of the specified mutable string.
	*
	* <p>The semantics of this method is essentially the same as that of
	* {@link java.util.Collection#add(Object)}, but 
	* this method will return a mutable string
	* equal to <code>s</code> currently in this set. The string will
	* <em>never</em> be <code>s</code>, as in the case <code>s</code> is
	* not in this set a {@linkplain MutableString#compact() compact copy}
	* of <code>s</code> will be stored instead.
	*
	* <p>The purpose of this method is similar to that of {@link String#intern()},
	* but obviously here the user has much greater control.
	*
	* @param s the mutable string that must be interned.
	* @return the mutable string equal to <code>s</code> stored in this set.
	*/

	public Term intern( final MutableString s ) {
		// Duplicate code from add()--keep in line!
		final int i = findInsertionPoint( s );
		if ( i < 0 ) return (Term)(key[ -( i + 1 ) ]);

		if ( state[ i ] == FREE ) free--;
		state[ i ] = OCCUPIED;
		final Term t = (Term)( key[ i ] = new Term( s ) );

		if ( ++count >= maxFill ) {
			int newP = Math.min( p + growthFactor(), PRIMES.length - 1 );
			// Just to be sure that size changes when p is very small.
			while( PRIMES[ newP ] == PRIMES[ p ] ) newP++;
			rehash( newP ); // Table too filled, let's rehash
		}
		if ( free == 0 ) rehash( p );
		return t;
	}
	
	//Copied from add(...) in the fastutil 6.5.11
    private int findInsertionPoint(MutableString k) {
        int pos = ( (k) == null ? 0x87fcd5c : it.unimi.dsi.fastutil.HashCommon.murmurHash3( (k).hashCode() ^ mask ) ) & mask;
          // There's always an unused entry.
          while( used[ pos ] ) {
           if ( ( (key[ pos ]) == null ? (k) == null : (key[ pos ]).equals(k) ) ) return pos;
           pos = ( pos + 1 ) & mask;
          }
        // TODO Auto-generated method stub
        return pos;
    }
}
