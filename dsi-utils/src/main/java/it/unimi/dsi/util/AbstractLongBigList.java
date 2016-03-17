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

import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.AbstractLongListIterator;
import it.unimi.dsi.fastutil.longs.LongListIterator;

import java.util.NoSuchElementException;

/** An abstract implementation of a {@link LongBigList}. Concrete subclasses must provide just
 * {@link LongBigList#length() length()} and {@link LongBigList#getLong(long) getLong()}.
 */

public abstract class AbstractLongBigList extends AbstractLongList implements LongBigList {

	 protected void ensureIndex( final long index ) {
		 if ( index < 0 ) throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
		 if ( index > length() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than list size (" + ( size() ) + ")" );
	 }

	 protected void ensureRestrictedIndex( final long index ) {
		 if ( index < 0 ) throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
		 if ( index >= length() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to list size (" + ( size() ) + ")" );
	 }

	public long set( long index, long value ) {
		throw new UnsupportedOperationException();
	}

	public void add( long index, long value ) {
		throw new UnsupportedOperationException();
	}
	
	public long removeLong( long index ) {
		throw new UnsupportedOperationException();
	}
	
	public LongBigList length( long newLength ) {
		throw new UnsupportedOperationException();
	}
	
	public long getLong( int index ) {
		return getLong( (long)index );
	}

	public int size() {
		final long length = length();
		if ( length > Integer.MAX_VALUE ) throw new IllegalStateException( "The number of elements of this big list (" + length + ") exceeds Integer.MAX_INT" );
		return (int)length;
	}

	protected static class LongSubBigList extends AbstractLongBigList implements java.io.Serializable {
		public static final long serialVersionUID = -7046029254386353129L;
		/** The list this sublist restricts. */
		protected final LongBigList l;
		/** Initial (inclusive) index of this sublist. */
		protected final long from;
		/** Final (exclusive) index of this sublist. */
		protected long to;

		private static final boolean ASSERTS = false;

		public LongSubBigList( final LongBigList l, final long from, final long to ) {
			this.l = l;
			this.from = from;
			this.to = to;
		}

		private void assertRange() {
			if ( ASSERTS ) {
				assert from <= l.length();
				assert to <= l.length();
				assert to >= from;
			}
		}

		public boolean add( final long k ) {
			l.add( to, k );
			to++;
			if ( ASSERTS ) assertRange();
			return true;
		}

		public void add( final int index, final long k ) {
			ensureIndex( index );
			l.add( from + index, k );
			to++;
			if ( ASSERTS ) assertRange();
		}

		public long getLong( long index ) {
			ensureRestrictedIndex( index );
			return l.getLong( from + index );
		}

		public long removeLong( long index ) {
			ensureRestrictedIndex( index );
			to--;
			return l.removeLong( from + index );
		}

		public long set( int index, long k ) {
			ensureRestrictedIndex( index );
			return l.set( from + index, k );
		}

		public void clear() {
			removeElements( 0, size() );
			if ( ASSERTS ) assertRange();
		}

		public long length() {
			return to - from;
		}

		public LongListIterator listIterator( final int index ) {
			ensureIndex( index );

			return new AbstractLongListIterator () {
				long pos = index, last = -1;

				public boolean hasNext() { return pos < size(); }
				public boolean hasPrevious() { return pos > 0; }
				public long nextLong() { if ( ! hasNext() ) throw new NoSuchElementException(); return l.getLong( from + ( last = pos++ ) ); }
				public long previousLong() { if ( ! hasPrevious() ) throw new NoSuchElementException(); return l.getLong( from + ( last = --pos ) ); }
				public int nextIndex() { return (int)pos; }
				public int previousIndex() { return (int)pos - 1; }
				public void add( long k ) {
					if ( last == -1 ) throw new IllegalStateException();
					LongSubBigList.this.add( pos++, k );
					last = -1;
					if ( ASSERTS ) assertRange();
				}
				public void set( long k ) {
					if ( last == -1 ) throw new IllegalStateException();
					LongSubBigList.this.set( last, k );
				}
				public void remove() {
					if ( last == -1 ) throw new IllegalStateException();
					LongSubBigList.this.removeLong( last );
					/*
					 * If the last operation was a next(), we are removing an element *before* us, and we
					 * must decrease pos correspondingly.
					 */
					if ( last < pos ) pos--;
					last = -1;
					if ( ASSERTS ) assertRange();
				}
			};
		}
	}

	public LongBigList subList( long from, long to ) {
		  ensureIndex( from );
		  ensureIndex( to );
		  if ( from > to ) throw new IndexOutOfBoundsException( "Start index (" + from + ") is greater than end index (" + to + ")" );

		  return new LongSubBigList( this, from, to );
	}
}
