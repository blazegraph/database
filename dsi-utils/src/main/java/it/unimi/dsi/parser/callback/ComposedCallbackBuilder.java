package it.unimi.dsi.parser.callback;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;

import java.util.Map;

/** A builder for composed callbacks. 
 * 
 * <P>To compose a series of callbacks, you must first create an instance
 * of this class, {@linkplain #add(Callback) add all required callbacks}, and
 * finally {@linkplain #compose() get the composed callback}, which will invoke (in order)
 * the callbacks.   
 */

public class ComposedCallbackBuilder {

	/** A sequence of callbacks to be called int turn. */
	
	private static final class ComposedCallback implements Callback {
		/** The number of callbacks in this composer. */
		final int size;

		/** The callback array. */	
		private Callback[] callback;

		/** An array of boolean representing continuation of the corresponding callback in {@link #callback}. */ 
		private boolean[] cont;

		private final ObjectArrayList<Callback> callbacks;

		private ComposedCallback( final ObjectArrayList<Callback> callbacks ) {
			super();
			this.callbacks = callbacks;
			this.size = callbacks.size();
			this.cont = new boolean[ size ];
			this.callback = new Callback[ size ];
			this.callbacks.toArray( callback );
		}

		public void configure( final BulletParser parser ) {
			for( int i = 0; i < size; i++ ) callback[ i ].configure( parser );
		}

		public void startDocument() {
			for( int i = 0; i < size; i++ ) {
				callback[ i ].startDocument();
				cont[ i ] = true;
			}
		}

		public boolean startElement( final Element tag, final Map<Attribute,MutableString> attrList ) {
			boolean retValue = false;
			for( int i = 0; i < size; i++ ) {
				if ( cont[ i ] && ! callback[ i ].startElement( tag, attrList ) ) cont[ i ] = false;
				retValue |= cont[ i ];
			}
			return retValue;
		}

		public boolean endElement( final Element tag ) { 
			boolean retValue = false;
			for( int i = 0; i < size; i++ ) {
				if ( cont[ i ] && ! callback[ i ].endElement( tag ) ) cont[ i ] = false;
				retValue |= cont[ i ];
			}
			return retValue;
		}

		public boolean characters( final char[] text, final int offset, final int length, final boolean flowBroken ) {
			boolean retValue = false;
			for( int i = 0; i < size; i++ ) {
				if ( cont[ i ] && ! callback[ i ].characters( text, offset, length, flowBroken ) ) cont[ i ] = false;
				retValue |= cont[ i ];
			}
			return retValue;
		}

		public boolean cdata( final Element element, final char[] text, final int offset, final int length ) {
			boolean retValue = false;
			for( int i = 0; i < size; i++ ) {
				if ( cont[ i ] && ! callback[ i ].cdata( element, text, offset, length ) ) cont[ i ] = false;
				retValue |= cont[ i ];
			}
			return retValue;
		}

		public void endDocument() {
			for( int i = 0; i < size; i++ ) callback[ i ].endDocument();
		}
	}

	/** The current list of callbacks. */
	private final ObjectArrayList<Callback> callbacks = ObjectArrayList.wrap( Callback.EMPTY_CALLBACK_ARRAY );
	
	/** Creates a new, empty callback composer. */
	public ComposedCallbackBuilder()  {}

	/** Adds a new callback to this builder at a specified position.
	 * 
	 * @param position a position in the current callback list.
	 * @param callback a callback.
	 */
	public void add( final int position, final Callback callback ) {
		callbacks.add( position, callback );
	}
	
	/** Adds a new callback to this builder.
	 * 
	 * @param callback a callback.
	 */

	public void add( final Callback callback ) {
		callbacks.add( callback );
	}

	/** Checks whether this callback builder is empty. 
	 * 
	 * @return true if this callback builder is empty.
	 */
	
	public boolean isEmpty() {
		return callbacks.isEmpty();
	}

	/** Returns the number of callbacks in this builder. 
	 *
	 * @return the number of callbacks in this composer. 
	 */
	public int size() {
		return callbacks.size();
	}

	/** Returns the composed callback produced by this builder.
	 * 
	 * @return a composed callback. 
	 */
	public Callback compose() {
		if ( isEmpty() ) return DefaultCallback.getInstance();
		if ( size() == 1 ) return callbacks.get( 0 );

		return new ComposedCallback( callbacks );
	}
}
