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

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;

import java.util.Map;

/** A decorator that prints on standard error all calls to the underlying callback.
 */
public class DebugCallbackDecorator implements Callback {

	/** The underlying callback. */
	private final Callback callback;

	public DebugCallbackDecorator( final Callback callback ) {
		this.callback = callback;
	}
	
	public boolean cdata( final Element element, final char[] text, final int offset, final int length ) {
		System.err.println( "cdata(" + new String( text, offset, length ) + ")" );
		return callback.cdata( element, text, offset, length );
	}

	
	public boolean characters( final char[] text, final int offset, final int length, final boolean flowBroken ) {
		System.err.println( "characters(" + new String( text, offset, length ) + ", " + flowBroken + ")" );
		return callback.characters( text, offset, length, flowBroken );
	}
	
	
	public void configure( final BulletParser parser ) {
		System.err.println( "configure()" );
		callback.configure( parser );
	}
	
	
	public void endDocument() {
		System.err.println( "endDocument()" );
		callback.endDocument();
	}
	
	public boolean endElement( final Element element ) {
		System.err.println( "endElement(" + element + ")" );
		return callback.endElement( element );
	}
	
	public boolean equals( final Object obj ) {
		return callback.equals( obj );
	}

	public int hashCode() {
		return callback.hashCode();
	}

	public void startDocument() {
		System.err.println( "startDocument()" );
		callback.startDocument();
	}
	
	public boolean startElement( final Element element, final Map<Attribute,MutableString> attrMap ) {
		System.err.println( "endElement(" + element + ", " + attrMap + ")" );
		return callback.startElement( element, attrMap );
	}

	public String toString() {
		return this.getClass().getName() + "(" + callback.toString() + ")";
	}
}
