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

/** A callback extracting text and titles.
 * 
 * <P>This callbacks extracts all text in the page, and the title.
 * The resulting
 * text is available through {@link #text}, and the title through {@link #title}.
 * 
 * <P>Note that {@link #text} and {@link #title} are never trimmed. 
 */


public class TextExtractor extends DefaultCallback {

	/** The text resulting from the parsing process. */
	public final MutableString text = new MutableString();
	/** The title resulting from the parsing process. */
	public final MutableString title = new MutableString();
	/** True if we are in the middle of the title. */
	private boolean inTitle;

	/**
	 * Configure the parser to parse text. 
	 */

	public void configure( final BulletParser parser ) {
		parser.parseText( true );
		// To get the title.
		parser.parseTags( true );
	}

	public void startDocument() {
		text.length( 0 );
		title.length( 0 );
		inTitle = false;
	}

	public boolean characters( final char[] characters, final int offset, final int length, final boolean flowBroken ) {
		text.append( characters, offset, length );
		if ( inTitle ) title.append( characters, offset, length );
		return true;
	}
	
	public boolean endElement( final Element element ) {
		// No element is allowed inside a title.
		inTitle = false;
		if ( element.breaksFlow ) {
			if ( inTitle ) title.append( ' ' );
			text.append( ' ' );
		}
		return true;
	}
	
	public boolean startElement( final Element element, final Map<Attribute,MutableString> attrMapUnused ) {
		// No element is allowed inside a title.
		inTitle = element == Element.TITLE;
		if ( element.breaksFlow ) {
			if ( inTitle ) title.append( ' ' );
			text.append( ' ' );
		}
		return true;
	}
	
}
