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

/** A callback for the {@linkplain it.unimi.dsi.parser.BulletParser bullet parser}.
 *
 * <P>This interface is very loosely inspired to the SAX2 interface. However, it
 * strives to be simple, and to be StringFree&trade;.
 * 
 * <P>By contract, all implementations of this interface are bound to be <em>reusable</em>:
 * by calling {@link #startDocument()}, a callback can be used again.
 * It <strong>must</strong> be safe to call {@link #startDocument()} any number of times.
 * 
 */

public interface Callback {

	/** A singleton empty callback array. */
	Callback[] EMPTY_CALLBACK_ARRAY = new Callback[ 0 ];

	/** Configure the parser for usage with this callback.
	 *
	 * <P>When a callback is registered with a parser, it needs to set up 
	 * the parser so that all data required by the callback is actually parsed.
	 * The configuration <strong>must</strong> be a monotone process&mdash;you
	 * can only <em>set</em> properties and <em>add</em> attribute types to
	 * be parsed.
	 */
	void configure( BulletParser parser );

	/** Receive notification of the beginning of the document.
	 *
	 * <P>The callback must use this method to reset its internal state so
	 * that it can be resued. It <strong>must</strong> be safe to invoke this method
	 * several times.
	 */
	void startDocument();

	/** Receive notification of the start of an element.
	 *
	 * <P>For simple elements, this is the only notification that the
	 * callback will ever receive.
	 *
	 * @param element the element whose opening tag was found.
	 * @param attrMap a map from {@link it.unimi.dsi.parser.Attribute}s to {@link MutableString}s.
	 * @return true to keep the parser parsing, false to stop it.
	 */
	boolean startElement( Element element, Map<Attribute,it.unimi.dsi.lang.MutableString> attrMap );

	/** Receive notification of the end of an element.
	 *
	 * <strong>Warning</strong>: unless specific decorators are used, in 
	 * general a callback will just receive notifications for elements
	 * whose closing tag appears <em>explicitly</em> in the document.
	 *
	 * <P>This method will never be called for element without closing tags,
	 * even if such a tag is found. 
	 * 
	 * @param element the element whose closing tag was found.
	 * @return true to keep the parser parsing, false to stop it.
	 */
	boolean endElement( Element element );

	/** Receive notification of character data inside an element.
	 * 
	 * <p>You must not write into <code>text</code>, as it could be passed
	 * around to many callbacks.
	 *
	 * <P><code>flowBroken</code> will be true iff
	 * the flow was broken before <code>text</code>. This feature makes it possible
	 * to extract quickly the text in a document without looking at the elements. 
	 *
	 * @param text an array containing the character data.
	 * @param offset the start position in the array.
     * @param length the number of characters to read from the array.
	 * @param flowBroken whether the flow is broken at the start of <code>text</code>.
	 * @return true to keep the parser parsing, false to stop it.
	 */
	boolean characters( char[] text, int offset, int length, boolean flowBroken );

	/** Receive notification of the content of a CDATA section. 
	 * 
	 * <p>CDATA sections in an HTML document are the result of meeting
	 * a <samp>STYLE</samp> or <samp>SCRIPT</samp> element. In that case, the element
	 * will be passed as first argument.
	 * 
	 * <p>You must not write into <code>text</code>, as it could be passed
	 * around to many callbacks.
	 *
	 * @param element the element enclosing the CDATA section, or <code>null</code> if the
	 * CDATA section was created with explicit markup.
	 * @param text an array containing the character data.
	 * @param offset the start position in the array.
     * @param length the number of characters to read from the array.
	 * @return true to keep the parser parsing, false to stop it.
	 */
	boolean cdata( Element element, char[] text, int offset, int length );

	/**    Receive notification of the end of the document. */

	void endDocument();
}
