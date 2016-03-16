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

/**
 * A default, do-nothing-at-all callback.
 * 
 * <P>Callbacks can inherit from this class and forget about methods they are not interested in.
 * 
 * <P>This class has a protected constructor. If you need an instance of this class, use
 * {@link #getInstance()}.
 */
public class DefaultCallback implements Callback {
	private static final DefaultCallback SINGLETON = new DefaultCallback();

	protected DefaultCallback() {}

	/**
	 * Returns the singleton instance of the default callback.
	 * 
	 * @return the singleton instance of the default callback.
	 */
	public static DefaultCallback getInstance() {
		return SINGLETON;
	}

	public void configure( final BulletParser parserUnused ) {}

	public void startDocument() {}

	public boolean startElement( final Element elementUnused, final Map<Attribute,MutableString> attrMapUnused ) {
		return true;
	}

	public boolean endElement( final Element elementUnused ) {
		return true;
	}

	public boolean characters( final char[] textUnused, final int offsetUnused, final int lengthUnused, final boolean flowBrokenUnused ) {
		return true;
	}

	public boolean cdata( final Element elementUnused, final char[] textUnused, final int offsetUnused, final int lengthUnused ) {
		return true;
	}

	public void endDocument() {}
}
