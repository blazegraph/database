package it.unimi.dsi.parser;

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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;

/** A factory for well-formed XML documents.
 * 
 * <p>This factory assumes that every new name of an element type or of an
 * attribute is new valid name. For entities, instead, resolution is
 * deferred to {@link it.unimi.dsi.parser.HTMLFactory}.
 * 
 * @author Sebastiano Vigna
 * @since 1.0.2
 */

public class WellFormedXmlFactory implements ParsingFactory {
	/** The load factor for all maps. */
	private static final float ONE_HALF = .5f;
	
	/** A (quick) map from attribute names to attributes. */
    final private Object2ObjectOpenHashMap<CharSequence,Attribute> name2Attribute = new Object2ObjectOpenHashMap<CharSequence,Attribute>( Hash.DEFAULT_INITIAL_SIZE, ONE_HALF );

    /** A (quick) map from element-type names to element types. */
    final private Object2ObjectOpenHashMap<CharSequence,Element> name2Element = new Object2ObjectOpenHashMap<CharSequence,Element>( Hash.DEFAULT_INITIAL_SIZE, ONE_HALF );
	
	public WellFormedXmlFactory() {}

	public Element getElement( final MutableString name ) {
		Element element = name2Element.get(name);
		if ( element == null ) {
			element = new Element(name);
			name2Element.put(element.name, element );
		}
		return element;
	}

	public Attribute getAttribute( final MutableString name ) {
		Attribute attribute = name2Attribute.get(name);
		if ( attribute == null ) {
			attribute = new Attribute(name);
			name2Attribute.put(attribute.name, attribute );
		}
		return attribute;
	}

	public Entity getEntity( final MutableString name ) {
		return HTMLFactory.INSTANCE.getEntity( name );
	}
}
