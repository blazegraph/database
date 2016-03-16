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

import it.unimi.dsi.lang.MutableString;

/** An SGML character entity. */

public final class Entity {

    /** The name of this entity. */
	public final CharSequence name;
	/** The Unicode character corresponding to this entity. */
	public final char character;

	/** Creates a new entity with the specified name and character.
	 *
	 * @param name the name of the new entity.
	 * @param character its character value.
	 */
	public Entity( final CharSequence name, final char character ) {
		this.name = new MutableString( name );
		this.character = character;
	}

	/** Returns the name of this entity.
	 * @return the name of this entity.
	 */
	
	public String toString() {
		return name.toString();
	}
}
