/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
package com.bigdata.rdf.sparql.ast;

import java.io.Serializable;

/**
 * A glue class for reporting the namespace of the lexicon relation and the
 * timestamp of the view of the lexicon relation through the function bops.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class GlobalAnnotations implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6220818059592500418L;

	/**
	 * The namespace of the lexicon relation.
	 */
	public final String lex;
	
	/**
	 * The timestamp of the view of the lexicon relation.
	 */
	public final long timestamp;

    /**
     * 
     * @param lex
     *            The namespace of the lexicon relation.
     * @param timestamp
     *            The timestamp of the view of the lexicon relation.
     */
	public GlobalAnnotations(final String lex, final long timestamp) {
		this.lex = lex;
		this.timestamp = timestamp;
	}

	/**
	 * Automatically generated.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((lex == null) ? 0 : lex.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	/**
	 * Automatically generated.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GlobalAnnotations other = (GlobalAnnotations) obj;
		if (lex == null) {
			if (other.lex != null)
				return false;
		} else if (!lex.equals(other.lex))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}
	
}
