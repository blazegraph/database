/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
package com.bigdata.blueprints;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.tinkerpop.blueprints.Element;

/**
 * Base class for {@link BigdataVertex} and {@link BigdataEdge}.  Handles
 * property-related methods.
 * 
 * @author mikepersonick
 *
 */
public abstract class BigdataElement implements Element {

	private static final List<String> blacklist = Arrays.asList(new String[] {
		"id", ""	
	});
	
	protected final URI uri;
	protected final BigdataGraph graph;
	
	public BigdataElement(final URI uri, final BigdataGraph graph) {
		this.uri = uri;
		this.graph = graph;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getProperty(final String property) {
		
		final URI p = graph.factory.toPropertyURI(property);
		
		return (T) graph.getProperty(uri, p);
		
	}

	@Override
	public Set<String> getPropertyKeys() {
		
		return graph.getPropertyKeys(uri);
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T removeProperty(final String property) {

		final URI p = graph.factory.toPropertyURI(property);
		
		return (T) graph.removeProperty(uri, p);
		
	}

	@Override
	public void setProperty(final String property, final Object val) {

		if (property == null || blacklist.contains(property)) {
			throw new IllegalArgumentException();
		}
		
		final URI p = graph.factory.toPropertyURI(property);
		
		final Literal o = graph.factory.toLiteral(val);
		
		graph.setProperty(uri, p, o);

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((graph == null) ? 0 : graph.hashCode());
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BigdataElement other = (BigdataElement) obj;
		if (graph == null) {
			if (other.graph != null)
				return false;
		} else if (!graph.equals(other.graph))
			return false;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

	@Override
    public String toString() {
        return uri.toString();
    }


}
