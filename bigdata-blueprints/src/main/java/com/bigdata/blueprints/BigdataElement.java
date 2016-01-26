/**
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
package com.bigdata.blueprints;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
		
		return (T) graph.getProperty(uri, property);
		
	}

	@Override
	public Set<String> getPropertyKeys() {
		
		return graph.getPropertyKeys(uri);
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T removeProperty(final String property) {

		return (T) graph.removeProperty(uri, property);
		
	}

	@Override
	public void setProperty(final String prop, final Object val) {

		if (prop == null || blacklist.contains(prop)) {
			throw new IllegalArgumentException();
		}
		
		graph.setProperty(uri, prop, val);

	}
	
//	/**
//	 * Simple extension for multi-valued properties.
//	 */
//	public void addProperty(final String prop, final Object val) {
//	    
//        if (prop == null || blacklist.contains(prop)) {
//            throw new IllegalArgumentException();
//        }
//        
//        graph.addProperty(uri, prop, val);
//	    
//	}
//
//	/**
//	 * Simple extension for multi-valued properties.
//	 */
//    @SuppressWarnings("unchecked")
//    public <T> List<T> getProperties(final String property) {
//        
//        return (List<T>) graph.getProperties(uri, property);
//        
//    }

	/**
	 * Generated code.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((graph == null) ? 0 : graph.hashCode());
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

    /**
     * Generated code.
     */
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
