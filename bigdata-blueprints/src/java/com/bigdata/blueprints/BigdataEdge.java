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

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDFS;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Edge implementation that wraps an Edge statement and points to a 
 * {@link BigdataGraph} instance.
 * 
 * @author mikepersonick
 *
 */
public class BigdataEdge extends BigdataElement implements Edge {

	private static final List<String> blacklist = Arrays.asList(new String[] {
	        "id", "", "label"
		});
		
	protected final Statement stmt;
	
	public BigdataEdge(final Statement stmt, final BigdataGraph graph) {
		super(stmt.getPredicate(), graph);
		
		this.stmt = stmt;
	}
	
	@Override
	public Object getId() {
		return graph.factory.fromEdgeURI(uri);
	}

	@Override
	public void remove() {
		graph.removeEdge(this);
	}

	@Override
	public String getLabel() {
		return (String) graph.getProperty(uri, RDFS.LABEL);
	}

	@Override
	public Vertex getVertex(final Direction dir) throws IllegalArgumentException {
		
		if (dir == Direction.BOTH) {
			throw new IllegalArgumentException();
		}
		
		final URI uri = (URI)  
				(dir == Direction.OUT ? stmt.getSubject() : stmt.getObject());
		
		final String id = graph.factory.fromVertexURI(uri);
		
		return graph.getVertex(id);
		
	}
	
	@Override
	public void setProperty(final String property, final Object val) {

		if (property == null || blacklist.contains(property)) {
			throw new IllegalArgumentException();
		}

		super.setProperty(property, val);

	}

    @Override
    public String toString() {
        final URI s = (URI) stmt.getSubject();
        final URI p = (URI) stmt.getPredicate();
        final URI o = (URI) stmt.getObject();
        return "e["+p.getLocalName()+"]["+s.getLocalName()+"->"+o.getLocalName()+"]";
    }
    
}
