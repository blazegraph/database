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

import java.util.Collection;
import java.util.LinkedList;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class BigdataGraphlet {

    private final Collection<? extends Vertex> vertices;
    private final Collection<? extends Edge> edges;
    
    public BigdataGraphlet() {
        this.vertices = new LinkedList<Vertex>();
        this.edges = new LinkedList<Edge>();
    }
    
    public BigdataGraphlet(final Collection<? extends Vertex> vertices, 
            final Collection<? extends Edge> edges) {
        this.vertices = vertices;
        this.edges = edges;
    }
    
    public Collection<? extends Vertex> getVertices() {
        return vertices;
    }
    
    public Collection<? extends Edge> getEdges() {
        return edges;
    }
    
}
