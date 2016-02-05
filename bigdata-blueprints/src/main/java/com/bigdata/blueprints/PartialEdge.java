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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class PartialEdge extends PartialElement implements Edge {
    
    private String label;
    
    private Vertex from;
    
    private Vertex to;
    
    public PartialEdge(final String id) {
        super(id);
    }

    public PartialEdge(final String id, final String from, final String to) {
        super(id);
        setFrom(new PartialVertex(from));
        setTo(new PartialVertex(to));
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Vertex getVertex(final Direction dir) throws IllegalArgumentException {
        
        if (dir == Direction.OUT) {
            return from;
        } else if (dir == Direction.IN) {
            return to;
        }
        
        throw new IllegalArgumentException();
        
    }
    
    public void setLabel(final String label) {
        this.label = label;
    }
    
    public void setFrom(final Vertex v) {
        this.from = v;
    }
    
    public void setTo(final Vertex v) {
        this.to = v;
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("id: " + super.getId());
        sb.append(", from: " + from.getId());
        sb.append(", to: " + to.getId());
        sb.append(", label: " + label);
        sb.append(", props: ");
        super.appendProps(sb);
        return sb.toString();
    }
    
}
