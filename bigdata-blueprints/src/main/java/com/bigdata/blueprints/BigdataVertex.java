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
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * Vertex implementation that wraps a Vertex URI and points to a 
 * {@link BigdataGraph} instance.
 * 
 * @author mikepersonick
 *
 */
public class BigdataVertex extends BigdataElement implements Vertex {

    private static final transient Logger log = Logger.getLogger(BigdataVertex.class);
    
	public BigdataVertex(final URI uri, final BigdataGraph graph) {
		super(uri, graph);
	}
	
	@Override
	public Object getId() {
	    
        if (log.isInfoEnabled())
            log.info("");
	    
		return graph.factory.fromURI(uri);
		
	}
	
	@Override
	public void remove() {
	    
        if (log.isInfoEnabled())
            log.info("");

		graph.removeVertex(this);
		
	}

	@Override
	public Edge addEdge(final String label, final Vertex to) {
	    
        if (log.isInfoEnabled())
            log.info("("+label+", "+to+")");
        
		return graph.addEdge(null, this, to, label);
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Edge> getEdges(final Direction dir, final String... labels) {
	    
        if (log.isInfoEnabled())
            log.info("("+dir+
                    (labels != null ? (", "+Arrays.toString(labels)) : "")
                            +")");
        
		final URI wild = null;
		
		try {
		    
    		if (dir == Direction.OUT) {
    		    
    			return graph.getEdges(uri, wild, labels);
    			
    		} else if (dir == Direction.IN) {
    		    
    			return graph.getEdges(wild, uri, labels);
    			
    		} else {
    		    
    			return graph.fuse(
    					graph.getEdges(uri, wild, labels),
    					graph.getEdges(wild, uri, labels));
    			
    		}
    		
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Vertex> getVertices(final Direction dir, final String... labels) {
	    
        if (log.isInfoEnabled())
            log.info("("+dir+
                    (labels != null ? (", "+Arrays.toString(labels)) : "")
                            +")");
        
		final URI wild = null;
		
		try {
		    
    		if (dir == Direction.OUT) {
    		    
    			return graph.getVertices(uri, wild, labels);
    			
    		} else if (dir == Direction.IN) {
    		    
    			return graph.getVertices(wild, uri, labels);
    			
    		} else {
    		    
    			return graph.fuse(
    					graph.getVertices(uri, wild, labels),
    					graph.getVertices(wild, uri, labels));
    			
    		}

        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

	}

	/**
	 * Not bothering to provide a SPARQL translation for vertex queries at
	 * this time.  I suspect that scan and filter works fine when starting from
	 * an individual vertex.
	 */
	@Override
	public VertexQuery query() {
	    
        if (log.isInfoEnabled())
            log.info("");
        
		return new DefaultVertexQuery(this);
		
	}

	@Override
	public String toString() {
	    
	    return "v["+uri.getLocalName()+"]";
	    
	}

    @Override
    public <T> T getProperty(final String prop) {
        
        if (log.isInfoEnabled())
            log.info("("+prop+")");
        
        return super.getProperty(prop);
    }

    @Override
    public Set<String> getPropertyKeys() {
        
        if (log.isInfoEnabled())
            log.info("");
        
        return super.getPropertyKeys();
        
    }

    @Override
    public <T> T removeProperty(final String prop) {
        
        if (log.isInfoEnabled())
            log.info("("+prop+")");
        
        return super.removeProperty(prop);
        
    }

    @Override
    public void setProperty(final String prop, final Object val) {
        
        if (log.isInfoEnabled())
            log.info("("+prop+", "+val+")");
        
        super.setProperty(prop, val);
        
    }

//    @Override
//    public void addProperty(final String prop, final Object val) {
//        
//        if (log.isInfoEnabled())
//            log.info("("+prop+", "+val+")");
//        
//        super.addProperty(prop, val);
//        
//    }
//
//    @Override
//    public <T> List<T> getProperties(final String prop) {
//        
//        if (log.isInfoEnabled())
//            log.info("("+prop+")");
//        
//        return super.getProperties(prop);
//        
//    }
	
}
