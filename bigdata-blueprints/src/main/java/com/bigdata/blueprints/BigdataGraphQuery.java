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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * Translate a low-performance Blueprints GraphQuery into a high-performance
 * SPARQL query.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphQuery implements GraphQuery {

    protected final static transient Logger log = Logger.getLogger(BigdataGraphQuery.class);
    
    /**
     * These are the only Predicate implementations we handle currently.
     */
    protected static List<Class> knownPredicates = Arrays.asList(new Class[] {
        BigdataPredicate.class,
        com.tinkerpop.blueprints.Query.Compare.class,
        com.tinkerpop.blueprints.Contains.class,
        com.tinkerpop.blueprints.Compare.class
    });
   
    /**
     * The graph.
     */
    private final BigdataGraph graph;
    
    /**
     * URI used for typing elements.
     */
    protected final URI TYPE;
    
    /**
     * URI used to represent a Vertex.
     */
    protected final URI VERTEX;
    
    /**
     * URI used to represent a Edge.
     */
    protected final URI EDGE;

    /**
     * URI used for labeling edges.
     */
    protected final URI LABEL;

    /**
     * The list of criteria.  Bigdata's query optimizer will re-order the
     * criteria based on selectivity and execute for maximum performance and
     * minimum IO.
     */
    private final List<Has> criteria = new LinkedList<Has>();

    /**
     * Limit the number of results.
     */
    private transient int limit = Integer.MAX_VALUE;
    
    public BigdataGraphQuery(final BigdataGraph graph) {
        this.graph = graph;
        this.TYPE = graph.getValueFactory().getTypeURI();
        this.VERTEX = graph.getValueFactory().getVertexURI();
        this.EDGE = graph.getValueFactory().getEdgeURI();
        this.LABEL = graph.getValueFactory().getLabelURI();
    }
    
    /**
     * Filter out elements that do not have a property with provided key.
     * 
     * ?s <key> ?value
     *
     * @param key the key of the property
     * @return the modified query object
     */
    @Override
    public GraphQuery has(final String key) {
        criteria.add(new Has(key));
        return this;
    }

    /**
     * Filter out elements that have a property with provided key.
     *
     * ?s ?p ?o .
     * filter not exists { ?s <key> ?value } .
     * 
     * @param key the key of the property
     * @return the modified query object
     */
    @Override
    public GraphQuery hasNot(final String key) {
        criteria.add(new HasNot(key));
        return this;
    }

    /**
     * Filter out elements that do not have a property value equal to provided value.
     *
     * ?s <key> <value> .
     * 
     * @param key   the key of the property
     * @param value the value to check against
     * @return the modified query object
     */
    @Override
    public GraphQuery has(final String key, final Object value) {
        criteria.add(new Has(key, value));
        return this;
    }

    /**
     * Filter out elements that have a property value equal to provided value.
     *
     * ?s ?p ?o .
     * filter not exists { ?s <key> <value> } .
     * 
     * @param key   the key of the property
     * @param value the value to check against
     * @return the modified query object
     */
    @Override
    public GraphQuery hasNot(final String key, final Object value) {
        criteria.add(new HasNot(key, value));
        return this;
    }

    /**
     * Filter out the element if it does not have a property with a comparable value.
     *
     * @param key     the key of the property
     * @param predicate the comparator to use for comparison
     * @param value  the value to check against
     * @return the modified query object
     */
    @Override
    public GraphQuery has(final String key, final Predicate predicate, final Object value) {
        if (!knownPredicates.contains(predicate.getClass())) {
            throw new IllegalArgumentException();
        }
        criteria.add(new Has(key, value, BigdataPredicate.toBigdataPredicate(predicate)));
        return this;
    }

    /**
     * Filter out the element if it does not have a property with a comparable value.
     *
     * @param key     the key of the property
     * @param value   the value to check against
     * @param compare the comparator to use for comparison
     * @return the modified query object
     */
    @Override
    @Deprecated
    public <T extends Comparable<T>> GraphQuery has(final String key, final T value, 
            final Compare compare) {
        return has(key, compare, value);
    }

    /**
     * Filter out the element of its property value is not within the provided interval.
     *
     * @param key        the key of the property
     * @param startValue the inclusive start value of the interval
     * @param endValue   the exclusive end value of the interval
     * @return the modified query object
     */
    @Override
    public <T extends Comparable<?>> GraphQuery interval(final String key, 
            final T startValue, final T endValue) {
        return has(key, BigdataPredicate.GTE, startValue)
                .has(key, BigdataPredicate.LT, endValue);
    }

    /**
     * Filter out the element if the take number of incident/adjacent elements to retrieve has already been reached.
     * 
     * @param limit the take number of elements to return
     * @return the modified query object
     */
    @Override
    public GraphQuery limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Execute the query and return the matching edges.
     *
     * @return the unfiltered incident edges
     */
    @Override
    public Iterable<Edge> edges() {
        try {
            final String queryStr = toQueryStr(EDGE);
            return graph.getEdges(queryStr);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Execute the query and return the vertices on the other end of the matching edges.
     *
     * @return the unfiltered adjacent vertices
     */
    @Override
    public Iterable<Vertex> vertices() {
        try {
            final String queryStr = toQueryStr(VERTEX);
            return graph.getVertices(queryStr, true);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Generate the SPARQL query.
     */
    protected String toQueryStr(final URI type) {
        
        final StringBuilder sb = new StringBuilder();

        if (type == VERTEX) {
            sb.append("construct { ?x <"+TYPE+"> <"+type+"> . }\n");
            sb.append("{\n  select distinct ?x where {\n");
        } else {
            sb.append("construct { ?from ?x ?to . }\n");
            sb.append("{\n  select distinct ?from ?x ?to where {\n");
            sb.append("    ?from ?x ?to .\n");
        }
        
        final BlueprintsValueFactory factory = graph.factory;
        
        boolean hasHas = false;
        
        int i = 1;
        for (Has has : criteria) {
            
            if (log.isTraceEnabled()) {
                log.trace(has);
            }
            
            if (has instanceof HasNot) {
                
                sb.append("    filter not exists { ");
                
                sb.append("?x <").append(factory.toPropertyURI(has.key)).append("> ");
                
                if (has.val != null) {
                    
                    final String val = factory.toLiteral(has.val).toString();
                    
                    sb.append(val).append(" .");
                    
                } else {
                    
                    final String var = "?val"+i;
                    
                    sb.append(var).append(" .");
                    
                }
                
                sb.append("}\n");

            } else {
                
                hasHas = true;
                
                sb.append("    ?x <").append(factory.toPropertyURI(has.key)).append("> ");
                
                if (has.val != null && 
                        (has.pred == null || has.pred == BigdataPredicate.EQ)) {
                    
                    final Literal val = factory.toLiteral(has.val);
                    
                    sb.append(val).append(" .\n");
                    
                } else {
                    
                    final String var = "?val"+i;
                            
                    sb.append(var).append(" .\n");
                    
                    if (has.pred != null) { 
                    
                        sb.append(toFilterStr(has.pred, var, has.val)).append("\n");
                        
                    }
                    
                }
                
            }
            
            i++;
            
        }
        
        // need a statement pattern for the filter not exists
        if (!hasHas) {
            
            sb.append("    ?x <"+TYPE+"> <").append(type).append("> .\n");
            
        }
        
//        sb.setLength(sb.length()-1);
        
        sb.append("  }");
        
        if (limit < Integer.MAX_VALUE) {
            
            sb.append(" limit " + factory.toLiteral(limit).getLabel());
            
        }
        
        sb.append("\n}");
        
        if (log.isTraceEnabled()) {
            log.trace("\n"+sb.toString());
        }
        
        return sb.toString();
        
    }
    
    /**
     * Generate a SPARQL filter string for a particular Predicate.
     */
    private String toFilterStr(final BigdataPredicate pred, final String var,
            final Object val) {
        
        final BlueprintsValueFactory factory = graph.factory;
        
        final StringBuilder sb = new StringBuilder();
        
        if (pred == BigdataPredicate.EQ) {
            
            throw new IllegalArgumentException();
            
        } else if (pred == BigdataPredicate.GT || pred == BigdataPredicate.GTE ||
                   pred == BigdataPredicate.LT || pred == BigdataPredicate.LTE ||
                   pred == BigdataPredicate.NE) {
            
            final Literal l = factory.toLiteral(val);
            
            sb.append("    filter(").append(var);
            
            switch(pred) {
            case GT:
                sb.append(" > "); break;
            case GTE:
                sb.append(" >= "); break;
            case LT:
                sb.append(" < "); break;
            case LTE:
                sb.append(" <= "); break;
            case NE:
                sb.append(" != "); break;
            default:
                break;
            }
            
            sb.append(l).append(") .");
            
        } else if (pred == BigdataPredicate.IN || pred == BigdataPredicate.NIN) {
            
            sb.append("    filter(");
            
            if (pred == BigdataPredicate.NIN) {
                sb.append("!(");
            }
            
            sb.append(var).append(" in (");
            
            final Collection<?> c = (Collection<?>) val;
            
            for (Object o : c) {
                
                final Literal l = factory.toLiteral(o);
                
                sb.append(l).append(", ");
                
            }
            
            sb.setLength(sb.length()-2);
            
            if (pred == BigdataPredicate.NIN) {
                sb.append(")");
            }
            
            sb.append(")) .");
            
        }
        
        return sb.toString();
        
    }

    /**
     * Standard criterion for filtering by the existence of a property and
     * optional value.
     * 
     * @author mikepersonick
     *
     */
    private class Has {
        
        private String key;
        
        private Object val;
        
        private BigdataPredicate pred;
        
        public Has(final String key) {
            this(key, null, null);
        }
        
        public Has(final String key, final Object val) {
            this(key, val, null);
        }
        
        public Has(final String key, final Object val, 
                final BigdataPredicate pred) {
            
            if (pred == BigdataPredicate.IN || pred == BigdataPredicate.NIN) {
                
                if (!(val instanceof Collection)) {
                    throw new IllegalArgumentException();
                }
                
                if (((Collection<?>) val).size() == 0) {
                    throw new IllegalArgumentException();
                }
                
            }

            this.key = key;
            
            if (pred == BigdataPredicate.IN && ((Collection<?>) val).size() == 1) {
            
                /*
                 * Simple optimization to replace a single value IN with 
                 * a simple EQ.
                 */
                this.val = ((Collection<?>) val).iterator().next();
                this.pred = null;
                
            } else {
            
                this.val = val;
                this.pred = pred;
            
            }
            
        }
        
        public String toString() {
            return "key: " + key + ", val: " + val + ", pred: " + pred;
        }
        
    }
    
    /**
     * Criterion for filtering by the non-existence of a property and
     * optional value.  Uses SPARQL filter not exists {}.
     * 
     * @author mikepersonick
     *
     */
    private class HasNot extends Has {
        
        public HasNot(final String key) {
            super(key);
        }
        
        public HasNot(final String key, final Object val) {
            super(key, val);
        }
        
    }
    
    
    
}
