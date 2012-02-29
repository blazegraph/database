/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * An extension point for external service calls which produce solution
 * multisets (a SPARQL <code>SERVICE</code>).
 */
public class ServiceNode extends GroupMemberNodeBase<IGroupMemberNode>
        implements IJoinNode, IGraphPatternContainer {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations,
            IJoinNode.Annotations, IGraphPatternContainer.Annotations {

        /**
         * The service {@link URI}, which will be resolved against the
         * {@link ServiceRegistry} to obtain a {@link ServiceCall} object.
         * 
         * FIXME This needs to be a value expression, which can evaluate to a
         * URI or a variable.  The query planner needs to handle the case where
         * it is a variable differently.
         */
        String SERVICE_URI = "serviceURI";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = "namespace";
     
        /**
         * The "SELECT" option.
         * 
         * TODO Lift out. This is used for many things in SPARQL UPDATE, not 
         * just for SPARQL Federation.
         */
        String SILENT = "silent";
        
        boolean DEFAULT_SILENT = false;
        
    }

    /**
     * Required deep copy constructor.
     */
    public ServiceNode(ServiceNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public ServiceNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param serviceURI
     *            The service URI. See {@link ServiceRegistry}
     * @param groupNode
     *            The graph pattern used to invoke the service.
     * 
     *            FIXME This needs to permit a TermNode for the serviceUri, not
     *            just a constant URI.
     */
    public ServiceNode(//
            final URI serviceURI, final IGroupNode<IGroupMemberNode> groupNode) {

        super(new BOp[] {}, null/* anns */);

        super.setProperty(Annotations.SERVICE_URI, serviceURI);

        super.setProperty(Annotations.GRAPH_PATTERN, groupNode);

    }

    /**
     * The service URI.
     */
    public URI getServiceURI() {

        return (URI) getRequiredProperty(Annotations.SERVICE_URI);

    }

    @SuppressWarnings("unchecked")
    public GraphPatternGroup<IGroupMemberNode> getGraphPattern() {

        return (GraphPatternGroup<IGroupMemberNode>) getProperty(Annotations.GRAPH_PATTERN);

    }

    public void setGraphPattern(
            final GraphPatternGroup<IGroupMemberNode> graphPattern) {

        /*
         * Clear the parent reference on the new where clause.
         * 
         * Note: This handles cases where a join group is lifted into a named
         * subquery. If we do not clear the parent reference on the lifted join
         * group it will still point back to its parent in the original join
         * group.
         */
        graphPattern.setParent(null);

        super.setProperty(Annotations.GRAPH_PATTERN, graphPattern);

    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isOptional() {
     
        return false;
        
    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isMinus() {
     
        return false;
        
    }

    final public boolean isSilent() {

        return getProperty(Annotations.SILENT, Annotations.DEFAULT_SILENT);

    }

    final public void setSilent(final boolean silent) {

        setProperty(Annotations.SILENT, silent);

    }

    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }

    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();

        final URI serviceURI = getServiceURI();

        sb.append("\n");
        sb.append(indent(indent));
        sb.append("SERVICE");
        if(isSilent()) {
            sb.append(" SILENT");
        }
        sb.append(" <"); // FIXME This is not always a URI (TermId or ValueExpr)
        sb.append(serviceURI);
        sb.append("> ");

        if (getGraphPattern() != null) {

            sb.append(" {");
            
            sb.append(getGraphPattern().toString(indent+1));
            
            sb.append("\n").append(indent(indent)).append("}");
            
        }
        
        final List<FilterNode> filters = getAttachedJoinFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        return sb.toString();

    }

}
