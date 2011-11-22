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
import com.bigdata.rdf.sparql.ast.GroupNodeBase.Annotations;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * An extension point for external service calls which produce solution
 * multisets (a SPARQL <code>SERVICE</code>).
 */
public class ServiceNode extends GroupMemberNodeBase<IGroupMemberNode>
        implements IJoinNode {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations,
            IJoinNode.Annotations {

        /**
         * The service {@link URI}, which will be resolved against the
         * {@link ServiceRegistry} to obtain a {@link ServiceCall} object.
         */
        String SERVICE_URI = "serviceURI";

        /**
         * The {@link GraphPatternGroup} modeling the
         * <code>group graph pattern</code> used to invoke the service.
         */
        String GROUP_NODE = "groupNode";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = "namespace";
        
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
     */
    public ServiceNode(//
            final URI serviceURI,
            final IGroupNode<IGroupMemberNode> groupNode) {

        super(new BOp[]{}, null/*anns*/);

        super.setProperty(Annotations.SERVICE_URI, serviceURI);

        super.setProperty(Annotations.GROUP_NODE, groupNode);

    }

    public URI getServiceURI() {

        return (URI) getRequiredProperty(Annotations.SERVICE_URI);

    }

    /**
     * The graph pattern which will be used provided to the service when it is
     * invoked.
     */
    @SuppressWarnings("unchecked")
    public GraphPatternGroup<IGroupMemberNode> getGroupNode() {

        return (GraphPatternGroup<IGroupMemberNode>) getProperty(Annotations.GROUP_NODE);

    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isOptional() {
     
        return false;
        
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
        sb.append("SERVICE <");
        sb.append(serviceURI);
        sb.append("> ");

        if (getGroupNode() != null) {

            sb.append(" {");
            
            sb.append(getGroupNode().toString(indent+1));
            
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
