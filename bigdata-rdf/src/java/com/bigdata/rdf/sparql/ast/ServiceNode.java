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

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * An extension point for external service calls which produce solution
 * multisets.
 */
public class ServiceNode extends GroupMemberNodeBase<IGroupMemberNode> {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations {

        /**
         * The function URI from the {@link FunctionRegistry}.
         */
        String SERVICE_URI = "serviceURI";

        /**
         * The <code>group graph pattern</code> used to invoke the service.
         */
        String GROUP_NODE = "groupNode";

        /**
         * The base name of the named solution set which will be generated when
         * the service is invoked.
         */
        String NAME = "name";
        
        /**
         * A {@link VarNode}[] specifying the join variables that will be used
         * when the named result set is join with the query. The join variables
         * MUST be bound for a solution to join.
         */
        String JOIN_VARS = "joinVars";

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param serviceURI
     *            The service URI. See {@link ServiceRegistry}
     * @param name
     *            The name of the solution set on which the service's results
     *            will be buffered.
     * @param groupNode
     *            The graph pattern used to invoke the service.
     * 
     *            FIXME If the service streams out results and is just run first
     *            within a named subquery then we do not need a "name" for the
     *            service node. The "name" is the namedSet of the subquery. If
     *            the service shows up by itself in the main body of a query
     *            then we just need to create a named subquery for the service
     *            invocation. It it is already part of a named subquery and
     *            there is no other service in the named subquery, then we just
     *            run it first in that named subquery. If there is another
     *            service in that named subquery, then we need to lift one of
     *            them out into its own named subquery (i.e., at most one
     *            "run first/ run once" operation in the named subquery to avoid
     *            a cross product).
     */
    public ServiceNode(//
            final String name,
            final URI serviceURI,
            final IGroupNode<IGroupMemberNode> groupNode) {

        super(new BOp[]{}, null/*anns*/);

        super.setProperty(Annotations.NAME, name);

        super.setProperty(Annotations.SERVICE_URI, serviceURI);

        super.setProperty(Annotations.GROUP_NODE, groupNode);

    }

    /**
     * The join variables to be used when the named result set is included into
     * the query. This should be set by an {@link IASTOptimizer} based on a
     * static analysis of the query.
     */
    public VarNode[] getJoinVars() {

        return (VarNode[]) getProperty(Annotations.JOIN_VARS);

    }

    /**
     * Set the join variables.
     *
     * @param joinVars
     *            The join variables.
     */
    public void setJoinVars(final VarNode[] joinVars) {

        setProperty(Annotations.JOIN_VARS, joinVars);

    }

    public String getName() {

        return (String) getRequiredProperty(Annotations.NAME);

    }

    public URI getServiceURI() {

        return (URI) getRequiredProperty(Annotations.SERVICE_URI);

    }

    /**
     * The graph pattern which will be used provided to the service when it is
     * invoked.
     */
    @SuppressWarnings("unchecked")
    public IGroupNode<IGroupMemberNode> getGroupNode() {

        return (IGroupNode<IGroupMemberNode>) getProperty(Annotations.GROUP_NODE);

    }

    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();

        final URI serviceURI = getServiceURI();
        final String name = getName();
        final VarNode[] joinVars = getJoinVars();

        sb.append("\n");
        sb.append(indent(indent));
        sb.append("SERVICE <");
        sb.append(serviceURI);
        sb.append("> ");
        sb.append("AS ");
        sb.append(name);
        
        if (joinVars != null) {

            sb.append(" JOIN ON (");

            boolean first = true;

            for (VarNode var : joinVars) {

                if (!first)
                    sb.append(",");

                sb.append(var);

                first = false;

            }

            sb.append(")");

        }

        if (getGroupNode() != null) {

            sb.append(" {");
            
            sb.append(getGroupNode().toString(indent+1));
            
            sb.append("\n").append(indent(indent)).append("}");
            
        }
        
        return sb.toString();

    }

}
