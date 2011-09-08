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
import com.bigdata.bop.join.HashJoinAnnotations;

/**
 * An extension point for external service calls which produce solution
 * multisets.
 *
 */
public class ServiceNode extends GroupMemberNodeBase {

    private static final long serialVersionUID = 1L;

    interface Annotations extends ValueExpressionNode.Annotations,HashJoinAnnotations {

        /**
         * The function URI from the {@link FunctionRegistry}.
         */
        String SERVICE_URI = ServiceNode.class.getName() + ".serviceURI";

        /**
         * The group node for the service
         */
        String GROUP_NODE = ServiceNode.class.getName() + ".groupNode";

        /**
         * The service call for the service
         */
        String SERVICE_CALL = ServiceNode.class.getName() + ".serviceCall";

        /**
         * The call name for the service
         */
        String NAME = ServiceNode.class.getName() + ".name";
    }

    /**
     * Construct a function node in the AST.
     *
     * @param functionURI
     *            the function URI. see {@link FunctionRegistry}
     * @param scalarValues
     *            One or more scalar values that are passed to the function
     * @param args
     *            the arguments to the function.
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

    public ServiceNode(final String name, final URI serviceURI,ServiceCall call){
        super(new BOp[]{}, null/*anns*/);

        super.setProperty(Annotations.NAME, name);

        super.setProperty(Annotations.SERVICE_URI, serviceURI);

        super.setProperty(Annotations.SERVICE_CALL, call);
    }

    /**
     * The join variables to be used when the named result set is included into
     * the query.
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

    public IGroupNode<IGroupMemberNode> getGroupNode(){

        return (IGroupNode<IGroupMemberNode>)getProperty(Annotations.GROUP_NODE);

    }

    public ServiceCall getServiceCall(){

        return (ServiceCall)getProperty(Annotations.SERVICE_CALL);

    }

    public void setServiceCall(ServiceCall call){
        setProperty(Annotations.SERVICE_CALL, call);
    }

    @Override
    public String toString(int indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent(indent));
        sb.append("SERVICE <");
        sb.append(getServiceURI().toString());
        sb.append("> ");
        if(getGroupNode()!=null){
            sb.append(getGroupNode().toString(indent));
        }
        final VarNode[] joinVars = getJoinVars();

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
        return sb.toString();

    }

}
