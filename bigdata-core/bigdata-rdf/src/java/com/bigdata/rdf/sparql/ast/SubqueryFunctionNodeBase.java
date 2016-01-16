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
/*
 * Created on Aug 26, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;

/**
 * A special function node for modeling value expression nodes which are
 * evaluated against an inner graph expression, such as EXISTS and NOT EXISTS.
 * Another possibility is IN(subSelect), where IN is evaluated against the
 * subquery result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class SubqueryFunctionNodeBase extends FunctionNode implements
        IGraphPatternContainer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends FunctionNode.Annotations,
            IGraphPatternContainer.Annotations {
        
        /**
         * Used to specify the query plan for FILTER (NOT) EXISTS. There are two
         * basic plans: vectored sub-plan and subquery with LIMIT ONE. Each plan
         * has its advantages.
         * <p>
         * Note: This annotation gets propagated to the {@link SubqueryRoot}
         * when the FILTER (NOT) EXISTS is turned into an ASK subquery.
         * 
         * @see FilterExistsModeEnum
         * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance
         *      for FILTER EXISTS </a>
         */
        String FILTER_EXISTS = QueryHints.FILTER_EXISTS;
        
        FilterExistsModeEnum DEFAULT_FILTER_EXISTS = QueryHints.DEFAULT_FILTER_EXISTS;

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public SubqueryFunctionNodeBase(final SubqueryFunctionNodeBase op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public SubqueryFunctionNodeBase(final BOp[] args,
            final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * @param anonvar
     *            An anonymous variable which will be bound by an ASK subquery.
     * @param graphPattern
     *            The inner value of the EXISTS function.
     */
    protected SubqueryFunctionNodeBase(final URI functionURI,
            final VarNode anonvar,
            final GraphPatternGroup<IGroupMemberNode> graphPattern) {

        super(functionURI, null/* scalarValues */,
                new ValueExpressionNode[] { anonvar });

        if (anonvar == null)
            throw new IllegalArgumentException();

        if (graphPattern == null)
            throw new IllegalArgumentException();

        setGraphPattern(graphPattern);

    }

    @Override
    @SuppressWarnings("unchecked")
    public GraphPatternGroup<IGroupMemberNode> getGraphPattern() {

        return (GraphPatternGroup<IGroupMemberNode>) getProperty(Annotations.GRAPH_PATTERN);

    }

    @Override
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

        setProperty(Annotations.GRAPH_PATTERN, graphPattern);

    }

    /**
     * 
     * @see Annotations#FILTER_EXISTS
     */
    public void setFilterExistsMode(final FilterExistsModeEnum newVal) {

        setProperty(Annotations.FILTER_EXISTS, newVal);
        
    }
    
    /**
     * @see Annotations#FILTER_EXISTS
     */
    public FilterExistsModeEnum getFilterExistsMode() {

        return getProperty(Annotations.FILTER_EXISTS,
                Annotations.DEFAULT_FILTER_EXISTS);

    }
    
    @Override
	protected void annotationValueToString(final StringBuilder sb, final BOp val, int i) {

        sb.append(val.toString(i));
        
	}

}
