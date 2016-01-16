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
package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.QueryRoot.Annotations;

/**
 * Contains the projection clause, where clause, and solution modified clauses.
 * 
 * @see IGroupNode
 * @see ProjectionNode
 * @see GroupByNode
 * @see HavingNode
 * @see OrderByNode
 * @see SliceNode
 */
abstract public class QueryBase extends QueryNodeBase implements
        IBindingProducerNode, IGraphPatternContainer, IProjectionDecl {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryNodeBase.Annotations,
            IGraphPatternContainer.Annotations, IProjectionDecl.Annotations {

        /**
         * The {@link QueryType}.
         */
        String QUERY_TYPE = "queryType";

        /**
         * The {@link ConstructNode} (optional).
         */
        String CONSTRUCT = "construct";

//        /**
//         * The {@link ProjectionNode} (optional). This is also used for DESCRIBE
//         * queries to capture the list of variables and IRIs which are then used
//         * to rewrite the DESCRIBE query into what amounts to a CONSTRUCT query.
//         * The resulting CONSTRUCT query will have a different
//         * {@link ProjectionNode} suitable for use with the generated
//         * {@link ConstructNode}.
//         */
//        String PROJECTION = "projection";

        /**
         * The {@link GroupByNode} (optional).
         */
        String GROUP_BY = "groupBy";

        /**
         * The {@link HavingNode} (optional).
         */
        String HAVING = "having";

        /**
         * The {@link OrderByNode} (optional).
         */
        String ORDER_BY = "orderBy";

        /**
         * The {@link SliceNode} (optional).
         */
        String SLICE = "slice";
        
        /**
         * When <code>true</code> inferred statements will not be stripped from
         * the access paths (default {@value #DEFAULT_INCLUDE_INFERRED}).
         */
        String INCLUDE_INFERRED = "includeInferred";
        
        boolean DEFAULT_INCLUDE_INFERRED = true;

        /**
         * The {@link Long} value giving the time limit for the query
         * (milliseconds).
         */
        String TIMEOUT = "timeout";

        long DEFAULT_TIMEOUT = Long.MAX_VALUE;

        /**
         * The BINDINGS clause (optional).
         */
        String BINDINGS_CLAUSE = "bindingsClause";
        
    }

    /**
     * Constructor is hidden to force people to declare the {@link QueryType}.
     */
    @SuppressWarnings("unused")
    private QueryBase() {
        
    }

    /**
     * Deep copy constructor.
     */
    public QueryBase(final QueryBase queryBase) {
    
        super(queryBase);
        
    }
    
    /**
     * Shallow copy constructor.
     */
    public QueryBase(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

    public QueryBase(final QueryType queryType) {
	    
        setQueryType(queryType);
	    
	}

    /**
     * Return the type of query. This provides access to information which may
     * otherwise be difficult to determine by inspecting the generated AST or
     * query plan.
     */
	public QueryType getQueryType() {
	    
        return (QueryType) getProperty(Annotations.QUERY_TYPE);

	}

    /**
     * Set the type of query.
     */
	public void setQueryType(final QueryType queryType) {
	    
        setProperty(Annotations.QUERY_TYPE, queryType);
	    
	}
	
    /**
     * Return the construction -or- <code>null</code> if there is no construction.
     */
    public ConstructNode getConstruct() {

        return (ConstructNode) getProperty(Annotations.CONSTRUCT);
        
    }
    
    /**
     * Set or clear the construction.
     * 
     * @param construction
     *            The construction (may be <code>null</code>).
     */
    public void setConstruct(final ConstructNode construct) {

        setProperty(Annotations.CONSTRUCT, construct);
        
        if (construct != null) {
        	
		   setQueryType(QueryType.CONSTRUCT);
		   
        }
        
    }
    
    @Override
    public void setProjection(final ProjectionNode projection) {

        setProperty(Annotations.PROJECTION, projection);

    }

    @Override
    public ProjectionNode getProjection() {

        return (ProjectionNode) getProperty(Annotations.PROJECTION);

    }
    
    @Override
    public Set<IVariable<?>> getProjectedVars(final Set<IVariable<?>> vars) {
        
        final ProjectionNode tmp = getProjection();
        
        if(tmp != null) {
            
            tmp.getProjectionVars(vars);
            
        }
        
        return vars;
        
    }

    /**
     * Return the set of variables on which the {@link ProjectionNode} for this
     * query depends (this is a NOP if there is no {@link ProjectionNode} for
     * the query, which can happen for an ASK query). This DOES NOT report the
     * variables which are projected OUT of the query, just those used by the
     * SELECT expressions.
     * 
     * @param vars
     *            The variables used by the select expressions are added to this
     *            set.
     * 
     * @return The caller's set.
     */
    public Set<IVariable<?>> getSelectExprVars(final Set<IVariable<?>> vars) {
        
        final ProjectionNode tmp = getProjection();
        
        if(tmp != null) {
            
            tmp.getSelectExprVars(vars);
            
        }
        
        return vars;
        
    }

    /**
     * Return the {@link GraphPatternGroup} for the WHERE clause.
     * 
     * @return The WHERE clause -or- <code>null</code>.
     */
    @SuppressWarnings({ "rawtypes" })
    public GraphPatternGroup getWhereClause() {

        // Note: Synonym for getGraphPattern.
        return getGraphPattern();

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
        
        super.setProperty(Annotations.GRAPH_PATTERN, graphPattern);

    }

    /**
     * Set the {@link GraphPatternGroup} for the WHERE clause.
     * 
     * @param whereClause
     *            The "WHERE" clause.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setWhereClause(final GraphPatternGroup whereClause) {

        // Note: synonym for setGraphPattern.
        setGraphPattern(whereClause);

    }
    
    /**
     * Return true if this query has a WHERE clause, false if not.
     */
    public boolean hasWhereClause() {
    	
    	return getProperty(Annotations.GRAPH_PATTERN) != null;
    	
    }
    
    /**
     * Return the {@link GroupByNode} -or- <code>null</code> if there is none.
     */
    public GroupByNode getGroupBy() {
        
        return (GroupByNode) getProperty(Annotations.GROUP_BY);
        
    }

    /**
     * Set or clear the {@link GroupByNode}.
     * 
     * @param groupBy
     *            The new value (may be <code>null</code>).
     */
    public void setGroupBy(final GroupByNode groupBy) {

        setProperty(Annotations.GROUP_BY, groupBy);
        
    }
    
    /**
     * Return the {@link HavingNode} -or- <code>null</code> if there is none.
     */
    public HavingNode getHaving() {
        
        return (HavingNode) getProperty(Annotations.HAVING);
        
    }

    /**
     * Set or clear the {@link HavingNode}.
     * 
     * @param having
     *            The new value (may be <code>null</code>).
     */
    public void setHaving(final HavingNode having) {

        setProperty(Annotations.HAVING, having);
        
    }

	/**
	 * Return the slice -or- <code>null</code> if there is no slice.
	 */
	public SliceNode getSlice() {
        
        return (SliceNode) getProperty(Annotations.SLICE);
	    
    }

    /**
     * Set or clear the slice.
     * 
     * @param slice
     *            The slice (may be <code>null</code>).
     */
    public void setSlice(final SliceNode slice) {

        setProperty(Annotations.SLICE, slice);
        
    }


    /**
     * Return <code>true</code> iff there is a {@link SliceNode} and either the
     * LIMIT and/or OFFSET has been specified with a non-default value.
     */
    public boolean hasSlice() {

        final SliceNode slice = getSlice();

        if (slice == null)
            return false;

        if (slice.getLimit() != SliceNode.Annotations.DEFAULT_LIMIT)
            return true;

        if (slice.getOffset() != SliceNode.Annotations.DEFAULT_OFFSET)
            return true;

        // The SLICE does not specify either LIMIT or OFFSET.
        return false;

    }

    /**
     * Return the order by clause -or- <code>null</code> if there is no order
     * by.
     */
    public OrderByNode getOrderBy() {
     
        return (OrderByNode) getProperty(Annotations.ORDER_BY);

    }

    /**
     * Set or clear the {@link OrderByNode}.
     * 
     * @param orderBy
     *            The order by (may be <code>null</code>).
     */
    public void setOrderBy(final OrderByNode orderBy) {
    
        setProperty(Annotations.ORDER_BY, orderBy);
        
    }

    /**
     * @see Annotations#INCLUDE_INFERRED
     */
    public boolean getIncludeInferred() {
        return getProperty(Annotations.INCLUDE_INFERRED,
                Annotations.DEFAULT_INCLUDE_INFERRED);
    }

    /**
     * @see Annotations#INCLUDE_INFERRED
     */
    public void setIncludeInferred(boolean includeInferred) {
        setProperty(Annotations.INCLUDE_INFERRED, includeInferred);
    }

    /**
     * @see Annotations#TIMEOUT
     */
    public long getTimeout() {
        return getProperty(Annotations.TIMEOUT, Annotations.DEFAULT_TIMEOUT);
    }

    /**
     * Set the timeout (milliseconds) for the query.
     * 
     * @see Annotations#TIMEOUT
     */
    public void setTimeout(long timeout) {
        setProperty(Annotations.TIMEOUT, timeout);
    }

    /**
     * Set the BINDINGS.
     * 
     * @param bindings
     */
    public void setBindingsClause(final BindingsClause bindings) {

        setProperty(Annotations.BINDINGS_CLAUSE, bindings);

    }

    /**
     * Return the BINDINGS.
     */
    public BindingsClause getBindingsClause() {

        return (BindingsClause) getProperty(Annotations.BINDINGS_CLAUSE);

    }
    
    @Override
	public String toString(final int indent) {
		
	    final String s = indent(indent);
	    
		final StringBuilder sb = new StringBuilder();

        final ConstructNode construct = getConstruct();
        final ProjectionNode projection = getProjection();
        @SuppressWarnings("unchecked")
        final IGroupNode<IGroupMemberNode> whereClause = getWhereClause();
        final GroupByNode groupBy = getGroupBy();
        final HavingNode having = getHaving();
        final OrderByNode orderBy = getOrderBy();
        final SliceNode slice = getSlice();
        final BindingsClause bindings = getBindingsClause();

        if (getQueryType() != null) {

            sb.append("\n").append(s).append("QueryType: ")
                    .append(getQueryType().toString());

        }

        if (getProperty(Annotations.INCLUDE_INFERRED) != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("includeInferred=" + getIncludeInferred());
        }

        if (getProperty(Annotations.TIMEOUT) != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("timeout=" + getTimeout());
        }

        if (construct != null && !construct.isEmpty()) {

            sb.append(construct.toString(indent));
            
        }

        if (projection != null && !projection.isEmpty()) {

		    sb.append(projection.toString(indent));
		    
		}

        if (whereClause != null) {

            sb.append(whereClause.toString(indent + 1));

        }

        if (groupBy != null && !groupBy.isEmpty()) {

            sb.append(groupBy.toString(indent));

        }

        if (having != null && !having.isEmpty()) {

            sb.append(having.toString(indent));

        }

        if (orderBy != null && !orderBy.isEmpty()) {

            sb.append(orderBy.toString(indent));

        }

        if (slice != null) {

            sb.append(slice.toString(indent));

        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        if (bindings != null) {

            sb.append(bindings.toString(indent + 1));

        }

        return sb.toString();

    }

}
