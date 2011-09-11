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
package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sail.QueryType;

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
abstract public class QueryBase extends QueryNodeBase {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryNodeBase.Annotations {

        /**
         * The {@link QueryType}.
         */
        String QUERY_TYPE = "queryType";

        /**
         * The {@link ConstructNode} (optional).
         */
        String CONSTRUCT = "construct";

        /**
         * The {@link ProjectionNode} (optional). This is also used for DESCRIBE
         * queries to capture the list of variables and IRIs which are then used
         * to rewrite the DESCRIBE query into what amounts to a CONSTRUCT query.
         * The resulting CONSTRUCT query will have a different
         * {@link ProjectionNode} suitable for use with the generated
         * {@link ConstructNode}.
         */
        String PROJECTION = "projection";

        /**
         * The top-level {@link IGroupNode} (optional).
         */
        String WHERE_CLAUSE = "whereClause";

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
        
    }
    
    /**
     * Return the projection -or- <code>null</code> if there is no projection.
     */
    public ProjectionNode getProjection() {

        return (ProjectionNode) getProperty(Annotations.PROJECTION);

    }

    /**
     * Set or clear the projection.
     * 
     * @param projection
     *            The projection (may be <code>null</code>).
     */
    public void setProjection(final ProjectionNode projection) {

        setProperty(Annotations.PROJECTION, projection);

    }

    /**
     * Return the {@link IGroupNode} (corresponds to the WHERE clause).
     */
    @SuppressWarnings("unchecked")
    public IGroupNode<IGroupMemberNode> getWhereClause() {

        return (IGroupNode<IGroupMemberNode>) getProperty(Annotations.WHERE_CLAUSE);

    }

    /**
     * Set the {@link IGroupNode} (corresponds to the WHERE clause).
     *  
     * @param whereClause
     *            The "WHERE" clause.
     */
    public void setWhereClause(final IGroupNode<IGroupMemberNode> whereClause) {
        
        setProperty(Annotations.WHERE_CLAUSE, whereClause);

        
    }
    
    /**
     * Return true if this query has a WHERE clause, false if not.
     */
    public boolean hasWhereClause() {
    	
    	return getProperty(Annotations.WHERE_CLAUSE) != null;
    	
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

	public String toString(final int indent) {
		
	    final String s = indent(indent);
	    
		final StringBuilder sb = new StringBuilder();

        final ConstructNode construct = getConstruct();
        final ProjectionNode projection = getProjection();
        final IGroupNode<IGroupMemberNode> whereClause = getWhereClause();
        final GroupByNode groupBy = getGroupBy();
        final HavingNode having = getHaving();
        final OrderByNode orderBy = getOrderBy();
        final SliceNode slice = getSlice();

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

        return sb.toString();

    }

}
