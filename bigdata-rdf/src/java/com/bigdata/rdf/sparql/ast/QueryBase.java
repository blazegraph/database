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
abstract public class QueryBase {

	private IGroupNode root;
    private ProjectionNode projection;
    private GroupByNode groupBy;
    private HavingNode having;
    private OrderByNode orderBy;
    private SliceNode slice;

	public QueryBase() {

	}

    /**
     * Return the {@link IGroupNode} (corresponds to the WHERE clause).
     */
    public IGroupNode getRoot() {

        return root;
        
    }

    /**
     * Set the {@link IGroupNode}.
     *  
     * @param root
     *            The "WHERE" clause.
     */
    public void setRoot(final IGroupNode root) {
        
        this.root = root;
        
    }
    
    /**
     * Return the projection -or- <code>null</code> if there is no projection.
     */
    public ProjectionNode getProjection() {
        
        return projection;
        
    }
    
    /**
     * Set or clear the projection.
     * 
     * @param projection
     *            The projection (may be <code>null</code>).
     */
    public void setProjection(final ProjectionNode projection) {

        this.projection = projection;
        
    }
    
    /**
     * Return the {@link GroupByNode} -or- <code>null</code> if there is none.
     */
    public GroupByNode getGroupBy() {
        
        return groupBy;
        
    }

    /**
     * Set or clear the {@link GroupByNode}.
     * 
     * @param groupBy
     *            The new value (may be <code>null</code>).
     */
    public void setGroupBy(final GroupByNode groupBy) {

        this.groupBy = groupBy;
        
    }
    
    /**
     * Return the {@link HavingNode} -or- <code>null</code> if there is none.
     */
    public HavingNode getHaving() {
        
        return having;
        
    }

    /**
     * Set or clear the {@link HavingNode}.
     * 
     * @param having
     *            The new value (may be <code>null</code>).
     */
    public void setHaving(final HavingNode having) {

        this.having = having;
        
    }

	/**
	 * Return the slice -or- <code>null</code> if there is no slice.
	 */
	public SliceNode getSlice() {
        
	    return slice;
	    
    }

    /**
     * Set or clear the slice.
     * 
     * @param slice
     *            The slice (may be <code>null</code>).
     */
    public void setSlice(final SliceNode slice) {

        this.slice = slice;
        
    }

    /**
     * Return the order by clause -or- <code>null</code> if there is no order
     * by.
     */
    public OrderByNode getOrderBy() {
     
        return orderBy;
        
    }

    /**
     * Set or clear the {@link OrderByNode}.
     * 
     * @param orderBy
     *            The order by (may be <code>null</code>).
     */
    public void setOrderBy(final OrderByNode orderBy) {
    
        this.orderBy = orderBy;
        
    }
    
	public String toString() {
		
		final StringBuilder sb = new StringBuilder();
		
        if (projection != null && !projection.isEmpty()) {

            sb.append("select");

		    sb.append(projection);
		    
		}

        if (root != null) {

            sb.append("\nwhere\n");

            sb.append(root.toString());

        }

        if (groupBy != null && !groupBy.isEmpty()) {

            sb.append("\n");

            sb.append(groupBy.toString());

        }

        if (having != null && !having.isEmpty()) {

            sb.append("\n");

            sb.append(having.toString());

        }

        if (orderBy != null && !orderBy.isEmpty()) {

            sb.append("\n");

            sb.append(orderBy.toString());

        }

        if (slice != null) {

            sb.append("\n");

            sb.append(slice.toString());

        }

        return sb.toString();

    }

}
