/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * The DELETE/INSERT operation can be used to remove or add triples from/to the
 * Graph Store based on bindings for a query pattern specified in a WHERE
 * clause:
 * 
 * <pre>
 * ( WITH IRIref )?
 * ( ( DeleteClause InsertClause? ) | InsertClause )
 * ( USING ( NAMED )? IRIref )*
 * WHERE GroupGraphPattern
 * </pre>
 * 
 * The DeleteClause and InsertClause forms can be broken down as follows:
 * 
 * <pre>
 * DeleteClause ::= DELETE  QuadPattern 
 * InsertClause ::= INSERT  QuadPattern
 * </pre>
 * 
 * @see http://www.w3.org/TR/sparql11-update/#deleteInsert
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME Need deleteClause and/or insertClause in addition to the
 *          whereClause.
 */
public class DeleteInsertGraph extends GraphUpdate implements
        IGraphPatternContainer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GraphUpdate.Annotations,
            IGraphPatternContainer.Annotations {

    }

    public DeleteInsertGraph() {

        super(UpdateType.DeleteInsert);
        
    }

    /**
     * @param op
     */
    public DeleteInsertGraph(DeleteInsertGraph op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public DeleteInsertGraph(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
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

}
