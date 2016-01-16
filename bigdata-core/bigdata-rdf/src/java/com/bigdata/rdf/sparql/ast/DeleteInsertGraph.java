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
 */
public class DeleteInsertGraph extends GraphUpdate implements
        IGraphPatternContainer, IDataSetNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GraphUpdate.Annotations,
            IGraphPatternContainer.Annotations, IDataSetNode.Annotations {

        /**
         * The optional DELETE clause.
         */
        String DELETE_CLAUSE = "deleteClause";
        
        /**
         * The optional INSERT clause.
         */
        String INSERT_CLAUSE = "insertClause";

    }

    public DeleteInsertGraph() {

        super(UpdateType.DeleteInsert);
        
    }

    /**
     * @param op
     */
    public DeleteInsertGraph(final DeleteInsertGraph op) {

        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public DeleteInsertGraph(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The data set can only be specified for the general case of
     * DELETE/INSERT. The data set is specific to the DELETE/INSERT operation to
     * which it is attached (it is not inherited or combined with the data set
     * for later operations in a sequence).
     */
    @Override
    public void setDataset(final DatasetNode dataset) {

        setProperty(Annotations.DATASET, dataset);

    }

    @Override
    public DatasetNode getDataset() {

        return (DatasetNode) getProperty(Annotations.DATASET);

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

    /**
     * Set the WHERE clause.
     */
    public void setWhereClause(
            final GraphPatternGroup<IGroupMemberNode> whereClause) {

        setGraphPattern(whereClause);
        
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
     * Return the template for the DELETE clause.
     */
    public QuadsDataOrNamedSolutionSet getDeleteClause() {

        return (QuadsDataOrNamedSolutionSet) getProperty(Annotations.DELETE_CLAUSE);

    }

    /**
     * Return the template for the INSERT clause.
     */
    public QuadsDataOrNamedSolutionSet getInsertClause() {

        return (QuadsDataOrNamedSolutionSet) getProperty(Annotations.INSERT_CLAUSE);

    }

    public void setDeleteClause(final QuadsDataOrNamedSolutionSet data) {

        setProperty(Annotations.DELETE_CLAUSE, data);

    }

    public void setInsertClause(final QuadsDataOrNamedSolutionSet data) {

        setProperty(Annotations.INSERT_CLAUSE, data);

    }

    @Override
    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        final DatasetNode dataset = getDataset();

        final QuadsDataOrNamedSolutionSet deleteClause = getDeleteClause();

        final QuadsDataOrNamedSolutionSet insertClause = getInsertClause();
        
        final GraphPatternGroup<?> whereClause = getWhereClause();
        
        if (dataset != null) {

            sb.append(dataset.toString(indent + 1));

        }

        if (deleteClause != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("DELETE ");
            sb.append(deleteClause.toString(indent + 2));
        }

        if (insertClause != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("INSERT ");
            sb.append(insertClause.toString(indent + 2));
        }

        if (whereClause != null) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append("WHERE");
            sb.append(whereClause.toString(indent + 2));
        }
        
        sb.append("\n");

        return sb.toString();

    }

}
