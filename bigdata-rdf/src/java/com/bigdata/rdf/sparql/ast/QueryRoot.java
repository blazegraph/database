/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

import com.bigdata.rdf.sail.QueryType;

/**
 * The top-level Query.
 * 
 * @see DatasetNode
 * @see IGroupNode
 * @see ProjectionNode
 * @see GroupByNode
 * @see HavingNode
 * @see OrderByNode
 * @see SliceNode
 */
public class QueryRoot extends QueryBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends QueryBase.Annotations {
        
        String DATASET = "dataset";
        
        String NAMED_SUBQUERIES = "namedSubqueries";
        
    }

//    private DatasetNode dataset;
//    
//    // optional list of subqueries (if any).
//    private NamedSubqueriesNode namedSubqueries;
    
    public QueryRoot(final QueryType queryType) {
        
        super(queryType);
        
    }

    /**
     * This is a root node. It may not be attached as a child of another node.
     * 
     * @throws UnsupportedOperationException
     */
    public void setParent(final IGroupNode<?> parent) {
    
        throw new UnsupportedOperationException();
        
    }
    
    public void setDataset(final DatasetNode dataset) {

        setProperty(Annotations.DATASET, dataset);

    }

    public DatasetNode getDataset() {

        return (DatasetNode) getProperty(Annotations.DATASET);

    }
    
    /**
     * Return the node for the named subqueries -or- <code>null</code> if there
     * it does not exist.
     */
    public NamedSubqueriesNode getNamedSubqueries() {
        
        return (NamedSubqueriesNode) getProperty(Annotations.NAMED_SUBQUERIES);
        
    }
    
    /**
     * Set or clear the named subqueries node.
     * 
     * @param namedSubqueries
     *            The named subqueries not (may be <code>null</code>).
     */
    public void setNamedSubqueries(final NamedSubqueriesNode namedSubqueries) {

        setProperty(Annotations.NAMED_SUBQUERIES, namedSubqueries);

    }

    public String toString(final int indent) {
        
        final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();

        final DatasetNode dataset = getDataset();

        final NamedSubqueriesNode namedSubqueries = getNamedSubqueries();
        
        if (dataset != null) {
            sb.append("\n");
            sb.append(s);
            sb.append(dataset.toString());
        }

        if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

            sb.append("\n");
            
            sb.append(s);
            
            sb.append("named subqueries");
            
            sb.append(namedSubqueries.toString(indent));

        }
        
        sb.append(super.toString(indent));
        
        return sb.toString();
        
    }

//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof QueryRoot))
//            return false;
//
//        if (!super.equals(o))
//            return false;
//
//        final QueryRoot t = (QueryRoot) o;
//
//        if (dataset == null) {
//            if (t.dataset != null)
//                return false;
//        } else {
//            if (!dataset.equals(t.dataset))
//                return false;
//        }
//
//        if (namedSubqueries == null) {
//            if (t.namedSubqueries != null)
//                return false;
//        } else {
//            if (!namedSubqueries.equals(t.namedSubqueries))
//                return false;
//        }
//
//        return true;
//
//    }

}
