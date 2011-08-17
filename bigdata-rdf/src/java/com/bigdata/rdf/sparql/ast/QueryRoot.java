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
 * 
 *      FIXME We will also have to support UPDATE, which is another root for a
 *      SPARQL construct.
 * 
 *      FIXME Add support for WITH {subquery} AS "name" and INCLUDE. The WITH
 *      must be in the top-level query. For example
 * 
 *      <pre>
 * SELECT ?var1
 * WITH {
 *         SELECT ?var9
 *         WHERE {
 *                 ?var9 p3:invoiceDocumentDate ?_var10.
 *                 ?var9 p3:paymentPeriod ?_var11. 
 *                 ?var9 rdf:type p3:Invoice
 *                 FILTER ((?_var10 <  """2012-01-01"""^^<http://www.w3.org/2001/XMLSchema#date>))
 *                 FILTER ((?_var11 >= """0"""^^<http://www.w3.org/2001/XMLSchema#int>))
 *         }
 * } AS %namedSet1
 *  WHERE {
 *         ?var9 p3:invoicePaymentDate ?var3.
 *          LET (?var1 := TEXT(?var3, "yyyy-MM")).  
 *         INCLUDE %namedSet1
 * }
 * </pre>
 */
public class QueryRoot extends QueryBase {

    private DatasetNode dataset;

    /**
     * 
     * @param root
     *            The "WHERE" clause.
     */
    public QueryRoot(final IGroupNode root) {

        super(root);

    }

    public void setDataset(final DatasetNode dataset) {

        this.dataset = dataset;

    }

    public DatasetNode getDataset() {

        return dataset;

    }

}
