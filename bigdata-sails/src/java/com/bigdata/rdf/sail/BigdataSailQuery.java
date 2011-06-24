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

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.AbstractQuery;

/**
 * Extension API for bigdata queries.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * 
 * @see IBigdataParsedQuery
 */
public interface BigdataSailQuery {
    
    /**
     * Returns a copy of the Sesame operator tree that will or would be
     * evaluated by this query (debugging purposes only).
     */
    TupleExpr getTupleExpr() throws QueryEvaluationException;
    
	/**
	 * Return query hints associated with this query. Query hints are embedded
	 * in query strings as namespaces. 
	 * 
	 * @return The query hints and never <code>null</code>.
	 * 
	 * @see QueryHints
	 */
    Properties getQueryHints();

//    /**
//     * Integration point for 3rd party operators and/or data sources such as an
//     * external GIS index. The supplied iteration will be used to feed initial
//     * solutions into the native query evaluation performed by bigdata.
//     * <p>
//     * This provides a batch oriented version of the ability to set some initial
//     * bindings on an {@link AbstractQuery} (you can provide many input
//     * {@link BindingSet}s using this method, not just some variables on the
//     * initial {@link BindingSet}).
//     * <p>
//     * This does not provide a "what-if" facility. Each input binding set
//     * provided via this method must be consistent with the data and the query
//     * for the bindings to flow through. Binding sets which are not consistent
//     * with the data and the query will be pruned.
//     * 
//     * @param bindings
//     *            The bindings to feed into the query evaluation.
//     * 
//     * @throws IllegalStateException
//     *             if the bindings have already been set to a non-
//     *             <code>null</code> value.
//     * 
//     * @see AbstractQuery#setBinding(String, org.openrdf.model.Value)
//     * 
//     * @see https://sourceforge.net/apps/trac/bigdata/ticket/267
//     */
//    void setBindingSets(
//            final CloseableIteration<BindingSet, QueryEvaluationException> bindings);
//
//    /**
//     * Return the iteration set by {@link #setBindingSets(CloseableIteration)}.
//     */
//    CloseableIteration<BindingSet, QueryEvaluationException> getBindingSets();

}
