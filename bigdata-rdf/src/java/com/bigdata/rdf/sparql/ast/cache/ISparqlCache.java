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
 * Created on Mar 26, 2012
 */

package com.bigdata.rdf.sparql.ast.cache;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.striterator.ICloseableIterator;

/**
 * A SPARQL solution set cache or a connection to a remove SPARQL cache or cache
 * fabric.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISparqlCache {

    /**
     * Initialize the cache / cache connection.
     */
    void init();
    
    /**
     * Close the cache / cache connection.
     */
    void close();

    /**
     * Clear the specified named solution set.
     * 
     * @param solutionSet
     *            The name of the solution set.
     * 
     * @return <code>true</code> iff a solution set by that name existed and was
     *         cleared.
     */
    boolean clearSolutions(AST2BOpContext ctx, String solutionSet);

    /**
     * Clear all named solution sets.
     */
    void clearAllSolutions(AST2BOpContext ctx);

    /**
     * Create a named solution set.
     * 
     * @param ctx
     * @param solutionSet
     *            The name of the solution set.
     * @param params
     *            The configuration parameters (optional).
     * 
     * @throws RuntimeException
     *             if a solution set exists for that name.
     */
    void createSolutions(AST2BOpContext ctx, String solutionSet, ISPO[] params);

    /**
     * Save the solutions a named solution set.
     * 
     * @param ctx
     * @param solutionSet
     *            The name of the solution set.
     * @param src
     *            The solutions.
     */
    void putSolutions(AST2BOpContext ctx, String solutionSet,
            ICloseableIterator<IBindingSet[]> src);

    /**
     * Read the solutions from a named solution set.
     * 
     * @param ctx
     * @param solutionSet
     *            The name of the solution set.
     * 
     * @return An iterator from which the solutions may be drained.
     * 
     * @throws IllegalStateException
     *             if no solution set with that name exists.
     */
    ICloseableIterator<IBindingSet[]> getSolutions(AST2BOpContext ctx,
            String solutionSet);

    /**
     * Return <code>true</code> iff a named solution set exists.
     * 
     * @param ctx
     * @param solutionSet
     *            The name of the solution set.
     *            
     * @return <code>true</code> iff a solution set having that name exists.
     */
    boolean existsSolutions(AST2BOpContext ctx, String solutionSet);
    
//    /**
//     * Return the result from the cache -or- <code>null</code> if there is a
//     * cache miss.
//     * 
//     * @param ctx
//     *            The {@link AST2BOpContext}.
//     *            
//     * @param queryOrSubquery
//     *            The query.
//     * 
//     * @return The cache hit -or- <code>null</code>
//     */
//    ICacheHit get(final AST2BOpContext ctx, final QueryBase queryOrSubquery);
//
//    /**
//     * Publish a solution set to the cache.
//     * 
//     * @param ctx
//     *            The query context in which the solution set was generated.
//     * @param queryOrSubquery
//     *            The query or subquery used to generate the solution set.
//     * @param src
//     *            An iterator which can drain the solution set.
//     * @return A reference to the cached solution set.
//     */
//    ICacheHit put(final AST2BOpContext ctx, final QueryBase queryOrSubquery,
//            final ICloseableIterator<IBindingSet> src);

}