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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Interface for declaring query hints.
 * 
 * @param <T>
 *            The generic type of the value space for the query hint.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IQueryHint<T> {

    /**
     * The name of the query hint.
     */
    String getName();

    /**
     * The default value for this query hint (many query hints provide overrides
     * of defaults).
     */
    T getDefault();

    /**
     * Validate the value, returning an object of the appropriate type.
     * 
     * @param value
     *            The value.
     * 
     * @return The validated value.
     * 
     * @throws RuntimeException
     *             if the value can not be validated.
     */
    T validate(String value);

    /**
     * Handle the query hint.
     * <p>
     * Note: The name of the query hint is no longer strongly coupled to the
     * name of the annotation. This method may be used to attach zero or more
     * annotations as appropriate to the AST structure. It may also be used to
     * change defaults in the {@link AST2BOpContext} or take similar actions.
     * <p>
     * Note: When <code>scope</code> EQ {@link QueryHintScope#Query}, the
     * implementation SHOULD also act on {@link AST2BOpContext#queryHints},
     * setting the value in the global scope.
     * 
     * @param ctx
     *            The query evaluation context.
     * @param queryRoot
     *            The root of the query. This is required to resolve the parent
     *            of a query hint inside of a FILTER.
     * @param scope
     *            The {@link QueryHintScope} specified for the query hint.
     * @param op
     *            An AST node to which the hint should bind.
     * @param value
     *            The value specified for the query hint.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for
     *      FILTER EXISTS </a>
     * @see <a href="http://trac.blazegraph.com/ticket/990"> Query hint not
     *      recognized in FILTER</a>
     */
    void handle(AST2BOpContext ctx, QueryRoot queryRoot, QueryHintScope scope,
            ASTBase op, T value);

}
