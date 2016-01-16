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
 * Created on Aug 23, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Interface for AST rewrites. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IASTOptimizer {

    /**
     * Optimize the AST.
     * 
     * @param context
     *            The evaluation context.
     * @param input
     *            The input to the optimizer, consisting of a queryNode and
     *            input binding set.
     * 
     * @return An instance of type QueryNodeWithBindingSet.
     */
    QueryNodeWithBindingSet optimize(
        AST2BOpContext context, QueryNodeWithBindingSet input);

}
