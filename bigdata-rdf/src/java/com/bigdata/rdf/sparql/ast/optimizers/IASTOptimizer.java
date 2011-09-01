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
/*
 * Created on Aug 23, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IQueryNode;

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
     * @param queryNode
     *            The AST (in).
     * @param bindingSet
     *            Optional array of zero or more input solutions.
     * 
     * @return The optimized AST.
     */
//    * @param dataset
//    *            The data set against which the AST will be evaluated.
    IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            /*DatasetNode dataset, */IBindingSet[] bindingSets);

}
