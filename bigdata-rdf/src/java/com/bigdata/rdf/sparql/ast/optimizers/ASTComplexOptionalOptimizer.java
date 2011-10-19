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
 * Created on Oct 19, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Rewrite a join group using two or more complex OPTIONAL groups using a hash
 * join pattern.
 * <p>
 * Queries with multiple complex optional groups can be rewritten into the hash
 * join of solution sets as follows.
 * 
 * <ol>
 * 
 * <li>First, create a hash index from the required joins and any simple
 * optional joins. Given the modeling abilities of the AST, this is most easily
 * achieved by converting the required joins into a named subquery. The join
 * variable(s) for that named subquery will the subset of variable(s) which are
 * shared by each of the complex OPTIONAL groups. The
 * {@link ASTNamedSubqueryOptimizer} already handles the assignment of join
 * variables, so we do not need to consider it further here.</li>
 * 
 * <li>For each complex optional group, use the solution step generated in (1)
 * and run the optional group as a named subquery producing a new solution set.
 * The WHERE clause of the named subquery should look like:
 * 
 * <pre>
 * {INCLUDE %set . OPTIONAL {...}}
 * </pre>
 * 
 * </li>
 * 
 * <li>Join all of the named solution sets in (2) back together. For example, if
 * there were two complex optional groups and the required joins resulted in
 * known bound variables for var1 and var2, then those result sets might be
 * combined as follows in the main WHERE clause of the rewritten query.
 * 
 * <pre>
 * INCLUDE %set1 .
 * INCLUDE %set2 JOIN ON (?var1, ?var2) .
 * </pre>
 * 
 * Note: The join variables for those INCLUDEs MUST be identified through static
 * analysis. Failure to use available join variables will result in an extremely
 * inefficient query plan as the full cross product of the solutions will be
 * compared to identify solutions which join.</li>
 * 
 * </ol>
 * 
 * TODO The rewrite into named subquery includes means that we wind up building
 * more hash indices than we strictly require as a hash index will also be built
 * at the start of each optional group. However, since the hash index at the
 * start of the optional group has exactly the same data as the named subquery
 * include's hash index, we should elide the step which builds the extra hash
 * index.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTComplexOptionalOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {
        return queryNode;
    }

}
