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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IQueryNode;

/**
 * Rewrites aspects of queries where bottom-up evaluation would produce
 * different results. This includes joins which are not "well designed" as
 * defined in section 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge
 * Pérez et al. It also includes the evaluation of filters when they are using
 * variables which are guarantee to not be bound in the context in which the
 * filter is evaluation.
 * <p>
 * Note: The test suite for this class is a set of DAWG tests which focus on
 * bottom up evaluation semantics, including:
 * <p>
 * Nested Optionals - 1 (Query is not well designed because there are no shared
 * variables in the intermediate join group and there is an embedded OPTIONAL
 * join group. Since ?v is not present in the intermediate join group the (:x3
 * :q ?w . OPTIONAL { :x2 :p ?v }) solutions must be computed first and then
 * joined against the (:x1 :p ?v) solutions.)
 * 
 * <pre>
 * SELECT *
 * { 
 *     :x1 :p ?v .
 *     OPTIONAL
 *     {
 *       :x3 :q ?w .
 *       OPTIONAL { :x2 :p ?v }
 *     }
 * }
 * </pre>
 * 
 * Filter-nested - 2 (Filter on variable ?v which is not in scope)
 * 
 * <pre>
 * SELECT ?v
 * { :x :p ?v . { FILTER(?v = 1) } }
 * </pre>
 * 
 * Filter-scope - 1 (Query is not well designed because there are no shared
 * variables in the intermediate join group and there is an embedded OPTIONAL
 * join group. Also, ?v is used in the FILTER but is not visible in that scope.)
 * 
 * <pre>
 * SELECT *
 * { 
 *     :x :p ?v . 
 *     { :x :q ?w 
 *       OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
 *     }
 * }
 * </pre>
 * 
 * Join-scope - 1 (Query is not well designed because there are no shared
 * variables in the intermediate group and there is an embedded OPTIONAL join
 * group.)
 * 
 * <pre>
 * SELECT *
 * { 
 *   ?X  :name "paul"
 *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
 * }
 * </pre>
 * 
 * We can handle the problems identified in these tests by a combination of
 * mechanisms:
 * <ul>
 * <li>FILTERs which use variables which can not be bound may be elimated, but
 * only if the variable is not bound in the provided {@link IBindingSet}[].
 * Since we need to consider exogenous bindings, this can not always solve the
 * problem for badly designed queries with a FILTER on an known unbound
 * variable.</li>
 * <li>Nested groups which do not share variables with their parent can be
 * lifted out into a named subquery. This has the same effect as bottom up
 * evaluation since we will run the named subquery first and then perform the
 * join against the parent group. However, in this case an exogenous binding
 * which causes a shared variable to exist would mean that the query could run
 * normally since the value in the outer group and the inner group would now be
 * correlated through the exogenous binding. E.g., <code?X</code> in the last
 * example above.</li>
 * </ul>
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
 * @see http://www.dcc.uchile.cl/~cgutierr/papers/sparql.pdf
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTBottomUpOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTBottomUpOptimizer() {
    }

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {
        
        /*
         * FIXME SUPPORT BOTTOM UP EVALUATION:
         */
        return queryNode;
    
    }

}
