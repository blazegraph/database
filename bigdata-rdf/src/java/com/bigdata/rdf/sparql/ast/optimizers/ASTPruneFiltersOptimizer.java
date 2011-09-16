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
 * Created on Sep 16, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IQueryNode;

/**
 * Prune the AST when a filter can not be evaluated because one or more
 * variables on which it depends will never be bound within the scope in which
 * the filter appears.
 * <p>
 * FILTERs which use variables which can not be bound may be elimated, but only
 * if the variable is not bound in the provided {@link IBindingSet}[].
 * 
 * FIXME Implement. This fixes one of the DAWG "bottom-up" evaluation tests, but
 * also test when we pass a {@link IBindingSet} to the optimizer with a binding
 * for <code>?v</code> since the filter MUST NOT be eliminated in that case.
 * 
 * <pre>
 * Filter-nested - 2 (Filter on variable ?v which is not in scope)
 * 
 * <pre>
 * SELECT ?v
 * { :x :p ?v . { FILTER(?v = 1) } }
 * </pre>
 * <p>
 * Note: This was handled historically by
 * 
 * <pre>
 * 
 * If the scope binding names are empty we can definitely
 * always fail the filter (since the filter's variables
 * cannot be bound).
 * 
 *                 if (filter.getBindingNames().isEmpty()) {
 *                     final IConstraint bop = new SPARQLConstraint(SparqlTypeErrorBOp.INSTANCE);
 *                     sop.setBOp(bop);
 * </pre>
 * 
 * We need to figure out the variables that are in scope when the filter is
 * evaluated and then filter them out when running the group in which the filter
 * exists (it runs as a subquery). If there are NO variables that are in scope,
 * then just fail the filter per the code above.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTPruneFiltersOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        return queryNode;
        
    }

}
