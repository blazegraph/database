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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Lift FILTERs which can be evaluated based solely on the bindings in the
 * parent group out of a child group. This helps because we will issue the
 * subquery for the child group less often (assuming that the filter rejects any
 * solutions). This optimizer is based on
 * {@link StaticAnalysis#getPreFilters(JoinGroupNode)}. Anything reported by
 * that method can be lifted out of the child group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTLiftPreFiltersOptimizer.java 5193 2011-09-15 14:18:56Z
 *          thompsonbry $
 * 
 *          FIXME Implement.
 */
public class ASTLiftPreFiltersOptimizer implements IASTOptimizer {

    @Override
    public QueryNodeWithBindingSet optimize(
       final AST2BOpContext context, final QueryNodeWithBindingSet input) {

       final IQueryNode queryNode = input.getQueryNode();
       final IBindingSet[] bindingSets = input.getBindingSets();     

       return new QueryNodeWithBindingSet(queryNode, bindingSets);
        
    }

}
