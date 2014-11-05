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
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizes SELECT COUNT(*) { triple-pattern } using the fast range count
 * mechanisms when that feature would produce exact results for the KB instance.
 * 
 * <h2>Cases handled by this optimizer</h2>
 * 
 * Basic combinations with identical semantics:
 * <pre>SELECT COUNT(DISTINCT *) {?s ?p ?o}</pre>
 * <pre>SELECT COUNT(REDUCED *) {?s ?p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s ?p ?o}</pre>
 * 
 * Combinations using a constrained range-count.
 * <pre>SELECT COUNT(*) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s :p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s ?p :o}</pre>
 * <pre>SELECT COUNT(*) {:s ?p :o}</pre>

 * Combinations using a constrained range-count where the triple pattern is
 * 1-unbound and the COUNT() references the unbound variable.
 * <pre>SELECT COUNT(?s) {?s :p :o}</pre>
 * <pre>SELECT COUNT(?p) {:s ?p :o}</pre>
 * <pre>SELECT COUNT(?o) {:s :p ?o}</pre>

 * Combinations using a constrained range-count with a QUADS mode access path.
 * <pre>SELECT COUNT(*) { GRAPH ?g {?s ?p ?o} }</pre>
 * <pre>SELECT COUNT(*) { GRAPH :g {:s ?p ?o} }</pre>
 * 
 * Combinations using a constrained range-count with a QUADS mode access path
 * where the triple pattern is 1-unbound and the COUNT() references the unbound variable.
 * <pre>SELECT COUNT(?s) { GRAPH :g {?s :p :o} }</pre>
 * <pre>SELECT COUNT(?g) { GRAPH ?g {:s :p :o} }</pre>
 * 
 * Combinations using a sub-select with nothing projected in:
 * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } }</pre>
 * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } :s :p :o .}</pre>
 * 
 * Combinations using a sub-select with something projected in:
 * <pre>SELECT * { ?s a :b . { SELECT COUNT(*) {?s ?p ?o} .}</pre>
 *
 * <h2>Correct rejection cases NOT handled by this optimizer</h2>
 *
 * Combinations using DISTINCT/REDUCED and a constrained range-count,
 * explicitly naming the variables, and having variables that are not in
 * the COUNT() aggregate and not projected in are NOT handled here. These
 * are covered by the {@link ASTDistinctTermScanOptimizer} instead:
 * 
 * <pre>SELECT COUNT(?p) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(DISTINCT ?p) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(REDUCED ?p) {:s ?p ?o}</pre>
 * 
 * Sub-select that would be handled as a distinct term scan with something
 * projected in.
 * <pre>SELECT * { ?s a :b . { SELECT COUNT(?p) {?s ?p ?o} .}</pre>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1037" > Rewrite SELECT
 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTFastRangeCountOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTFastRangeCountOptimizer() {
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

    	return queryNode;
    	
    }


}
