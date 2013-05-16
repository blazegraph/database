/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer.Annotations;

/**
 * Calculate the estimated cardinality of a join group.
 * 
 * TODO Calculate estimated cardinality of other things like subqueries and
 * unions.
 * 
 * @author mikepersonick
 *
 */
public class ASTCardinalityOptimizer extends AbstractJoinGroupOptimizer
		implements IASTOptimizer {

	public ASTCardinalityOptimizer() {
		/*
		 * The cardinality of a join group can only be calculated if all of its
		 * {@link IBindingProducerNode} children have a known cardinality.
		 * Thus we must go depth first. 
		 */
		super(true /* childFirst */, false /* optimizeServiceNodes */);
	}
	
    /**
     * Calculate the estimated cardinality of a join group.
     * 
     * We will do this by first dividing the group into subgroups that share
     * variables.  For each subgroup, we will calculate the estimated cardinality
     * of that subgroup.  Then we will multiple those cardinalities together
     * to get an estimate of cardinality for the whole group.
     * 
     * The estimated cardinality of a subgroup will be calculated using the
     * same logic contained in the ASTStaticJoinOptimizer.
     */
	protected void optimizeJoinGroup(final AST2BOpContext ctx, 
    		final StaticAnalysis sa, final IBindingSet[] bSets,
    		final JoinGroupNode group) {

		final long cardinality = Long.MAX_VALUE;
		
		final List<IBindingProducerNode> nodes = 
				group.getChildren(IBindingProducerNode.class);
		
		for (IBindingProducerNode node : nodes) {
			
			if (node.getProperty(Annotations.ESTIMATED_CARDINALITY) == null) {
				
				/*
				 * Cannot optimize.
				 */
				return;
				
			}
			
		}
		
		group.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
        
    }
	
}
