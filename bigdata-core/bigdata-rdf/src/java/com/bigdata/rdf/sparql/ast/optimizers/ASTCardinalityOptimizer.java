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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.UnionNode;
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

    private static final transient Logger log = Logger.getLogger(ASTCardinalityOptimizer.class);
    
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

//		final long cardinality = Long.MAX_VALUE;
		
	    for (IGroupMemberNode child : group.getChildren()) {
	        
	        if (child instanceof UnionNode) {
	            
	            final UnionNode union = (UnionNode) child;

	            boolean canEstimate = true;
	            
	            for (JoinGroupNode join : union.getChildren()) {
	                
	                /*
	                 * We can only attach an estimate to the union if we
	                 * get an estimate for all of its children.
	                 */
	                canEstimate &= 
	                        join.getProperty(Annotations.ESTIMATED_CARDINALITY) != null;
	                
	            }

	            if (canEstimate) {
	                
	                long cardinality = 0;
	                
	                for (JoinGroupNode join : union.getChildren()) {

	                    cardinality += (long) 
	                            join.getProperty(Annotations.ESTIMATED_CARDINALITY);
	                    
	                }
	                
	                if (log.isDebugEnabled()) {
	                    log.debug("able to estimate the cardinality for a union: " + cardinality);
	                }
	                
	                union.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
	                
	            }
	            
	        } else if (child instanceof ArbitraryLengthPathNode) {
	            
	            final ArbitraryLengthPathNode alp = 
	                    (ArbitraryLengthPathNode) child; 
	            
	            final long cardinality = alp.getEstimatedCardinality(null);
	            
	            if (cardinality < Long.MAX_VALUE) {
	                alp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
	            }
	            
	        }
	        
	    }
	    
		final List<IBindingProducerNode> nodes = 
				group.getChildren(IBindingProducerNode.class);
		
		if (nodes.size() == 1) {
		    
		    final IBindingProducerNode node = nodes.get(0);
		    
		    if (node.getProperty(Annotations.ESTIMATED_CARDINALITY) != null) {
		        
		        final long cardinality = (long)
		                node.getProperty(Annotations.ESTIMATED_CARDINALITY);
		        
		        if (log.isDebugEnabled()) {
		            log.debug("setting cardinality on a singleton group: " + cardinality);
		        }
		        
		        group.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
		        
		    }
		    
		} else {
		    
		    /*
		     * TODO Calculate estimated cardinality according to logic in
		     * ASTStaticJoinOptimizer.
		     */
		    
		}
		
//		group.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
        
    }
	
}
