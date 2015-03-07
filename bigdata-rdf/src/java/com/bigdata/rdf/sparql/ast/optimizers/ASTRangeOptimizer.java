/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

/**
 * Attach range bops.
 */
public class ASTRangeOptimizer extends AbstractJoinGroupOptimizer
		implements IASTOptimizer {

	private static final transient Logger log = Logger.getLogger(ASTRangeOptimizer.class);
	
	public ASTRangeOptimizer() {
		super(false /* childFirst */, true /* optimizeServiceNodes */);
	}
	
    /**
     * Optimize the join group, attach range nodes.
     */
    @SuppressWarnings("rawtypes")
	protected void optimizeJoinGroup(final AST2BOpContext ctx, 
    		final StaticAnalysis sa, final IBindingSet[] bSets, final JoinGroupNode group) {

    	/*
    	 * First see if we have anything "rangeable".
    	 */
    	boolean rangeSafe = false;
        for (IQueryNode node : group) {

            if (!(node instanceof StatementPatternNode))
                continue;
            
            final StatementPatternNode sp = (StatementPatternNode) node;
            
            rangeSafe |= sp.getQueryHintAsBoolean(QueryHints.RANGE_SAFE, false);
            
        }
        if (!rangeSafe)
        	return;
        
		final Map<VarNode, RangeNode> ranges = 
				new LinkedHashMap<VarNode, RangeNode>();
    	
        for (IQueryNode node : group) {

            if (!(node instanceof FilterNode))
                continue;
            
            final FilterNode filter = (FilterNode) node;
            
            if (log.isDebugEnabled())
            	log.debug(filter);
            
            if (!(filter.getValueExpressionNode() instanceof FunctionNode))
            	continue;
            
            final FunctionNode function = (FunctionNode) 
            	filter.getValueExpressionNode();
            
            if (log.isDebugEnabled())
            	log.debug(function);
            
            processFunction(function, ranges);

        }
        
        // didn't find any
        if (ranges.isEmpty())
        	return;
    	
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        for (IQueryNode node : group) {

            if (!(node instanceof StatementPatternNode))
                continue;
            
            final StatementPatternNode sp = (StatementPatternNode) node;
            
            if (!sp.getQueryHintAsBoolean(QueryHints.RANGE_SAFE, false))
            	continue;

            if (!sp.o().isVariable())
            	continue;
            
            final VarNode v = (VarNode) sp.o();
            
			if (!ranges.containsKey(v))
				continue;

			final RangeNode range = ranges.get(v);
			
			final RangeBOp bop = toRangeBOp(range, globals);
			
			if (log.isDebugEnabled()) {
				log.debug("attaching a range:\n" + range + "\n to statement pattern: " + sp);
			}
			
			range.setRangeBOp(bop);
			
			sp.setRange(range);
			
		}
    	
    }
    
    /**
     * Public static faciliatates the test cases.
     */
    public static RangeBOp toRangeBOp(
    		final RangeNode range, final GlobalAnnotations globals) {
    	
//    	final IVariable<? extends IV> var = range.var().getValueExpression();
    	
    	final RangeBOp bop = new RangeBOp();
    	
    	final ValueExpressionNode from = range.from();

    	if (from != null) {
    		
    		final IValueExpression<? extends IV> ve = 
    			AST2BOpUtility.toVE(globals, from);
    		
    		bop.setFrom(ve);
    		
    	}
    	
    	final ValueExpressionNode to = range.to();

    	if (to != null) {
    		
    		final IValueExpression<? extends IV> ve = 
    			AST2BOpUtility.toVE(globals, to);
    		
    		bop.setTo(ve);
    		
    	}
    	
    	return bop;
    	
    }
    
    private void processFunction(
    		final FunctionNode function, final Map<VarNode, RangeNode> ranges) {
    	
    	final URI uri = function.getFunctionURI();
    	
    	if (uri.equals(FunctionRegistry.AND)) {
    		
    		final ValueExpressionNode left = (ValueExpressionNode) function.get(0);
    		
    		if (left instanceof FunctionNode) {
    			
    			processFunction((FunctionNode) left, ranges);
    			
    		}
    		
    		final ValueExpressionNode right = (ValueExpressionNode) function.get(1);
    		
    		if (right instanceof FunctionNode) {
    			
    			processFunction((FunctionNode) right, ranges);
    			
    		}
    		
    	} else if (uri.equals(FunctionRegistry.GT) || uri.equals(FunctionRegistry.GE)) {
    
    		final ValueExpressionNode left = (ValueExpressionNode) function.get(0);
    		final ValueExpressionNode right = (ValueExpressionNode) function.get(1);
    		
    		// ?left > ?right
    		if (left instanceof VarNode) {
    			addLowerBound((VarNode) left, right, ranges);
    		}
    		
    		if (right instanceof VarNode) {
    			addUpperBound((VarNode) right, left, ranges);
    		}
    		
    	} else if (uri.equals(FunctionRegistry.LT) || uri.equals(FunctionRegistry.LE)) {
    	    
    		final ValueExpressionNode left = (ValueExpressionNode) function.get(0);
    		final ValueExpressionNode right = (ValueExpressionNode) function.get(1);
    		
    		// ?left < ?right
    		if (left instanceof VarNode) {
    			addUpperBound((VarNode) left, right, ranges);
    		}
    		
    		if (right instanceof VarNode) {
    			addLowerBound((VarNode) right, left, ranges);
    		}
    		
    	}
    	
    }
    
    private void addUpperBound(final VarNode var, final ValueExpressionNode ve, 
    		final Map<VarNode, RangeNode> ranges) {
    	
    	RangeNode range = ranges.get(var);
    	if (range == null) {
    		range = new RangeNode(var);
    		ranges.put(var, range);
    	}

    	ValueExpressionNode to = range.to();
    	if (to == null) {
    		to = (ValueExpressionNode) ve.clone();
    	} else {
    		to = FunctionNode.MIN(to, ve);
    	}
    	
    	range.setTo(to);
    	
    }
    
    private void addLowerBound(final VarNode var, final ValueExpressionNode ve, 
    		final Map<VarNode, RangeNode> ranges) {
    	
    	RangeNode range = ranges.get(var);
    	if (range == null) {
    		range = new RangeNode(var);
    		ranges.put(var, range);
    	}

    	ValueExpressionNode from = range.from();
    	if (from == null) {
    		from = (ValueExpressionNode) ve.clone();
    	} else {
    		from = FunctionNode.MAX(from, ve);
    	}
    	
    	range.setFrom(from);
    	
    }
    
}
