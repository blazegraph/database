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
package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.sparql.ast.PathNode.PathMod;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.optimizers.ASTALPServiceOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.StaticOptimizer;

/**
 * A special kind of AST node that represents the SPARQL 1.1 arbitrary length
 * path operator. This node has a single child arg - a JoinGroupNode consisting
 * of other operators (the path) that must be run to fixed point. This node also
 * has several annotations that define the schematics (the left and right sides
 * and the lower and upper bounds) of the arbitrary length path.
 */
public class ArbitraryLengthPathNode 
	extends GroupMemberNodeBase<ArbitraryLengthPathNode> 
		implements IBindingProducerNode, IReorderableNode {

    private static final transient Logger log = Logger.getLogger(ArbitraryLengthPathNode.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupNodeBase.Annotations {

    	/**
    	 * The left term - can be a variable or a constant.
    	 */
        String LEFT_TERM = Annotations.class.getName() + ".leftTerm";

    	/**
    	 * The right term - can be a variable or a constant.
    	 */
        String RIGHT_TERM = Annotations.class.getName() + ".rightTerm";

        /**
         * The left transitivity variable.
         */
        String TRANSITIVITY_VAR_LEFT = Annotations.class.getName() + ".transitivityVarLeft";

        /**
         * The right transitivity variable.
         */
        String TRANSITIVITY_VAR_RIGHT = Annotations.class.getName() + ".transitivityVarRight";
        
        /**
         * The lower bound on the number of rounds to run.  Can be zero (0) or
         * one (1).  A lower bound of zero is a special kind of path - the
         * Zero Length Path.  A zero length path connects a vertex to itself
         * (in graph parlance).  In the context of arbitrary length paths it
         * means we bind the input onto the output regardless of whether they
         * are actually connected via the path or not.
         */
        String LOWER_BOUND =  Annotations.class.getName() + ".lowerBound";

        /**
         * The upper bound on the number of rounds to run.
         */
        String UPPER_BOUND =  Annotations.class.getName() + ".upperBound";
        
    }
	
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ArbitraryLengthPathNode(ArbitraryLengthPathNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public ArbitraryLengthPathNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Fully construct an arbitrary length path node with all required
     * annotations.
     */
    public ArbitraryLengthPathNode(final TermNode left, final TermNode right, 
    		final VarNode transitivityVarLeft, final VarNode transitivityVarRight,
    		final PathMod mod) {
    	this(new BOp[] { new JoinGroupNode() }, NV.asMap(
    			new NV(Annotations.LEFT_TERM, left),
    			new NV(Annotations.RIGHT_TERM, right),
    			new NV(Annotations.TRANSITIVITY_VAR_LEFT, transitivityVarLeft),
    			new NV(Annotations.TRANSITIVITY_VAR_RIGHT, transitivityVarRight),
    			new NV(Annotations.LOWER_BOUND, mod == PathMod.ONE_OR_MORE ? 1L : 0L),
    			new NV(Annotations.UPPER_BOUND, mod == PathMod.ZERO_OR_ONE ? 1L : Long.MAX_VALUE)
    			));    			
    }
    
    /**
     * Fully construct an arbitrary length path node with all required
     * annotations.
     */
    public ArbitraryLengthPathNode(final TermNode left, final TermNode right, 
            final VarNode transitivityVarLeft, final VarNode transitivityVarRight,
            final long lowerBound, final long upperBound) {
        this(new BOp[] { new JoinGroupNode() }, NV.asMap(
                new NV(Annotations.LEFT_TERM, left),
                new NV(Annotations.RIGHT_TERM, right),
                new NV(Annotations.TRANSITIVITY_VAR_LEFT, transitivityVarLeft),
                new NV(Annotations.TRANSITIVITY_VAR_RIGHT, transitivityVarRight),
                new NV(Annotations.LOWER_BOUND, lowerBound),
                new NV(Annotations.UPPER_BOUND, upperBound)
                ));             
    }
    
    /**
     * Returns the left term.
     */
    public TermNode left() {
    	return (TermNode) super.getRequiredProperty(Annotations.LEFT_TERM);
    }
    
    /**
     * Returns the right term.
     */
    public TermNode right() {
    	return (TermNode) super.getRequiredProperty(Annotations.RIGHT_TERM);
    }
    
    /**
     * Return the left transitivity var.
     */
    public VarNode tVarLeft() {
    	return (VarNode) super.getRequiredProperty(Annotations.TRANSITIVITY_VAR_LEFT);
    }
    
    /**
     * Return the right transitivity var.
     */
    public VarNode tVarRight() {
    	return (VarNode) super.getRequiredProperty(Annotations.TRANSITIVITY_VAR_RIGHT);
    }
    
    /**
     * Return the lower bound.
     */
    public long lowerBound() {
    	return (Long) super.getRequiredProperty(Annotations.LOWER_BOUND);
    }
    
    /**
     * Return the upper bound.
     */
    public long upperBound() {
    	return (Long) super.getRequiredProperty(Annotations.UPPER_BOUND);
    }
    
    /**
     * Return the subgroup.
     */
    public JoinGroupNode subgroup() {
    	return (JoinGroupNode) get(0);
    }
    
    /**
     * Return the variables bound by the path - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getMaybeProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        addProducedBinding(left(), producedBindings);
        addProducedBinding(right(), producedBindings);
        for (StatementPatternNode sp : subgroup().getStatementPatterns()) {
            addProducedBinding(sp.s(), producedBindings);
            addProducedBinding(sp.p(), producedBindings);
            addProducedBinding(sp.o(), producedBindings);
            addProducedBinding(sp.c(), producedBindings);
        }
        
        return producedBindings;

    }
    
    /**
     * Return the variables bound by the path - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getDefinitelyProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        addProducedBinding(left(), producedBindings);
        addProducedBinding(right(), producedBindings);
        
        return producedBindings;

    }
    
    /**
     * Return the set of variables used by this ALP node (statement pattern
     * terms and inside filters).  Used to determine what needs to be projected
     * into the op.
     */
    public Set<IVariable<?>> getUsedVars() {
        
        final Set<IVariable<?>> used = new LinkedHashSet<IVariable<?>>();
        
        addUsedVar(left(), used);
        addUsedVar(right(), used);

        for (StatementPatternNode sp : subgroup().getStatementPatterns()) {
            addUsedVar(sp.s(), used);
            addUsedVar(sp.p(), used);
            addUsedVar(sp.o(), used);
            addUsedVar(sp.c(), used);
            for (FilterNode filter : sp.getAttachedJoinFilters()) {
                final Iterator<BOp> it = BOpUtility.preOrderIteratorWithAnnotations(filter);
                while (it.hasNext()) {
                    final BOp bop = it.next();
                    if (bop instanceof TermNode) {
                        addUsedVar((TermNode) bop, used);
                    }
                }
            }
        }
        for (FilterNode filter : subgroup().getChildren(FilterNode.class)) {
            final Iterator<BOp> it = BOpUtility.preOrderIteratorWithAnnotations(filter);
            while (it.hasNext()) {
                final BOp bop = it.next();
                if (bop instanceof TermNode) {
                    addUsedVar((TermNode) bop, used);
                }
            }
        }
        
        return used;
        
    }

    private void addUsedVar(final TermNode t, 
            final Set<IVariable<?>> vars) {
        
        addVar(t, vars, true);
        
    }
    
    private void addProducedBinding(final TermNode t, 
            final Set<IVariable<?>> producedBindings) {
        
        addVar(t, producedBindings, false);
        
    }
    
    /**
     * This handles the special case where we've wrapped a Var with a Constant
     * because we know it's bound, perhaps by the exogenous bindings.  If we
     * don't handle this case then we get the join vars wrong.
     * 
     * @see StaticAnalysis._getJoinVars
     */
    private void addVar(final TermNode t, 
            final Set<IVariable<?>> producedBindings, final boolean addAnonymous) {
    	
    	if (t instanceof VarNode) {
    		
    	    if (addAnonymous || !((VarNode) t).isAnonymous()) {
    	        producedBindings.add(((VarNode) t).getValueExpression());
    	    }
            
    	} else if (t instanceof ConstantNode) {
    		
    		final ConstantNode cNode = (ConstantNode) t;
    		final Constant<?> c = (Constant<?>) cNode.getValueExpression();
    		final IVariable<?> var = c.getVar();
    		if (var != null) {
    			producedBindings.add(var);
    		}
    		
    	}
    	
    }

	@Override
	public String toString(int indent) {

		final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(s).append(getClass().getSimpleName());
        sb.append("(left=").append(left()).append(", right=").append(right()).append(") {");
        sb.append(subgroup().toString(indent+1));
        sb.append("\n").append(s);
        sb.append("}");

        final Long rangeCount = (Long) getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

        if (rangeCount != null) {
            sb.append(" AST2BOpBase.estimatedCardinality=");
            sb.append(getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY).toString());
        }
        
        return sb.toString();

	}

	@Override
	public boolean isReorderable() {

		final long estCard = getEstimatedCardinality(null);
		
		return estCard >= 0 && estCard < Long.MAX_VALUE;
		
	}

	@Override
	public long getEstimatedCardinality(final StaticOptimizer opt) {
		
		final JoinGroupNode group = subgroup();
		
		long zeroMatchAdjustment = 0;
		/*
		 * if lowerBound() is zero, and both ?s and ?o are
		 * variables then we (notionally) match
		 * any subject or object in the triple store,
		 * see:
		 * 
		 * http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#defn_evalPP_ZeroOrOnePath
		 * 
		 * Despite this not being implemented, the optimizer does better
		 * knowing this correctly.
		 */
		if (lowerBound() == 0 ) {
			int fixedCount = (left() instanceof VarNode ? 1 : 0) + (right() instanceof VarNode ? 1 : 0);
			switch (fixedCount) {
			case 0:
				zeroMatchAdjustment = left().getValue().equals(right().getValue())?1:0;
				break;
			case 1:
				zeroMatchAdjustment = 1;
				break;
			case 2:
				zeroMatchAdjustment =  Long.MAX_VALUE / 2;
				// The following is more accurate, but more expensive and unnecessary.
				// db.getURICount() + db.getBNodeCount(); 
//				System.err.println("adj: "+zeroMatchAdjustment);
				break;
			}
		}
		
		/*
		 * TODO finish the ASTCardinalityOptimizer
		 */
		BOp pathExpr = null;
		if (group.arity() == 1) {
			
		    pathExpr = group.get(0);
		    
		} else {
		    
		    for (BOp node : group.getChildren()) {
		        final ASTBase astNode = (ASTBase) node;
		        if (astNode.getQueryHintAsBoolean(
		                ASTALPServiceOptimizer.PATH_EXPR, false)) {
		            pathExpr = node;
		            break;
		        }
		    }

		}
		
        final long result;
	    if (pathExpr != null && pathExpr.getProperty(
	            AST2BOpBase.Annotations.ESTIMATED_CARDINALITY) != null) {
	        
            final long estCard = pathExpr.getProperty(
                    AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 
                    Long.MAX_VALUE); 
	            
            result = (estCard < Long.MAX_VALUE) ? 
                    estCard + zeroMatchAdjustment : Long.MAX_VALUE;
                
        } else {
	            
            result = Long.MAX_VALUE;
                   
        }
	                
        if (log.isDebugEnabled()) {
            log.debug("reported cardinality: " + result);
        }
        
        return result;
        
	}

    
}
