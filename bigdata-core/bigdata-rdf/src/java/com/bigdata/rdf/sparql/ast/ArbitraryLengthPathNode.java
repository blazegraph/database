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
package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
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
        
        /**
         * The middle term - can be a variable or a constant.
         */
        String MIDDLE_TERM = Annotations.class.getName() + ".middleTerm";

        /**
         * The variable representing the visited edge. Bound using the binding 
         * from the middle term. Only used by ALP service when projecting edges.
         */
        String EDGE_VAR = Annotations.class.getName() + ".edgeVar";
        
        /**
         * A set of intermediate variables (VarNodes) used by the ALP node
         * that should be dropped from the solutions after each round.
         */
        String DROP_VARS = Annotations.class.getName() + ".dropVars";
        
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
    		final VarNode tVarLeft, final VarNode tVarRight,
    		final PathMod mod) {
    	this(new BOp[] { new JoinGroupNode() }, NV.asMap(
    			new NV(Annotations.LEFT_TERM, left),
    			new NV(Annotations.RIGHT_TERM, right),
    			new NV(Annotations.TRANSITIVITY_VAR_LEFT, tVarLeft),
    			new NV(Annotations.TRANSITIVITY_VAR_RIGHT, tVarRight),
    			new NV(Annotations.LOWER_BOUND, mod == PathMod.ONE_OR_MORE ? 1L : 0L),
    			new NV(Annotations.UPPER_BOUND, mod == PathMod.ZERO_OR_ONE ? 1L : Long.MAX_VALUE)
    			));
    	
    	final Set<VarNode> dropVars = new LinkedHashSet<>();
    	dropVars.add(tVarLeft);
        dropVars.add(tVarRight);
    	setProperty(Annotations.DROP_VARS, dropVars);
    }
    
    /**
     * Fully construct an arbitrary length path node with all required
     * annotations.
     */
    public ArbitraryLengthPathNode(final TermNode left, final TermNode right, 
            final VarNode tVarLeft, final VarNode tVarRight,
            final long lowerBound, final long upperBound) {
        this(new BOp[] { new JoinGroupNode() }, NV.asMap(
                new NV(Annotations.LEFT_TERM, left),
                new NV(Annotations.RIGHT_TERM, right),
                new NV(Annotations.TRANSITIVITY_VAR_LEFT, tVarLeft),
                new NV(Annotations.TRANSITIVITY_VAR_RIGHT, tVarRight),
                new NV(Annotations.DROP_VARS, new ArrayList<VarNode>()),
                new NV(Annotations.LOWER_BOUND, lowerBound),
                new NV(Annotations.UPPER_BOUND, upperBound)
                ));             
        
        final Set<VarNode> dropVars = new LinkedHashSet<>();
        dropVars.add(tVarLeft);
        dropVars.add(tVarRight);
        setProperty(Annotations.DROP_VARS, dropVars);
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
     * Returns the (optional) middle term.
     */
    public TermNode middle() {
        return (VarNode) super.getProperty(Annotations.MIDDLE_TERM);
    }
    
    /**
     * Return the (optional) edge var.
     */
    public VarNode edgeVar() {
        return (VarNode) super.getProperty(Annotations.EDGE_VAR);
    }
    
    /**
     * Set the edge var and middle term.  Only used by the ALP service when 
     * projecting edges.
     */
    public void setEdgeVar(final VarNode edgeVar, final TermNode middle) {
        setProperty(Annotations.MIDDLE_TERM, middle);
        setProperty(Annotations.EDGE_VAR, edgeVar);
    }
    
    /**
     * Set the vars that should be dropped after each round.
     * 
     * @see Annotations#DROP_VARS
     */
    public void setDropVars(final Set<VarNode> dropVars) {
        super.setProperty(Annotations.DROP_VARS, dropVars);
    }
    
    /**
     * Add a var that should be dropped after each round.
     * 
     * @see Annotations#DROP_VARS
     */
    public void addDropVar(final VarNode dropVar) {
        dropVars().add(dropVar);
    }
    
    /**
     * Get the vars that should be dropped after each round.
     * 
     * @see Annotations#DROP_VARS
     */
    @SuppressWarnings("unchecked")
    public Set<VarNode> dropVars() {
        return (Set<VarNode>) super.getProperty(Annotations.DROP_VARS);
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

        final Set<IVariable<?>> producedBindings = getDefinitelyProducedBindings();

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

        addVar(left(), producedBindings, true);
        addVar(right(), producedBindings, true);
        
        final VarNode edgeVar = edgeVar();
        if (edgeVar != null) {
            addProducedBinding(edgeVar, producedBindings);
        }
        
        return producedBindings;

    }
    
    /**
     * Return the set of variables used by this ALP node (statement pattern
     * terms and inside filters). Used to determine what needs to be projected
     * into the op.
     */
    public Set<IVariable<?>> getUsedVars() {
        
        final Set<IVariable<?>> used = getDefinitelyProducedBindings();
        
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

//		final long estCard = getEstimatedCardinality(null);
//		
//		return estCard >= 0 && estCard < Long.MAX_VALUE;

	    /*
	     * I think it's always better to allow this thing to be re-ordered.
	     * If it shares join variables with anything that is usually going
	     * to produce a better order, regardless of whether we can calculate
	     * the true cardinality of the underlying subgroup.  Reordering by
	     * variable-sharing is better than not re-ordering it at all.
	     */
	    return true;
	    
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
		
        if (log.isDebugEnabled()) {
            log.debug("zma: " + zeroMatchAdjustment);
        }
		
        /*
         * Normal simple ALP node will have the cardinality on the group.
         */
        final long groupCard = group.getProperty(
              AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 
              Long.MAX_VALUE);
        
        if (groupCard < Long.MAX_VALUE) {
            
            final long result = groupCard + zeroMatchAdjustment;
            if (log.isDebugEnabled()) {
                log.debug("reported cardinality: " + result);
            }
            return result;
            
        }
        
        /*
         * Question mark ALP nodes with single statement pattern inherit the
         * cardinality of the inner statement pattern, in case there is a
         * single statement pattern 
         */
        if (group.arity() == 1 && upperBound()>=1 && upperBound()<Long.MAX_VALUE) {
           
            BOp pathExpr = group.get(0);
            
            if (pathExpr != null && pathExpr.getProperty(
                  AST2BOpBase.Annotations.ESTIMATED_CARDINALITY) != null) {
            
               final long estCard = pathExpr.getProperty(
                     AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 
                     Long.MAX_VALUE); 
                
               /**
                * The upper bound tells us how often the pattern will be
                * executed, we multiply it with the estimated cardinality and
                * add the zero match adjustment.
                */
               if (estCard < Long.MAX_VALUE) {
                  return upperBound()*estCard + zeroMatchAdjustment;
               }
               
            }
        }
        
        
        long result = 0;
        
        /*
         * Try to find a path expr (ALP service).  We would not need to do this
         * if we were calculating the cardinality of the join groups.
         * 
         * TODO finish the ASTCardinalityOptimizer
         */
        final Iterator<BOp> it = 
                BOpUtility.preOrderIteratorWithAnnotations(group);

        while (it.hasNext()) {
            final BOp bop = it.next();
            
            if (log.isDebugEnabled()) {
                log.debug("considering:\n"+bop);
            }
            
            if (!(bop instanceof StatementPatternNode)) {
                if (log.isDebugEnabled()) {
                    log.debug("continuing");
                }
                continue;
            }
            final StatementPatternNode sp = (StatementPatternNode) bop;
            
            if (!sp.getQueryHintAsBoolean(ASTALPServiceOptimizer.PATH_EXPR, false)) {
                if (log.isDebugEnabled()) {
                    log.debug("continuing");
                }
                continue;
            }
            
            if (sp.getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY) == null) {
                if (log.isDebugEnabled()) {
                    log.debug("continuing");
                }
                continue;
            }
                
            final long estCard = sp.getProperty(
                    AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 
                    Long.MAX_VALUE);
            
            if (estCard == Long.MAX_VALUE) {
                result = Long.MAX_VALUE;
            }
                
            if (result == Long.MAX_VALUE) {
                if (log.isDebugEnabled()) {
                    log.debug("continuing");
                }
                continue;
            }

            result += estCard;
            
        }
        
        if (result > 0) {
            
            if (log.isDebugEnabled()) {
                log.debug("found a path expression");
            }
            
            result += zeroMatchAdjustment;
            
            if (log.isDebugEnabled()) {
                log.debug("reported cardinality: " + result);
            }
            
            return result;
            
        }
        
        if (log.isDebugEnabled()) {
            log.debug("could not find a path expr");
        }
        
        /*
         * Must be a complex alp like: ?x (<a>/<b>)* ?y
         */
	    /*
	     * We can't be certain of the exact cardinality, but we know if it 
	     * shares variables with an ancestor it will probably still do better
	     * than a statement pattern with known cardinality that does not share
	     * any variables.
	     */
        return Long.MAX_VALUE / 2;
        
	}

   @Override
   public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
      return new HashSet<IVariable<?>>();
   }

   @Override
   public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
      return sa.getSpannedVariables(this, true, new HashSet<IVariable<?>>());
   }    
}
