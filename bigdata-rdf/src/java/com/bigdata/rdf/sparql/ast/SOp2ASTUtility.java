package com.bigdata.rdf.sparql.ast;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.Var;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sop.SOp;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;
import com.bigdata.rdf.sail.sop.UnsupportedOperatorException;

public class SOp2ASTUtility {

	private static final transient Logger log = Logger.getLogger(SOp2ASTUtility.class);
	
	
	public static final QueryRoot convert(final SOpTree sopTree) {
		
		final SOpGroup sopRoot = sopTree.getRoot();
		
		final IGroupNode astRoot = convert(sopRoot);
		
		final QueryRoot query = new QueryRoot(astRoot);
		
//		query.setDistinct(false);
//		
//		query.setOffset(0l);
//		
//		query.setLimit(Long.MAX_VALUE);
//		
//		query.addOrderBy(orderBy);

		return query;
		
	}
	
	private static final IGroupNode convert(final SOpGroup sopGroup) {
		
		if (log.isDebugEnabled()) {
			log.debug("converting:\n"+sopGroup.getGroup());
		}
		
		final boolean optional = sopGroup.size() != 0 && 
			SOp2BOpUtility.isOptional(sopGroup);
		
		final IGroupNode astGroup;
		
		if (SOp2BOpUtility.isUnion(sopGroup)) {
			
			astGroup = new UnionNode(optional);
			
		} else {
			
			astGroup = new JoinGroupNode(optional);
			
			for (SOp sop : sopGroup) {
				
				final BOp bop = sop.getBOp();
				
				if (bop instanceof Predicate) {
					
					final Predicate pred = (Predicate) bop;
					
					final StatementPattern sp = (StatementPattern) sop.getOperator();
					
					final TermNode s = toTermNode(sp.getSubjectVar());
					final TermNode p = toTermNode(sp.getPredicateVar());
					final TermNode o = toTermNode(sp.getObjectVar());
					final TermNode c = toTermNode(sp.getContextVar());

					final Scope scope = sp.getScope();
					
					astGroup.addChild(new StatementPatternNode(s, p, o, c, scope));
					
					
				} else {
					
					final SPARQLConstraint constraint = (SPARQLConstraint) bop;
					
					final IValueExpression<? extends IV> ve = 
						constraint.getValueExpression();
					
					astGroup.addChild(new FilterNode(new ValueExpressionNode(ve)));
					
				}
				
			}
			
		}
		
		final SOpGroups children = sopGroup.getChildren();
		
		if (children != null) {
		
			for (SOpGroup sopChild : children) {
				
				final IGroupNode astChild = convert(sopChild);
				
				astGroup.addChild(astChild);
				
			}
			
		}
		
		return astGroup;
		
	}
	
	private static final TermNode toTermNode(final Predicate pred, final int i, 
			final Var var) {
		
		if (i >= pred.arity()) {
			return null;
		}
		
		final BOp bop = pred.get(i);
		
		if (bop instanceof IVariable) {
			return new VarNode((IVariable<IV>) bop);
		} else {
			return new ConstantNode((IConstant<IV>) bop);
		}
		
	}
	
	/**
	 * Generate a bigdata term from a Sesame term.
	 * <p>
	 * This method will throw an exception if the Sesame term is bound and the
	 * value does not exist in the lexicon.
	 */
    private static final TermNode toTermNode(final Var var) 
    		throws UnsupportedOperatorException {
    	
    	if (var == null) {
    		return null;
    	}
    	
        final String name = var.getName();
        final BigdataValue val = (BigdataValue) var.getValue();
        if (val == null) {
            return new VarNode(name);
        } else {
            final IV iv = val.getIV();
            if (iv == null) {
            	return new DummyConstantNode(val);
            }
            if (var.isAnonymous())
                return new ConstantNode(new Constant<IV>(iv));
            else 
                return new ConstantNode(new Constant<IV>(com.bigdata.bop.Var.var(name), iv));
        }
        
    }
	

}
