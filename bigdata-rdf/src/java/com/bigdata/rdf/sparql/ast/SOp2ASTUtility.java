package com.bigdata.rdf.sparql.ast;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.sail.sop.SOp;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;

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
					
					final TermNode s = toTermNode(pred, 0);
					final TermNode p = toTermNode(pred, 1);
					final TermNode o = toTermNode(pred, 2);
					final TermNode c = toTermNode(pred, 3);

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
	
	private static final TermNode toTermNode(final Predicate pred, final int i) {
		
		if (i >= pred.arity()) {
			return null;
		}
		
		final BOp bop = pred.get(i);
		
		if (bop instanceof IVariable) {
			return new VarNode(((IVariable) bop).getName());
		} else {
			final IV iv = ((IConstant<IV>) bop).get();
			if (iv.isNullIV()) {
				return new DummyConstantNode(iv.getValue());
			}
			return new ConstantNode(iv);
		}
		
	}
	
}
