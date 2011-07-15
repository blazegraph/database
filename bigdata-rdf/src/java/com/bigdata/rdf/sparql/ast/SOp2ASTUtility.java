package com.bigdata.rdf.sparql.ast;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
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
					
					astGroup.addChild(new StatementPatternNode(pred));
					
				} else {
					
					final SPARQLConstraint constraint = (SPARQLConstraint) bop;
					
					final IValueExpression<? extends IV> ve = 
						constraint.getValueExpression();
					
					astGroup.addChild(new FilterNode(ve));
					
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
	
}
