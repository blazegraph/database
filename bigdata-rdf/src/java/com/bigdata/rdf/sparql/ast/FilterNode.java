package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * AST node models a value expression which imposes a constraint.
 */
public class FilterNode extends GroupMemberValueExpressionNodeBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//	private final IValueExpressionNode ve;
	
//	private final Set<IVariable<?>> consumedVars;
//	
//	private final INeedsMaterialization.Requirement materializationRequirement;
//	
//	private final Set<IVariable<IV>> varsToMaterialize;

    /**
     * 
     * @param ve
     *            A value expression which places a constraint on the query.
     */
	public FilterNode(final IValueExpressionNode ve) {
		
        super(new BOp[] { (BOp) ve }, null/* anns */);

	}
	
	public IValueExpression<? extends IV> getValueExpression() {

	    return getValueExpressionNode().getValueExpression();
	    
	}
	
    public IValueExpressionNode getValueExpressionNode() {

        return (IValueExpressionNode) get(0);
        
	}

//	public Set<IVariable<?>> getConsumedVars() {
//		return consumedVars;
//	}
	
//	public INeedsMaterialization.Requirement getRequirement() {
//		return materializationRequirement;
//	}
//	
//	public Set<IVariable<IV>> getVarsToMaterialize() {
//		return varsToMaterialize;
//	}

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder(indent(indent));

        sb.append("filter(").append(getValueExpressionNode()).append(")");

        return sb.toString();

    }

//    @Override
//    public boolean equals(Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof FilterNode))
//            return false;
//
//        final FilterNode t = (FilterNode) o;
//
//        if (!ve.equals(t.ve))
//            return false;
//
//        return true;
//
//    }

}
