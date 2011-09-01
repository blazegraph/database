package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * AST node models the assignment of a value expression to a variable.
 */
public class AssignmentNode extends GroupMemberValueExpressionNodeBase 
		implements IValueExpressionNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     * @param var
     *            The variable which will be bound.
     * @param ve
     *            The value expression to be computed.
     */
    public AssignmentNode(final VarNode var, final IValueExpressionNode ve) {

        super(new BOp[] { var, (BOp) ve }, null/* anns */);

    }

    public VarNode getVarNode() {
        
        return (VarNode) get(0);
        
    }

    public IVariable<IV> getVar() {

        return getVarNode().getValueExpression();
        
    }

    /**
     * TODO Review. I believe that AssignmentNode.getValueExpression() should
     * always return the Bind(). Right now it only returns the RIGHT argument.
     */
    public IValueExpressionNode getValueExpressionNode() {
     
        return (IValueExpressionNode) get(1);
        
    }

    public IValueExpression<? extends IV> getValueExpression() {

        return getValueExpressionNode().getValueExpression();
        
    }
    
    public void setValueExpression(final IValueExpression<? extends IV> ve) {
    	
    	getValueExpressionNode().setValueExpression(ve);
    	
    }

    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder(indent(indent));

        final VarNode var = getVarNode();

        final IValueExpressionNode ve = getValueExpressionNode();
        
        if (ve == var) {

            sb.append("?");
            
            sb.append(var.getValueExpression().toString());
            
        } else {
            
            sb.append("(LET ");

            sb.append("?").append(getVar().toString());

            sb.append(":=");

            sb.append(ve.toString());

            sb.append(")");

        }
        
        return sb.toString();
        
    }

//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof AssignmentNode))
//            return false;
//
//        final AssignmentNode t = (AssignmentNode) o;
//
//        if (!var.equals(t.var))
//            return false;
//
//        if (!ve.equals(t.ve))
//            return false;
//
//        return true;
//
//    }

}
