package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * AST node models the assignment of a value expression to a variable.
 */
public class AssignmentNode extends GroupMemberValueExpressionNodeBase
        implements IValueExpressionNode, Comparable<AssignmentNode>,
        IBindingProducerNode, IValueExpressionNodeContainer {

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

    /**
     * Required deep copy constructor.
     */
    public AssignmentNode(AssignmentNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public AssignmentNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    public VarNode getVarNode() {
        
        return (VarNode) get(0);
        
    }

    public IVariable<IV> getVar() {

        return getVarNode().getValueExpression();
        
    }

    /**
     * {@inheritDoc}
     * 
     * TODO Review. I believe that AssignmentNode.getValueExpression() should
     * always return the Bind(). Right now it only returns the RIGHT argument.
     * This assumption is build into the GROUP_BY handling in
     * {@link AST2BOpUtility}.
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
    
    public void invalidate() {
        
        getValueExpressionNode().invalidate();
        
    }

    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder(indent(indent));

        final VarNode var = getVarNode();

        final IValueExpressionNode ve = getValueExpressionNode();
        
        if (ve == var) {

//            sb.append("?");
//            
//            sb.append(var.getValueExpression().toString());
            sb.append(ve.toString());
            
        } else {
            
            sb.append("( ");

            sb.append(ve.toString());

            sb.append(" AS ");

//            sb.append("?").append(getVar().toString());
            sb.append(var.toString());

            sb.append(" )");

        }
        
        return sb.toString();
        
    }

    /**
     * Orders {@link AssignmentNode}s by the variable names.
     */
    @Override
    public int compareTo(AssignmentNode o) {

        return getVar().getName().compareTo(o.getVar().getName());
        
    }

}
