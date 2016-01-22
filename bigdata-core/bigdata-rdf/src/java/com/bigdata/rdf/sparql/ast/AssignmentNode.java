package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ModifiableBOpBase;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

/**
 * AST node models the assignment of a value expression to a variable
 * 
 * <pre>
 * BIND( valueExpr AS ?var )
 * </pre>
 * 
 * where args[0] is the {@link VarNode}<br>
 * 
 * where args[1] is the {@link IValueExpression}<br>
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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public AssignmentNode(final AssignmentNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public AssignmentNode(final BOp[] args, final Map<String, Object> anns) {

		super(args, anns);

		if (!(args[0] instanceof VarNode))
			throw new IllegalArgumentException();

    }

	/**
	 * The variable onto which the assignment is bound (as a {@link VarNode}).
	 * 
	 * @return For <code>BIND(valueExpr AS ?var)</code> this returns
	 *         <code>?var</code> as a {@link VarNode}.
	 */
    public VarNode getVarNode() {
        
        return (VarNode) get(0);
        
    }

	/**
	 * The variable onto which the assignment is bound (as an {@link IVariable}).
	 * 
	 * @return For <code>BIND(valueExpr AS ?var)</code> this returns
	 *         <code>?var</code> as an {@link IVariable}.
	 */
    @SuppressWarnings("rawtypes")
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
    @Override
    public IValueExpressionNode getValueExpressionNode() {
     
        return (IValueExpressionNode) get(1);
        
    }

	@Override
    @SuppressWarnings("rawtypes")
    public IValueExpression<? extends IV> getValueExpression() {

        return getValueExpressionNode().getValueExpression();
        
    }
    
	@Override
    @SuppressWarnings("rawtypes")
    public void setValueExpression(final IValueExpression<? extends IV> ve) {
    	
    	getValueExpressionNode().setValueExpression(ve);
    	
    }

    @Override
    public void invalidate() {
        
        getValueExpressionNode().invalidate();
        
    }

    @Override
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
    public int compareTo(final AssignmentNode o) {

        return getVar().getName().compareTo(o.getVar().getName());
        
    }


    @Override
    public int replaceAllWith(final BOp oldVal, final BOp newVal) {
        if (oldVal.equals(get(0)) && !(newVal instanceof VarNode)) {
            return 0;
        }
        return super.replaceAllWith(oldVal, newVal);
    }

    @Override
    public ModifiableBOpBase setArgs(final BOp[] args) {
        assert args[0] instanceof VarNode;
        return super.setArgs(args);
    }

    @Override
    public ModifiableBOpBase setArg(final int index, final BOp newArg) {
        assert index != 0 || newArg instanceof VarNode;
        return super.setArg(index, newArg);
    }
    
    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {

       final IValueExpressionNode vexp = getValueExpressionNode();
       
       Set<IVariable<?>> requiredBound = new HashSet<IVariable<?>>();
       if (vexp!=null && vexp instanceof ValueExpressionNode) {
          
          requiredBound = 
             sa.getSpannedVariables(
                (ValueExpressionNode)vexp, true, requiredBound);
          
       }

       return requiredBound;
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
       /*
        * Nothing that helps us here (beyond the things we need) 
        */
       return new HashSet<IVariable<?>>();
    }    


}
