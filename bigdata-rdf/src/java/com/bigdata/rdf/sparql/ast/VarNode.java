package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;

/**
 * Used to represent a variable in the AST.
 * <p>
 * Note: Unlike {@link Var}, this class does NOT support reference testing for
 * equality. Multiple {@link VarNode} instances MAY exist for the same
 * {@link IVariable}. You must either compare the value returned by
 * {@link #getVar()} (which is canonical) or use {@link #equals(Object)}.
 * 
 * @author mikepersonick
 */
public class VarNode extends TermNode {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2368082533964951789L;

	
	//    private boolean anonymous;
    interface Annotations extends TermNode.Annotations {
        
        String ANONYMOUS = VarNode.class.getName() + ".anonymous";
        
        boolean DEFAULT_ANONYMOUS = false;
        
    }
    
    public VarNode(final String var) {
    	
    	this(Var.var(var));
    	
    }
    
	public VarNode(final IVariable<IV> var) {
		
		super(BOpBase.NOARGS, null);
		
		setValueExpression(var);
		
	}
	
    /**
     * Return <code>true</code> if this is an anonymous variable (anonymous
     * variables are introduced during a rewrite of the query in which blank
     * nodes are replaced by anonymous variables). This marker is useful mainly
     * when reading the AST tree as an aid to understanding where a given
     * variable came from in the original query.
     */
    public boolean isAnonymous() {

        return getProperty(Annotations.ANONYMOUS, Annotations.DEFAULT_ANONYMOUS);
        
	}

    /**
     * Mark this as an anonymous variable (one introduced during a query rewrite
     * in place of a blank node).
     * 
     * @param anonymous
     */
	public void setAnonymous(final boolean anonymous) {

	    setProperty(Annotations.ANONYMOUS, anonymous);
	    
	}

	/**
	 * Return <code>true</code> iff the variable is <code>*</code>.
	 */
	public boolean isWildcard() {

		return getValueExpression().isWildcard();
	    
	}
	
	/**
	 * Strengthen return type.
	 */
	@Override
	public IVariable<IV> getValueExpression() {
		
		return (IVariable<IV>) super.getValueExpression();
		
	}
	
    /**
     * Overridden to mark anonymous variables.
     * 
     * @see #isAnonymous()
     */
    @Override
    public String toString() {

        return "VarNode(" + getValueExpression() + ")"
                + (isAnonymous() ? "[anonymous]" : "");

    }
 
//    @Override
//    public boolean equals(Object obj) {
//        
//        if (obj == this)
//            return true;
//        
//        if (!(obj instanceof VarNode))
//            return false;
//
//        final VarNode t = (VarNode) obj;
//
//        if (!getVar().equals(t.getVar()))
//            return false;
//
//        if (anonymous != t.anonymous)
//            return false;
//        
//        return true;
//        
//    }
	
}
