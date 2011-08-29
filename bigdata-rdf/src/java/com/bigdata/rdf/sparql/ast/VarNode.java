package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
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
    private static final long serialVersionUID = 1L;

    //    private boolean anonymous;
    interface Annotations extends TermNode.Annotations {
        
        String ANONYMOUS = "anonymous";
        
        boolean DEFAULT_ANONYMOUS = false;
        
    }
    
	public VarNode(final IVariable<IV> var) {
		
		super(var);
		
	}
	
	public VarNode(final String var) {
		
		super(Var.var(var));
		
	}
	
    public IVariable<IV> getVar() {

        final IValueExpression e = getValueExpression();

        if (e instanceof Constant) {

            /*
             * Note: This is an ugly hack. We should be preserving the original
             * term and not just the ValueExpression.
             */

            return (IVariable<IV>) ((Constant) e)
                    .getProperty(Constant.Annotations.VAR);

        }

        return (IVariable<IV>) e;

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

        return ((Var<?>) getValueExpression()).isWildcard();
	    
	}
	
    /**
     * Overridden to mark anonymous variables.
     * 
     * @see #isAnonymous()
     */
    @Override
    public String toString() {

        return "VarNode(" + getVar() + ")"
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
