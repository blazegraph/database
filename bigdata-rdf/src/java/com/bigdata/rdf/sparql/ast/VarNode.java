package com.bigdata.rdf.sparql.ast;

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

	public VarNode(final IVariable<IV> var) {
		
		super(var);
		
	}
	
	public VarNode(final String var) {
		
		super(Var.var(var));
		
	}
	
	public IVariable<IV> getVar() {
		
		return (IVariable<IV>) getValueExpression();

    }

    @Override
    public boolean equals(Object obj) {
        
        if (obj == this)
            return true;
        
        if (obj instanceof VarNode)
            return getVar().equals(((VarNode) obj).getVar());

        return false;
        
    }
	
}
