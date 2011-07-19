package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;

/**
 * Used to represent a variable in the AST.
 * 
 * @author mikepersonick
 */
public class VarNode extends TermNode {

	public VarNode(final String var) {
		
		super(Var.var(var));
		
	}
	
	public IVariable<IV> getVar() {
		
		return (IVariable<IV>) getValueExpression();
		
	}
	
}
