/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql.ast;

import java.util.List;

/**
 * Abstract supertype of {@link ASTQueryContainer} and
 * {@link ASTUpdateContainer}
 * 
 * @author Jeen Broekstra
 */
public abstract class ASTOperationContainer extends SimpleNode {

	/**
	 * @param id
	 */
	public ASTOperationContainer(int id) {
		super(id);
	}

	public ASTOperationContainer(SyntaxTreeBuilder p, int id) {
		super(p, id);
	}

	@Override
	public Object jjtAccept(SyntaxTreeBuilderVisitor visitor, Object data)
		throws VisitorException
	{
		return visitor.visit(this, data);
	}

	public ASTBaseDecl getBaseDecl() {
		return super.jjtGetChild(ASTBaseDecl.class);
	}

	public ASTOperation getOperation() {
		return super.jjtGetChild(ASTOperation.class);
	}

	public List<ASTPrefixDecl> getPrefixDeclList() {
		return super.jjtGetChildren(ASTPrefixDecl.class);
	}
}
