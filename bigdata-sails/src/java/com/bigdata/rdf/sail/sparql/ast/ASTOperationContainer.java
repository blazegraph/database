/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql.ast;

import java.util.List;

import com.bigdata.rdf.sail.sparql.ast.ASTBaseDecl;
import com.bigdata.rdf.sail.sparql.ast.ASTOperation;
import com.bigdata.rdf.sail.sparql.ast.ASTPrefixDecl;
import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sail.sparql.ast.ASTUpdateContainer;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilderVisitor;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;

/**
 * Abstract supertype of {@link ASTQueryContainer} and
 * {@link ASTUpdateContainer}
 * 
 * @author Jeen Broekstra
 */
public abstract class ASTOperationContainer extends SimpleNode {

    private String source;

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

	public void setSourceString(String source) {
        this.source = source;
    }
    
    public String getSourceString() {
        return source;
    }

}
