/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql.ast;

import java.util.List;

import com.bigdata.rdf.sail.sparql.ast.ASTDatasetClause;
import com.bigdata.rdf.sail.sparql.ast.SimpleNode;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;

/**
 * @author jeen
 */
public abstract class ASTOperation extends SimpleNode {

	/**
	 * @param id
	 */
	public ASTOperation(int id) {
		super(id);
	}

	/**
	 * @param p
	 * @param id
	 */
	public ASTOperation(SyntaxTreeBuilder p, int id) {
		super(p, id);
	}

	public List<ASTDatasetClause> getDatasetClauseList() {
		return jjtGetChildren(ASTDatasetClause.class);
	}
	
	
}
