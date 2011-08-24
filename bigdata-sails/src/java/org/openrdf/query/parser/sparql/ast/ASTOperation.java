/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql.ast;

import java.util.List;

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
