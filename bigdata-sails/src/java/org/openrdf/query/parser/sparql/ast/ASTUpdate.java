/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql.ast;

/**
 * @author jeen
 */
public abstract class ASTUpdate extends ASTOperation {

	/**
	 * @param id
	 */
	public ASTUpdate(int id) {
		super(id);
	}

	public ASTUpdate(SyntaxTreeBuilder p, int id) {
		super(p, id);
	}

}
