/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql.ast;

import java.util.List;

import com.bigdata.rdf.sail.sparql.ast.ASTGroupClause;
import com.bigdata.rdf.sail.sparql.ast.ASTHavingClause;
import com.bigdata.rdf.sail.sparql.ast.ASTLimit;
import com.bigdata.rdf.sail.sparql.ast.ASTOffset;
import com.bigdata.rdf.sail.sparql.ast.ASTOperation;
import com.bigdata.rdf.sail.sparql.ast.ASTOrderClause;
import com.bigdata.rdf.sail.sparql.ast.ASTWhereClause;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;

public abstract class ASTQuery extends ASTOperation {

	public ASTQuery(int id) {
		super(id);
	}

	public ASTQuery(SyntaxTreeBuilder p, int id) {
		super(p, id);
	}

	public ASTWhereClause getWhereClause() {
		return jjtGetChild(ASTWhereClause.class);
	}

	public ASTOrderClause getOrderClause() {
		return jjtGetChild(ASTOrderClause.class);
	}

	public ASTGroupClause getGroupClause() {
		return jjtGetChild(ASTGroupClause.class);
	}
	
	public ASTHavingClause getHavingClause() {
		return jjtGetChild(ASTHavingClause.class);
	}

	public boolean hasLimit() {
		return getLimit() != null;
	}

	public ASTLimit getLimit() {
		return jjtGetChild(ASTLimit.class);
	}

	public boolean hasOffset() {
		return getOffset() != null;
	}

	public ASTOffset getOffset() {
		return jjtGetChild(ASTOffset.class);
	}
}
