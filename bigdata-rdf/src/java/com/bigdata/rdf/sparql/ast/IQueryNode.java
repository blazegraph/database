package com.bigdata.rdf.sparql.ast;

/**
 * This is the basic interface for any AST operator that appears in the query
 * plan.
 */
public interface IQueryNode {

	/**
	 * Pretty print with an indent.
	 */
	String toString(final int indent);
    
}
