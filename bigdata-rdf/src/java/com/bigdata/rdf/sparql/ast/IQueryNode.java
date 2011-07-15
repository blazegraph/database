package com.bigdata.rdf.sparql.ast;

/**
 * This is the basic interface for any AST operator that appears in the query
 * plan. Query nodes will be one of the following specific types:
 * <ul>
 * <li>Statement patterns ({@link StatementPatternNode})</li>
 * <li>Filters ({@link FilterNode})</li>
 * <li>Unions ({@link UnionNode})</li>
 * <li>Join groups (optional or otherwise) ({@link JoinGroupNode})</li>
 * </ul>
 * <p>
 * Other types of query nodes will undoubtedly come online as we migrate to
 * Sparql 1.1.
 */
public interface IQueryNode {

	/**
	 * Set the group to which this bindings producer node belongs. Should only
	 * be called the group node during the addChild() method.
	 */
	void setParent(final IGroupNode parent);

	/**
	 * Return the group to which this bindings producer node belongs.
	 */
	IGroupNode getParent();
	
	/**
	 * Pretty print with an indent.
	 */
	String toString(final int indent);
	
}
