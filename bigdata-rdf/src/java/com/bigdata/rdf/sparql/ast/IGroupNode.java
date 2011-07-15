package com.bigdata.rdf.sparql.ast;


/**
 * A type of query node that groups a set of query nodes together. This is
 * the interface used by the {@link UnionNode} and {@link JoinGroupNode} query
 * nodes.
 */
public interface IGroupNode extends IQueryNode, Iterable<IQueryNode> {

	/**
	 * Add a child to this group.  Child can be a statment pattern, a filter,
	 * or another group.
	 */
	IGroupNode addChild(final IQueryNode child);
	
	/**
	 * Remove a child from this group.
	 */
	IGroupNode removeChild(final IQueryNode child);

	/**
	 * Return the # of children.
	 */
	int getChildCount();
	
	/**
	 * Return whether or not this is an optional group. Optional groups may
	 * or may not produce variable bindings, but will not prune incoming
	 * solutions based on whether or not they bind. Optional groups might
	 * be composed of a single statement pattern, a single statement pattern
	 * with filters, or a more complex join that could include subgroups.
	 */
	boolean isOptional();
	
}
