package com.bigdata.rdf.sparql.ast;

/**
 * A type of query node that groups a set of query nodes together. This is the
 * interface used by the {@link UnionNode} and {@link JoinGroupNode} query
 * nodes.
 */
public interface IGroupNode<E extends IGroupMemberNode> extends
        IGroupMemberNode, Iterable<E> {

    /**
     * Add a child to this group. Child can be a statement pattern, a filter, or
     * another group.
     * 
     * @return this
     */
    IGroupNode<E> addChild(final E child);

    /**
     * Remove a child from this group.
     * 
     * @return this
     */
    IGroupNode<E> removeChild(final E child);

    /**
     * Return <code>true</code> iff the group is empty.
     */
    boolean isEmpty();
    
    /**
     * Return the #of children which are direct members of the group.
     */
    int size();

//    /**
//     * Return whether or not this is an optional group. Optional groups may or
//     * may not produce variable bindings, but will not prune incoming solutions
//     * based on whether or not they bind. Optional groups might be composed of a
//     * single statement pattern, a single statement pattern with filters, or a
//     * more complex join that could include subgroups.
//     */
//    boolean isOptional();

}
