package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;

import cutthecrap.utils.striterators.Striterator;

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
	 * Pretty print with an indent.
	 */
	String toString(final int indent);

    /**
     * Visit this node and then all of its children recursively. You can wrap
     * this up as a {@link Striterator} in order to expand the iteration to
     * visit {@link IValueExpressionNode} in order to filter for variables
     * (which may appear on either the {@link IQueryNode} or
     * {@link IValueExpressionNode} side of the AST), find the unique variables,
     * etc.
     * <p>
     * Note: This does not visit {@link IValueExpressionNode} in the AST. They
     * are part of a separate hierarchy.
     * 
     * FIXME Move this onto {@link BOp}. It will just use {@link BOpUtility} to
     * do the work.  Versions with and w/o annotations.
     */
    Iterator<IQueryNode> preOrderIterator();
    
}
