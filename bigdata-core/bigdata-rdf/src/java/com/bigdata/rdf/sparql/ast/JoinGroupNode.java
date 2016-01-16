/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.constraints.InBOp;
import com.bigdata.rdf.sparql.ast.hints.OptimizerQueryHint;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * An optional or non-optional collection of query nodes that run together in
 * a group.
 */
public class JoinGroupNode extends GraphPatternGroup<IGroupMemberNode> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupNodeBase.Annotations {

        /**
         * The graph variable or constant iff this {@link JoinGroupNode} models
         * a GraphPatternGroup. When not present, your have to read up the
         * parent chain to locate the dominating graph context.
         */
        String CONTEXT = "context";
        
        /**
         * A property that may be set by a query hint. When set, the value is a
         * {@link QueryOptimizerEnum}.
         * 
         * @see QueryHints#OPTIMIZER
         * @see OptimizerQueryHint
         */
        String OPTIMIZER = QueryHints.OPTIMIZER;
        
    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     * <p>
     * <strong>Note: There is a nasty potential for a conflict here.  This constructor is part
     * of the deep copy semantics.  The <code>JoinGroupNode(IGroupMemberNode child)</code<
     * constructor adds a child to a node (which also sets the parent of the child to the
     * node). However, this DEEP COPY constructor DOES NOT set the parent reference!!!
     * </strong>
     * This means that the following code DOES NOT create a nested structure of a JoinGroupNode in a JoinGroupNode.
     * <pre>
     * new JoinGroupNode(new JoinGroupNode); // COPY CONSTRUCTOR PATTERN.
     * </pre>
     * Instead you MUST do this incrementally
     * <pre>
     * final JoinGroupNode parent = new JoinGroupNode();
     * parent.addChild(new JoinGroupNode());
     * </pre>
     * and if you need to add things to the child, then the pattern becomes:
     * <pre>
     * final JoinGroupNode parent = new JoinGroupNode();
     * final JoinGroupNode child1 = new JoinGroupNode()
     * parent.addChild(child1);
     * child1.addChild(...);
     * </pre>
     */
    public JoinGroupNode(final JoinGroupNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public JoinGroupNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * Construct a non-optional join group.
     */
    public JoinGroupNode() {

        super();

    }

    /**
     * Construct a non-optional join group having the specified child as its
     * initial member.
     */
    public JoinGroupNode(final IGroupMemberNode child) {

        super();
        
        addChild(child);

    }

    public JoinGroupNode(final boolean optional) {

        super();
        
        setOptional(optional);
        
    }

    /**
     * Construct a possibly optional group having the specified child as its
     * initial member.
     * 
     * @param optional
     *            <code>true</code> iff the group is optional.
     * @param child
     *            The initial child.
     */
    public JoinGroupNode(final boolean optional, final IGroupMemberNode child) {

        super();

        setOptional(optional);

        addChild(child);

    }

    /**
     * Construct a GRAPH group having the specified child as its initial member.
     * 
     * @param context
     *            The variable or constant for the GRAPH group.
     * @param child
     *            The initial child.
     */
    public JoinGroupNode(final TermNode context, final IGroupMemberNode child) {

        super();

        setContext(context);

        addChild(child);
        
    }

    /**
     * Set the context for a GroupGraphPattern.
     * 
     * @param context
     *            The context (may be <code>null</code>).
     */
    public void setContext(final TermNode context) {
        
        setProperty(Annotations.CONTEXT, context);

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the context associated with this
     * {@link JoinGroupNode} if it is defined and otherwise read up the parent
     * chain.
     */
    @Override
    public TermNode getContext() {

        final TermNode context = (TermNode) getProperty(Annotations.CONTEXT);

        if (context != null)
            return context;

        // Note: the base class will test the parent.
        return super.getContext();

    }

    @Override
    final public boolean isOptional() {
        
        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
        
    }
    
    final public void setOptional(final boolean optional) {
        
        setProperty(Annotations.OPTIONAL, optional);
        
    }
    
    @Override
    final public boolean isMinus() {

        return getProperty(Annotations.MINUS, Annotations.DEFAULT_MINUS);

    }
    
    public void setMinus(final boolean minus) {

        setProperty(Annotations.MINUS, minus);
        
    }

    /**
     * Return the {@link QueryOptimizerEnum} that is in effect for this
     * {@link JoinGroupNode}. This will be the value specified through
     * {@link QueryHints#OPTIMIZER} and otherwise the default value given by
     * {@link QueryHints#DEFAULT_OPTIMIZER}.
     * 
     * @return The effective query optimizer for this join group.
     */
    final public QueryOptimizerEnum getQueryOptimizer() {

        return getProperty(Annotations.OPTIMIZER, QueryHints.DEFAULT_OPTIMIZER);

    }
    
	/**
	 * Return only the statement pattern child nodes in this group.
	 */
	public List<StatementPatternNode> getStatementPatterns() {
		
		return getChildren(StatementPatternNode.class); 
		
	}

	/**
	 * Return the #of statement patterns.
	 */
    public int getStatementPatternCount() {

        int n = 0;

        for (IQueryNode node : this) {

            if (node instanceof StatementPatternNode) {

                n++;

            }

        }

        return n;

    }
    
    /**
     * Return the #of required statement patterns (does not include those
     * flagged as OPTIONAL).
     */
    public int getRequiredStatementPatternCount() {

        int n = 0;

        for (IQueryNode node : this) {

            if (node instanceof StatementPatternNode) {

                if(!((StatementPatternNode)node).isOptional()) {
                
                    n++;

                }

            }

        }

        return n;

    }
    
	/**
	 * Return only the {@link ServiceNode} child nodes in this group.
	 */
	public List<ServiceNode> getServiceNodes() {
		
		return getChildren(ServiceNode.class); 
		
	}

	/**
	 * Return only the {@link NamedSubqueryInclude} child nodes in this group.
	 */
	public List<NamedSubqueryInclude> getNamedSubqueryIncludes() {
		
		return getChildren(NamedSubqueryInclude.class); 
		
	}

    /**
     * Return any <code>LET x:= expr</code> or <code>(expr AS ?x)</code> nodes
     * in <i>this</i> group (these are modeled in exactly the same way by the
     * AST {@link AssignmentNode}).
     * <p>
     * Note: {@link AssignmentNode}s MUST NOT be reordered. They MUST be
     * evaluated left-to-right in the order given in the original query.
     */
    public List<AssignmentNode> getAssignments(){
		
		return getChildren(AssignmentNode.class); 
    }

	/**
	 * Return only the filter child nodes in this group.
	 */
	public List<FilterNode> getAllFiltersInGroup() {
		
		return getChildren(FilterNode.class); 
		
    }

    /**
     * Return the set of IN filters for this group.
     * 
     * FIXME We need to move away from the DataSetJoin class and replace it with
     * an IPredicate to which we have attached an inline access path. That
     * transformation needs to happen in a rewrite rule, which means that we
     * will wind up removing the IN filter and replacing it with an AST node for
     * that inline AP (something conceptually similar to a statement pattern but
     * for a column projection of the variable for the IN expression). That way
     * we do not have to magically "subtract" the known "IN" filters out of the
     * join- and post- filters.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
     *      DataSetJoin with an "inline" access path.)
     */
    public List<FilterNode> getInFilters() {
        
        final List<FilterNode> filters = new LinkedList<FilterNode>();

        for (IQueryNode node : this) {

            if (!(node instanceof FilterNode))
                continue;

            /*
             * FIXME The "data set join" hack can be enabled by making this
             * [true]. I have it disabled for the moment since I am in the
             * middle of changing all of this static analysis stuff.
             * 
             * Note: You will also have to modify the join- and post- filter
             * methods in the StaticAnalysis class for this to work. They need
             * to subtract out the IN filters if they have already been applied.
             */
            if (false) {

                final FilterNode filter = (FilterNode) node;

                if (filter.getValueExpression() instanceof InBOp) {

                    if (((InBOp) filter.getValueExpression())
                            .getValueExpression() instanceof IVariable) {

                        filters.add(filter);

                    }

                }

            }

        }

        return filters;
        
    }

    public List<IReorderableNode> getReorderableChildren() {
    	final List<IReorderableNode> nodes = getChildren(IReorderableNode.class);
    	final Iterator<IReorderableNode> it = nodes.iterator();
    	while (it.hasNext()) {
    		final IReorderableNode node = it.next();
    		if (ASTStaticJoinOptimizer.log.isDebugEnabled()) {
        		ASTStaticJoinOptimizer.log.debug(node);
        		ASTStaticJoinOptimizer.log.debug(node.isReorderable());
    		}
    		if (!node.isReorderable()) {
    			it.remove();
    		}
    	}   	
    	return nodes;
    }

	/*
	 * Note: I took this out and put a simpler version of toString(indent) into
	 * the base class.  The multiple passes over the AST when rendering it into
	 * a String were hiding order differences within the group node which were
	 * making it difficult to identify test failures.  The filters also make it
	 * likely that we were failing to show some children of the AST because they
	 * did not match any of the tested interfaces.
	 * 
	 * BBT 8/30/2011
	 */
	
//	public String toString(final int indent) {
//    ....
//	}

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {

       final Set<IVariable<?>> requiredBound = new HashSet<IVariable<?>>();
       for (IGroupMemberNode child : getChildren()) {
          requiredBound.addAll(child.getRequiredBound(sa));
       }
       
       
       return requiredBound;

    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
       
       final Set<IVariable<?>> desiredBound = new HashSet<IVariable<?>>();
       for (IGroupMemberNode child : getChildren()) {
          desiredBound.addAll(child.getDesiredBound(sa));
       }
       
       
       return desiredBound;
    }
}
