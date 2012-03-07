package com.bigdata.rdf.sparql.ast;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.constraints.InBOp;
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
        
    }
    
    /**
     * Required deep copy constructor.
     */
    public JoinGroupNode(JoinGroupNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public JoinGroupNode(BOp[] args, Map<String, Object> anns) {

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
    public JoinGroupNode(final boolean optional, IGroupMemberNode child) {

        super();
        
        setOptional(optional);
        
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
	 * Return only the statement pattern child nodes in this group.
	 */
	public List<StatementPatternNode> getStatementPatterns() {
		
		final List<StatementPatternNode> spNodes = 
			new LinkedList<StatementPatternNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof StatementPatternNode) {
				
				spNodes.add((StatementPatternNode) node);
				
			}
			
		}
		
		return spNodes;
		
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
		
		final List<ServiceNode> serviceNodes = 
			new LinkedList<ServiceNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof ServiceNode) {
				
				serviceNodes.add((ServiceNode) node);
				
			}
			
		}
		
		return serviceNodes;
		
	}

	/**
	 * Return only the {@link NamedSubqueryInclude} child nodes in this group.
	 */
	public List<NamedSubqueryInclude> getNamedSubqueryIncludes() {
		
		final List<NamedSubqueryInclude> namedSubqueryIncludes = 
			new LinkedList<NamedSubqueryInclude>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof NamedSubqueryInclude) {
				
				namedSubqueryIncludes.add((NamedSubqueryInclude) node);
				
			}
			
		}
		
		return namedSubqueryIncludes;
		
	}

	/**
	 * Return only the {@link NamedSubqueryInclude} child nodes in this group.
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getChildren(final Class<T> type) {
		
		final List<T> children = new LinkedList<T>();
		
		for (IQueryNode node : this) {
			
			if (type.isAssignableFrom(node.getClass())) {
				
				children.add((T) node);
				
			}
			
		}
		
		return children;
		
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
        
        final List<AssignmentNode> assignments = new LinkedList<AssignmentNode>();
        
        for (IQueryNode node : this) {
            
            if (node instanceof AssignmentNode) {
                
                assignments.add((AssignmentNode) node);
                
            }
            
        }
        
        return assignments;
        
    }

	/**
	 * Return only the filter child nodes in this group.
	 */
	public List<FilterNode> getAllFiltersInGroup() {
		
		final List<FilterNode> filters = new LinkedList<FilterNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof FilterNode) {
				
				filters.add((FilterNode) node);
				
			}
			
		}
		
		return filters;
		
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

}
