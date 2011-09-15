package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.InBOp;

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

        super(optional);
		
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

    @Deprecated
    @Override
	public Set<IVariable<?>> getIncomingBindings(final Set<IVariable<?>> vars) {
				
		IGroupNode<?> parent = getParent();
		
		while (parent != null) {
			
			if (parent instanceof JoinGroupNode) {
				
				final JoinGroupNode joinGroup = (JoinGroupNode) parent;
				
                /*
                 * Note: This needs to be a non-recursive definition of the
                 * definitely produced bindings. Just those for *this* group for
                 * each parent considered.
                 */

				joinGroup.getDefinatelyProducedBindings(vars, false/*recursive*/);
				
			}
			
			parent = parent.getParent();
			
		}
		
		return vars;
		
	}

    @Deprecated
    public Set<IVariable<?>> getDefinatelyProducedBindings(
            final Set<IVariable<?>> vars, final boolean recursive) {

        for (IGroupMemberNode child : this) {
            
            if (child instanceof StatementPatternNode) {

                /*
                 * Add anything bound by a statement pattern.
                 */
                
                final StatementPatternNode sp = (StatementPatternNode) child;
                
                vars.addAll(sp.getProducedBindings());
            
            } else if(recursive && child instanceof GraphPatternGroup<?>) {

                /*
                 * Add anything bound by a child group.
                 */
                
                final GraphPatternGroup<?> group = (GraphPatternGroup<?>) child;

                if (!group.isOptional()) {

                    vars.addAll(group.getDefinatelyProducedBindings(vars,
                            recursive));

                }

            }

        }

        /*
         * Note: Assignments which have an error cause the variable to be left
         * unbound rather than failing the solution. Therefore assignment nodes
         * are handled as "maybe" bound, not "must" bound.
         */

        return vars;
        
    }
  
    /**
     * {@inheritDoc}
     * <P>
     * Note: <code>IF</code> semantics: If evaluating the first argument raises
     * an error, then an error is raised for the evaluation of the IF
     * expression. (This greatly simplifies the analysis of the EBV of the IF
     * value expressions, but there is still uncertainty concerning whether the
     * THEN or the ELSE is executed for a given solution.) Also, <code>IF</code>
     * is not allowed to conditionally bind a variable in the THEN/ELSE
     * expressions so we do not include it here.
     */
    @Override
    @Deprecated
    public Set<IVariable<?>> getMaybeProducedBindings(
            final Set<IVariable<?>> vars, final boolean recursive) {

        // Add in anything definitely produced by this group (w/o recursion).
        getDefinatelyProducedBindings(vars, false/* recursive */);

        /*
         * Note: Assignments which have an error cause the variable to be left
         * unbound rather than failing the solution. Therefore assignment nodes
         * are handled as "maybe" bound, not "must" bound.
         */

        for(AssignmentNode bind : getAssignments()) {
            
            vars.add(bind.getVar());
            
        }

        if (recursive) {

            /*
             * Add in anything "maybe" produced by a child group.
             */

            for (IGroupMemberNode child : this) {

                if (child instanceof GraphPatternGroup<?>) {

                    final GraphPatternGroup<?> group = (GraphPatternGroup<?>) child;

                    vars.addAll(group.getMaybeProducedBindings(vars, recursive));

                }

            }

        }

        return vars;

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
     * Return any <code>LET x:= expr</code> or <code>(expr AS ?x)</code> nodes
     * in <i>this</i> group (these are modeled in exactly the same way by the
     * AST {@link AssignmentNode}).
     * <p>
     * Note: {@link AssignmentNode}s MUST NOT be reordered. They MUST be
     * evaluated left-to-right in the order given in the original query.
     */
    public List<AssignmentNode> getAssignments(){
        
        final List<AssignmentNode> assignments = new ArrayList<AssignmentNode>();
        
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
	public List<FilterNode> getFilters() {
		
		final List<FilterNode> filters = new LinkedList<FilterNode>();
		
		for (IQueryNode node : this) {
			
			if (node instanceof FilterNode) {
				
				filters.add((FilterNode) node);
				
			}
			
		}
		
		return filters;
		
    }

    /**
     * Return only the filter child nodes in this group that will be fully bound
     * before running any of the joins in this group.
     * <p>
     * Note: Anything returned by this method should be lifted into the parent
     * group since it can be run before this group is evaluated. By lifting the
     * pre-filters into the parent group we can avoid issuing as many as-bound
     * subqueries for this group since those which fail the filter will not be
     * issued.
     * 
     * @return The filters which should either be run before the non-optional
     *         join graph or (preferably) lifted into the parent group.
     */
	@Deprecated
    public Collection<FilterNode> getPreFilters() {

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getIncomingBindings(new LinkedHashSet<IVariable<?>>());

        /*
         * Get the filters that are bound by this set of known bound variables.
         */
        final Collection<FilterNode> filters = getBoundFilters(knownBound);

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
    public List<FilterNode> getKnownInFilters() {
        
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

    /**
     * Return only the filter child nodes in this group that will be fully bound
     * only by running the joins in this group.
     * 
     * @return The filters to be attached to the non-optional join graph for
     *         this group.
     */
    @Deprecated
    public Collection<FilterNode> getJoinFilters() {

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getIncomingBindings(new LinkedHashSet<IVariable<?>>());

        /*
         * Add all the "must" bound variables for this group.
         * 
         * Note: We do not recursively compute the "must" bound variables for
         * this step because we are only interested in a FILTER which can be
         * attached to a non-optional JOIN run within this group.
         */
        getDefinatelyProducedBindings(knownBound, false/* recursive */);
		
        /*
         * Get the filters that are bound by this set of known bound variables.
         */
		final Collection<FilterNode> filters = getBoundFilters(knownBound);
		
        /*
         * Remove the preConditional filters (those fully bound by just incoming
         * bindings).
         */
		filters.removeAll(getPreFilters());
        filters.removeAll(getKnownInFilters());
		return filters;
		
	}

    /**
     * Return only the filter child nodes in this group that will not be fully
     * bound even after running the joins in this group. It is possible that
     * some of these filters will be fully bound due to nested optionals and
     * unions.
     * 
     * @return The filters to be run last in the group (after the nested
     *         optionals and unions).
     */
    @Deprecated
	public Collection<FilterNode> getPostFilters() {

		/*
		 * Start with all the filters in this group.
		 */
        final Collection<FilterNode> filters = getFilters();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getIncomingBindings(new LinkedHashSet<IVariable<?>>());

        /*
         * Add all the "must" bound variables for this group.
         * 
         * Note: We do not recursively compute the "must" bound variables for
         * this step because we are only interested in a FILTER which can be
         * attached to a non-optional JOIN run within this group.
         */
        getDefinatelyProducedBindings(knownBound, false/* recursive */);

        /*
         * Get the filters that are bound by this set of known bound variables.
         */
        final Collection<FilterNode> preAndJoinFilters = getBoundFilters(knownBound);

        /*
         * Remove the preFilters and joinFilters, leaving only the postFilters.
         * 
         * Note: This approach deliberately will report any filter which would
         * not have already been run for the group.
         */
		filters.removeAll(preAndJoinFilters);
        filters.removeAll(getKnownInFilters());
		return filters;
		
	}

    /**
     * Return any filters can not succeed based on the "incoming", "must" and
     * "may" bound variables for this group. These filters are candidates for
     * pruning.
     * <p>
     * Note: Filters containing a {@link FunctionNode} for
     * {@link FunctionRegistry#BOUND} MUST NOT be pruned and are NOT reported by
     * this method.
     * 
     * @return The filters which are known to fail.
     * 
     *         TODO It is possible to prune a BOUND(?x) or NOT BOUND(?x) filter
     *         through a more detailed analysis of the value expression. If the
     *         variable <code>?x</code> simply does not appear in the group or
     *         any child of that group, then BOUND(?x) can be replaced by
     *         <code>false</code> and NOT BOUND(?x) by <code>true</code>.
     *         <p>
     *         However, in order to do this we must also look at any exogenous
     *         solution(s) (those supplied with the query when it is being
     *         evaluated). If the variable is bound in some exogenous solutions
     *         then it could be bound when the FILTER is run and the filter can
     *         not be pruned.
     */
    @Deprecated
    public Collection<FilterNode> getFiltersToPrune() {

        /*
         * Start with all the filters in this group.
         */
        final Collection<FilterNode> filters = getFilters();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> maybeBound = getIncomingBindings(new LinkedHashSet<IVariable<?>>());

        /*
         * Add all "must" / "may" bound variables for this group (recursively).
         */
        getMaybeProducedBindings(maybeBound, true/* recursive */);

        /*
         * Get the filters that are bound by this set of "maybe" bound variables.
         */
        final Collection<FilterNode> maybeFilters = getBoundFilters(maybeBound);

        /*
         * Remove the maybe bound filters, leaving only those which can not
         * succeed.
         */
        filters.removeAll(maybeFilters);
        
        /*
         * Collect all maybeFilters which use BOUND(). These can not be failed
         * as easily.
         */
        
        final Set<FilterNode> isBoundFilters = new LinkedHashSet<FilterNode>();
        
        for (FilterNode filter : maybeFilters) {

            final IValueExpressionNode node = filter.getValueExpressionNode();
            
            if (node instanceof FunctionNode) {
            
                if (((FunctionNode) node).isBound()) {
                
                    isBoundFilters.add(filter);
                    
                }
                
            }
            
        }

        // Remove filters which use BOUND().
        filters.removeAll(isBoundFilters);
        
        return filters;
        
    }
    
	/**
	 * Helper method to determine the set of filters that will be fully bound
	 * assuming the specified set of variables is bound.
	 */
    @Deprecated
    private final Collection<FilterNode> getBoundFilters(
            final Set<IVariable<?>> knownBound) {

        final Collection<FilterNode> filters = new LinkedList<FilterNode>();

        for (IQueryNode node : this) {

            if (!(node instanceof FilterNode))
                continue;

            final FilterNode filter = (FilterNode) node;

            final Set<IVariable<?>> filterVars = filter.getConsumedVars();

            boolean allBound = true;

            for (IVariable<?> v : filterVars) {

                allBound &= knownBound.contains(v);

            }

            if (allBound) {

                filters.add(filter);

            }

        }

        return filters;

    }

    /**
     * A "simple optional" is an optional sub-group that contains only one
     * statement pattern, no sub-groups of its own, and no filters that require
     * materialized variables. We can lift these "simple optionals" into the
     * parent group without incurring the costs of launching a
     * {@link SubqueryOp}.
     * 
     * TODO Move to rewrite rule and unit test.
     */
    @Deprecated
	public boolean isSimpleOptional() {
		
		// first, the whole group must be optional
		if (!isOptional()) {
			return false;
		}
		
		/*
		 * Second, make sure we have only one statement pattern, no sub-queries,
		 * and no filters that require materialization.
		 */
		StatementPatternNode sp = null;
		
		for (IQueryNode node : this) {
			
		    if (node instanceof StatementPatternNode) {
			
				// already got one
				if (sp != null) {
					return false;
				}
				
				sp = (StatementPatternNode) node;
				
			} else if (node instanceof FilterNode) {
				
				final FilterNode filter = (FilterNode) node;
				
                final INeedsMaterialization req = filter
                        .getMaterializationRequirement();

                if (req.getRequirement() != INeedsMaterialization.Requirement.NEVER) {

					return false;
					
				}
            
			} else {
			
			    /*
			     * Anything else will queer the deal.
			     */
			    
			    return false;
			    
			}
			
		}

        // if we've made it this far, we are simple optional
        return sp != null;

	}

    /**
     * Get the single "simple optional" statement pattern.
     * <p>
     * See {@link #isSimpleOptional()}.
     * 
     * TODO Move to rewrite rule and unit test.
     */
    @Deprecated
	public StatementPatternNode getSimpleOptional() {
		
		if (!isSimpleOptional()) {
			throw new RuntimeException("not a simple optional join group");
		}
		
		for (IQueryNode node : this) {
			
			if (node instanceof StatementPatternNode) {
				
				return (StatementPatternNode) node;
				
			}
			
		}
		
		throw new RuntimeException("not a simple optional join group");
		
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
//		
//		final String _indent = indent(indent);
//		
//		final StringBuilder sb = new StringBuilder(_indent);
//		
//		if (isOptional()) {
//		
//		    sb.append(_indent).append("optional {\n");
//		    
//		} else {
//
//		    sb.append(_indent).append("{\n");
//		    
//		}
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof StatementPatternNode)) {
//                continue;
//            }
//            sb.append(n.toString(indent + 1)).append("\n");
//        }
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof FilterNode)) {
//                continue;
//            }
//            sb.append(n.toString(indent + 1)).append("\n");
//        }
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof UnionNode)) {
//                continue;
//            }
//            sb.append(((UnionNode) n).toString(indent + 1)).append("\n");
//        }
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof JoinGroupNode)) {
//                continue;
//            }
//            sb.append(((JoinGroupNode) n).toString(indent + 1)).append("\n");
//        }
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof SubqueryRoot)) {
//                continue;
//            }
//            sb.append(((SubqueryRoot) n).toString(indent + 1)).append("\n");
//        }
//
//        for (IQueryNode n : this) {
//            if (!(n instanceof AssignmentNode)) {
//                continue;
//            }
//            sb.append(((AssignmentNode) n).toString(indent + 1)).append("\n");
//        }
//
//        sb.append(_indent).append("}");
//
//        return sb.toString();
//
//	}

}
