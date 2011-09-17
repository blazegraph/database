package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.sparql.ast.optimizers.ASTGraphGroupOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;

/**
 * A node in the AST representing a statement pattern.
 * 
 * TODO This should inherit the context dynamically from the parent rather than
 * requiring the context to be specified explicitly. See
 * {@link ASTGraphGroupOptimizer}.
 */
public class StatementPatternNode extends
        GroupMemberNodeBase<StatementPatternNode> implements
        IBindingProducerNode {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations {

        /**
         * The {@link Scope} (required).
         */
        String SCOPE = "scope";
        
        /**
         * Boolean flag indicates that a {@link StatementPatternNode} was lifted
         * out of an optional {@link JoinGroupNode} and has OPTIONAL semantics.
         * 
         * @see ASTSimpleOptionalOptimizer
         */
        String SIMPLE_OPTIONAL = "simpleOptional";
    
        boolean DEFAULT_SIMPLE_OPTIONAL = false;
        
        /**
         * An optional {@link List} of {@link FilterNode}s for constraints which
         * MUST run <em>with</em> the JOIN for this statement pattern.
         * <p>
         * Note: This is specified when a simple optional is identified and the
         * filters are lifted with the optional statement pattern into the
         * parent group. Under these circumstances the FILTER(s) MUST be
         * attached to the JOIN in order to preserve the OPTIONAL semantics of
         * the join (if the filters are run in the pipeline after the join then
         * the join+constraints no longer has the same semantics as the optional
         * group with that statement pattern and filters).
         * <p>
         * Note: The need to maintain the correct semantics for the simple
         * optional group (statement pattern plus filter(s)) is also the reason
         * why the lifted FILTER(s) MUST NOT require the materialization of any
         * variables which would not have been bound before that JOIN. Since
         * variables bound by the JOIN for the optional statement pattern will
         * not be materialized, filters attached to that JOIN can not require
         * materialization of variables bound by the JOIN (though they can
         * depend on variables already bound by the required joins in the parent
         * group).
         * 
         * @see ASTSimpleOptionalOptimizer
         */
        String FILTERS = "filters";
        
    }
    
    /**
     * Required deep copy constructor.
     */
    public StatementPatternNode(StatementPatternNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public StatementPatternNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * A triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, Scope.DEFAULT_CONTEXTS);

    }

    /**
     * A quad pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     * @param scope
     *            Either {@link Scope#DEFAULT_CONTEXTS} or
     *            {@link Scope#NAMED_CONTEXTS}.
     */
//	@SuppressWarnings("unchecked")
    public StatementPatternNode(
			final TermNode s, final TermNode p, final TermNode o, 
			final TermNode c, final Scope scope) {

        super(new BOp[] { s, p, o, c }, scope == null ? null/* anns */: NV
                .asMap(new NV(Annotations.SCOPE, scope)));

		if (s == null || p == null || o == null) {
		
		    throw new IllegalArgumentException();
		    
		}

	}
	
	public TermNode s() {
		return (TermNode) get(0);
	}

	public TermNode p() {
	    return (TermNode) get(1);
	}

	public TermNode o() {
	    return (TermNode) get(2);
	}

	public TermNode c() {
	    return (TermNode) get(3);
	}
	
    public Scope getScope() {

        return (Scope) getProperty(Annotations.SCOPE);
        
    }

    /**
     * Return <code>true</code> the {@link StatementPatternNode} was lifted out
     * of an optional {@link JoinGroupNode} and has OPTIONAL semantics.
     * 
     * @see Annotations#SIMPLE_OPTIONAL
     */
    public boolean isSimpleOptional() {
        
        return getProperty(Annotations.SIMPLE_OPTIONAL,
                Annotations.DEFAULT_SIMPLE_OPTIONAL);
        
    }

    /**
     * Mark this {@link StatementPatternNode} as one which was lifted out of a
     * "simple optional" group and which therefore has "optional" semantics (we
     * will do an optional join for it).
     */
    public void setSimpleOptional(final boolean simpleOptional) {
        
        setProperty(Annotations.SIMPLE_OPTIONAL,simpleOptional);
        
    }

    /**
     * Return the FILTER(s) associated with this statement pattern. Such filters
     * will be run with the JOIN for this statement pattern. As such, they MUST
     * NOT rely on materialization of variables which would not have been bound
     * before that JOIN.
     * 
     * @see Annotations#FILTERS
     * @see ASTSimpleOptionalOptimizer
     */
    public List<FilterNode> getFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);
        
        if (filters == null) {
        
            return Collections.emptyList();
            
        }

        return Collections.unmodifiableList(filters);
        
    }

    public void setFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }
    
    /**
     * Return <code>true</code> if none of s, p, o, or c is a variable.
     */
    public boolean isGround() {

        if (s() instanceof VarNode)
            return false;

        if (p() instanceof VarNode)
            return false;
        
        if (o() instanceof VarNode)
            return false;
        
        if (c() instanceof VarNode)
            return false;
        
        return true;
        
    }

    /**
     * Return the variables used by the predicate - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        final TermNode s = s();
        final TermNode p = p();
        final TermNode o = o();
        final TermNode c = c();

        if (s instanceof VarNode) {
            producedBindings.add(((VarNode) s).getValueExpression());
        }

        if (p instanceof VarNode) {
            producedBindings.add(((VarNode) p).getValueExpression());
        }

        if (o instanceof VarNode) {
            producedBindings.add(((VarNode) o).getValueExpression());
        }

        if (c != null && c instanceof VarNode) {
            producedBindings.add(((VarNode) c).getValueExpression());
        }

        return producedBindings;

    }

	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append(toShortString());

        final List<FilterNode> filters = getFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

	    return sb.toString();
		
	}

	@Override
    public String toShortString() {
        
	    final StringBuilder sb = new StringBuilder();

        sb.append("StatementPatternNode(");
        sb.append(s()).append(", ");
        sb.append(p()).append(", ");
        sb.append(o());

        final TermNode c = c();
        if (c != null) {
            sb.append(", ").append(c);
        }

        final Scope scope = getScope();
        if (scope != null) {
            sb.append(", ").append(scope);
        }

        sb.append(")");
        
        if(isSimpleOptional()) {
            sb.append(" [simpleOptional]");
        }
        if (!getFilters().isEmpty()) {
            sb.append(" [#filters=" + getFilters().size() + "]");
        }

        return sb.toString();
    }

}
