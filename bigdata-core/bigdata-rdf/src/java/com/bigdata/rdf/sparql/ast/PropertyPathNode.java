package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.sparql.ast.optimizers.ASTGraphGroupOptimizer;

/**
 * A node in the AST representing a triple pattern with a property path.  The
 * predicate for this triple pattern will be a {@link PathNode} instead of a
 * {@link TermNode} as in a normal statement pattern.  This is modeled closely
 * after StatementPatternNode.
 */
public class PropertyPathNode extends
        GroupMemberNodeBase<PropertyPathNode> implements IBindingProducerNode {

    /**
	 * 
	 */
	private static final long serialVersionUID = -1697848422480337886L;

	public interface Annotations extends GroupMemberNodeBase.Annotations {

        /**
         * The {@link Scope} (required).
         * 
         * @see ASTGraphGroupOptimizer
         */
        String SCOPE = "scope";
        
    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public PropertyPathNode(final PropertyPathNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public PropertyPathNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * A triple pattern. The {@link Scope} will be
     * {@link Scope#DEFAULT_CONTEXTS}, the context will be <code>null</code>.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @see #PropertyPathNode(TermNode, PathNode,  TermNode, TermNode, Scope)
     */
    public PropertyPathNode(final TermNode s, final PathNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, Scope.DEFAULT_CONTEXTS);

    }

    /**
     * A quad pattern.
     * <p>
     * Note: When a {@link PropertyPathNode} appears in a WHERE clause, the
     * {@link Scope} should be marked as {@link Scope#DEFAULT_CONTEXTS} if it is
     * NOT embedded within a GRAPH clause and otherwise as
     * {@link Scope#NAMED_CONTEXTS}.
     * <p>
     * The context position of the statement should be <code>null</code> unless
     * it is embedded within a GRAPH clause, in which case the context is the
     * context specified for the parent GRAPH clause.
     * <p>
     * A <code>null</code> context in {@link Scope#DEFAULT_CONTEXTS} is
     * interpreted as the RDF merge of the graphs in the defaultGraph (as
     * specified by the {@link DatasetNode}). When non-<code>null</code> (it can
     * be bound by the SPARQL UPDATE <em>WITH</em> clause), the defaultGraph
     * declared by the {@link DatasetNode} is ignored and the context is bound
     * to the constant specified in that <em>WITH</em> clause.
     * <p>
     * Absent any other constraints on the query, an unbound variable context in
     * {@link Scope#NAMED_CONTEXTS} may be bound to any named graph specified by
     * the {@link DatasetNode}.
     * 
     * @param s
     *            The subject (variable or constant; required).
     * @param p
     *            The property path (required).
     * @param o
     *            The subject (variable or constant; required).
     * @param c
     *            The context (variable or constant; optional).
     * @param scope
     *            Either {@link Scope#DEFAULT_CONTEXTS} or
     *            {@link Scope#NAMED_CONTEXTS} (required).
     * 
     * @throws IllegalArgumentException
     *             if <i>s</i>, <i>p</i>, or <i>o</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>scope</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>scope</i> is {@link Scope#NAMED_CONTEXTS} and <i>c</i>
     *             is <code>null</code>.
     */
    public PropertyPathNode(final TermNode s, final PathNode p,
            final TermNode o, final TermNode c, final Scope scope) {

        super(new BOp[] { s, p, o, c }, scope == null ? null/* anns */: NV
                .asMap(new NV(Annotations.SCOPE, scope)));

        if (scope == null)
            throw new IllegalArgumentException();
        
		if (s == null || p == null || o == null)
		    throw new IllegalArgumentException();

        if (scope == Scope.NAMED_CONTEXTS && c == null)
            throw new IllegalArgumentException();
		
	}

	/**
	 * The variable or constant for the subject position (required).
	 */
    final public TermNode s() {

        return (TermNode) get(0);

    }

	/**
	 * The property path (required).
	 */
    final public PathNode p() {

        return (PathNode) get(1);

    }

	/**
	 * The variable or constant for the object position (required).
	 */
    final public TermNode o() {

        return (TermNode) get(2);

    }

	/**
	 * The variable or constant for the context position (required iff in quads
	 * mode).
	 */
    final public TermNode c() {

        return (TermNode) get(3);

    }

	final public void setC(final TermNode c) {

		this.setArg(3, c);
		
    }
    
    /**
     * The scope for this statement pattern (either named graphs or default
     * graphs).
     * 
     * @see Annotations#SCOPE
     * @see Scope
     */
    final public Scope getScope() {

        return (Scope) getRequiredProperty(Annotations.SCOPE);
        
    }

	final public void setScope(final Scope scope) {

		if (scope == null)
			throw new IllegalArgumentException();
    	
    		setProperty(Annotations.SCOPE, scope);
    	
    }
    
	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append(toShortString());

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        return sb.toString();
		
	}

	@Override
    public String toShortString() {
        
	    final StringBuilder sb = new StringBuilder();

	    final Integer id = (Integer)getProperty(BOp.Annotations.BOP_ID);
        sb.append("PropertyPathNode");
        if (id != null) {
            sb.append("[").append(id.toString()).append("]");
        }
        sb.append("(");
        sb.append(s()).append(", ");
        sb.append(p()).append(", ");
        sb.append(o());

        final TermNode c = c();
        if (c != null) {
            sb.append(", ").append(c);
        }

        sb.append(")");
        
		final Scope scope = getScope();
		if (scope != null) {
			sb.append(" [scope=" + scope + "]");
		}

        return sb.toString();
    }

    /**
     * Return the variables used by the predicate - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        final TermNode s = s();
        final TermNode o = o();
        final TermNode c = c();

        addProducedBindings(s, producedBindings);
        addProducedBindings(o, producedBindings);
        addProducedBindings(c, producedBindings);
        
        return producedBindings;

    }
    
    /**
     * This handles the special case where we've wrapped a Var with a Constant
     * because we know it's bound, perhaps by the exogenous bindings. If we
     * don't handle this case then we get the join vars wrong.
     * 
     * @see StaticAnalysis._getJoinVars
     */
    private void addProducedBindings(final TermNode t,
            final Set<IVariable<?>> producedBindings) {

        if (t instanceof VarNode) {

            producedBindings.add(((VarNode) t).getValueExpression());

        } else if (t instanceof ConstantNode) {

            final ConstantNode cNode = (ConstantNode) t;
            final Constant<?> c = (Constant<?>) cNode.getValueExpression();
            final IVariable<?> var = c.getVar();
            if (var != null) {
                producedBindings.add(var);
            }

        }

    }
	
    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
        return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
        return sa.getSpannedVariables(this, true, new HashSet<IVariable<?>>());
    }
}
