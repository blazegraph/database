package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;

/**
 * A node in the AST representing a statement pattern.
 * 
 * TODO This should inherit the context dynamically from the parent rather than
 * requiring the context to be specified explicitly.
 */
public class StatementPatternNode extends
        GroupMemberNodeBase<StatementPatternNode> {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations {

        String SCOPE = "scope";
    
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

        return sb.toString();
    }

}
