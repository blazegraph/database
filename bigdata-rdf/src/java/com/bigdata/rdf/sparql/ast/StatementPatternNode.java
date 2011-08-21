package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IVariable;

/**
 * A node in the AST representing a statement pattern.
 */
public class StatementPatternNode extends GroupMemberNodeBase {

	private final TermNode s, p, o, c;
	
    private final Scope scope;

    private final Set<IVariable<?>> producedBindings;

    /**
     * A triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, null/* scope */);

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
	public StatementPatternNode(
			final TermNode s, final TermNode p, final TermNode o, 
			final TermNode c, final Scope scope) {
		
		if (s == null || p == null || o == null) {
			throw new IllegalArgumentException();
		}
		
		this.s = s;
		this.p = p;
		this.o = o;
		this.c = c;
		this.scope = scope;
		
		producedBindings = new LinkedHashSet<IVariable<?>>();
		
		if (s instanceof VarNode) {
			producedBindings.add(((VarNode) s).getVar());
		}
		if (p instanceof VarNode) {
			producedBindings.add(((VarNode) p).getVar());
		}
		if (o instanceof VarNode) {
			producedBindings.add(((VarNode) o).getVar());
		}
		if (c != null && c instanceof VarNode) {
			producedBindings.add(((VarNode) c).getVar());
		}
		
	}
	
	public TermNode s() {
		return s;
	}

	public TermNode p() {
		return p;
	}

	public TermNode o() {
		return o;
	}

	public TermNode c() {
		return c;
	}
	
	public Scope getScope() {
		return scope;
	}

	/**
	 * Return the variables used by the predicate - i.e. what this node will
	 * attempt to bind when run.
	 */
	public Set<IVariable<?>> getProducedBindings() {

	    return producedBindings;
	    
	}
	
	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder(indent(indent));

		sb.append("sp(");
		sb.append(s).append(", ");
		sb.append(p).append(", ");
		sb.append(o);
		
		if (c != null) {
			sb.append(", ").append(c);
		}
		
		if (scope != null) {
			sb.append(", ").append(scope);
		}
		
		sb.append(")");
		
		return sb.toString();
		
	}

    public boolean equals(final Object obj) {

        if (this == obj)
            return true;

        if (!(obj instanceof StatementPatternNode))
            return false;

        final StatementPatternNode t = (StatementPatternNode) obj;

        if (!s.equals(t.s))
            return false;
        if (!p.equals(t.p))
            return false;
        if (!o.equals(t.o))
            return false;
        if (c == null) {
            if (t.c != null)
                return false;
        } else {
            if (!c.equals(t.c))
                return false;
        }
        if (scope == null) {
            if (t.scope != null)
                return false;
        } else {
            if (!scope.equals(t.scope))
                return false;
        }

        return true;

    }

}
