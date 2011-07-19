package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IVariable;

/**
 * A node in the AST representing a statement pattern.
 */
public class StatementPatternNode extends QueryNodeBase
		implements IQueryNode {

	private final TermNode s, p, o, c;
	
	private final Scope scope;
	
	private final Set<IVariable<?>> producedBindings;
	
	public StatementPatternNode(
			final TermNode s, final TermNode p, final TermNode o) {
		
		this(s, p, o, null, null);
		
	}
		
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

	public String toString() {
		
		return toString(0);
		
	}
	
	public String toString(final int indent) {
		
		final String _indent;
		if (indent <= 0) {
			
			_indent = "";
			
		} else {
			
			final StringBuilder sb = new StringBuilder();
			for (int i = 0; i < indent; i++) {
				sb.append(" ");
			}
			_indent = sb.toString();
			
		}
		
		final StringBuilder sb = new StringBuilder();

		sb.append(_indent).append("sp(");
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

}
