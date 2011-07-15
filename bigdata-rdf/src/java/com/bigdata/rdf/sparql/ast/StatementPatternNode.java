package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.Predicate;

/**
 * A node in the AST representing a statement pattern.
 */
public class StatementPatternNode extends QueryNodeBase
		implements IQueryNode {

	private final Predicate pred;
	
	private final Set<IVariable<?>> producedBindings;
	
	public StatementPatternNode(final Predicate pred) {
		
		this.pred = pred;
		
		producedBindings = new LinkedHashSet<IVariable<?>>();
		
		for (BOp arg : pred.args()) {
			if (arg instanceof IVariable) {
				producedBindings.add((IVariable<?>) arg);
			}
		}
		
	}

	/**
	 * Return the bigdata predicate.
	 */
	public Predicate getPredicate() {
		return pred;
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

		sb.append(_indent).append("sp(").append(pred).append(")");
		
		return sb.toString();
		
	}

}
