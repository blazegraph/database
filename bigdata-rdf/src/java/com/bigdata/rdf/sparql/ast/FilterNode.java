package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

public class FilterNode extends QueryNodeBase
		implements IQueryNode {

	private final IValueExpression<? extends IV> ve;
	
	private final Set<IVariable<?>> consumedVars;
	
	private final INeedsMaterialization.Requirement materializationRequirement;
	
	private final Set<IVariable<IV>> varsToMaterialize;

	public FilterNode(final IValueExpression<? extends IV> ve) {
		
		this.ve = ve;

		consumedVars = new LinkedHashSet<IVariable<?>>();
		final Iterator<IVariable<?>> it = BOpUtility.getSpannedVariables(ve);
		while (it.hasNext()) {
			consumedVars.add(it.next());
		}
		
		varsToMaterialize = new LinkedHashSet<IVariable<IV>>();
		materializationRequirement = gatherVarsToMaterialize(ve, varsToMaterialize);
		
	}
	
	public IValueExpression<? extends IV> getValueExpression() {
		return ve;
	}
	
	/**
	 * Return the set of variables that will be used by this constraint to
	 * determine which solutions will continue on through the pipeline and
	 * which will be filtered out.
	 */
	public Set<IVariable<?>> getConsumedVars() {
		return consumedVars;
	}
	
	/**
	 * Return the materialization requirement for this filter. Many filters
	 * require materialized variables to do their filtering. Some filters
	 * can work on both materialized terms and internal values (a good
	 * example of this is CompareBOp).
	 */
	public INeedsMaterialization.Requirement getMaterializationRequirement() {
		return materializationRequirement;
	}
	
	/**
	 * Return the set of variables that will need to be materialized in the
	 * binding set in order for this filter to evaluate.
	 */
	public Set<IVariable<IV>> getVarsToMaterialize() {
		return varsToMaterialize;
	}

	/**
	 * Static helper used to determine materialization requirements.
	 */
	private static INeedsMaterialization.Requirement gatherVarsToMaterialize(
			final IValueExpression c, final Set<IVariable<IV>> terms) {
		
    	boolean materialize = false;
    	boolean always = false;
    	
		final Iterator<BOp> it = BOpUtility.preOrderIterator(c);
		
		while (it.hasNext()) {
			
			final BOp bop = it.next();
			
			if (bop instanceof INeedsMaterialization) {
				
				final INeedsMaterialization bop2 = (INeedsMaterialization) bop;
				
				final Set<IVariable<IV>> t = bop2.getTermsToMaterialize();
				
				if (t.size() > 0) {
					
					terms.addAll(t);
					
					materialize = true;
					
					// if any bops have terms that always needs materialization
					// then mark the whole constraint as such
					if (bop2.getRequirement() == Requirement.ALWAYS) {
						
						always = true;
						
					}
					
				}
				
			}
			
		}
		
		return materialize ?
				(always ? Requirement.ALWAYS : Requirement.SOMETIMES) :
					Requirement.NEVER;
		
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

		sb.append(_indent).append("filter(").append(ve).append(")");
		
		return sb.toString();
		
	}

}
